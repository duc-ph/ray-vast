import json

import boto3
import botocore.exceptions
import pytest
import yaml
from moto import mock_aws

from ray_vast import commands

CLUSTER = "rvtest-ab12"
REGION = "us-west-2"
VAST_KEY = "vast-secret-key-0123456789"
CLUSTER_YAML = {
    "cluster_name": CLUSTER,
    "max_workers": 0,
    "provider": {"region": REGION, "image": "rayproject/ray:2.59.0-py311-cpu", "max_dollars_per_hour": 1.0},
    "head_node_type": "head",
    "available_node_types": {
        "head": {"resources": {"CPU": 0}, "node_config": {}, "max_workers": 0},
        "gpu": {"resources": {"CPU": 4, "GPU": 1}, "max_workers": 0,
                "node_config": {"offer_query": {"gpu_ram": {"gte": 20000}}}},
    },
}


class FakeSsh:
    """Records every call. `fail_at` is the index of the call that raises; `ray_version` is what the image reports."""

    def __init__(self, fail_at=None, ray_version="2.59.0"):
        self.calls = []  # (kind, ip, command or remote path, stdin)
        self.fail_at = fail_at
        self.ray_version = ray_version

    def _record(self, kind, ip, what, stdin=None):
        self.calls.append((kind, ip, what, stdin))
        if self.fail_at == len(self.calls) - 1:
            raise commands.SshError(f"injected failure at call {self.fail_at}")

    def run(self, ip, command, stdin=None, timeout=900):
        self._record("run", ip, command, stdin)
        return f"{self.ray_version}\n/site-packages/ray_vast/shim/librayvast_shim.so\n" if "ray_vast; print" in command else ""

    def copy(self, ip, local, remote):
        self._record("copy", ip, remote)


class FakeVast:
    def __init__(self, instances=()):
        self.instances = list(instances)

    def list_instances(self):
        return list(self.instances)

    def destroy_instance(self, instance_id):
        self.instances = [i for i in self.instances if i["id"] != instance_id]


@pytest.fixture
def aws(monkeypatch, tmp_path):
    for k, v in {"AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "x", "AWS_DEFAULT_REGION": REGION}.items():
        monkeypatch.setenv(k, v)
    ubuntu = {  # moto has no Ubuntu 24.04 image; give it one (MOTO_AMIS_PATH replaces its image list)
        "ami_id": "ami-0a1b2c3d4e5f60718", "name": "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-20260101",
        "description": "Ubuntu 24.04", "owner_id": commands.UBUNTU_OWNER, "public": True, "virtualization_type": "hvm",
        "architecture": "x86_64", "state": "available", "platform": None, "image_type": "machine", "hypervisor": "xen",
        "root_device_name": "/dev/sda1", "root_device_type": "ebs", "sriov": "simple", "tags": {}}
    (tmp_path / "amis.json").write_text(json.dumps([ubuntu]))
    monkeypatch.setenv("MOTO_AMIS_PATH", str(tmp_path / "amis.json"))
    with mock_aws():
        yield boto3.Session(region_name=REGION)


@pytest.fixture
def cluster_yaml(tmp_path):
    path = tmp_path / "cluster.yaml"
    path.write_text(yaml.safe_dump(CLUSTER_YAML))
    return path


@pytest.fixture
def wheel(tmp_path):
    path = tmp_path / "ray_vast-0.0.0-py3-none-any.whl"
    path.write_bytes(b"wheel")
    return path


def run_up(aws, cluster_yaml, wheel, tmp_path, ssh=None, out=None):
    ssh = ssh or FakeSsh()
    commands.up(str(cluster_yaml), aws, ssh, key_name="k", wheel=wheel, vast_key=VAST_KEY, operator_ip="203.0.113.7",
           state_dir=tmp_path / "state", sleep=lambda s: None, out=out or (lambda m: None))
    return ssh


def what_is_left(aws, vast):
    """Everything that belongs to the cluster, found by tag or by name."""
    ec2, iam = commands._clients(aws, REGION)
    name = f"ray-vast-{CLUSTER}"
    left = [i["InstanceId"] for i in commands._head_instances(ec2, CLUSTER)]
    left += [g["GroupId"] for g in ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [name]}])["SecurityGroups"]]
    left += [p["InstanceProfileName"] for p in iam.list_instance_profiles()["InstanceProfiles"] if p["InstanceProfileName"] == name]
    left += [r["RoleName"] for r in iam.list_roles()["Roles"] if r["RoleName"] == name]
    left += [i["id"] for i in vast.list_instances() if i["label"] == f"ray-vast:{CLUSTER}"]
    return left


def test_up_creates_every_resource(aws, cluster_yaml, wheel, tmp_path):
    ssh = run_up(aws, cluster_yaml, wheel, tmp_path)
    ec2, iam = commands._clients(aws, REGION)
    name = f"ray-vast-{CLUSTER}"

    [head] = commands._head_instances(ec2, CLUSTER)
    assert head["InstanceType"] == "t3.large"
    assert head["KeyName"] == "k"
    assert head["MetadataOptions"]["HttpTokens"] == "required"
    assert head["IamInstanceProfile"]["Arn"].endswith(f"instance-profile/{name}")
    assert {"Key": commands.TAG_KEY, "Value": CLUSTER} in head["Tags"]
    volume = ec2.describe_volumes(VolumeIds=[head["BlockDeviceMappings"][0]["Ebs"]["VolumeId"]])["Volumes"][0]
    assert (volume["Size"], volume["VolumeType"]) == (60, "gp3")

    [group] = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [name]}])["SecurityGroups"]
    assert head["SecurityGroups"][0]["GroupId"] == group["GroupId"]
    assert [(p["FromPort"], p["ToPort"], [r["CidrIp"] for r in p["IpRanges"]]) for p in group["IpPermissions"]] == [(22, 22, ["203.0.113.7/32"])]

    assert iam.get_instance_profile(InstanceProfileName=name)["InstanceProfile"]["Roles"][0]["RoleName"] == name
    assert {"Key": commands.TAG_KEY, "Value": CLUSTER} in iam.get_role(RoleName=name)["Role"]["Tags"]
    policy = iam.get_role_policy(RoleName=name, PolicyName="head")["PolicyDocument"]
    scoped = [s for s in policy["Statement"] if s["Resource"] != "*"]
    assert scoped[0]["Resource"].endswith(f"security-group/{group['GroupId']}")
    assert "ec2:DescribeSecurityGroups" in policy["Statement"][0]["Action"]

    assert (json.loads((tmp_path / "state" / f"{CLUSTER}.json").read_text()))["security_group_id"] == group["GroupId"]
    assert ssh.calls[0][2].startswith("cloud-init status")  # waited for SSH and Docker first


def test_up_starts_the_head_container_and_keeps_secrets_off_command_lines(aws, cluster_yaml, wheel, tmp_path):
    ssh = run_up(aws, cluster_yaml, wheel, tmp_path)
    commands = [c[2] for c in ssh.calls]

    [start] = [c for c in commands if "docker run -d" in c]
    for part in ("--init", "--restart unless-stopped", "--network host", "RAY_enable_autoscaler_v2=1", "RAY_CLOUD_INSTANCE_ID=head",
                 "RAY_NODE_TYPE_NAME=head", "RAY_USE_TLS=1", "RAY_AUTH_MODE=token", "LD_PRELOAD=/site-packages/ray_vast/shim/librayvast_shim.so",
                 "RAYVAST_LOCALHOST_PORTS=6384,6385", "ray start --head --block", "--num-cpus=0", "--port=6379",
                 "--node-manager-port=6380", "--runtime-env-agent-port=6385", "--autoscaling-config="):
        assert part in start
    assert "--node-ip-address=" in start and "RAYVAST_PUBLIC_IP=" in start and "RAYVAST_PRIVATE_IP=" in start
    assert any(c.endswith("ray status") for c in commands)

    stdin_of = {c[2]: c[3] for c in ssh.calls if c[3]}
    assert VAST_KEY not in "\n".join(commands)
    [(key_command, _)] = [(c, s) for c, s in stdin_of.items() if s == VAST_KEY]
    assert key_command.startswith("umask 077") and key_command.endswith("/secrets/vast_api_key")

    [(_, config_text)] = [(c, s) for c, s in stdin_of.items() if c.endswith("cluster.yaml")]
    provider = yaml.safe_load(config_text)["provider"]
    assert VAST_KEY not in config_text
    assert provider["module"] == "ray_vast.provider.VastNodeProvider"
    assert provider["secrets_dir"].endswith("/secrets") and provider["head_ip"] and provider["security_group_id"].startswith("sg-")

    # The wheel is the only file copied; the CA is made on the head (inside a container), not copied.
    assert [c[2] for c in ssh.calls if c[0] == "copy"] == [f"/home/ubuntu/ray-vast-{CLUSTER}/build/{wheel.name}"]
    assert any("write_head_files" in c and "docker run --rm" in c for c in commands)


@pytest.mark.parametrize("version, warns", [("2.59.3", False), ("2.60.0", True)])
def test_up_warns_about_untested_ray_version(aws, cluster_yaml, wheel, tmp_path, version, warns):
    messages = []
    run_up(aws, cluster_yaml, wheel, tmp_path, ssh=FakeSsh(ray_version=version), out=messages.append)
    assert any("tested with 2.59" in m for m in messages) == warns


def test_up_shows_config_warnings(aws, cluster_yaml, wheel, tmp_path):
    raw = dict(CLUSTER_YAML, provider={**CLUSTER_YAML["provider"], "allow_non_datacenter": True})
    cluster_yaml.write_text(yaml.safe_dump(raw))
    messages = []
    run_up(aws, cluster_yaml, wheel, tmp_path, out=messages.append)
    assert any(m.startswith("warning:") and "allow_non_datacenter" in m for m in messages)


def test_up_denied_by_iam_creates_nothing_else(aws, cluster_yaml, wheel, tmp_path):
    def deny(**kwargs):
        raise botocore.exceptions.ClientError({"Error": {"Code": "AccessDenied", "Message": "no iam:CreateRole"}}, "CreateRole")
    aws.events.register("before-call.iam.CreateRole", deny)
    with pytest.raises(botocore.exceptions.ClientError, match="AccessDenied"):
        run_up(aws, cluster_yaml, wheel, tmp_path)
    assert what_is_left(aws, FakeVast()) == []
    ec2, _ = commands._clients(aws, REGION)
    assert [g["GroupName"] for g in ec2.describe_security_groups()["SecurityGroups"]] == ["default"]


def test_down_removes_every_resource_and_only_ours(aws, cluster_yaml, wheel, tmp_path):
    run_up(aws, cluster_yaml, wheel, tmp_path)
    vast = FakeVast([{"id": 1, "label": f"ray-vast:{CLUSTER}"}, {"id": 2, "label": f"ray-vast:{CLUSTER}"},
                     {"id": 3, "label": "ray-vast:other"}, {"id": 4, "label": None}])
    messages = []
    left = commands.down(CLUSTER, REGION, aws, vast, tmp_path / "state", sleep=lambda s: None, out=messages.append)
    assert left == [] and messages[-1] == "left: nothing"
    assert what_is_left(aws, vast) == []
    assert sorted(i["id"] for i in vast.instances) == [3, 4]
    assert list((tmp_path / "state").glob(f"{CLUSTER}.*")) == []


def test_down_with_nothing_to_remove(aws, tmp_path):
    assert commands.down(CLUSTER, REGION, aws, FakeVast(), tmp_path, sleep=lambda s: None, out=lambda m: None) == []


def test_down_waits_for_the_network_interface_to_go(aws, cluster_yaml, wheel, tmp_path):
    run_up(aws, cluster_yaml, wheel, tmp_path)
    failures = []

    def busy(**kwargs):
        if len(failures) < 3:
            failures.append(1)
            raise botocore.exceptions.ClientError({"Error": {"Code": "DependencyViolation", "Message": "in use"}}, "DeleteSecurityGroup")
    aws.events.register("before-call.ec2.DeleteSecurityGroup", busy)
    assert commands.down(CLUSTER, REGION, aws, FakeVast(), tmp_path / "state", sleep=lambda s: None, out=lambda m: None) == []
    assert len(failures) == 3


def test_down_reports_what_is_left(aws, cluster_yaml, wheel, tmp_path):
    run_up(aws, cluster_yaml, wheel, tmp_path)

    class StuckVast(FakeVast):
        def destroy_instance(self, instance_id):
            pass  # vast keeps listing it

    vast = StuckVast([{"id": 1, "label": f"ray-vast:{CLUSTER}"}])
    left = commands.down(CLUSTER, REGION, aws, vast, tmp_path / "state", sleep=lambda s: None, out=lambda m: None)
    assert left == ["vast instance 1"]


AWS_STEPS = [
    "iam.CreateRole", "ec2.CreateSecurityGroup", "ec2.AuthorizeSecurityGroupIngress", "iam.PutRolePolicy", "iam.CreateInstanceProfile",
    "iam.AddRoleToInstanceProfile", "ec2.DescribeImages", "ec2.RunInstances", "ec2.DescribeInstances",
]


@pytest.mark.parametrize("operation", AWS_STEPS)
def test_down_after_up_failed_at_each_aws_step_leaves_nothing(aws, cluster_yaml, wheel, tmp_path, operation):
    def fail(**kwargs):
        raise RuntimeError(f"injected failure in {operation}")
    aws.events.register(f"before-call.{operation}", fail)
    with pytest.raises(RuntimeError, match="injected"):
        run_up(aws, cluster_yaml, wheel, tmp_path)
    clean = boto3.Session(region_name=REGION)  # without the failing hook
    vast = FakeVast()
    assert commands.down(CLUSTER, REGION, clean, vast, tmp_path / "state", sleep=lambda s: None, out=lambda m: None) == []
    assert what_is_left(clean, vast) == []


def test_down_after_up_failed_at_each_ssh_step_leaves_nothing(aws, cluster_yaml, wheel, tmp_path):
    ssh_calls = len(run_up(aws, cluster_yaml, wheel, tmp_path).calls)
    commands.down(CLUSTER, REGION, aws, FakeVast(), tmp_path / "state", sleep=lambda s: None, out=lambda m: None)
    assert ssh_calls >= 10
    for fail_at in range(ssh_calls):
        # The retry loops swallow SshError, so the failing call is permanent only when the failure repeats.
        class Always(FakeSsh):
            def _record(self, kind, ip, what, stdin=None):
                self.calls.append((kind, ip, what, stdin))
                if len(self.calls) - 1 >= self.fail_at:
                    raise commands.SshError("injected")
        with pytest.raises(commands.SshError):
            run_up(aws, cluster_yaml, wheel, tmp_path, ssh=Always(fail_at=fail_at))
        vast = FakeVast([{"id": 9, "label": f"ray-vast:{CLUSTER}"}])
        assert commands.down(CLUSTER, REGION, aws, vast, tmp_path / "state", sleep=lambda s: None, out=lambda m: None) == []
        assert what_is_left(aws, vast) == []


def test_up_retries_while_the_instance_profile_is_not_ready(aws, cluster_yaml, wheel, tmp_path):
    failures = []

    def not_ready(**kwargs):
        if len(failures) < 2:
            failures.append(1)
            raise botocore.exceptions.ClientError({"Error": {"Code": "InvalidParameterValue", "Message": "Invalid IAM Instance Profile name"}}, "RunInstances")
    aws.events.register("before-call.ec2.RunInstances", not_ready)
    run_up(aws, cluster_yaml, wheel, tmp_path)
    assert len(failures) == 2
