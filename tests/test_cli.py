import copy
import importlib.metadata
import json
import os
import select
import shlex
import subprocess
import sys
import urllib.parse
from pathlib import Path

import boto3
import botocore.exceptions
import botocore.waiter
import pytest
import yaml
from botocore.awsrequest import AWSResponse
from moto import mock_aws

from ray_vast import cli, commands
from ray_vast.vast_api import VastError

CLUSTER = "rvtest-ab12"
REGION = "us-west-2"
VAST_KEY = "vast-secret-key-0123456789"
CLUSTER_YAML = {
    "cluster_name": CLUSTER,
    "provider": {"image": "rayproject/ray:2.59.0-py311-cpu", "max_dollars_per_hour": 1.0},
    "head_node_type": "head",
    "available_node_types": {
        "head": {"resources": {"CPU": 0}, "node_config": {"region": REGION}},
        "gpu": {"resources": {"CPU": 4, "GPU": 1}, "max_workers": 0,
                "node_config": {"offer_query": {"gpu_ram": {"gte": 20000}}}},
    },
}


class FakeSsh:
    """Records every call. `fail_at` is the index of the call that raises; `ray_version` is what the image reports."""

    def __init__(self, fail_at=None, ray_version="2.59.0", image_bytes="4500000000", image_info=None):
        """`image_info`: the stdout of the image probe; by default the marker line with `ray_version` and a shim path."""
        self.calls = []  # (kind, ip, command or remote path, stdin)
        self.fail_at = fail_at
        self.ray_version = ray_version
        self.image_info = image_info
        self.image_bytes = image_bytes
        self.interactive_calls = []
        self.timeouts = {}  # command -> timeout it was run with

    def _record(self, kind, ip, what, stdin=None):
        self.calls.append((kind, ip, what, stdin))
        if self.fail_at == len(self.calls) - 1:
            raise commands.SshError(f"injected failure at call {self.fail_at}")

    def run(self, ip, command, stdin=None, timeout=900):
        self._record("run", ip, command, stdin)
        self.timeouts[command] = timeout
        if command.startswith("cat ") and command.endswith("/token"):
            return "tok-0123456789abcdef\n"
        if "docker image inspect" in command:
            return self.image_bytes + "\n"
        if "import os, ray, ray_vast" in command:
            return self.image_info if self.image_info is not None else \
                f"RAYVAST_IMAGE_INFO {self.ray_version} /site-packages/ray_vast/shim/librayvast_shim.so\n"
        return ""

    def copy(self, ip, local, remote):
        self._record("copy", ip, remote)

    def interactive(self, ip, *args):
        self.interactive_calls.append((ip, args))
        return 0


class FakeVast:
    def __init__(self, instances=()):
        self.instances = list(instances)

    def list_instances(self):
        return list(self.instances)

    def destroy_instance(self, instance_id):
        self.instances = [i for i in self.instances if i["id"] != instance_id]


def make_ubuntu_image(session, region):
    """moto reads MOTO_AMIS_PATH once per process, so other test files can make it too late. Make the "Ubuntu" image through
    the API instead: an image of a throwaway instance, owned by the test account, which the CLI is told to look up."""
    ec2 = session.client("ec2", region_name=region)
    base = "ami-12c6146b"  # one of moto's default images; `describe_images()` lists about 1300 of them and takes 3 s
    [seed] = ec2.run_instances(ImageId=base, MinCount=1, MaxCount=1)["Instances"]
    ec2.create_image(InstanceId=seed["InstanceId"], Name="ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-20260101")
    ec2.terminate_instances(InstanceIds=[seed["InstanceId"]])


@pytest.fixture
def aws(monkeypatch, tmp_path):
    for k, v in {"AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "x", "AWS_DEFAULT_REGION": REGION}.items():
        monkeypatch.setenv(k, v)
    with mock_aws():
        session = boto3.Session(region_name=REGION)
        make_ubuntu_image(session, REGION)
        monkeypatch.setattr(commands, "UBUNTU_OWNER", session.client("sts").get_caller_identity()["Account"])
        yield session


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
    commands.up(str(cluster_yaml), yaml.safe_load(cluster_yaml.read_text()), aws, ssh, profile="my-profile", wheel=wheel, vast_key=VAST_KEY, operator_ip="203.0.113.7",
           state_dir=tmp_path / "state", sleep=lambda s: None, out=out or (lambda m: None))
    return ssh


def what_is_left(aws, vast):
    """Everything that belongs to the cluster, found by tag or by name. Asks AWS directly, not through `commands`."""
    ec2, iam = aws.client("ec2"), aws.client("iam")
    name = f"ray-vast-{CLUSTER}"
    live = ["pending", "running", "stopping", "stopped", "shutting-down"]
    left = [i["InstanceId"] for r in ec2.describe_instances(Filters=[
        {"Name": f"tag:{commands.TAG_KEY}", "Values": [CLUSTER]}, {"Name": "instance-state-name", "Values": live}])["Reservations"]
        for i in r["Instances"]]
    left += [k["KeyName"] for k in ec2.describe_key_pairs(Filters=[{"Name": "key-name", "Values": [name]}])["KeyPairs"]]
    group_names = [f"{name}-{i}" for i in range(5)]
    left += [g["GroupId"] for g in ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": group_names}])["SecurityGroups"]]
    for get, kwargs in ((iam.get_instance_profile, {"InstanceProfileName": name}), (iam.get_role, {"RoleName": name})):
        try:
            get(**kwargs)
            left.append(name)
        except iam.exceptions.NoSuchEntityException:
            pass
    left += [i["id"] for i in vast.list_instances() if i["label"] == f"ray-vast:{CLUSTER}"]
    return left


def test_up_creates_every_resource(aws, cluster_yaml, wheel, tmp_path):
    ssh = run_up(aws, cluster_yaml, wheel, tmp_path)
    ec2, iam = commands._clients(aws, REGION)
    name = f"ray-vast-{CLUSTER}"

    [head] = commands._head_instances(ec2, CLUSTER)
    assert head["InstanceType"] == "t3.large"
    assert head["KeyName"] == name
    assert head["MetadataOptions"]["HttpTokens"] == "required"
    assert head["IamInstanceProfile"]["Arn"].endswith(f"instance-profile/{name}")
    assert {"Key": commands.TAG_KEY, "Value": CLUSTER} in head["Tags"]
    volume = ec2.describe_volumes(VolumeIds=[head["BlockDeviceMappings"][0]["Ebs"]["VolumeId"]])["Volumes"][0]
    assert (volume["Size"], volume["VolumeType"]) == (60, "gp3")
    assert {"Key": commands.TAG_KEY, "Value": CLUSTER} in volume["Tags"]

    groups = ec2.describe_security_groups(Filters=[{"Name": f"tag:{commands.TAG_KEY}", "Values": [CLUSTER]}])["SecurityGroups"]
    groups.sort(key=lambda g: g["GroupName"])
    assert [g["GroupName"] for g in groups] == [f"{name}-{i}" for i in range(5)]
    group_ids = [g["GroupId"] for g in groups]
    assert sorted(g["GroupId"] for g in head["SecurityGroups"]) == sorted(group_ids)  # all 5 on the head's interface
    assert [(p["FromPort"], p["ToPort"], [r["CidrIp"] for r in p["IpRanges"]]) for p in groups[0]["IpPermissions"]] == [(22, 22, ["203.0.113.7/32"])]
    assert all(g["IpPermissions"] == [] for g in groups[1:])

    assert iam.get_instance_profile(InstanceProfileName=name)["InstanceProfile"]["Roles"][0]["RoleName"] == name
    assert {"Key": commands.TAG_KEY, "Value": CLUSTER} in iam.get_role(RoleName=name)["Role"]["Tags"]
    assert {"Key": commands.TAG_KEY, "Value": CLUSTER} in iam.get_instance_profile(InstanceProfileName=name)["InstanceProfile"]["Tags"]
    policy = iam.get_role_policy(RoleName=name, PolicyName="head")["PolicyDocument"]
    assert policy["Statement"] == [
        {"Effect": "Allow", "Action": ["ec2:DescribeSecurityGroups", "ec2:DescribeInstances"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["ec2:AuthorizeSecurityGroupIngress", "ec2:RevokeSecurityGroupIngress"],
         "Resource": [f"arn:aws:ec2:{REGION}:123456789012:security-group/{g}" for g in group_ids]}]  # moto's account

    assert (json.loads((tmp_path / "state" / f"{CLUSTER}.json").read_text()))["security_group_ids"] == group_ids
    assert ssh.calls[0][2].startswith("cloud-init status")  # waited for SSH and Docker first


@pytest.mark.parametrize("cloud_init_exit, docker_exit, ready", [(0, 0, True), (2, 0, True), (1, 0, True), (0, 1, False), (2, 1, False)])
def test_up_waits_for_cloud_init_then_needs_only_a_working_docker(aws, cluster_yaml, wheel, tmp_path, cloud_init_exit, docker_exit, ready):
    """Seen on a live head: `cloud-init status --wait` exited 2 ("done, with recoverable errors"),
    so `up` failed although cloud-init had ended. The head is ready when cloud-init has ended and Docker answers."""
    ssh = run_up(aws, cluster_yaml, wheel, tmp_path)
    [wait] = [c[2] for c in ssh.calls if c[2].startswith("cloud-init status")]
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "cloud-init").write_text(f"#!/bin/sh\nexit {cloud_init_exit}\n")
    (bin_dir / "sudo").write_text(f"#!/bin/sh\n[ \"$1 $2\" = \"docker info\" ] && exit {docker_exit}\nexit 99\n")
    for tool in bin_dir.iterdir():
        tool.chmod(0o755)
    done = subprocess.run(["bash", "-c", wait], env={"PATH": f"{bin_dir}:/usr/bin:/bin"})
    assert (done.returncode == 0) is ready, (wait, done.returncode)


def test_up_starts_the_head_container_and_keeps_secrets_off_command_lines(aws, cluster_yaml, wheel, tmp_path):
    ssh = run_up(aws, cluster_yaml, wheel, tmp_path)
    lines = [c[2] for c in ssh.calls]

    # The head container: Ray's head is the main process, on the host network, with no restart policy (no self-healing).
    [start] = [shlex.split(c) for c in lines if "docker run -d" in c]
    image = start.index(f"ray-vast-{CLUSTER}:head")
    options, container_command = start[:image], start[image + 1:]
    assert container_command[:4] == ["ray", "start", "--head", "--block"]
    assert not any(o.startswith("--restart") for o in options)
    assert "--network" in options and options[options.index("--network") + 1] == "host"
    env = dict(o.split("=", 1) for flag, o in zip(options, options[1:]) if flag == "-e")
    secrets_dir = f"/home/ubuntu/ray-vast-{CLUSTER}/secrets"
    assert (env["RAY_enable_autoscaler_v2"], env["RAY_USE_TLS"], env["RAY_AUTH_MODE"]) == ("1", "1", "token")
    assert env["RAY_AUTH_TOKEN_PATH"] == f"{secrets_dir}/token" and env["RAY_TLS_SERVER_KEY"] == f"{secrets_dir}/head.key"
    assert f"--node-ip-address={env['RAYVAST_PUBLIC_IP']}" in container_command
    assert "--num-cpus=0" in container_command  # the head runs no tasks
    assert "--init" in options
    # Docker's default /dev/shm is 64 MB; Ray's object store (30% of RAM) then falls back to /tmp. Like Ray's
    # cluster launcher, the head host sets shm to 0.3 x 1.1 of its RAM, so it follows the head instance type.
    assert "--shm-size=$(awk '/MemTotal/ {print int($2 * 0.33)}' /proc/meminfo)k" in options
    assert f"--autoscaling-config=/home/ubuntu/ray-vast-{CLUSTER}/cluster.yaml" in container_command
    assert env["RAY_CLOUD_INSTANCE_ID"] == "head" and env["RAY_NODE_TYPE_NAME"] == "head"
    assert env["LD_PRELOAD"] == "/site-packages/ray_vast/shim/librayvast_shim.so"
    assert env["RAY_TLS_CA_CERT"] == f"{secrets_dir}/ca.pem" and env["RAY_TLS_SERVER_CERT"] == f"{secrets_dir}/head.pem"
    assert env["RAYVAST_LOCALHOST_PORTS"] == "6384,6385" and env["RAYVAST_PRIVATE_IP"].startswith("10.")
    assert any(c.endswith("ray status") for c in lines)  # `up` waited until Ray answered

    # No secret on any command line: the vast key goes through stdin into a 600 file; the config on the head has none.
    stdin_of = {c[2]: c[3] for c in ssh.calls if c[3]}
    assert VAST_KEY not in "\n".join(lines)
    assert "RAY_AUTH_TOKEN=" not in " ".join(lines)
    [(key_command, _)] = [(c, s) for c, s in stdin_of.items() if s == VAST_KEY]
    assert key_command.startswith("umask 077") and key_command.endswith("/secrets/vast_api_key")
    [(_, config_text)] = [(c, s) for c, s in stdin_of.items() if c.startswith("cat >") and c.endswith("cluster.yaml")]
    provider = yaml.safe_load(config_text)["provider"]
    assert VAST_KEY not in config_text
    assert provider["module"] == "ray_vast.provider.VastNodeProvider"
    assert provider["secrets_dir"] == secrets_dir and provider["head_ip"] == env["RAYVAST_PUBLIC_IP"]
    assert len(provider["security_group_ids"]) == 5 and all(g.startswith("sg-") for g in provider["security_group_ids"])

    # The wheel is the only file copied; the CA is made on the head (inside a container), not copied.
    assert [c[2] for c in ssh.calls if c[0] == "copy"] == [f"/home/ubuntu/ray-vast-{CLUSTER}/build/{wheel.name}"]
    assert any("write_head_files" in c and "docker run --rm" in c for c in lines)


@pytest.mark.parametrize("version, warns", [("2.59.3", False), ("2.60.0", True)])
def test_up_warns_about_untested_ray_version(aws, cluster_yaml, wheel, tmp_path, version, warns):
    messages = []
    run_up(aws, cluster_yaml, wheel, tmp_path, ssh=FakeSsh(ray_version=version), out=messages.append)
    assert any("tested with 2.59" in m for m in messages) == warns


# The stdout of `docker run --rm rayproject/ray:2.59.0-py311-gpu ...`: the NVIDIA ENTRYPOINT prints this banner
# before it runs the command.
NVIDIA_BANNER = """
==========
== CUDA ==
==========

CUDA Version 12.1.1

WARNING: The NVIDIA Driver was not detected.  GPU functionality will not be available.
   Use the NVIDIA Container Toolkit to start this container with GPU support; see
   https://docs.nvidia.com/datacenter/cloud-native/ .

"""


def test_up_reads_the_image_info_after_an_entrypoint_banner(aws, cluster_yaml, wheel, tmp_path):
    shim = "/home/ray/anaconda3/lib/python3.11/site-packages/ray_vast/shim/librayvast_shim.so"
    ssh = FakeSsh(image_info=NVIDIA_BANNER + f"RAYVAST_IMAGE_INFO 2.59.0 {shim}\n")
    messages = []
    run_up(aws, cluster_yaml, wheel, tmp_path, ssh=ssh, out=messages.append)
    start = next(c[2] for c in ssh.calls if c[0] == "run" and "docker run -d" in c[2])
    assert f"-e LD_PRELOAD={shim} " in start
    assert not any("tested with" in m for m in messages)


def test_up_without_the_image_info_line_stops_with_a_clear_error(aws, cluster_yaml, wheel, tmp_path):
    ssh = FakeSsh(image_info=NVIDIA_BANNER + "2.59.0\n/some/path\n")
    with pytest.raises(commands.CliError, match="RAYVAST_IMAGE_INFO"):
        run_up(aws, cluster_yaml, wheel, tmp_path, ssh=ssh)


def test_up_shows_config_warnings(aws, cluster_yaml, wheel, tmp_path):
    raw = copy.deepcopy(CLUSTER_YAML)
    raw["available_node_types"]["gpu"]["resources"]["CPU"] = 55
    cluster_yaml.write_text(yaml.safe_dump(raw))
    messages = []
    run_up(aws, cluster_yaml, wheel, tmp_path, out=messages.append)
    assert any(m.startswith("warning:") and "above 54, the most processes that one worker runs at once" in m for m in messages)


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


def test_down_reports_a_role_that_could_not_be_deleted_even_with_many_roles(aws, cluster_yaml, wheel, tmp_path):
    run_up(aws, cluster_yaml, wheel, tmp_path)
    iam = aws.client("iam")
    for n in range(150):  # an unpaginated list_roles would show only the first page
        iam.create_role(RoleName=f"other-{n:03d}", AssumeRolePolicyDocument="{}")
    aws.events.register("before-call.iam.DeleteRole", lambda **kw: (AWSResponse("https://iam", 200, {}, None), {}))
    messages = []
    left = commands.down(CLUSTER, REGION, aws, FakeVast(), tmp_path / "state", sleep=lambda s: None, out=messages.append)
    iam.get_role(RoleName=f"ray-vast-{CLUSTER}")  # really still there
    assert left == [f"role ray-vast-{CLUSTER}"] and messages[-1] == f"left: role ray-vast-{CLUSTER}"


def test_down_never_deletes_a_same_named_group_without_our_tag(aws, tmp_path):
    ec2 = aws.client("ec2")
    foreign = ec2.create_security_group(GroupName=f"ray-vast-{CLUSTER}-0", Description="not ours")["GroupId"]
    left = commands.down(CLUSTER, REGION, aws, FakeVast(), tmp_path, sleep=lambda s: None, out=lambda m: None)
    assert [g["GroupId"] for g in ec2.describe_security_groups(GroupIds=[foreign])["SecurityGroups"]] == [foreign]
    assert left == []  # it is not ours, so it is not "left"


def test_down_without_a_vast_key_still_cleans_aws_and_warns(aws, cluster_yaml, wheel, tmp_path):
    run_up(aws, cluster_yaml, wheel, tmp_path)
    messages = []
    left = commands.down(CLUSTER, REGION, aws, None, tmp_path / "state", sleep=lambda s: None, out=messages.append)
    assert left == [] and what_is_left(aws, FakeVast()) == []
    warning = [m for m in messages if "vast" in m and "not swept" in m]
    assert warning and "vastai" in warning[0] and f"ray-vast:{CLUSTER}" in warning[0]


def test_up_on_an_existing_cluster_stops_before_creating_anything(aws, cluster_yaml, wheel, tmp_path):
    run_up(aws, cluster_yaml, wheel, tmp_path)
    ec2 = aws.client("ec2")
    before = (len(ec2.describe_instances()["Reservations"]), len(ec2.describe_security_groups()["SecurityGroups"]))
    ssh = FakeSsh()
    with pytest.raises(commands.CliError, match=f"cluster {CLUSTER} already exists; use `ray-vast down` first"):
        run_up(aws, cluster_yaml, wheel, tmp_path, ssh=ssh)
    assert (len(ec2.describe_instances()["Reservations"]), len(ec2.describe_security_groups()["SecurityGroups"])) == before
    assert ssh.calls == []


def test_up_with_a_state_file_but_no_head_stops_too(aws, cluster_yaml, wheel, tmp_path):
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / f"{CLUSTER}.json").write_text("{}")
    with pytest.raises(commands.CliError, match="already exists"):
        run_up(aws, cluster_yaml, wheel, tmp_path)
    assert what_is_left(aws, FakeVast()) == []


@pytest.mark.parametrize("image_bytes, expected", [("4500000000", 5), ("5000000000", 5), ("", None)])
def test_up_writes_the_image_size_into_the_head_config(aws, cluster_yaml, wheel, tmp_path, image_bytes, expected):
    ssh = run_up(aws, cluster_yaml, wheel, tmp_path, ssh=FakeSsh(image_bytes=image_bytes))
    [config_text] = [c[3] for c in ssh.calls if c[2].startswith("cat >") and c[2].endswith("cluster.yaml")]
    assert yaml.safe_load(config_text)["provider"].get("image_size_gb") == expected


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
    "iam.CreateRole", "ec2.CreateKeyPair", "ec2.CreateSecurityGroup", "ec2.AuthorizeSecurityGroupIngress", "iam.PutRolePolicy", "iam.CreateInstanceProfile",
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


@pytest.mark.slow  # one full up and down per SSH call (5 s)
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


def test_up_does_not_retry_other_invalid_parameters(aws, cluster_yaml, wheel, tmp_path):
    calls = []

    def bad(**kwargs):
        calls.append(1)
        raise botocore.exceptions.ClientError({"Error": {"Code": "InvalidParameterValue", "Message": "Invalid instance type"}}, "RunInstances")
    aws.events.register("before-call.ec2.RunInstances", bad)
    with pytest.raises(botocore.exceptions.ClientError, match="Invalid instance type"):
        run_up(aws, cluster_yaml, wheel, tmp_path)
    assert len(calls) == 1


def test_dashboard_fetches_the_token_and_opens_the_tunnel(tmp_path):
    ssh, messages = FakeSsh(), []
    code = commands.dashboard(ssh, "192.0.2.4", tmp_path, CLUSTER, out=messages.append)
    token_file = tmp_path / CLUSTER / "token"
    assert token_file.read_text() == "tok-0123456789abcdef"
    assert oct(token_file.stat().st_mode & 0o777) == "0o600" and oct(token_file.parent.stat().st_mode & 0o777) == "0o700"
    assert ssh.calls[0][2] == f"cat /home/ubuntu/ray-vast-{CLUSTER}/secrets/token"
    text = "\n".join(messages)
    assert f"RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH={token_file}" in text and "tok-0123456789abcdef" not in text
    assert ssh.interactive_calls == [("192.0.2.4", ("-N", "-L", "8265:127.0.0.1:8265"))] and code == 0


def test_down_removes_the_cluster_directory(aws, tmp_path):
    (tmp_path / CLUSTER).mkdir()
    (tmp_path / CLUSTER / "token").write_text("t")
    commands.down(CLUSTER, REGION, aws, FakeVast(), tmp_path, sleep=lambda s: None, out=lambda m: None)
    assert not (tmp_path / CLUSTER).exists()


# --- the public seam: `ray_vast.cli.main(argv)` -> `commands.run` -----------------------------------------------------

PROFILES = []  # the profile_name of each boto3.Session that `run` built (the `home` fixture records them)


@pytest.fixture
def home(aws, monkeypatch, tmp_path):
    """A fake home (state dir `~/.ray-vast`), no AWS_PROFILE, a vast key in the environment, a fixed operator IP."""
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setenv("VAST_API_KEY", VAST_KEY)
    monkeypatch.setattr(commands, "_public_ip", lambda: "203.0.113.7")
    # `run` builds its own session; give it the moto one (with its hooks), and record the profile it asked for.
    monkeypatch.setattr(boto3, "Session", lambda **kwargs: PROFILES.append(kwargs.get("profile_name")) or aws)
    PROFILES.clear()
    return tmp_path / "home"



@pytest.fixture
def fake_ssh(monkeypatch):
    """Replaces the `Ssh` class (a subprocess boundary); records how it was built. `fake_ssh.fake` is the FakeSsh it hands out."""
    class Holder:
        built = []
        fake = FakeSsh()
        real = commands.Ssh
    monkeypatch.setattr(commands, "Ssh", lambda key_file, known_hosts: Holder.built.append((key_file, known_hosts)) or Holder.fake)
    return Holder


def main_up(cluster_yaml, wheel, *extra):
    return cli.main(["up", str(cluster_yaml), "--wheel", str(wheel), *extra])


def test_main_up_with_a_bad_field_value_prints_one_line_and_creates_nothing(aws, home, fake_ssh, cluster_yaml, wheel, capsys):
    raw = dict(CLUSTER_YAML, provider={**CLUSTER_YAML["provider"], "max_dollars_per_hour": -1})
    cluster_yaml.write_text(yaml.safe_dump(raw))
    assert main_up(cluster_yaml, wheel) == 1
    err = capsys.readouterr().err
    assert err.startswith("ray-vast up: ") and "max_dollars_per_hour" in err and err.count("\n") == 1
    assert VAST_KEY not in err
    assert what_is_left(aws, FakeVast()) == []


@pytest.mark.parametrize("command", ["up", "down", "attach", "dashboard"])
def test_main_with_a_missing_yaml_file_prints_one_line(home, tmp_path, capsys, command):
    assert cli.main([command, str(tmp_path / "nope.yaml")]) == 1
    err = capsys.readouterr().err
    assert err.startswith(f"ray-vast {command}: ") and "nope.yaml" in err and err.count("\n") == 1


@pytest.mark.parametrize("text, problem", [
    ("cluster_name: x\nprovider: {region: us-west-2\n", "line"),  # an unclosed flow mapping: a YAML syntax error
    ("- just\n- a list\n", "mapping"),
])
def test_main_with_a_bad_yaml_prints_one_line(home, tmp_path, capsys, text, problem):
    path = tmp_path / "bad.yaml"
    path.write_text(text)
    assert cli.main(["down", str(path)]) == 1
    err = capsys.readouterr().err
    assert err.startswith("ray-vast down: ") and "bad.yaml" in err and problem in err and err.count("\n") == 1


def test_main_up_that_fails_on_the_head_exits_1_and_points_to_down(aws, home, fake_ssh, cluster_yaml, wheel, capsys):
    fake_ssh.fake = FakeSsh(fail_at=2)  # the scp of the wheel
    assert main_up(cluster_yaml, wheel) == 1
    assert "ray-vast up: injected failure at call 2" in capsys.readouterr().err  # no traceback


def test_main_up_that_fails_in_aws_exits_1_names_the_permission_and_points_to_down(aws, home, fake_ssh, cluster_yaml, wheel, capsys):
    def deny(**kwargs):
        raise botocore.exceptions.ClientError({"Error": {"Code": "AccessDenied", "Message": "no iam:CreateRole"}}, "CreateRole")
    aws.events.register("before-call.iam.CreateRole", deny)
    assert main_up(cluster_yaml, wheel) == 1
    err = capsys.readouterr().err
    assert "AWS error AccessDenied: no iam:CreateRole" in err and "lacks a permission" in err
    assert f"ray-vast down {cluster_yaml}" in err


def test_main_up_on_an_existing_cluster_exits_1_without_the_down_hint(aws, home, fake_ssh, cluster_yaml, wheel, capsys):
    assert main_up(cluster_yaml, wheel) == 0
    capsys.readouterr()
    assert main_up(cluster_yaml, wheel) == 1
    err = capsys.readouterr().err
    assert f"cluster {CLUSTER} already exists; use `ray-vast down` first" in err
    assert "to remove what was created" not in err


def test_main_up_builds_the_wheel_with_a_timeout(aws, home, fake_ssh, cluster_yaml, monkeypatch, capsys):
    seen = {}

    def fake_build(argv, **kwargs):
        seen.update(kwargs)
        raise subprocess.TimeoutExpired(argv, kwargs.get("timeout"))
    monkeypatch.setattr(subprocess, "run", fake_build)
    assert cli.main(["up", str(cluster_yaml)]) == 1
    assert seen["timeout"] > 0
    assert "timed out" in capsys.readouterr().err
    assert what_is_left(aws, FakeVast()) == []


UP_THAT_BLOCKS = """
import sys, threading
from ray_vast import cli, commands
commands._public_ip = lambda: "203.0.113.7"
commands.up = lambda *a, **kw: threading.Event().wait()  # the AWS and SSH work; it blocks so that the test sees the stream mid-run
cli.main(sys.argv[1:])
"""


@pytest.mark.slow  # starts a Python process
def test_main_up_progress_lines_reach_a_pipe_at_once(cluster_yaml, wheel, tmp_path):
    env = {**os.environ, "HOME": str(tmp_path), "VAST_API_KEY": VAST_KEY, "AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "x"}
    env.pop("AWS_PROFILE", None)
    proc = subprocess.Popen([sys.executable, "-c", UP_THAT_BLOCKS, "up", str(cluster_yaml), "--wheel", str(wheel)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    try:
        ready, _, _ = select.select([proc.stdout], [], [], 15)
        assert ready, f"no stdout line while `up` runs; stderr: {proc.stderr.read1().decode() if proc.poll() is not None else ''}"
        assert proc.stdout.readline().decode().startswith(f"installing ray-vast from the wheel {wheel}")
    finally:
        proc.kill()
        proc.wait()


@pytest.fixture
def running_cluster(aws, home, fake_ssh, cluster_yaml, wheel, monkeypatch):
    assert main_up(cluster_yaml, wheel) == 0
    monkeypatch.setattr(commands, "Ssh", fake_ssh.real)  # attach and dashboard use the real class, over a patched subprocess
    return [i for i in commands._head_instances(aws.client("ec2"), CLUSTER)][0]["PublicIpAddress"]


def test_main_attach_opens_a_shell_on_the_head_with_the_generated_key(running_cluster, home, cluster_yaml, monkeypatch):
    calls = []
    monkeypatch.setattr(subprocess, "call", lambda argv: calls.append(argv) or 7)
    assert cli.main(["attach", str(cluster_yaml)]) == 7  # ssh's exit code is passed on
    [argv] = calls
    assert argv[0] == "ssh" and argv[argv.index("-i") + 1] == str(home / ".ray-vast" / CLUSTER / "ssh_key.pem")
    assert argv[-2:] == ["-t", f"ubuntu@{running_cluster}"]


def test_main_dashboard_copies_the_token_and_opens_the_tunnel(running_cluster, home, cluster_yaml, monkeypatch, capsys):
    ran, called = [], []
    monkeypatch.setattr(subprocess, "run", lambda argv, **kw: ran.append(argv) or subprocess.CompletedProcess(argv, 0, "tok-abc\n", ""))
    monkeypatch.setattr(subprocess, "call", lambda argv: called.append(argv) or 0)
    assert cli.main(["dashboard", str(cluster_yaml)]) == 0
    assert ran[0][-1] == f"cat /home/ubuntu/ray-vast-{CLUSTER}/secrets/token" and ran[0][-2] == f"ubuntu@{running_cluster}"
    assert (home / ".ray-vast" / CLUSTER / "token").read_text() == "tok-abc"
    [tunnel] = called
    assert tunnel[tunnel.index("-L") + 1] == "8265:127.0.0.1:8265" and "-N" in tunnel and tunnel[-1] == f"ubuntu@{running_cluster}"
    assert "tok-abc" not in capsys.readouterr().out


def test_main_dashboard_ctrl_c_closes_the_tunnel_with_one_line_and_exit_0(running_cluster, cluster_yaml, monkeypatch, capsys):
    monkeypatch.setattr(subprocess, "run", lambda argv, **kw: subprocess.CompletedProcess(argv, 0, "tok-abc\n", ""))

    def ctrl_c(argv):  # subprocess.call kills the ssh child before it raises
        raise KeyboardInterrupt
    monkeypatch.setattr(subprocess, "call", ctrl_c)
    assert cli.main(["dashboard", str(cluster_yaml)]) == 0
    captured = capsys.readouterr()
    assert captured.out.splitlines()[-1] == "tunnel closed" and "Traceback" not in captured.err + captured.out


@pytest.mark.parametrize("command", ["attach", "dashboard"])
def test_main_attach_and_dashboard_without_a_running_head_exit_1(aws, home, cluster_yaml, capsys, command):
    assert cli.main([command, str(cluster_yaml)]) == 1
    assert f"no running head for cluster {CLUSTER}" in capsys.readouterr().err


def test_main_down_without_a_vast_key_cleans_aws_warns_and_exits_0(running_cluster, aws, home, cluster_yaml, monkeypatch, capsys):
    monkeypatch.delenv("VAST_API_KEY")
    assert cli.main(["down", str(cluster_yaml)]) == 0  # needs no SSH key either
    out = capsys.readouterr().out
    assert "not swept" in out and "left: nothing" in out
    assert what_is_left(aws, FakeVast()) == []


# --- behaviors that a mutation audit found the tests above did not pin --------------------------------------------------

def no_op_response(**kwargs):
    """A `before-call` hook result: AWS answers 200 and does nothing."""
    return AWSResponse("https://aws", 200, {}, None), {}


def test_up_runs_the_slow_head_steps_with_long_timeouts(aws, cluster_yaml, wheel, tmp_path):
    ssh = run_up(aws, cluster_yaml, wheel, tmp_path)
    timeouts = ssh.timeouts
    [build] = [t for c, t in timeouts.items() if c.startswith("sudo docker build")]
    [cloud_init] = [t for c, t in timeouts.items() if c.startswith("cloud-init status")]
    assert (build, cloud_init) == (1800, 600)


def test_ssh_run_has_a_timeout_and_sends_secrets_on_stdin(monkeypatch, tmp_path):
    seen = {}

    def fake_run(argv, **kwargs):
        seen.update(argv=argv, **kwargs)
        raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
    monkeypatch.setattr(subprocess, "run", fake_run)
    ssh = commands.Ssh(tmp_path / "k.pem", tmp_path / "known_hosts")
    with pytest.raises(commands.SshError, match=r"ssh 192\.0\.2\.4: timed out after 900 s: true"):
        ssh.run("192.0.2.4", "true", stdin="secret-on-stdin")
    assert seen["timeout"] == 900 and seen["input"] == "secret-on-stdin" and "secret-on-stdin" not in seen["argv"]
    with pytest.raises(commands.SshError, match="timed out after 5 s"):
        ssh.run("192.0.2.4", "true", timeout=5)
    assert seen["timeout"] == 5


def test_up_on_a_cluster_with_a_head_but_no_state_file_stops_too(aws, cluster_yaml, wheel, tmp_path):
    run_up(aws, cluster_yaml, wheel, tmp_path)
    (tmp_path / "state" / f"{CLUSTER}.json").unlink()
    ssh = FakeSsh()
    with pytest.raises(commands.CliError, match="already exists"):
        run_up(aws, cluster_yaml, wheel, tmp_path, ssh=ssh)
    assert ssh.calls == []


def test_down_terminates_the_head_before_it_destroys_vast_instances_and_waits_for_it(aws, cluster_yaml, wheel, tmp_path, monkeypatch):
    run_up(aws, cluster_yaml, wheel, tmp_path)
    [head] = commands._head_instances(aws.client("ec2"), CLUSTER)
    order, waited = [], []
    aws.events.register("before-call.ec2.TerminateInstances", lambda **kw: order.append("terminate head"))

    class Vast(FakeVast):
        def destroy_instance(self, instance_id):
            order.append(f"destroy vast {instance_id}")
            super().destroy_instance(instance_id)
    monkeypatch.setattr(botocore.waiter.Waiter, "wait", lambda self, **kwargs: waited.append((self.name, kwargs)))
    vast = Vast([{"id": 1, "label": f"ray-vast:{CLUSTER}"}])
    assert commands.down(CLUSTER, REGION, aws, vast, tmp_path / "state", sleep=lambda s: None, out=lambda m: None) == []
    assert order == ["terminate head", "destroy vast 1"]
    assert waited == [("InstanceTerminated", {"InstanceIds": [head["InstanceId"]]})]


@pytest.mark.parametrize("hook, kind", [
    ("ec2.TerminateInstances", "EC2 instance"), ("ec2.DeleteSecurityGroup", "security group"), ("iam.DeleteInstanceProfile", "instance profile")])
def test_down_reports_each_kind_of_resource_that_could_not_be_removed(aws, cluster_yaml, wheel, tmp_path, monkeypatch, hook, kind):
    run_up(aws, cluster_yaml, wheel, tmp_path)
    monkeypatch.setattr(botocore.waiter.Waiter, "wait", lambda self, **kwargs: None)  # the head never terminates; do not wait for it
    aws.events.register(f"before-call.{hook}", no_op_response)
    left = commands.down(CLUSTER, REGION, aws, FakeVast(), tmp_path / "state", sleep=lambda s: None, out=lambda m: None)
    assert len(left) == (5 if kind == "security group" else 1) and all(item.startswith(kind + " ") for item in left)


def test_down_removes_the_known_hosts_file(aws, tmp_path):
    (tmp_path / f"{CLUSTER}.known_hosts").write_text("192.0.2.4 ssh-ed25519 AAAA")
    commands.down(CLUSTER, REGION, aws, FakeVast(), tmp_path, sleep=lambda s: None, out=lambda m: None)
    assert not (tmp_path / f"{CLUSTER}.known_hosts").exists()


def test_dashboard_fixes_the_modes_of_an_old_token_file_and_directory(tmp_path):
    (tmp_path / CLUSTER).mkdir(mode=0o755)
    (tmp_path / CLUSTER).chmod(0o755)
    (tmp_path / CLUSTER / "token").write_text("old-token")
    (tmp_path / CLUSTER / "token").chmod(0o644)
    commands.dashboard(FakeSsh(), "192.0.2.4", tmp_path, CLUSTER, out=lambda m: None)
    token_file = tmp_path / CLUSTER / "token"
    assert token_file.read_text() == "tok-0123456789abcdef"
    assert oct(token_file.stat().st_mode & 0o777) == "0o600" and oct(token_file.parent.stat().st_mode & 0o777) == "0o700"


def test_main_down_exits_1_when_something_is_left(running_cluster, aws, home, cluster_yaml, capsys, monkeypatch):
    monkeypatch.setattr(commands, "VastClient", lambda key: FakeVast())
    aws.events.register("before-call.iam.DeleteRole", no_op_response)
    assert cli.main(["down", str(cluster_yaml)]) == 1
    assert f"left: role ray-vast-{CLUSTER}" in capsys.readouterr().out


def test_down_sweeps_again_until_vast_lists_no_instance_of_the_cluster(aws, tmp_path):
    """vast can list a destroyed instance for a while, and a late rental can appear; `down` lists again until none."""
    class LateVast(FakeVast):
        def __init__(self, instances):
            super().__init__(instances)
            self.lists = 0

        def list_instances(self):
            self.lists += 1
            if self.lists == 2:  # an instance that the head rented just before it stopped
                self.instances.append({"id": 5, "label": f"ray-vast:{CLUSTER}"})
            return super().list_instances()

    vast = LateVast([{"id": 1, "label": f"ray-vast:{CLUSTER}"}, {"id": 3, "label": "ray-vast:other"}])
    sleeps = []
    left = commands.down(CLUSTER, REGION, aws, vast, tmp_path / "state", sleep=sleeps.append, out=lambda m: None)
    assert left == [] and [i["id"] for i in vast.instances] == [3] and len(sleeps) == 2


def test_down_goes_on_after_a_failed_destroy_and_tries_again(aws, tmp_path, caplog):
    class FlakyVast(FakeVast):
        failed = False

        def destroy_instance(self, instance_id):
            if not self.failed:
                self.failed = True
                raise VastError(f"DELETE /instances/{instance_id}/: HTTP 500: busy", status=500)
            super().destroy_instance(instance_id)

    vast = FlakyVast([{"id": 1, "label": f"ray-vast:{CLUSTER}"}, {"id": 2, "label": f"ray-vast:{CLUSTER}"}])
    left = commands.down(CLUSTER, REGION, aws, vast, tmp_path / "state", sleep=lambda s: None, out=lambda m: None)
    assert left == [] and vast.instances == []
    assert "HTTP 500: busy" in caplog.text


# --- where the head image gets ray-vast: `--wheel` > the source tree > PyPI ------------------------------------------------

@pytest.fixture
def installed(monkeypatch, tmp_path):
    """ray-vast runs as an installed package (`pip install ray-vast`): no pyproject.toml above the package."""
    # A patch, not a boundary fake: the other way is to install the package into a fresh venv per test (slow).
    monkeypatch.setattr(commands, "SOURCE_TREE", tmp_path / "site-packages")


def dockerfile_of(ssh):
    [text] = [c[3] for c in ssh.calls if c[2].endswith("/Dockerfile")]
    return text


def test_main_up_from_an_installed_package_installs_its_own_version_from_pypi(aws, home, fake_ssh, cluster_yaml, installed, capsys):
    assert cli.main(["up", str(cluster_yaml)]) == 0
    pin = f"ray-vast=={importlib.metadata.version('ray-vast')}"  # the installed package's own version
    assert dockerfile_of(fake_ssh.fake).splitlines() == [
        f"FROM {CLUSTER_YAML['provider']['image']}", f"RUN pip install --no-cache-dir {pin}"]
    assert [c for c in fake_ssh.fake.calls if c[0] == "copy"] == []
    assert f"installing {pin} from PyPI" in capsys.readouterr().out


def test_main_up_has_no_index_url_flag(aws, home, fake_ssh, cluster_yaml, installed, capsys):
    # A release test downloads the wheel from TestPyPI (`pip download`) and passes it with --wheel.
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["up", str(cluster_yaml), "--index-url", "https://test.pypi.org/simple/"])
    assert exit_info.value.code == 2 and "unrecognized arguments: --index-url" in capsys.readouterr().err
    assert fake_ssh.fake.calls == []


def test_main_up_with_a_wheel_copies_it_and_says_so(aws, home, fake_ssh, cluster_yaml, wheel, capsys):
    assert main_up(cluster_yaml, wheel) == 0
    assert f"COPY {wheel.name} " in dockerfile_of(fake_ssh.fake)
    assert f"installing ray-vast from the wheel {wheel}" in capsys.readouterr().out


def test_main_up_in_a_source_tree_builds_the_wheel_there_and_says_so(aws, home, fake_ssh, cluster_yaml, monkeypatch, capsys):
    def fake_build(argv, **kwargs):
        outdir = Path(argv[argv.index("--outdir") + 1])
        (outdir / "ray_vast-9.9.9-py3-none-any.whl").write_bytes(b"wheel")
        return subprocess.CompletedProcess(argv, 0)
    monkeypatch.setattr(subprocess, "run", fake_build)
    assert cli.main(["up", str(cluster_yaml)]) == 0
    assert "COPY ray_vast-9.9.9-py3-none-any.whl " in dockerfile_of(fake_ssh.fake)
    assert f"installing ray-vast from a wheel built from the source tree {commands.SOURCE_TREE}" in capsys.readouterr().out


def test_main_up_with_a_failed_head_image_build_names_the_version(aws, home, fake_ssh, cluster_yaml, installed, capsys):
    class PipFails(FakeSsh):
        def run(self, ip, command, stdin=None, timeout=900):
            if "docker build" in command:
                self._record("run", ip, command, stdin)
                raise commands.SshError("ssh: exit 1: sudo docker build\nERROR: No matching distribution found for ray-vast==9.9.9")
            return super().run(ip, command, stdin, timeout)
    fake_ssh.fake = PipFails()
    assert cli.main(["up", str(cluster_yaml)]) == 1
    err = capsys.readouterr().err
    assert (f"the head image build failed while installing ray-vast=={importlib.metadata.version('ray-vast')} from PyPI; "
            "see the build output below") in err
    assert "No matching distribution found" in err  # the build output stays in the message
    assert f"ray-vast down {cluster_yaml}" in err


# --- SSH key per cluster, head settings in the head node type, the IAM boundary, the saved profile ------------------

def test_up_makes_a_key_pair_for_the_cluster_and_saves_its_private_key_with_mode_600(aws, cluster_yaml, wheel, tmp_path):
    run_up(aws, cluster_yaml, wheel, tmp_path)
    name = f"ray-vast-{CLUSTER}"
    [pair] = aws.client("ec2").describe_key_pairs(KeyNames=[name])["KeyPairs"]
    assert {"Key": commands.TAG_KEY, "Value": CLUSTER} in pair["Tags"]
    key_file = tmp_path / "state" / CLUSTER / "ssh_key.pem"
    assert "PRIVATE KEY-----" in key_file.read_text()
    assert oct(key_file.stat().st_mode & 0o777) == "0o600" and oct(key_file.parent.stat().st_mode & 0o777) == "0o700"


def test_down_deletes_the_key_pair_and_the_private_key(aws, cluster_yaml, wheel, tmp_path):
    run_up(aws, cluster_yaml, wheel, tmp_path)
    messages = []
    assert commands.down(CLUSTER, REGION, aws, FakeVast(), tmp_path / "state", sleep=lambda s: None, out=messages.append) == []
    assert aws.client("ec2").describe_key_pairs()["KeyPairs"] == []
    assert not (tmp_path / "state" / CLUSTER / "ssh_key.pem").exists()
    assert f"deleted key pair ray-vast-{CLUSTER}" in messages


def test_down_never_deletes_a_same_named_key_pair_without_our_tag(aws, tmp_path):
    ec2 = aws.client("ec2")
    ec2.create_key_pair(KeyName=f"ray-vast-{CLUSTER}")
    assert commands.down(CLUSTER, REGION, aws, FakeVast(), tmp_path, sleep=lambda s: None, out=lambda m: None) == []
    assert len(ec2.describe_key_pairs()["KeyPairs"]) == 1


def test_down_reports_a_key_pair_that_could_not_be_deleted(aws, cluster_yaml, wheel, tmp_path):
    run_up(aws, cluster_yaml, wheel, tmp_path)
    aws.events.register("before-call.ec2.DeleteKeyPair", no_op_response)
    left = commands.down(CLUSTER, REGION, aws, FakeVast(), tmp_path / "state", sleep=lambda s: None, out=lambda m: None)
    assert left == [f"key pair ray-vast-{CLUSTER}"]


def test_up_takes_the_region_and_the_instance_type_from_the_head_node_type(aws, cluster_yaml, wheel, tmp_path):
    raw = yaml.safe_load(cluster_yaml.read_text())
    raw["available_node_types"]["head"]["node_config"] = {"region": "eu-central-1", "instance_type": "m5.xlarge"}
    cluster_yaml.write_text(yaml.safe_dump(raw))
    make_ubuntu_image(aws, "eu-central-1")
    ssh = run_up(aws, cluster_yaml, wheel, tmp_path)
    [head] = commands._head_instances(aws.client("ec2", region_name="eu-central-1"), CLUSTER)
    assert head["InstanceType"] == "m5.xlarge"
    assert commands._head_instances(aws.client("ec2", region_name=REGION), CLUSTER) == []
    [config_text] = [c[3] for c in ssh.calls if c[2].startswith("cat >") and c[2].endswith("cluster.yaml")]
    assert yaml.safe_load(config_text)["provider"]["region"] == "eu-central-1"  # the provider on the head needs it


def test_up_makes_the_head_role_with_the_permissions_boundary(aws, cluster_yaml, wheel, tmp_path):
    run_up(aws, cluster_yaml, wheel, tmp_path)
    role = aws.client("iam").get_role(RoleName=f"ray-vast-{CLUSTER}")["Role"]
    assert role["PermissionsBoundary"]["PermissionsBoundaryArn"] == "arn:aws:iam::123456789012:policy/ray-vast-head-boundary"


def aws_error(code, operation, message=""):
    def hook(**kwargs):
        raise botocore.exceptions.ClientError({"Error": {"Code": code, "Message": message}}, operation)
    return hook


def no_boundary(**kwargs):  # what AWS answers for a missing boundary (seen live); moto does not check that it exists
    raise botocore.exceptions.ClientError({"Error": {"Code": "NoSuchEntity", "Message": "Scope ARN: arn:aws:iam::123456789012:policy/"
                                           "ray-vast-head-boundary does not exist or is not attachable."}}, "CreateRole")


def test_up_creates_the_boundary_policy_from_the_packaged_file_when_it_is_missing(aws, cluster_yaml, wheel, tmp_path):
    run_up(aws, cluster_yaml, wheel, tmp_path)
    iam = aws.client("iam")
    arn = "arn:aws:iam::123456789012:policy/ray-vast-head-boundary"
    version = iam.get_policy(PolicyArn=arn)["Policy"]["DefaultVersionId"]
    document = iam.get_policy_version(PolicyArn=arn, VersionId=version)["PolicyVersion"]["Document"]
    document = json.loads(urllib.parse.unquote(document)) if isinstance(document, str) else document
    packaged = json.loads((Path(commands.__file__).parent / "iam" / "ray-vast-head-boundary.json").read_text())
    assert document == packaged


def test_up_uses_an_existing_boundary_policy_and_down_keeps_it(aws, cluster_yaml, wheel, tmp_path):
    iam = aws.client("iam")
    iam.create_policy(PolicyName="ray-vast-head-boundary", PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "ec2:DescribeInstances", "Resource": "*"}]}))
    run_up(aws, cluster_yaml, wheel, tmp_path)
    assert commands.down(CLUSTER, REGION, aws, FakeVast(), tmp_path / "state", sleep=lambda s: None, out=lambda m: None) == []
    [policy] = iam.list_policies(Scope="Local")["Policies"]
    assert policy["PolicyName"] == "ray-vast-head-boundary" and policy["DefaultVersionId"] == "v1"  # not replaced, not deleted


def test_up_waits_until_a_new_boundary_policy_is_usable(aws, cluster_yaml, wheel, tmp_path):
    answers = [no_boundary, no_boundary]
    aws.events.register("before-call.iam.CreateRole", lambda **kwargs: answers.pop(0)(**kwargs) if answers else None)
    run_up(aws, cluster_yaml, wheel, tmp_path)
    assert answers == []
    assert aws.client("iam").get_role(RoleName=f"ray-vast-{CLUSTER}")["Role"]["PermissionsBoundary"]


def test_up_without_the_boundary_policy_and_without_create_policy_stops_with_a_pointer_to_the_docs_and_leaves_nothing(aws, cluster_yaml, wheel, tmp_path):
    aws.events.register("before-call.iam.CreatePolicy", aws_error("AccessDenied", "CreatePolicy"))
    aws.events.register("before-call.iam.CreateRole", no_boundary)
    with pytest.raises(commands.CliError, match=r"ray-vast-head-boundary does not exist.*cannot create it. An admin creates it once: "
                                                 r"https://duc-ph.github.io/ray-vast/reference/iam/#the-boundary-policy"):
        run_up(aws, cluster_yaml, wheel, tmp_path)
    assert what_is_left(aws, FakeVast()) == []
    assert not (tmp_path / "state" / f"{CLUSTER}.json").exists()  # so the next `up` can run at once


def test_up_with_an_existing_boundary_policy_and_without_create_policy_works(aws, cluster_yaml, wheel, tmp_path):
    aws.events.register("before-call.iam.CreatePolicy", aws_error("AccessDenied", "CreatePolicy"))
    run_up(aws, cluster_yaml, wheel, tmp_path)
    assert aws.client("iam").get_role(RoleName=f"ray-vast-{CLUSTER}")["Role"]["PermissionsBoundary"]


def test_up_in_a_region_without_a_default_vpc_stops_with_the_command_and_leaves_nothing(aws, cluster_yaml, wheel, tmp_path):
    aws.events.register("before-call.ec2.DescribeVpcs", lambda **kwargs: (AWSResponse("https://aws", 200, {}, None), {"Vpcs": []}))
    with pytest.raises(commands.CliError, match=rf"region {REGION} has no default VPC. Create it once with "
                                                 rf"`aws ec2 create-default-vpc --region {REGION}`. Nothing was created."):
        run_up(aws, cluster_yaml, wheel, tmp_path)
    assert what_is_left(aws, FakeVast()) == []
    assert aws.client("iam").list_policies(Scope="Local")["Policies"] == []
    assert not (tmp_path / "state" / f"{CLUSTER}.json").exists()


def test_up_saves_the_profile_in_the_state_file(aws, cluster_yaml, wheel, tmp_path):
    run_up(aws, cluster_yaml, wheel, tmp_path)
    assert json.loads((tmp_path / "state" / f"{CLUSTER}.json").read_text())["profile"] == "my-profile"


def test_main_down_without_a_profile_uses_the_profile_that_up_saved(aws, home, fake_ssh, cluster_yaml, wheel, monkeypatch):
    monkeypatch.setattr(commands, "VastClient", lambda key: FakeVast())
    assert main_up(cluster_yaml, wheel, "--profile", "saved-profile") == 0
    assert cli.main(["down", str(cluster_yaml)]) == 0
    assert PROFILES == ["saved-profile", "saved-profile"]


def test_main_down_with_a_profile_uses_it_and_not_the_saved_one(aws, home, fake_ssh, cluster_yaml, wheel, monkeypatch):
    monkeypatch.setattr(commands, "VastClient", lambda key: FakeVast())
    assert main_up(cluster_yaml, wheel, "--profile", "saved-profile") == 0
    assert cli.main(["down", str(cluster_yaml), "--profile", "flag-profile"]) == 0
    assert PROFILES[-1] == "flag-profile"


def test_main_up_connects_with_the_generated_key(aws, home, fake_ssh, cluster_yaml, wheel):
    assert main_up(cluster_yaml, wheel) == 0
    [(key_file, known_hosts)] = fake_ssh.built
    assert key_file == home / ".ray-vast" / CLUSTER / "ssh_key.pem"
    assert known_hosts == home / ".ray-vast" / f"{CLUSTER}.known_hosts"


@pytest.mark.parametrize("command", ["attach", "dashboard"])
def test_main_attach_and_dashboard_without_the_generated_key_exit_1(running_cluster, home, cluster_yaml, capsys, command):
    (home / ".ray-vast" / CLUSTER / "ssh_key.pem").unlink()
    assert cli.main([command, str(cluster_yaml)]) == 1
    assert f"no SSH key for cluster {CLUSTER}" in capsys.readouterr().err


@pytest.mark.parametrize("command", ["up", "down"])
def test_main_with_a_bad_cluster_name_prints_one_line_and_creates_nothing(aws, home, fake_ssh, cluster_yaml, capsys, command):
    cluster_yaml.write_text(yaml.safe_dump(dict(CLUSTER_YAML, cluster_name="bad name")))
    assert cli.main([command, str(cluster_yaml)]) == 1
    assert "cluster_name 'bad name' must match" in capsys.readouterr().err
    assert fake_ssh.fake.calls == []


@pytest.mark.parametrize("command", ["down", "attach", "dashboard"])
def test_main_with_an_unknown_profile_prints_one_line_without_a_traceback(tmp_path, monkeypatch, cluster_yaml, capsys, command):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "none"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "none"))
    assert cli.main([command, str(cluster_yaml), "--profile", "bogus"]) == 1
    err = capsys.readouterr().err
    assert err.strip().splitlines() == [f"ray-vast {command}: the AWS profile 'bogus' does not exist (--profile, AWS_PROFILE or the profile "
                                        "that `up` saved)"]


def test_main_down_without_aws_credentials_prints_one_line_without_a_traceback(tmp_path, monkeypatch, cluster_yaml, capsys):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "none"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "none"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("VAST_API_KEY", "x")
    assert cli.main(["down", str(cluster_yaml)]) == 1
    err = capsys.readouterr().err
    assert err.strip().splitlines() == ["ray-vast down: no AWS credentials. Give --profile, set AWS_PROFILE, or set up the default "
                                        "credentials"]
