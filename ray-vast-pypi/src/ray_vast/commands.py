"""The work of the `ray-vast` commands (plan step 10): `up`, `down`, `attach`, `dashboard`. `cli.py` parses the
arguments and calls `run`; the split keeps `ray-vast --help` free of boto3, yaml and the other dependencies.

`up` makes, in this order: an IAM role, a security group, the role's policy, an instance profile, and the EC2 head
(all tagged `ray-vast-cluster=<name>`). On the head it builds a derived image (`provider.image` plus the ray-vast
wheel), makes the secrets inside a container, and starts the Ray head as the main process of a container.
`down` finds everything by tag or by name, so it works when `up` failed half way, when the head is gone, and
when `~/.ray-vast/<cluster>.json` is missing.

Decisions:
- ray-vast is not on PyPI yet, so `up` builds the wheel on the laptop (`python -m build`, from the source tree, or
  `--wheel FILE`), copies it to the head and installs it in the derived image.
- `down` terminates the head first, so its autoscaler cannot rent new workers while the vast sweep runs.
"""
import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

import boto3
import botocore.exceptions
import yaml

from . import config as config_module
from .aws_sg import ec2_client_config
from .vast_api import VastClient

TAG_KEY = "ray-vast-cluster"
TESTED_RAY_VERSIONS = ("2.59",)
UBUNTU_OWNER = "099720109477"
UBUNTU_AMI_NAME = "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"
SSH_USER = "ubuntu"
CONTAINER = "ray-vast-head"
HEAD_PORTS = {  # fixed Ray ports on the head (docs/test-results.md "Ports")
    "node-manager-port": 6380, "object-manager-port": 6381, "metrics-export-port": 6382,
    "dashboard-agent-grpc-port": 6383, "dashboard-agent-listen-port": 6384, "runtime-env-agent-port": 6385,
    "min-worker-port": 10002, "max-worker-port": 10100,
}
JOB_PORT, RUNTIME_ENV_PORT = 6384, 6385
LIVE_STATES = ["pending", "running", "stopping", "stopped", "shutting-down"]
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$")
USER_DATA = """#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y docker.io
systemctl enable --now docker
"""
DOCKERFILE = """FROM {image}
COPY {wheel} /tmp/{wheel}
RUN pip install --no-cache-dir /tmp/{wheel}
"""
# Prints the Ray version and the path of the shim inside the derived image.
IMAGE_INFO = ("import os, ray, ray_vast; print(ray.__version__); "
              "print(os.path.join(os.path.dirname(ray_vast.__file__), 'shim', 'librayvast_shim.so'))")


class CliError(Exception):
    """A failure with a message for the user (no traceback)."""


class SshError(CliError):
    pass


class Ssh:
    """`ssh` and `scp` as subprocesses. A fake with the same methods stands in for it in unit tests."""

    def __init__(self, key_file, known_hosts):
        self._options = ["-i", str(key_file), "-o", "StrictHostKeyChecking=accept-new", "-o", f"UserKnownHostsFile={known_hosts}",
                         "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15"]

    def run(self, ip, command, stdin=None, timeout=900):
        """Run `command` on the head and return its stdout. The command line must hold no secret; secrets go in `stdin`."""
        try:
            done = subprocess.run(["ssh", *self._options, f"{SSH_USER}@{ip}", command], input=stdin, capture_output=True,
                                  text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise SshError(f"ssh {ip}: timed out after {timeout} s: {command[:120]}") from None
        if done.returncode != 0:
            raise SshError(f"ssh {ip}: exit {done.returncode}: {command[:120]}\n{done.stderr[-600:]}")
        return done.stdout

    def copy(self, ip, local, remote):
        done = subprocess.run(["scp", *self._options, str(local), f"{SSH_USER}@{ip}:{remote}"], capture_output=True, text=True, timeout=300)
        if done.returncode != 0:
            raise SshError(f"scp to {ip}: exit {done.returncode}\n{done.stderr[-600:]}")

    def interactive(self, ip, *args):
        return subprocess.call(["ssh", *self._options, *args, f"{SSH_USER}@{ip}"])


def _clients(session, region):
    cfg = ec2_client_config()
    return session.client("ec2", region_name=region, config=cfg), session.client("iam", config=cfg)


def _head_instances(ec2, cluster):
    reservations = ec2.describe_instances(Filters=[
        {"Name": f"tag:{TAG_KEY}", "Values": [cluster]}, {"Name": "instance-state-name", "Values": LIVE_STATES}])["Reservations"]
    return [i for r in reservations for i in r["Instances"]]


def _vast_instances(vast, cluster):
    return [i for i in vast.list_instances() if i.get("label") == f"ray-vast:{cluster}"]


def _read_vast_key():
    key = os.environ.get("VAST_API_KEY")
    if not key:
        path = Path.home() / ".config" / "vastai" / "vast_api_key"
        if not path.exists():
            raise CliError("no vast key: set VAST_API_KEY or put it in ~/.config/vastai/vast_api_key")
        key = path.read_text()
    return key.strip()


def up(config_path, session, ssh, *, key_name, wheel, vast_key, operator_ip, state_dir, sleep=time.sleep, out=print):
    config, warnings = config_module.load_and_bootstrap(config_path)
    cluster, provider = config["cluster_name"], config["provider"]
    if not NAME_RE.match(cluster):
        raise CliError(f"cluster_name {cluster!r} must match {NAME_RE.pattern}")
    for warning in warnings:
        out(f"warning: {warning}")
    region, name = provider["region"], f"ray-vast-{cluster}"
    tags = [{"Key": TAG_KEY, "Value": cluster}, {"Key": "Name", "Value": name}]
    ec2, iam = _clients(session, region)
    account = session.client("sts", config=ec2_client_config()).get_caller_identity()["Account"]

    # 1. AWS resources. The role comes first: if the user lacks IAM permissions, nothing else exists yet.
    iam.create_role(RoleName=name, Tags=tags, AssumeRolePolicyDocument=json.dumps({
        "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}]}))
    group_id = ec2.create_security_group(GroupName=name, Description=f"ray-vast head {cluster}", TagSpecifications=[
        {"ResourceType": "security-group", "Tags": tags}])["GroupId"]
    ec2.authorize_security_group_ingress(GroupId=group_id, IpPermissions=[{
        "IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": f"{operator_ip}/32", "Description": "operator ssh"}]}])
    iam.put_role_policy(RoleName=name, PolicyName="head", PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["ec2:DescribeSecurityGroups", "ec2:DescribeInstances"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["ec2:AuthorizeSecurityGroupIngress", "ec2:RevokeSecurityGroupIngress"],
         "Resource": f"arn:aws:ec2:{region}:{account}:security-group/{group_id}"}]}))
    iam.create_instance_profile(InstanceProfileName=name, Tags=tags)
    iam.add_role_to_instance_profile(InstanceProfileName=name, RoleName=name)

    images = ec2.describe_images(Owners=[UBUNTU_OWNER], Filters=[
        {"Name": "name", "Values": [UBUNTU_AMI_NAME]}, {"Name": "state", "Values": ["available"]}])["Images"]
    if not images:
        raise CliError("no Ubuntu 24.04 amd64 image found")
    ami = max(images, key=lambda i: i["CreationDate"])["ImageId"]
    for attempt in range(12):  # a new instance profile can take several seconds to be usable
        try:
            launched = ec2.run_instances(
                ImageId=ami, InstanceType=provider["head_instance_type"], MinCount=1, MaxCount=1, KeyName=key_name,
                SecurityGroupIds=[group_id], IamInstanceProfile={"Name": name}, UserData=USER_DATA,
                MetadataOptions={"HttpTokens": "required", "HttpEndpoint": "enabled"},
                BlockDeviceMappings=[{"DeviceName": "/dev/sda1", "Ebs": {"VolumeSize": 60, "VolumeType": "gp3", "DeleteOnTermination": True}}],
                TagSpecifications=[{"ResourceType": rt, "Tags": tags} for rt in ("instance", "volume")])
            break
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidParameterValue" or "nstance Profile" not in str(e) or attempt == 11:
                raise
            sleep(5)
    instance_id = launched["Instances"][0]["InstanceId"]
    out(f"head instance {instance_id} starting")
    ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])
    instance = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]
    head_ip, private_ip = instance["PublicIpAddress"], instance["PrivateIpAddress"]
    out(f"head IP {head_ip}")
    state_file = Path(state_dir) / f"{cluster}.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps({
        "cluster": cluster, "region": region, "head_instance_id": instance_id, "head_ip": head_ip,
        "security_group_id": group_id, "role_name": name, "instance_profile_name": name}, indent=2))

    # 2. On the head: wait for SSH and Docker.
    for attempt in range(60):
        try:
            ssh.run(head_ip, "cloud-init status --wait >/dev/null && sudo docker info >/dev/null", timeout=600)
            break
        except SshError:
            if attempt == 59:
                raise
            sleep(5)

    # 3. Derived image: provider.image + the ray-vast wheel. Read the Ray version and the shim path from it.
    work = f"/home/{SSH_USER}/{name}"
    secrets, build, image_tag = f"{work}/secrets", f"{work}/build", f"{name}:head"
    ssh.run(head_ip, f"mkdir -p -m 700 {secrets} {build}")
    ssh.copy(head_ip, wheel, f"{build}/{wheel.name}")
    ssh.run(head_ip, f"cat > {build}/Dockerfile", stdin=DOCKERFILE.format(image=provider["image"], wheel=wheel.name))
    ssh.run(head_ip, f"sudo docker build -t {image_tag} {build}", timeout=1800)
    ray_version, shim_path = ssh.run(head_ip, f"sudo docker run --rm {image_tag} python -c {shlex.quote(IMAGE_INFO)}").split()
    if not ray_version.startswith(tuple(v + "." for v in TESTED_RAY_VERSIONS)):
        out(f"warning: the image has Ray {ray_version}; ray-vast is tested with {', '.join(TESTED_RAY_VERSIONS)}")

    # 4. Config and secrets on the head. The CA key is made inside a container and never leaves the head.
    head_provider = {k: v for k, v in provider.items() if k not in ("key_name", "key_file")}
    head_provider.update(head_ip=head_ip, secrets_dir=secrets, security_group_id=group_id)
    head_config = {**config, "provider": head_provider}
    ssh.run(head_ip, f"cat > {work}/cluster.yaml", stdin=yaml.safe_dump(head_config))
    ssh.run(head_ip, f"umask 077 && cat > {secrets}/vast_api_key", stdin=vast_key)
    ssh.run(head_ip, f"sudo docker run --rm -v {secrets}:{secrets} {image_tag} python -c "
                     + shlex.quote(f"from ray_vast import pki; pki.write_head_files({secrets!r}, {head_ip!r}, {provider['cert_max_age_days']})"))

    # 5. The head: `ray start --head --block` is the main process, so Docker restarts the whole head if Ray's monitor dies (S2).
    env = {
        "RAY_enable_autoscaler_v2": "1", "RAY_CLOUD_INSTANCE_ID": "head", "RAY_NODE_TYPE_NAME": config["head_node_type"],
        "RAY_USE_TLS": "1", "RAY_TLS_SERVER_CERT": f"{secrets}/head.pem", "RAY_TLS_SERVER_KEY": f"{secrets}/head.key",
        "RAY_TLS_CA_CERT": f"{secrets}/ca.pem", "RAY_AUTH_MODE": "token", "RAY_AUTH_TOKEN_PATH": f"{secrets}/token",
        "LD_PRELOAD": shim_path, "RAYVAST_PUBLIC_IP": head_ip, "RAYVAST_PRIVATE_IP": private_ip,
        "RAYVAST_LOCALHOST_PORTS": f"{JOB_PORT},{RUNTIME_ENV_PORT}",
    }
    ray_start = ["ray", "start", "--head", "--block", f"--node-ip-address={head_ip}", "--port=6379", "--num-cpus=0",
                 "--dashboard-host=127.0.0.1", *(f"--{k}={v}" for k, v in HEAD_PORTS.items()), f"--autoscaling-config={work}/cluster.yaml"]
    ssh.run(head_ip, " ".join([
        f"sudo docker rm -f {CONTAINER} >/dev/null 2>&1;", "sudo docker run -d", f"--name {CONTAINER}", "--init",
        "--restart unless-stopped", "--network host", f"-v {work}:{work}",
        *(f"-e {k}={shlex.quote(v)}" for k, v in env.items()), image_tag, *map(shlex.quote, ray_start)]))

    # 6. Wait until Ray answers.
    for attempt in range(60):
        try:
            ssh.run(head_ip, f"sudo docker exec {CONTAINER} ray status", timeout=60)
            break
        except SshError:
            if attempt == 59:
                raise
            sleep(5)
    out(f"cluster {cluster} is up. Head IP {head_ip}.")
    out(f"  dashboard: ray-vast dashboard {config_path}   (then open http://127.0.0.1:8265)")
    out(f"  shell:     ray-vast attach {config_path}")
    return head_ip


def down(cluster, region, session, vast, state_dir, sleep=time.sleep, out=print):
    """Remove everything of the cluster. Returns the list of what is left (empty on success)."""
    name = f"ray-vast-{cluster}"
    ec2, iam = _clients(session, region)

    # Terminate the head first: its autoscaler cannot rent new workers during the sweep.
    head_ids = [i["InstanceId"] for i in _head_instances(ec2, cluster)]
    if head_ids:
        ec2.terminate_instances(InstanceIds=head_ids)
        out(f"terminating head {', '.join(head_ids)}")

    for _ in range(60):
        found = _vast_instances(vast, cluster)
        if not found:
            break
        for instance in found:
            vast.destroy_instance(instance["id"])
        out(f"destroyed {len(found)} vast instance(s)")
        sleep(5)

    if head_ids:
        ec2.get_waiter("instance_terminated").wait(InstanceIds=head_ids)

    for group in ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [name]}])["SecurityGroups"]:
        for attempt in range(60):  # the network interface of the head can outlive the instance for a while
            try:
                ec2.delete_security_group(GroupId=group["GroupId"])
                break
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] != "DependencyViolation" or attempt == 59:
                    raise
                sleep(5)
        out(f"deleted security group {group['GroupId']}")

    try:
        for role in iam.get_instance_profile(InstanceProfileName=name)["InstanceProfile"]["Roles"]:
            iam.remove_role_from_instance_profile(InstanceProfileName=name, RoleName=role["RoleName"])
        iam.delete_instance_profile(InstanceProfileName=name)
        out(f"deleted instance profile {name}")
    except iam.exceptions.NoSuchEntityException:
        pass
    try:
        for policy in iam.list_role_policies(RoleName=name)["PolicyNames"]:
            iam.delete_role_policy(RoleName=name, PolicyName=policy)
        iam.delete_role(RoleName=name)
        out(f"deleted role {name}")
    except iam.exceptions.NoSuchEntityException:
        pass

    for path in (Path(state_dir) / f"{cluster}.json", Path(state_dir) / f"{cluster}.known_hosts"):
        path.unlink(missing_ok=True)

    left = [f"vast instance {i['id']}" for i in _vast_instances(vast, cluster)]
    left += [f"EC2 instance {i['InstanceId']}" for i in _head_instances(ec2, cluster)]
    left += [f"security group {g['GroupId']}" for g in ec2.describe_security_groups(
        Filters=[{"Name": "group-name", "Values": [name]}])["SecurityGroups"]]
    left += [f"instance profile {p['InstanceProfileName']}" for p in iam.list_instance_profiles()["InstanceProfiles"] if p["InstanceProfileName"] == name]
    left += [f"role {r['RoleName']}" for r in iam.list_roles()["Roles"] if r["RoleName"] == name]
    out("left: " + (", ".join(left) if left else "nothing"))
    return left


def _public_ip():
    with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10) as response:
        return response.read().decode().strip()


def run(args):
    """Run the command in `args` (from cli.py) and return the exit code."""
    try:
        with open(args.cluster_yaml) as f:
            raw = yaml.safe_load(f)
        cluster, provider = raw.get("cluster_name"), raw.get("provider") or {}
        if not cluster or not provider.get("region"):
            raise CliError("the YAML needs cluster_name and provider.region")
        if not NAME_RE.match(cluster):
            raise CliError(f"cluster_name {cluster!r} must match {NAME_RE.pattern}")
        session = boto3.Session(profile_name=args.profile, region_name=provider["region"])
        state_dir = Path.home() / ".ray-vast"
        state_dir.mkdir(exist_ok=True)
        key_file = getattr(args, "key_file", None) or provider.get("key_file")
        if args.command != "down" and not key_file:
            raise CliError("no SSH key: pass --key-file or set provider.key_file")
        ssh = Ssh(Path(key_file).expanduser(), state_dir / f"{cluster}.known_hosts") if key_file else None

        if args.command == "up":
            key_name = args.key_name or provider.get("key_name")
            if not key_name:
                raise CliError("no EC2 key pair: pass --key-name or set provider.key_name")
            wheel = Path(args.wheel).expanduser() if args.wheel else None
            with tempfile.TemporaryDirectory() as tmp:
                if wheel is None:
                    source = Path(__file__).resolve().parents[2]
                    if not (source / "pyproject.toml").exists():
                        raise CliError("not running from a source tree: pass --wheel")
                    subprocess.run([sys.executable, "-m", "build", "--wheel", "--outdir", tmp, str(source)], check=True, capture_output=True)
                    wheel = next(Path(tmp).glob("ray_vast-*.whl"))
                up(args.cluster_yaml, session, ssh, key_name=key_name, wheel=wheel, vast_key=_read_vast_key(),
                   operator_ip=_public_ip(), state_dir=state_dir)
        elif args.command == "down":
            left = down(cluster, provider["region"], session, VastClient(_read_vast_key()), state_dir)
            return 1 if left else 0
        else:
            ec2, _ = _clients(session, provider["region"])
            heads = [i for i in _head_instances(ec2, cluster) if i["State"]["Name"] == "running"]
            if not heads:
                raise CliError(f"no running head for cluster {cluster}")
            ip = heads[0]["PublicIpAddress"]
            if args.command == "attach":
                return ssh.interactive(ip, "-t")
            print("dashboard at http://127.0.0.1:8265 (Ctrl-C to close the tunnel)")
            return ssh.interactive(ip, "-N", "-L", "8265:127.0.0.1:8265")
    except botocore.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        message = f"ray-vast {args.command}: AWS error {code}: {e.response['Error'].get('Message', '')}"
        if "Denied" in code or code == "UnauthorizedOperation":
            message += "\nThe AWS user lacks a permission that ray-vast needs (IAM role, security group, EC2)."
        if args.command == "up":
            message += f"\nRun `ray-vast down {args.cluster_yaml}` to remove what was created."
        print(message, file=sys.stderr)
        return 1
    except (CliError, subprocess.CalledProcessError) as e:
        print(f"ray-vast {args.command}: {e}", file=sys.stderr)
        return 1
    return 0
