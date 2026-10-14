"""The work of the `ray-vast` commands (plan step 10): `up`, `down`, `attach`, `dashboard`. `cli.py` parses the
arguments and calls `run`; the split keeps `ray-vast --help` free of boto3, yaml and the other dependencies.

`up` makes, in this order: an IAM role, 5 security groups, the role's policy, an instance profile, and the EC2 head
(all tagged `ray-vast-cluster=<name>`). On the head it builds a derived image (`provider.image` plus ray-vast), makes the secrets inside a container, and starts the Ray head as the main process of a container.
`down` finds everything by tag or by name, so it works when `up` failed half way, when the head is gone, and
when `~/.ray-vast/<cluster>.json` is missing.

Decisions:
- The derived image gets ray-vast from, in this order: `--wheel FILE` (copied to the head); `--index-url URL` (pip
  installs `ray-vast==<this CLI's version>` from that index, with PyPI as an extra index); the source tree, when
  the CLI runs from a git checkout (`python -m build` on the laptop, for development); else `ray-vast==<this CLI's
  version>` from PyPI. Without `--wheel`, the head runs the same code as the CLI.
- `down` terminates the head first, so its autoscaler cannot rent new workers while the vast sweep runs.
"""
import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

import boto3
import botocore.exceptions
import yaml

from . import __version__
from . import config as config_module
from . import pki
from .aws_sg import ec2_client_config
from .vast_api import VastClient, label, sweep

TAG_KEY = "ray-vast-cluster"
SECURITY_GROUPS = 5
BUILD_TIMEOUT = 600  # seconds for `python -m build` on the laptop
SOURCE_TREE = Path(__file__).resolve().parents[2]  # holds pyproject.toml when ray-vast runs from a git checkout
TESTED_RAY_VERSIONS = ("2.59",)
UBUNTU_OWNER = "099720109477"
UBUNTU_AMI_NAME = "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"
SSH_USER = "ubuntu"
CONTAINER = "ray-vast-head"
HEAD_PORTS = {  # fixed Ray ports on the head (docs/design.md 7.1)
    "node-manager-port": 6380, "object-manager-port": 6381, "metrics-export-port": 6382,
    "dashboard-agent-grpc-port": 6383, "dashboard-agent-listen-port": 6384, "runtime-env-agent-port": 6385,
    "min-worker-port": 10002, "max-worker-port": 10100,
}
JOB_PORT, RUNTIME_ENV_PORT = 6384, 6385
LIVE_STATES = ["pending", "running", "stopping", "stopped", "shutting-down"]
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$")
# AWS gives no own error code for an instance profile that has not propagated yet: RunInstances answers with the generic
# `InvalidParameterValue` and this text in the message ("Invalid IAM Instance Profile name/ARN"). Match on the text.
PROFILE_NOT_READY = "Invalid IAM Instance Profile"
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
DOCKERFILE_INDEX = """FROM {image}
RUN pip install --no-cache-dir {requirement}
"""
# Prints one line `RAYVAST_IMAGE_INFO <Ray version> <shim path>` inside the derived image. `up` keeps the image's
# ENTRYPOINT (NVIDIA's prepares CUDA), and an ENTRYPOINT can print to stdout first, so `up` reads only the marker line.
IMAGE_INFO_MARKER = "RAYVAST_IMAGE_INFO"
IMAGE_INFO = (f"import os, ray, ray_vast; print({IMAGE_INFO_MARKER!r}, ray.__version__, "
              "os.path.join(os.path.dirname(ray_vast.__file__), 'shim', 'librayvast_shim.so'))")


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


def _our_groups(ec2, cluster):
    """Security groups of the cluster: by name AND tag, so a same-named group of someone else is never touched."""
    return ec2.describe_security_groups(Filters=[
        {"Name": "group-name", "Values": [f"ray-vast-{cluster}-{i}" for i in range(SECURITY_GROUPS)]},
        {"Name": f"tag:{TAG_KEY}", "Values": [cluster]}])["SecurityGroups"]


def _vast_instances(vast, cluster):
    return [i for i in vast.list_instances() if i.get("label") == label(cluster)]


def _read_vast_key():
    key = os.environ.get("VAST_API_KEY")
    if not key:
        path = Path.home() / ".config" / "vastai" / "vast_api_key"
        if not path.exists():
            raise CliError("no vast key: set VAST_API_KEY or put it in ~/.config/vastai/vast_api_key")
        key = path.read_text()
    return key.strip()


def _mask_userinfo(text, url):
    """Replace the user and password of `url` (`https://user:token@host/...`) in `text` with `***`. The pattern
    also masks the end of them when `text` starts in the middle (an stderr tail)."""
    _, at, host = (url or "").partition("://")[2].partition("/")[0].rpartition("@")
    if not at:
        return text
    return re.sub(r"[^\s/]*@" + re.escape(host), lambda m: "***@" + host, text)


def up(config_path, session, ssh, *, key_name, wheel=None, index_url=None, vast_key, operator_ip, state_dir, sleep=time.sleep, out=print):
    try:
        config, warnings = config_module.load_and_bootstrap(config_path)
    except ValueError as e:  # a field check failed: a user error, so one line and no traceback
        raise CliError(f"{config_path}: {e}") from None
    cluster, provider = config["cluster_name"], config["provider"]
    if not NAME_RE.match(cluster):
        raise CliError(f"cluster_name {cluster!r} must match {NAME_RE.pattern}")
    for warning in warnings:
        out(f"warning: {warning}")
    region, name = provider["region"], f"ray-vast-{cluster}"
    tags = [{"Key": TAG_KEY, "Value": cluster}, {"Key": "Name", "Value": name}]
    ec2, iam = _clients(session, region)
    if _head_instances(ec2, cluster) or (Path(state_dir) / f"{cluster}.json").exists():
        raise CliError(f"cluster {cluster} already exists; use `ray-vast down` first")
    account = session.client("sts", config=ec2_client_config()).get_caller_identity()["Account"]

    # 1. AWS resources. The role comes first: if the user lacks IAM permissions, nothing else exists yet.
    iam.create_role(RoleName=name, Tags=tags, AssumeRolePolicyDocument=json.dumps({
        "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}]}))
    # 5 groups: the AWS default limit for one network interface. Worker IP rules fill them in order (design 8.3).
    group_ids = [ec2.create_security_group(GroupName=f"{name}-{i}", Description=f"ray-vast head {cluster}", TagSpecifications=[
        {"ResourceType": "security-group", "Tags": tags}])["GroupId"] for i in range(SECURITY_GROUPS)]
    ec2.authorize_security_group_ingress(GroupId=group_ids[0], IpPermissions=[{
        "IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": f"{operator_ip}/32", "Description": "operator ssh"}]}])
    iam.put_role_policy(RoleName=name, PolicyName="head", PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["ec2:DescribeSecurityGroups", "ec2:DescribeInstances"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["ec2:AuthorizeSecurityGroupIngress", "ec2:RevokeSecurityGroupIngress"],
         "Resource": [f"arn:aws:ec2:{region}:{account}:security-group/{g}" for g in group_ids]}]}))
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
                SecurityGroupIds=group_ids, IamInstanceProfile={"Name": name}, UserData=USER_DATA,
                MetadataOptions={"HttpTokens": "required", "HttpEndpoint": "enabled"},
                BlockDeviceMappings=[{"DeviceName": "/dev/sda1", "Ebs": {"VolumeSize": 60, "VolumeType": "gp3", "DeleteOnTermination": True}}],
                TagSpecifications=[{"ResourceType": rt, "Tags": tags} for rt in ("instance", "volume")])
            break
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidParameterValue" or PROFILE_NOT_READY not in e.response["Error"].get("Message", "") or attempt == 11:
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
        "security_group_ids": group_ids, "role_name": name, "instance_profile_name": name}, indent=2))

    # 2. On the head: wait for SSH and Docker.
    for attempt in range(60):
        try:
            ssh.run(head_ip, "cloud-init status --wait >/dev/null && sudo docker info >/dev/null", timeout=600)
            break
        except SshError:
            if attempt == 59:
                raise
            sleep(5)

    # 3. Derived image: provider.image + ray-vast (the wheel, or this CLI's own version from the package index).
    # Read the Ray version and the shim path from it.
    work = f"/home/{SSH_USER}/{name}"
    secrets, build, image_tag = f"{work}/secrets", f"{work}/build", f"{name}:head"
    ssh.run(head_ip, f"mkdir -p -m 700 {secrets} {build}")
    if wheel:
        ssh.copy(head_ip, wheel, f"{build}/{wheel.name}")
        dockerfile = DOCKERFILE.format(image=provider["image"], wheel=wheel.name)
    else:
        # TestPyPI does not hold all of ray-vast's dependencies, so PyPI is an extra index. pip takes the highest version
        # of each package from either index: this is for release tests only.
        index = f"--index-url {shlex.quote(index_url)} --extra-index-url https://pypi.org/simple " if index_url else ""
        dockerfile = DOCKERFILE_INDEX.format(image=provider["image"], requirement=f"{index}ray-vast=={__version__}")
    ssh.run(head_ip, f"cat > {build}/Dockerfile", stdin=dockerfile)
    try:
        ssh.run(head_ip, f"sudo docker build -t {image_tag} {build}", timeout=1800)
    except SshError as e:
        if wheel:
            raise
        raise CliError(_mask_userinfo(
            f"the head image build failed while installing ray-vast=={__version__} from {index_url or 'PyPI'}; see the "
            f"build output below. Run `ray-vast down {config_path}` to remove what was created.\n{e}", index_url)) from None
    stdout = ssh.run(head_ip, f"sudo docker run --rm {image_tag} python -c {shlex.quote(IMAGE_INFO)}")
    info = [line.split() for line in stdout.splitlines() if line.startswith(IMAGE_INFO_MARKER + " ")]
    if len(info) != 1 or len(info[0]) != 3:
        raise CliError(f"the image check printed no single `{IMAGE_INFO_MARKER} <ray version> <shim path>` line; "
                       f"its stdout ends with: {stdout[-300:]!r}")
    _, ray_version, shim_path = info[0]
    if not ray_version.startswith(tuple(v + "." for v in TESTED_RAY_VERSIONS)):
        out(f"warning: the image has Ray {ray_version}; ray-vast is tested with {', '.join(TESTED_RAY_VERSIONS)}")

    # 4. Config and secrets on the head. The CA key is made inside a container and never leaves the head.
    head_provider = {k: v for k, v in provider.items() if k not in ("key_name", "key_file")}
    head_provider.update(head_ip=head_ip, secrets_dir=secrets, security_group_ids=group_ids)
    # The build pulled provider.image; its size sets the workers' pull time limit (provider.image_size_gb), unless the user set it.
    size_bytes = ssh.run(head_ip, f"sudo docker image inspect --format '{{{{.Size}}}}' {shlex.quote(provider['image'])}").strip()
    if size_bytes.isdigit() and "image_size_gb" not in head_provider:
        head_provider["image_size_gb"] = -(-int(size_bytes) // 10**9)
    head_config = {**config, "provider": head_provider}
    ssh.run(head_ip, f"cat > {work}/cluster.yaml", stdin=yaml.safe_dump(head_config))
    ssh.run(head_ip, f"umask 077 && cat > {secrets}/vast_api_key", stdin=vast_key)
    ssh.run(head_ip, f"sudo docker run --rm -v {secrets}:{secrets} {image_tag} python -c "
                     + shlex.quote(f"from ray_vast import pki; pki.write_head_files({secrets!r}, {head_ip!r}, {provider['cert_max_age_days']})"))

    # 5. The head: `ray start --head --block` is the main process, with no restart policy (no self-healing, design 7.3):
    # if Ray's monitor dies, Ray stops the head, and the workers destroy themselves.
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
        # shm for Ray's object store (30% of RAM; Docker's default is 64 MB): 0.3 x 1.1 of RAM, as Ray's launcher does.
        """"--shm-size=$(awk '/MemTotal/ {print int($2 * 0.33)}' /proc/meminfo)k\"""", "--network host", f"-v {work}:{work}",
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
    expiry = pki.expiry_warning(ssh.run(head_ip, f"cat {secrets}/ca.pem").encode())
    if expiry:
        out(f"warning: {expiry}")
    out(f"cluster {cluster} is up. Head IP {head_ip}.")
    out(f"  dashboard: ray-vast dashboard {config_path}   (then open http://127.0.0.1:8265)")
    out(f"  shell:     ray-vast attach {config_path}")
    return head_ip


def down(cluster, region, session, vast, state_dir, sleep=time.sleep, out=print):
    """Remove everything of the cluster. Returns the list of what is left (empty on success). `vast` is None without a vast key."""
    name = f"ray-vast-{cluster}"
    ec2, iam = _clients(session, region)

    # Terminate the head first: its autoscaler cannot rent new workers during the sweep.
    head_ids = [i["InstanceId"] for i in _head_instances(ec2, cluster)]
    if head_ids:
        ec2.terminate_instances(InstanceIds=head_ids)
        out(f"terminating head {', '.join(head_ids)}")

    if vast is None:
        out(f"warning: no vast key, so vast instances were not swept. Set VAST_API_KEY and run `ray-vast down` again, or list "
            f"them with `vastai show instances` and destroy those with label {label(cluster)} (`vastai destroy instance ID`).")
    for _ in range(60 if vast else 0):  # list again until vast lists none (a destroy can show late)
        found = sweep(vast, vast.list_instances(), cluster)
        if not found:
            break
        out(f"destroyed {len(found)} vast instance(s)")
        sleep(5)

    if head_ids:
        ec2.get_waiter("instance_terminated").wait(InstanceIds=head_ids)

    for group in _our_groups(ec2, cluster):
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
    shutil.rmtree(Path(state_dir) / cluster, ignore_errors=True)

    left = [f"vast instance {i['id']}" for i in _vast_instances(vast, cluster)] if vast else []
    left += [f"EC2 instance {i['InstanceId']}" for i in _head_instances(ec2, cluster)]
    left += [f"security group {g['GroupId']}" for g in _our_groups(ec2, cluster)]
    for kind, get, kwargs in (("instance profile", iam.get_instance_profile, {"InstanceProfileName": name}),
                              ("role", iam.get_role, {"RoleName": name})):
        try:
            get(**kwargs)
            left.append(f"{kind} {name}")
        except iam.exceptions.NoSuchEntityException:
            pass
    out("left: " + (", ".join(left) if left else "nothing"))
    return left


def dashboard(ssh, ip, state_dir, cluster, out=print):
    """Copy the cluster token to `<state_dir>/<cluster>/token`, print the env line for the Ray CLI, open the tunnel."""
    token = ssh.run(ip, f"cat /home/{SSH_USER}/ray-vast-{cluster}/secrets/token").strip()
    token_file = Path(state_dir) / cluster / "token"
    token_file.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    token_file.parent.chmod(0o700)
    token_file.unlink(missing_ok=True)  # O_EXCL makes the mode apply to a fresh file
    with os.fdopen(os.open(token_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as f:
        f.write(token)
    out("for `ray job submit` set:")
    out(f"  export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH={token_file}")
    out("dashboard at http://127.0.0.1:8265 (Ctrl-C to close the tunnel); use --address http://127.0.0.1:8265")
    try:
        return ssh.interactive(ip, "-N", "-L", "8265:127.0.0.1:8265")
    except KeyboardInterrupt:  # Ctrl-C is the normal way to close the tunnel; subprocess.call has already killed ssh
        out("tunnel closed")
        return 0


def _public_ip():
    with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10) as response:
        return response.read().decode().strip()


def run(args):
    """Run the command in `args` (from cli.py) and return the exit code."""
    try:
        # A missing file and bad YAML are user errors: one line, no traceback.
        try:
            with open(args.cluster_yaml) as f:
                raw = yaml.safe_load(f)
        except OSError as e:
            raise CliError(f"cannot read {args.cluster_yaml}: {e.strerror or e}") from None
        except yaml.YAMLError as e:
            raise CliError(f"bad YAML in {args.cluster_yaml}: {' '.join(str(e).split())}") from None
        if not isinstance(raw, dict):
            raise CliError(f"{args.cluster_yaml}: the YAML must be a mapping (cluster_name:, provider:, ...)")
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
            if args.wheel and args.index_url:
                raise CliError("--wheel and --index-url do not go together: pass one of them")
            # The URL goes into a Dockerfile RUN line: a space or a newline there would change the instruction.
            if args.index_url and not re.fullmatch(r"https?://\S+", args.index_url):
                raise CliError("--index-url must be a plain http:// or https:// URL without spaces")
            # Where the head image gets ray-vast: --wheel, else --index-url, else the source tree (development), else PyPI.
            wheel = Path(args.wheel).expanduser() if args.wheel else None
            with tempfile.TemporaryDirectory() as tmp:
                if wheel is None and not args.index_url and (SOURCE_TREE / "pyproject.toml").exists():
                    subprocess.run([sys.executable, "-m", "build", "--wheel", "--outdir", tmp, str(SOURCE_TREE)],
                                   check=True, capture_output=True, timeout=BUILD_TIMEOUT)
                    wheel = next(Path(tmp).glob("ray_vast-*.whl"))
                    print(f"installing ray-vast from a wheel built from the source tree {SOURCE_TREE}")
                elif wheel:
                    print(f"installing ray-vast from the wheel {wheel}")
                else:
                    print(f"installing ray-vast=={__version__} from {_mask_userinfo(args.index_url or 'PyPI', args.index_url)}")
                up(args.cluster_yaml, session, ssh, key_name=key_name, wheel=wheel, index_url=args.index_url, vast_key=_read_vast_key(),
                   operator_ip=_public_ip(), state_dir=state_dir)
        elif args.command == "down":
            try:
                vast = VastClient(_read_vast_key())
            except CliError:
                vast = None  # `down` warns that the vast instances were not swept; AWS is still cleaned up
            left = down(cluster, provider["region"], session, vast, state_dir)
            return 1 if left else 0
        else:
            ec2, _ = _clients(session, provider["region"])
            heads = [i for i in _head_instances(ec2, cluster) if i["State"]["Name"] == "running"]
            if not heads:
                raise CliError(f"no running head for cluster {cluster}")
            ip = heads[0]["PublicIpAddress"]
            if args.command == "attach":
                return ssh.interactive(ip, "-t")
            return dashboard(ssh, ip, state_dir, cluster)
    except botocore.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        message = f"ray-vast {args.command}: AWS error {code}: {e.response['Error'].get('Message', '')}"
        if "Denied" in code or code == "UnauthorizedOperation":
            message += "\nThe AWS user lacks a permission that ray-vast needs (IAM role, security group, EC2)."
        if args.command == "up":
            message += f"\nRun `ray-vast down {args.cluster_yaml}` to remove what was created."
        print(message, file=sys.stderr)
        return 1
    except subprocess.TimeoutExpired as e:
        print(f"ray-vast {args.command}: timed out after {e.timeout} s: {shlex.join(map(str, e.cmd))}", file=sys.stderr)
        return 1
    except (CliError, subprocess.CalledProcessError) as e:
        print(f"ray-vast {args.command}: {e}", file=sys.stderr)
        return 1
    return 0
