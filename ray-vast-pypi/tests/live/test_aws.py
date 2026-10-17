"""Group `aws` (docs/test-plan.md, claim matrix A01-A10): real AWS, no vast rental. A02 needs the account with no
ray-vast role and changes the account policy: its marker `exclusive_account` makes it run before every cluster `up`. The
`aws_only` cluster (the head only) has its destructive scenario A07 and its `down` (A01) last in this file. About 60 min, EC2 under $0.30."""
import json
import os
import re
import secrets
import signal
import socket
import subprocess
import time
from pathlib import Path

import boto3
import botocore.exceptions
import pytest
import yaml

import livekit
from livekit import check
from cluster import ADMIN_PROFILE, BIN, aws_leftovers, wait_port
from window import wait_until

pytestmark = pytest.mark.aws
REGION = "us-west-2"
BOUNDARY = "ray-vast-head-boundary"
JOB_DIR = Path(__file__).parent / "jobs"


def admin(service, region=REGION):
    return boto3.Session(profile_name=ADMIN_PROFILE, region_name=region).client(service)


def small_config(suffix):
    """A CPU-image cluster that no worker type can rent for (for the scenarios that need only AWS)."""
    return {"cluster_name": f"rvtest-{suffix}-{secrets.token_hex(2)}",
            "provider": {"image": "rayproject/ray:2.59.0-py311-cpu", "max_dollars_per_hour": 0.001},
            "head_node_type": "head",
            "available_node_types": {"head": {"resources": {"CPU": 0}, "node_config": {"region": REGION}},
                                     "w": {"resources": {"CPU": 4, "GPU": 1}, "min_workers": 0, "max_workers": 1,
                                           "node_config": {"offer_query": {"gpu_ram": {"gte": 900000}}}}}}


def run_until(cluster, args, ready, timeout=900):
    """Start `ray-vast <args>` and send SIGINT (Ctrl-C) once `ready()` is true (or the command ended). Returns its output.
    Not ready in `timeout` s: the Ctrl-C still goes out, then a TimeoutError."""
    proc = subprocess.Popen([str(BIN / "ray-vast"), *map(str, args)], env=cluster.env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, start_new_session=True)
    waited = wait_until(lambda: proc.poll() is not None or ready(), timeout, poll_s=1)
    proc.send_signal(signal.SIGINT)
    out = proc.communicate(timeout=300)[0]
    with open(cluster.dir / "cli.log", "a") as f:
        f.write(f"\n=== ray-vast {' '.join(map(str, args))} (Ctrl-C when ready; exit {proc.returncode})\n{out}")
    if not waited:
        raise TimeoutError(f"ray-vast {args[0]}: not ready in {timeout} s ({waited.error or 'no error'}): {out[-300:]}")
    return out


# --- no cluster fixture ------------------------------------------------------------------------------------------------


@pytest.mark.exclusive_account
@pytest.mark.security
def test_A02_boundary_policy_missing(record, new_cluster, tmp_path):
    """With no cluster: delete the boundary policy, `up` stops before it makes anything, then the documented command
    makes the policy again (always, in `finally`)."""
    iam = admin("iam")
    account = admin("sts").get_caller_identity()["Account"]
    arn = f"arn:aws:iam::{account}:policy/{BOUNDARY}"
    roles = [r["RoleName"] for page in iam.get_paginator("list_roles").paginate() for r in page["Roles"]
             if r["RoleName"].startswith("ray-vast-")]
    check(record, "no ray-vast role uses the boundary now", not roles, roles)
    for version in iam.list_policy_versions(PolicyArn=arn)["Versions"]:
        if not version["IsDefaultVersion"]:
            iam.delete_policy_version(PolicyArn=arn, VersionId=version["VersionId"])
    iam.delete_policy(PolicyArn=arn)
    cluster = None
    try:
        cluster = new_cluster(small_config("a02"), record.dir, tmp_path / "home")
        done = cluster.cli("up", cluster.yaml, "--profile", ADMIN_PROFILE)
        expected = (f"the IAM policy {BOUNDARY} does not exist in AWS account {account}. Create it once (one command): "
                    "https://duc-ph.github.io/ray-vast/getting-started/#aws-setup. Nothing was created.")
        check(record, "up exits 1 with the message and the link", done.returncode == 1 and expected in done.stderr, done.stderr[-400:])
        left = aws_leftovers(cluster.name, REGION)  # before any `down`: the claim is that `up` made nothing
        check(record, "nothing was made", left == [], left)
        check(record, "no state file", not (cluster.state_dir / f"{cluster.name}.json").exists(), "")
    finally:
        # The getting-started command, verbatim except the profile name.
        command = ("IAM_DIR=\"$(python -c 'import ray_vast, os; print(os.path.dirname(ray_vast.__file__))')/iam\"\n"
                   f"aws iam create-policy --policy-name {BOUNDARY} \\\n"
                   f"    --policy-document \"file://$IAM_DIR/{BOUNDARY}.json\" --profile {ADMIN_PROFILE}\n")
        done = subprocess.run(["bash", "-c", command], env={**os.environ, "PATH": f"{BIN}:{os.environ['PATH']}"},
                              capture_output=True, text=True, timeout=120)
        record.evidence("recreate.txt", f"$ {command}\nexit {done.returncode}\n{done.stdout}{done.stderr}")
        assert done.returncode == 0, done.stderr
    check(record, "the documented command made the policy again", iam.get_policy(PolicyArn=arn)["Policy"]["PolicyName"] == BOUNDARY, "")


@pytest.mark.failure
def test_A03_down_recovery(record, new_cluster, tmp_path):
    """(a) Ctrl-C of `up` after the key pair exists, then `down`. (b, c, d) Ctrl-C after the head started; the head is
    terminated by hand; the state file and the SSH key are deleted; `down --profile` still removes everything. The end of
    the test checks AWS and vast after each `down` (the `new_cluster` factory: livekit.end_cluster)."""
    a = new_cluster(small_config("a03a"), record.dir / "a", tmp_path / "home_a")
    out = run_until(a, ["up", a.yaml, "--profile", ADMIN_PROFILE], lambda: a.key_file.exists())
    record.data("a_up_output_tail", out[-300:])
    check(record, "(a) up was stopped after the key pair", a.key_file.exists() or "Traceback" in out or "KeyboardInterrupt" in out, out[-200:])
    done = a.down()
    check(record, "(a) down: left nothing", done.returncode == 0 and "left: nothing" in done.stdout, done.stdout[-300:])

    b = new_cluster(small_config("a03b"), record.dir / "b", tmp_path / "home_b")
    ec2 = admin("ec2")
    from ray_vast import commands
    run_until(b, ["up", b.yaml, "--profile", ADMIN_PROFILE], lambda: bool(commands._head_instances(ec2, b.name)))
    heads = commands._head_instances(ec2, b.name)
    check(record, "(c) a head exists", heads, [h["InstanceId"] for h in heads])
    ids = [h["InstanceId"] for h in heads]
    ec2.terminate_instances(InstanceIds=ids)
    # Not the boto3 waiter: it fails at once when it sees the head still `pending`.
    terminated = wait_until(lambda: [i["State"]["Name"] for r in ec2.describe_instances(InstanceIds=ids)["Reservations"]
                                     for i in r["Instances"]], 600, done=lambda states: all(s == "terminated" for s in states), poll_s=10)
    check(record, "(c) the head is terminated by hand (within 600 s)", terminated.ok, terminated.seen or terminated.error)
    if b.state_dir.joinpath(f"{b.name}.json").exists():
        done = b.cli("attach", b.yaml)
        check(record, "attach without a running head: one-line error", done.returncode == 1 and f"no running head for cluster {b.name}"
              in done.stderr, done.stderr[-200:])
    (b.state_dir / f"{b.name}.json").unlink(missing_ok=True)
    import shutil
    shutil.rmtree(b.state_dir / b.name, ignore_errors=True)  # the SSH key: `down` needs none
    done = b.down()  # no state file: `down --profile`
    check(record, "(b, c, d) down without state file, key or head: left nothing", done.returncode == 0 and "left: nothing" in done.stdout,
          done.stdout[-300:])


@pytest.mark.security
def test_A10_restricted_user_denials(record, restricted_user):
    session = restricted_user.session
    iam, ec2 = session.client("iam"), session.client("ec2", region_name=REGION)
    account = session.client("sts").get_caller_identity()["Account"]
    boundary = f"arn:aws:iam::{account}:policy/{BOUNDARY}"
    trust = json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"},
                                                                 "Action": "sts:AssumeRole"}]})
    calls = {
        "CreateRole ray-vast-negtest without a boundary": lambda: iam.create_role(RoleName="ray-vast-negtest", AssumeRolePolicyDocument=trust),
        "CreateRole ray-vast-negtest with AdministratorAccess as the boundary": lambda: iam.create_role(
            RoleName="ray-vast-negtest", AssumeRolePolicyDocument=trust, PermissionsBoundary="arn:aws:iam::aws:policy/AdministratorAccess"),
        "CreateRole negtest-other with the right boundary": lambda: iam.create_role(
            RoleName="negtest-other", AssumeRolePolicyDocument=trust, PermissionsBoundary=boundary),
        "PutRolePolicy on a role not named ray-vast-*": lambda: iam.put_role_policy(
            RoleName="negtest-other", PolicyName="x", PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": "s3:ListAllMyBuckets", "Resource": "*"}]})),
        "CreatePolicy": lambda: iam.create_policy(PolicyName="ray-vast-negtest", PolicyDocument=json.dumps({"Version": "2012-10-17",
                                                    "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]})),
        "DeleteRolePermissionsBoundary": lambda: iam.delete_role_permissions_boundary(RoleName="ray-vast-negtest"),
        "CreateSecurityGroup without the ray-vast-cluster tag": lambda: ec2.create_security_group(
            GroupName="ray-vast-negtest", Description="negtest"),
    }
    lines = []
    for name, call in calls.items():
        try:
            call()
            code = "ALLOWED"
        except botocore.exceptions.ClientError as e:
            code = e.response["Error"]["Code"]
        lines.append(f"{name}: {code}")
        check(record, f"{name} is denied", code in ("AccessDenied", "UnauthorizedOperation"), code)
    record.evidence("denials.txt", "\n".join(lines) + "\n")


# --- the aws_only cluster -------------------------------------------------------------------------------------------


@pytest.mark.security
def test_A05_head_network_exposure(record, aws_only):
    head = aws_only.head_ip
    reach = {}
    for port in (6379, 7443, 7444, 8265, 10001, 6384, 6385):
        try:
            socket.create_connection((head, port), timeout=5).close()
            reach[port] = "open"
        except OSError as e:
            reach[port] = type(e).__name__
    record.data("from_operator", reach)
    for port, result in reach.items():
        check(record, f"port {port} is closed from the operator machine", result != "open", result)
    listen = aws_only.ssh("sudo ss -ltnH")
    record.evidence("ss_ltn.txt", listen)
    for port in (8265, 6384, 6385):
        addresses = {line.split()[3].rsplit(":", 1)[0] for line in listen.splitlines() if line.split()[3].endswith(f":{port}")}
        check(record, f"port {port} listens on 127.0.0.1 only", addresses == {"127.0.0.1"}, sorted(addresses))
    # T9: with a job running, the head sends no packet to its own public IP (the shim rewrites those connects).
    capture = subprocess.Popen([*aws_only.ssh_args(), f"sudo timeout 60 tcpdump -nni any -c 50 'dst host {head} and not port 22' 2>&1"],
                               stdout=subprocess.PIPE, text=True)
    time.sleep(3)
    with aws_only.tunnel() as env:
        status = aws_only.job(env, ["python", "-c", "import ray; ray.init(); print(ray.cluster_resources())"], record.dir,
                              name="activity", timeout=300).status
    out = capture.communicate(timeout=120)[0]
    record.evidence("tcpdump_to_own_public_ip.txt", out)
    packets = re.search(r"(\d+) packets captured", out)
    check(record, "job during the capture", status == "SUCCEEDED", status)
    check(record, "0 packets to the head's own public IP", packets is not None and packets.group(1) == "0", packets and packets.group(0))


def test_A08_no_offer_and_budget_texts(record, aws_only):
    w = aws_only.window()
    script = ("import ray\nray.init()\n"
              "refs = [ray.remote(lambda: 1).options(resources={'impossible': 1}, num_gpus=1).remote(),\n"
              "        ray.remote(lambda: 1).options(resources={'poor': 1}, num_gpus=1).remote()]\n"
              "ray.wait(refs, timeout=600)\n")
    texts = {"no offer": "no rentable offer matches the node type query",
             "budget": "every matching offer would pass max_dollars_per_hour"}

    def found():  # {key: the first launch_failed line with its text}
        failed = w.events("launch_failed")
        return {key: hits[0].line for key, text in texts.items() for hits in [[e for e in failed if text in e.reason]] if hits}

    with aws_only.tunnel() as env:
        demand = aws_only.job(env, ["python", "-c", script], record.dir, name="demand", wait=False)
        found = wait_until(found, 420, done=lambda seen: len(seen) == 2, poll_s=15).seen or {}
        status = aws_only.head_exec("ray status")
        demand.stop()
    record.evidence("monitor_slice.log", "\n".join(w.lines()))
    record.evidence("ray_status.txt", status)
    for key, text in texts.items():
        check(record, f"monitor.log: {text}", key in found, found.get(key, "")[:300])
    check(record, "ray status shows LaunchFailed", "LaunchFailed" in status, "")
    check(record, "0 vast rentals", aws_only.instances() == [] and not any(r.get("instances") for r in aws_only.timeline), "")


@pytest.mark.security
def test_A09_token_on_every_ray_call(record, aws_only):
    with aws_only.tunnel() as env:
        bare = {k: v for k, v in env.items() if not k.startswith("RAY_AUTH")}
        done = subprocess.run([str(BIN / "ray"), "job", "submit", "--address", env["RAY_ADDRESS"], "--", "echo", "hi"], env=bare,
                              capture_output=True, text=True, timeout=300)
        record.evidence("submit_without_token.txt", done.stdout + done.stderr)
        check(record, "ray job submit without RAY_AUTH_* fails", done.returncode != 0, (done.stdout + done.stderr)[-300:])
        script = ("import os, time\nfrom ray.job_submission import JobSubmissionClient\n"
                  "c = JobSubmissionClient(os.environ['RAY_ADDRESS'])\nsid = c.submit_job(entrypoint='echo 12345')\n"
                  "while str(c.get_job_status(sid).value) not in ('SUCCEEDED', 'FAILED', 'STOPPED'): time.sleep(2)\n"
                  "print('STATUS', c.get_job_status(sid).value, c.get_job_logs(sid).strip())\n")
        done = subprocess.run([str(BIN / "python"), "-c", script], env=env, capture_output=True, text=True, timeout=300)
        record.evidence("job_submission_client.txt", done.stdout + done.stderr)
        check(record, "JobSubmissionClient with the export line: SUCCEEDED", "STATUS SUCCEEDED" in done.stdout and "12345" in done.stdout,
              done.stdout[-200:])


@pytest.mark.security
def test_A07_the_boundary_caps_the_head_role(record, aws_only):
    """Add an inline policy with more rights to the head role; from the head (IMDS credentials) the calls outside the
    boundary are denied, and a rule on an untagged group is denied, while a rule on a cluster group works."""
    iam, ec2 = admin("iam"), admin("ec2")
    role = f"ray-vast-{aws_only.name}"
    livekit.assert_ours(role)
    untagged_name = f"ray-vast-{aws_only.name}-untagged"
    livekit.assert_ours(untagged_name)
    untagged = ec2.create_security_group(GroupName=untagged_name, Description="A07 untagged")["GroupId"]
    iam.put_role_policy(RoleName=role, PolicyName="livetest-extra", PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:ListAllMyBuckets", "ec2:CreateSecurityGroup", "ec2:AuthorizeSecurityGroupIngress"], "Resource": "*"}]}))
    groups = aws_only.state["security_group_ids"]
    try:
        time.sleep(20)  # IAM changes take some seconds
        script = f"""
import boto3, botocore.exceptions, json
s = boto3.Session(region_name="{REGION}")
out = {{}}
def tryit(name, f):
    try:
        f(); out[name] = "ALLOWED"
    except botocore.exceptions.ClientError as e:
        out[name] = e.response["Error"]["Code"]
perm = [{{"IpProtocol": "tcp", "FromPort": 1, "ToPort": 1, "IpRanges": [{{"CidrIp": "192.0.2.1/32", "Description": "a07"}}]}}]
tryit("s3 ListAllMyBuckets", lambda: s.client("s3").list_buckets())
tryit("ec2 CreateSecurityGroup", lambda: s.client("ec2").create_security_group(GroupName="a07-x", Description="x"))
tryit("authorize on an untagged group", lambda: s.client("ec2").authorize_security_group_ingress(GroupId="{untagged}", IpPermissions=perm))
tryit("authorize on cluster group 1", lambda: s.client("ec2").authorize_security_group_ingress(GroupId="{groups[1]}", IpPermissions=perm))
tryit("revoke on cluster group 1", lambda: s.client("ec2").revoke_security_group_ingress(GroupId="{groups[1]}", IpPermissions=perm))
print("A07", json.dumps(out))
"""
        import shlex
        out = aws_only.ssh(f"sudo docker exec ray-vast-head python -c {shlex.quote(script)}")
        record.evidence("from_the_head.txt", out)
        result = json.loads(re.search(r"A07 (\{.*\})", out).group(1))
        for name in ("s3 ListAllMyBuckets", "ec2 CreateSecurityGroup", "authorize on an untagged group"):
            check(record, f"{name} is denied", result[name] in ("AccessDenied", "UnauthorizedOperation"), result[name])
        for name in ("authorize on cluster group 1", "revoke on cluster group 1"):
            check(record, f"{name} works", result[name] == "ALLOWED", result[name])
    finally:
        iam.delete_role_policy(RoleName=role, PolicyName="livetest-extra")
        ec2.delete_security_group(GroupId=untagged)


def test_A01_head_only_life_cycle_with_a_saved_profile(record, aws_only):
    """The `up` of the fixture ran with `--profile` and no AWS_PROFILE. Then: the state file, the key, the head, attach,
    dashboard (saved profile, explicit profile wins, Ctrl-C), the missing-key error, and `down` with no flag (last)."""
    if aws_only.kept:
        pytest.skip("--keep-cluster: the cluster stays")
    up = aws_only.up_output
    record.evidence("up.txt", up["stdout"] + up["stderr"])
    record.data("up_s", up["wall_s"])
    lines = up["stdout"].splitlines()
    check(record, "up: first line names the source", lines[0].startswith("installing ray-vast from a wheel built from the source tree"), lines[0])
    for pattern in (r"^head instance i-\w+ starting$", r"^head IP [\d.]+$", rf"^cluster {aws_only.name} is up\. Head IP [\d.]+\.$",
                    rf"^  dashboard: ray-vast dashboard \S+   \(then open http://127\.0\.0\.1:8265\)$", r"^  shell:     ray-vast attach \S+$"):
        check(record, f"up output line {pattern}", any(re.match(pattern, line) for line in lines), "")
    state = aws_only.state
    record.evidence("state.json", json.dumps(state, indent=1))
    check(record, "state file: region, profile, head, groups, role, instance profile",
          state.get("region") == REGION and state.get("profile") == ADMIN_PROFILE and state.get("head_instance_id") and
          len(state.get("security_group_ids", [])) == 5 and state.get("role_name") == state.get("instance_profile_name") == f"ray-vast-{aws_only.name}",
          {k: v for k, v in state.items() if k != "security_group_ids"})
    key = aws_only.key_file
    check(record, "ssh_key.pem mode 600, its directory 700", oct(key.stat().st_mode & 0o777) == "0o600" and
          oct(key.parent.stat().st_mode & 0o777) == "0o700", (oct(key.stat().st_mode & 0o777), oct(key.parent.stat().st_mode & 0o777)))

    ec2 = admin("ec2")
    head = ec2.describe_instances(InstanceIds=[state["head_instance_id"]])["Reservations"][0]["Instances"][0]
    image = ec2.describe_images(ImageIds=[head["ImageId"]])["Images"][0]
    volume = ec2.describe_volumes(VolumeIds=[head["BlockDeviceMappings"][0]["Ebs"]["VolumeId"]])["Volumes"][0]
    eips = ec2.describe_addresses(Filters=[{"Name": "instance-id", "Values": [head["InstanceId"]]}])["Addresses"]
    record.data("head", {"type": head["InstanceType"], "image": image["Name"], "volume": [volume["Size"], volume["VolumeType"]],
                         "imds": head["MetadataOptions"]["HttpTokens"], "eips": len(eips)})
    check(record, "head: t3.large (the default), Ubuntu 24.04 amd64", head["InstanceType"] == "t3.large" and
          "ubuntu-noble-24.04-amd64" in image["Name"], image["Name"])
    check(record, "head: 60 GiB gp3, IMDSv2 required, no Elastic IP", volume["Size"] == 60 and volume["VolumeType"] == "gp3" and
          head["MetadataOptions"]["HttpTokens"] == "required" and not eips, "")
    name = f"ray-vast-{aws_only.name}"
    tagged = {"instance": head["Tags"], "volume": volume.get("Tags", []),
              "key pair": ec2.describe_key_pairs(KeyNames=[name])["KeyPairs"][0].get("Tags", [])}
    for group in ec2.describe_security_groups(GroupIds=state["security_group_ids"])["SecurityGroups"]:
        tagged[group["GroupName"]] = group["Tags"]
    tagged["role"] = admin("iam").get_role(RoleName=name)["Role"].get("Tags", [])
    for what, tags in tagged.items():
        check(record, f"{what} has the tag ray-vast-cluster={aws_only.name}", {"Key": "ray-vast-cluster", "Value": aws_only.name} in tags, "")
    boundary = admin("iam").get_role(RoleName=name)["Role"].get("PermissionsBoundary", {}).get("PermissionsBoundaryArn", "")
    check(record, "the head role has the boundary", boundary.endswith(f":policy/{BOUNDARY}"), boundary)
    shm = aws_only.ssh("sudo docker exec ray-vast-head df -k /dev/shm | tail -1; grep MemTotal /proc/meminfo")
    shm_kb, mem_kb = int(shm.split()[1]), int(re.search(r"MemTotal:\s+(\d+)", shm).group(1))
    check(record, "head /dev/shm = 0.33 x RAM", abs(shm_kb / mem_kb - 0.33) < 0.02, round(shm_kb / mem_kb, 3))
    head_yaml = yaml.safe_load(aws_only.ssh(f"cat /home/ubuntu/ray-vast-{aws_only.name}/cluster.yaml"))
    check(record, "the head YAML has the measured image_size_gb", isinstance(head_yaml["provider"].get("image_size_gb"), int),
          head_yaml["provider"].get("image_size_gb"))

    # attach and dashboard with the saved profile (no --profile, no AWS_PROFILE)
    done = aws_only.cli("attach", aws_only.yaml, input="echo ATTACH_OK $(whoami)\nexit\n", timeout=120)
    check(record, "attach with the saved profile opens a shell on the head", "ATTACH_OK ubuntu" in done.stdout, done.stdout[-200:])
    proc = subprocess.Popen([str(BIN / "ray-vast"), "dashboard", str(aws_only.yaml)], env=aws_only.env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, start_new_session=True)
    try:
        first = [proc.stdout.readline() for _ in range(3)]  # line-buffered: the lines arrive while the tunnel runs
        ok = wait_port(8265, 60)
        token_file = aws_only.state_dir / aws_only.name / "token"
        env_line = f"  export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH={token_file}\n"
        check(record, "dashboard prints the export line before the tunnel ends", env_line in first, first)
        check(record, "the tunnel opens 8265", ok, "")
        check(record, "token file mode 600", oct(token_file.stat().st_mode & 0o777) == "0o600", oct(token_file.stat().st_mode & 0o777))
        os.killpg(proc.pid, signal.SIGINT)  # Ctrl-C in a terminal goes to the whole process group (ray-vast and its ssh)
        rest = proc.communicate(timeout=60)[0]
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
    record.evidence("dashboard.txt", "".join(first) + rest)
    check(record, "Ctrl-C prints `tunnel closed`, exit 0", "tunnel closed" in rest and proc.returncode == 0, (rest[-100:], proc.returncode))
    for how, extra, env in (("AWS_PROFILE=bogus", [], {"AWS_PROFILE": "bogus"}), ("--profile bogus", ["--profile", "bogus"], {})):
        done = aws_only.cli("dashboard", aws_only.yaml, *extra, env=env, timeout=120)
        check(record, f"dashboard with {how} fails on that profile (it wins over the saved one)", done.returncode != 0 and
              "bogus" in done.stderr, done.stderr.strip()[-200:])
    hidden = aws_only.key_file.with_suffix(".hidden")
    aws_only.key_file.rename(hidden)
    try:
        done = aws_only.cli("attach", aws_only.yaml, timeout=120)
        check(record, "attach without the SSH key: the documented error", done.returncode == 1 and
              f"no SSH key for cluster {aws_only.name} at" in done.stderr, done.stderr[-200:])
    finally:
        hidden.rename(aws_only.key_file)
    check(record, "0 vast rentals", not any(r.get("instances") for r in aws_only.timeline), "")

    # down with no flag: the saved profile
    started = time.time()
    done = aws_only.down()
    record.evidence("down.txt", done.stdout + done.stderr)
    record.data("down_s", round(time.time() - started, 1))
    out = done.stdout.splitlines()
    check(record, "down: terminating head first", out and out[0].startswith("terminating head i-"), out[:1])
    check(record, "down: 5 groups, key pair, instance profile, role", sum(line.startswith("deleted security group sg-") for line in out) == 5
          and f"deleted key pair {name}" in out and f"deleted instance profile {name}" in out and f"deleted role {name}" in out, out)
    check(record, "down: left nothing, exit 0", done.returncode == 0 and out[-1] == "left: nothing", out[-1:])
    check(record, "state files and the key are gone", not (aws_only.state_dir / f"{aws_only.name}.json").exists() and
          not aws_only.key_file.parent.exists(), "")
