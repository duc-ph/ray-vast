"""The two cluster kinds of the suite (their interface: livecloud.py) and the cloud guards. `Cluster`: a suite cluster, the
`ray-vast` CLI in a private HOME, SSH to the head, a tunnel, jobs. `ReleaseCluster`: the getting-started tutorial. Every
command output goes to `<dir>/cli.log`. Secrets (the cluster token, join tokens) stay in memory."""
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from contextlib import contextmanager
from pathlib import Path

import boto3
import yaml

from ray_vast import commands
from ray_vast.config import HEAD_INSTANCE_TYPE
from ray_vast.vast_api import VastClient, label

import livejob
import window
from livepoller import Poller
from liverun import assert_ours

BIN = Path(sys.executable).parent
RAY_VAST = [str(BIN / "ray-vast")]
JOBS = Path(__file__).parent / "jobs"
ADMIN_PROFILE = os.environ.get("RAYVAST_LIVE_PROFILE", "default")
MONITOR_LOG = "/tmp/ray/session_latest/logs/monitor.log"


def vast_key():
    return commands._read_vast_key()


def vast():
    return VastClient(vast_key())


def credit_sample():
    return {"t": time.time(), "credit": vast()._request("GET", "/users/current/").json()["credit"]}


def invoices():
    return vast()._request("GET", "/users/me/invoices/?owner=me").json().get("invoices", [])


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_port(port, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
            return True
        except OSError:
            time.sleep(1)
    return False


class Cluster:
    """A suite cluster (a YAML of tests/live/clusters/). `run` is the liverun.Run of the suite: the cluster joins it here,
    once. `kept` (--keep-cluster, decided in conftest.py): the run does not end the cluster."""

    def __init__(self, run, config, out_dir, home, kept=False, profile=ADMIN_PROFILE, credentials_file=None):
        self.kept = kept
        self.name = config["cluster_name"]
        assert_ours(f"ray-vast-{self.name}")
        self.config = config
        self.dir = Path(out_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.home = Path(home)
        (self.home / ".ray-vast").mkdir(parents=True, exist_ok=True)
        self.yaml = self.dir / "cluster.yaml"
        self.yaml.write_text(yaml.safe_dump(config, sort_keys=False))
        self.profile = profile
        head = config["available_node_types"][config["head_node_type"]]["node_config"]
        self.region, self.head_instance_type = head["region"], head.get("instance_type", HEAD_INSTANCE_TYPE)
        aws_dir = Path.home() / ".aws"
        self.env = {**os.environ, "HOME": str(self.home), "VAST_API_KEY": vast_key(), "PYTHONUNBUFFERED": "1",
                    "AWS_CONFIG_FILE": os.environ.get("AWS_CONFIG_FILE", str(aws_dir / "config")),
                    "AWS_SHARED_CREDENTIALS_FILE": str(credentials_file or aws_dir / "credentials"),
                    "PATH": f"{BIN}:{os.environ['PATH']}", "AWS_PROFILE": profile}
        self.secrets = []  # the cluster token and the join tokens, for the secret scan; never written
        self.records = [self.dir]  # the directories that liverun.end_cluster scans; Run.record adds each scenario's
        self.up_output = self.last_down = None
        self.poller = Poller(vast, label(self.name), self.dir / "vast_timeline.jsonl", self.secrets)
        self.run = run
        run.register(self)

    # --- the CLI ---------------------------------------------------------------------------------------------------
    def cli(self, *args, env=None, timeout=1800, log=True, input=None):
        started = time.time()
        stdin = {"input": input} if input is not None else {"stdin": subprocess.DEVNULL}
        done = subprocess.run([*RAY_VAST, *map(str, args)], env={**self.env, **(env or {})}, capture_output=True, text=True,
                              timeout=timeout, **stdin)
        if log:
            with open(self.dir / "cli.log", "a") as f:
                f.write(f"\n=== {time.strftime('%FT%TZ', time.gmtime(started))} ray-vast {' '.join(map(str, args))} "
                        f"(exit {done.returncode}, {time.time() - started:.1f} s)\n{done.stdout}{done.stderr}")
        return done

    def up(self):
        started = time.time()
        self.last_down = None  # a new `up` makes resources again
        done = self.cli("up", self.yaml)
        self.up_output = {"returncode": done.returncode, "stdout": done.stdout, "stderr": done.stderr, "wall_s": round(time.time() - started, 1)}
        if done.returncode != 0:
            raise RuntimeError(f"ray-vast up {self.name} failed (exit {done.returncode}): {(done.stderr or done.stdout)[-800:]}")
        self.collect_token()
        return self

    def down(self, *extra, env=None):
        """`down`. The result is `last_down` (liverun.end_cluster runs no second `down` after a clean one)."""
        self.last_down = self.cli("down", self.yaml, *extra, env=env)
        return self.last_down

    @property
    def timeline(self):
        return self.poller.timeline

    # --- state and SSH ---------------------------------------------------------------------------------------------
    @property
    def state_dir(self):
        return self.home / ".ray-vast"

    @property
    def state(self):
        path = self.state_dir / f"{self.name}.json"
        return json.loads(path.read_text()) if path.exists() else {}

    @property
    def head_ip(self):
        return self.state["head_ip"]

    @property
    def key_file(self):
        return self.state_dir / self.name / "ssh_key.pem"

    def ssh_args(self, *options):
        return ["ssh", "-i", str(self.key_file), "-o", f"UserKnownHostsFile={self.state_dir / (self.name + '.known_hosts')}",
                "-o", "StrictHostKeyChecking=accept-new", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", *options,
                f"ubuntu@{self.head_ip}"]

    def ssh(self, command, timeout=300, check=True):
        done = subprocess.run([*self.ssh_args(), command], capture_output=True, text=True, timeout=timeout, stdin=subprocess.DEVNULL)
        if check and done.returncode != 0:
            raise RuntimeError(f"ssh {command[:100]}: exit {done.returncode}: {done.stderr[-400:]}")
        return done.stdout

    def head_exec(self, command, timeout=300, check=True):
        import shlex
        return self.ssh(f"sudo docker exec ray-vast-head bash -c {shlex.quote(command)}", timeout=timeout, check=check)

    def head_runs(self):
        """True when the state file exists and the head container runs (a kept cluster can be reused)."""
        if not self.state:
            return False
        done = subprocess.run([*self.ssh_args(), "sudo docker inspect -f '{{.State.Running}}' ray-vast-head"], capture_output=True,
                              text=True, timeout=120, stdin=subprocess.DEVNULL)
        return done.returncode == 0 and done.stdout.strip() == "true"

    # --- the head adapter of the observation window (window.py) ------------------------------------------------------
    def window(self, from_start=False):
        """An observation window on this cluster (window.Window): the head log events and the vast instances since now
        (`from_start`: since the first line of the head log)."""
        return window.Window(self, from_start=from_start)

    def monitor_log(self, start_line=1):
        return self.head_exec(f"tail -n +{start_line} {MONITOR_LOG}", timeout=120).splitlines()

    def monitor_log_length(self):
        return int(self.head_exec(f"wc -l < {MONITOR_LOG}", timeout=120).strip())

    def head_time_zone(self):
        """The IANA time zone of the head container: its TZ (the rayproject/ray images set America/Los_Angeles), else the
        zone of /etc/localtime; "" for UTC."""
        return self.head_exec('echo "${TZ:-$(readlink -f /etc/localtime 2>/dev/null | sed -n "s|.*/zoneinfo/||p")}"', timeout=120).strip()

    def collect_token(self):
        token = self.ssh(f"cat /home/ubuntu/ray-vast-{self.name}/secrets/token").strip()
        if token:
            self.secrets.append(token)
        return token

    # --- the dashboard tunnel and jobs -----------------------------------------------------------------------------
    @contextmanager
    def tunnel(self):
        """A private SSH tunnel to the dashboard on a free local port (not `ray-vast dashboard`, which always uses 8265, so
        two clusters can run at once). Yields the env for the Ray job CLI and the Python API."""
        port = free_port()
        token_file = self.state_dir / self.name / "suite_token"
        token_file.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        token_file.write_text(self.collect_token())
        token_file.chmod(0o600)
        proc = subprocess.Popen([*self.ssh_args("-N", "-o", "ExitOnForwardFailure=yes", "-L", f"{port}:127.0.0.1:8265")],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        try:
            if not wait_port(port):
                raise RuntimeError("the dashboard tunnel did not open")
            yield {**self.env, "RAY_AUTH_MODE": "token", "RAY_AUTH_TOKEN_PATH": str(token_file),
                   "RAY_ADDRESS": f"http://127.0.0.1:{port}"}
        finally:
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=30)

    def job(self, env, entrypoint, out_dir=None, **options):
        """One Ray job on this cluster through the tunnel `env` (livejob.run: its working dir, runtime env, secrets, result
        prefix, timeout, wait). The submit command and the log go to `out_dir` (default: the cluster's directory); the
        job's secrets join the run's secret scan. Returns the livejob.Job."""
        for secret in options.get("secrets", ()):
            self.run.secret(secret)
        return livejob.run(env, entrypoint, out_dir=out_dir or self.dir, **options)

    def node_facts(self, env, out_dir, name="node_facts", check=True):
        """jobs/node_facts.py on every worker: its NODE_FACTS rows, each with the `machine_id` of its vast instance. A job
        that does not succeed raises RuntimeError (check=False: the rows that it printed)."""
        job = self.job(env, ["python", "node_facts.py"], out_dir, name=name, timeout=900,
                       working_dir=livejob.workdir(JOBS / "node_facts.py", JOBS / "paircommon.py"))
        if check and job.status != "SUCCEEDED":
            raise RuntimeError(f"node facts {name}: {job.status}")
        machines = {str(i["id"]): i.get("machine_id") for i in self.instances()}
        return [dict(row, machine_id=machines.get(str(row.get("container_id")))) for row in job.lines("NODE_FACTS")]

    # --- vast --------------------------------------------------------------------------------------------------------
    def instances(self):
        return self.poller.instances()

    def processes_at_once(self, instance_id):
        """N of one worker: the `processes at once N` of the start line in its container log (None without the line)."""
        found = re.search(r"bootstrap: limits: .*, processes at once (\d+),", self.container_log(instance_id))
        return int(found.group(1)) if found else None

    def container_log(self, instance_id):
        """The container log of one vast instance (fetch it before the destroy: vast keeps none after)."""
        result = vast()._request("PUT", f"/instances/request_logs/{instance_id}/", json={"tail": "20000"}).json()
        url = result.get("result_url")
        for _ in range(30):
            try:
                with urllib.request.urlopen(url, timeout=30) as response:
                    return response.read().decode(errors="replace")
            except Exception:
                time.sleep(2)
        return ""


class ReleaseCluster:
    """The cluster of the release group (test_release.py): the second adapter of the cluster seam, next to
    Cluster. It follows the getting-started tutorial verbatim, so it differs from a suite cluster in three ways: its name
    (`gpu-24gb`, from the packaged YAML) is not a suite name, it runs the plain `ray-vast` CLI of a clean venv with
    ray-vast from PyPI, and its YAML is the packaged one (only `region` changes). Like every cluster it joins the run when it
    is made, and liverun.end_cluster ends it. `up()` is the tutorial up to `ray-vast up`; `shell(script)` runs a script in
    the tutorial's directory (`work`) with the tutorial's env (`env`).
    `gpu-24gb` is not a suite name, so a user can have a cluster of that name on the same account. The release cluster
    therefore refuses to start when a vast instance with its label or an AWS resource of `ray-vast-gpu-24gb` already
    exists: it raises before it joins the run, and nothing ends or destroys anything. Its end sweeps only the vast
    instances that its own poller saw (liverun.end_cluster)."""

    def __init__(self, run, out_dir, home, region, version):
        self.name, self.region, self.version, self.kept = "gpu-24gb", region, version, False
        found = [i["id"] for i in vast().list_instances() if i.get("label") == label(self.name)]
        left = _aws_leftovers(self.name, region, ADMIN_PROFILE)
        if found or left:
            raise RuntimeError(f"a cluster {self.name} already exists on this account (vast instances {found}, AWS {left}): "
                               "the release test would end it, so it does not start")
        self.dir, self.home = Path(out_dir), Path(home)
        self.work = self.home / "work"
        self.work.mkdir(parents=True, exist_ok=True)
        self.yaml = self.work / "cluster.yaml"
        self.config = {}  # the packaged YAML, after the `cp` of `up`
        # The tutorial's `.env`: the AWS keys of the admin profile and the vast key. No AWS profile file and no key
        # variable reaches the CLI, so the keys can only come from `.env`.
        aws = boto3.Session(profile_name=ADMIN_PROFILE).get_credentials().get_frozen_credentials()
        dot_env = {"AWS_ACCESS_KEY_ID": aws.access_key, "AWS_SECRET_ACCESS_KEY": aws.secret_key, "VAST_API_KEY": vast_key()}
        if aws.token:
            dot_env["AWS_SESSION_TOKEN"] = aws.token
        (self.work / ".env").write_text("".join(f"{k}={v}\n" for k, v in dot_env.items()))
        self.env = {k: v for k, v in os.environ.items() if k not in ("VIRTUAL_ENV", "AWS_PROFILE", *dot_env)}
        self.env.update(HOME=str(self.home), AWS_CONFIG_FILE=str(self.home / "no-aws-config"),
                        AWS_SHARED_CREDENTIALS_FILE=str(self.home / "no-aws-credentials"))
        self.records, self.secrets = [self.dir], [aws.secret_key, dot_env["VAST_API_KEY"]]
        self.poller = Poller(vast, label(self.name), self.dir / "vast_timeline.jsonl", self.secrets)
        self.up_output = self.last_down = self.started = None
        run.register(self)

    @property
    def timeline(self):
        return self.poller.timeline

    @property
    def head_instance_type(self):
        """The head type of the packaged YAML (after the `cp` of `up`), else the default."""
        head = (self.config.get("available_node_types") or {}).get(self.config.get("head_node_type"), {})
        return head.get("node_config", {}).get("instance_type", HEAD_INSTANCE_TYPE)

    def instances(self):
        return self.poller.instances()

    def shell(self, script, timeout=1800):
        done = subprocess.run(["bash", "-c", f"cd {self.work} && {script}"], env=self.env, capture_output=True, text=True,
                              timeout=timeout)
        with open(self.dir / "cli.log", "a") as f:
            f.write(f"\n=== {time.strftime('%FT%TZ', time.gmtime())} {script} (exit {done.returncode})\n{done.stdout}{done.stderr}")
        return done

    def up(self):
        self.started, self.last_down = time.time(), None
        done = self.shell('python3 -m venv .venv && source .venv/bin/activate && pip install -q ray-vast "ray[default]==2.59.*" && '
                          'ray-vast --help >/dev/null && python -c "import ray_vast; print(ray_vast.__version__)"')
        if done.returncode != 0 or not done.stdout.strip().endswith(self.version):
            raise RuntimeError(f"the install of ray-vast {self.version} from PyPI failed: {(done.stderr or done.stdout)[-500:]}")
        done = self.shell('source .venv/bin/activate && cp "$(python -c \'import ray_vast, os; print(os.path.dirname(ray_vast.__file__))\')/'
                          f'examples/gpu-24gb.yaml" cluster.yaml && sed -i "s/region: us-west-2/region: {self.region}/" cluster.yaml')
        if done.returncode != 0:
            raise RuntimeError(f"the cp of gpu-24gb.yaml failed: {done.stderr[-300:]}")
        self.config = yaml.safe_load(self.yaml.read_text())
        if self.config["cluster_name"] != self.name:
            raise RuntimeError(f"the packaged YAML names the cluster {self.config['cluster_name']}, not {self.name}")
        started = time.time()
        done = self.shell("source .venv/bin/activate && ray-vast up cluster.yaml", timeout=2400)
        self.up_output = {"returncode": done.returncode, "stdout": done.stdout, "stderr": done.stderr, "wall_s": round(time.time() - started, 1)}
        if done.returncode != 0:
            raise RuntimeError(f"ray-vast up {self.name} failed (exit {done.returncode}): {(done.stderr or done.stdout)[-800:]}")
        return self

    def down(self):
        """The tutorial's `ray-vast down cluster.yaml`. Before it, the token file of `ray-vast dashboard` joins the secrets."""
        token = self.home / ".ray-vast" / self.name / "token"
        if token.exists() and token.read_text().strip() not in self.secrets:
            self.secrets.append(token.read_text().strip())
        if not (self.work / ".venv" / "bin" / "ray-vast").exists():  # the install failed: no cluster was made
            return subprocess.CompletedProcess(["ray-vast", "down"], 0, "the install failed before `up`: nothing to end\nleft: nothing\n", "")
        self.last_down = self.shell("source .venv/bin/activate && ray-vast down cluster.yaml")
        return self.last_down


VOLUME_GONE_S = 180


def aws_leftovers(cluster, region, profile=ADMIN_PROFILE):
    """The independent AWS check (as the admin): every resource of the cluster that is left. EC2 deletes the head's
    volume (DeleteOnTermination) some seconds after the instance terminates, so while only volumes are left, the check
    looks again for up to VOLUME_GONE_S."""
    deadline = time.time() + VOLUME_GONE_S
    left = _aws_leftovers(cluster, region, profile)
    while left and all(x.startswith("volume ") for x in left) and time.time() < deadline:
        time.sleep(10)
        left = _aws_leftovers(cluster, region, profile)
    return left


def _aws_leftovers(cluster, region, profile):
    session = boto3.Session(profile_name=profile, region_name=region)
    ec2, iam = session.client("ec2"), session.client("iam")
    name, tag = f"ray-vast-{cluster}", [{"Name": f"tag:{commands.TAG_KEY}", "Values": [cluster]}]
    left = [f"EC2 instance {i['InstanceId']}" for i in commands._head_instances(ec2, cluster)]
    left += [f"security group {g['GroupId']}" for g in ec2.describe_security_groups(
        Filters=[{"Name": "group-name", "Values": [f"{name}-*"]}])["SecurityGroups"]]
    left += [f"volume {v['VolumeId']}" for v in ec2.describe_volumes(Filters=tag)["Volumes"]]
    left += [f"key pair {k['KeyName']}" for k in ec2.describe_key_pairs(Filters=[{"Name": "key-name", "Values": [name]}])["KeyPairs"]]
    for kind, get, kwargs in (("role", iam.get_role, {"RoleName": name}),
                              ("instance profile", iam.get_instance_profile, {"InstanceProfileName": name})):
        try:
            get(**kwargs)
            left.append(f"{kind} {name}")
        except iam.exceptions.NoSuchEntityException:
            pass
    return left


def aws_account_leftovers(regions, skip, profile=ADMIN_PROFILE):
    """The account-wide AWS check at the end of a run (as the admin): every resource with a suite cluster name
    (`ray-vast-rvtest-*`) in `regions` (instances and volumes by the cluster tag, security groups, key pairs) and in IAM
    (roles, instance profiles), except the kept clusters in `skip`. It finds what a cluster that never reached
    liverun.end_cluster left. A suite run in parallel on the same account would show here too."""
    session = boto3.Session(profile_name=profile)
    kept = [f"ray-vast-{name}" for name in skip]

    def ours(name):
        return name.startswith("ray-vast-rvtest-") and not any(name == k or name.startswith(k + "-") for k in kept)

    left = []
    for region in regions:
        ec2 = session.client("ec2", region_name=region)
        tag = [{"Name": "tag-key", "Values": [commands.TAG_KEY]}]
        reservations = ec2.describe_instances(Filters=tag + [{"Name": "instance-state-name", "Values": commands.LIVE_STATES}])["Reservations"]
        tagged = [(f"EC2 instance {i['InstanceId']}", i) for r in reservations for i in r["Instances"]]
        tagged += [(f"volume {v['VolumeId']}", v) for v in ec2.describe_volumes(Filters=tag)["Volumes"]]
        for what, resource in tagged:
            cluster = next((t["Value"] for t in resource.get("Tags", []) if t["Key"] == commands.TAG_KEY), "")
            if ours(f"ray-vast-{cluster}"):
                left.append(f"{what} ({cluster}, {region})")
        name_filter = [{"Name": "group-name", "Values": ["ray-vast-rvtest-*"]}]
        left += [f"security group {g['GroupId']} ({g['GroupName']}, {region})" for g in ec2.describe_security_groups(Filters=name_filter)["SecurityGroups"]
                 if ours(g["GroupName"])]
        left += [f"key pair {k['KeyName']} ({region})" for k in ec2.describe_key_pairs(Filters=[{"Name": "key-name", "Values": ["ray-vast-rvtest-*"]}])["KeyPairs"]
                 if ours(k["KeyName"])]
    iam = session.client("iam")
    left += [f"role {r['RoleName']}" for page in iam.get_paginator("list_roles").paginate() for r in page["Roles"] if ours(r["RoleName"])]
    left += [f"instance profile {p['InstanceProfileName']}" for page in iam.get_paginator("list_instance_profiles").paginate()
             for p in page["InstanceProfiles"] if ours(p["InstanceProfileName"])]
    return left


def sweep_label(labels, prefixes=(), ids=None):
    """Destroy every vast instance whose label is one of `labels` (exactly), or starts with one of `prefixes`; list again
    until 0. Returns the log lines. A prefix must be the suite's test label `ray-vast-test-<yyyymmdd>`, which only the
    suite uses: a cluster label is never a prefix, so a user's cluster with a longer name is never destroyed. `ids`: only
    the instances with these IDs (for a cluster whose name is not a suite name: the IDs that the run saw)."""
    bad = [p for p in prefixes if not re.fullmatch(r"ray-vast-test-\d{8}", p)]
    if bad:
        raise ValueError(f"a sweep prefix must be the suite's test label ray-vast-test-<yyyymmdd>, not {bad}")
    client, lines = vast(), []
    for _ in range(60):
        found = [i for i in client.list_instances() if isinstance(i.get("label"), str) and
                 (i["label"] in labels or i["label"].startswith(tuple(prefixes))) and (ids is None or i["id"] in ids)]
        lines.append(f"{time.strftime('%FT%TZ', time.gmtime())} {len(found)} left")
        if not found:
            return lines
        for i in found:
            client.destroy_instance(i["id"])
            lines.append(f"destroyed {i['id']} ({i['label']})")
        time.sleep(5)
    raise RuntimeError("sweep: instances still listed after 60 rounds")
