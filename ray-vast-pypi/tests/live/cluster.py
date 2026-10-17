"""A live cluster for the suite: the `ray-vast` CLI in a private HOME, SSH to the head, a tunnel, jobs, and the AWS and vast
checks. Every command output goes to `<dir>/cli.log`. Secrets (the cluster token, join tokens) stay in memory."""
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from contextlib import contextmanager
from pathlib import Path

import boto3
import yaml

from ray_vast import commands
from ray_vast.vast_api import VastClient, label

import livejob
import livekit
import window

BIN = Path(sys.executable).parent
RAY_VAST = [str(BIN / "ray-vast")]
REPO = Path(__file__).resolve().parents[3]
MANUAL = REPO / "tests" / "manual"
JOBS = Path(__file__).parent / "jobs"
ADMIN_PROFILE = os.environ.get("RAYVAST_LIVE_PROFILE", "personal")
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
    """`run` is the livekit.Run of the suite: the cluster joins it here, once. `kept` (--keep-cluster): the run does not
    end the cluster."""

    def __init__(self, run, fixture, config, out_dir, home, kept=False, profile=ADMIN_PROFILE, credentials_file=None):
        self.fixture = fixture
        self.kept = kept
        self.name = config["cluster_name"]
        livekit.assert_ours(f"ray-vast-{self.name}")
        self.config = config
        self.dir = Path(out_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.home = Path(home)
        (self.home / ".ray-vast").mkdir(parents=True, exist_ok=True)
        self.yaml = self.dir / "cluster.yaml"
        self.yaml.write_text(yaml.safe_dump(config, sort_keys=False))
        self.profile = profile
        self.region = config["available_node_types"][config["head_node_type"]]["node_config"]["region"]
        aws_dir = Path.home() / ".aws"
        self.env = {**os.environ, "HOME": str(self.home), "VAST_API_KEY": vast_key(), "PYTHONUNBUFFERED": "1",
                    "AWS_CONFIG_FILE": os.environ.get("AWS_CONFIG_FILE", str(aws_dir / "config")),
                    "AWS_SHARED_CREDENTIALS_FILE": str(credentials_file or aws_dir / "credentials"),
                    "PATH": f"{BIN}:{os.environ['PATH']}"}
        self.env.pop("AWS_PROFILE", None)
        self.secrets = []  # the cluster token and the join tokens, for the secret scan; never written
        self.records = [self.dir]  # the directories that livekit.end_cluster scans; the record fixture adds each scenario's
        self.up_output = self.last_down = None
        self.timeline = []
        self._poller = None
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
        done = self.cli("up", self.yaml, "--profile", self.profile)
        self.up_output = {"returncode": done.returncode, "stdout": done.stdout, "stderr": done.stderr, "wall_s": round(time.time() - started, 1)}
        if done.returncode != 0:
            raise RuntimeError(f"ray-vast up {self.name} failed (exit {done.returncode}): {(done.stderr or done.stdout)[-800:]}")
        self.collect_token()
        return self

    def down(self, *extra, env=None):
        """`down`; without a state file (an earlier `down` deleted it) `down` needs `--profile` (cli.md), so it gets the
        cluster's profile. The result is `last_down` (livekit.end_cluster runs no second `down` after a clean one)."""
        if not self.state and "--profile" not in extra:
            extra = (*extra, "--profile", self.profile)
        self.last_down = self.cli("down", self.yaml, *extra, env=env)
        return self.last_down

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
        """One Ray job on this cluster through the tunnel `env` (livejob.run: its files or working dir, env vars, secrets,
        result prefix, timeout, wait). The submit command and the log go to `out_dir` (default: the cluster's directory);
        the job's secrets join the run's secret scan. Returns the livejob.Job."""
        return livejob.run(env, entrypoint, out_dir=out_dir or self.dir, sink=self.run.secret, **options)

    def probe(self, env, out_dir, name="probe", check=True):
        """jobs/node_probe.py on every worker: its NODE_PROBE rows, each with the `machine_id` of its vast instance. A probe
        job that does not succeed raises RuntimeError (check=False: the rows that it printed)."""
        job = self.job(env, ["python", "node_probe.py"], out_dir, name=name, timeout=900,
                       files=[JOBS / "node_probe.py", MANUAL / "c10" / "jobs" / "c10common.py"])
        if check and job.status != "SUCCEEDED":
            raise RuntimeError(f"node probe {name}: {job.status}")
        machines = {str(i["id"]): i.get("machine_id") for i in self.instances()}
        return [dict(row, machine_id=machines.get(str(row.get("container_id")))) for row in job.lines("NODE_PROBE")]

    # --- vast --------------------------------------------------------------------------------------------------------
    def start_poller(self, period=10):
        """Every `period` s: the cluster's vast instances into `vast_timeline.jsonl` (no env), and their join tokens into
        memory for the secret scan."""
        stop = threading.Event()

        def run():
            with open(self.dir / "vast_timeline.jsonl", "a") as f:
                while not stop.is_set():
                    row = {"t": round(time.time(), 1)}
                    try:
                        found = [i for i in vast().list_instances() if i.get("label") == label(self.name)]
                        for i in found:
                            env = i.get("extra_env") or []
                            pairs = env.items() if isinstance(env, dict) else [p for p in env if isinstance(p, (list, tuple)) and len(p) == 2]
                            token = next((v for k, v in pairs if k == "RAYVAST_JOIN_TOKEN"), None)
                            if token and token not in self.secrets:
                                self.secrets.append(token)
                        row["instances"] = [{k: i.get(k) for k in ("id", "actual_status", "dph_total", "machine_id", "public_ipaddr",
                                                                   "start_date", "cpu_cores_effective", "gpu_name")} for i in found]
                    except Exception as e:  # one bad sample must not end the timeline
                        row["error"] = type(e).__name__
                    self.timeline.append(row)
                    f.write(json.dumps(row) + "\n")
                    f.flush()
                    stop.wait(period)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        self._poller = (stop, thread)

    def stop_poller(self):
        if self._poller:
            self._poller[0].set()
            self._poller[1].join(timeout=60)
            self._poller = None

    def instances(self):
        return [i for i in vast().list_instances() if i.get("label") == label(self.name)]

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
    is made, and livekit.end_cluster ends it. `up()` is the tutorial up to `ray-vast up`; `shell(script)` runs a script in
    the tutorial's directory (`work`) with the tutorial's env (`env`)."""

    def __init__(self, run, out_dir, home, region, version):
        self.fixture, self.name, self.region, self.version, self.kept = "release", "gpu-24gb", region, version, False
        self.dir, self.home = Path(out_dir), Path(home)
        self.work = self.home / "work"
        self.work.mkdir(parents=True, exist_ok=True)
        self.yaml = self.work / "cluster.yaml"
        self.config = {}  # the packaged YAML, after the `cp` of `up`
        self.env = {**os.environ, "HOME": str(self.home), "VAST_API_KEY": vast_key(), "AWS_PROFILE": ADMIN_PROFILE,
                    "AWS_CONFIG_FILE": str(Path("~/.aws/config").expanduser()),
                    "AWS_SHARED_CREDENTIALS_FILE": str(Path("~/.aws/credentials").expanduser())}
        self.env.pop("VIRTUAL_ENV", None)
        self.records, self.secrets, self.timeline, self._poller = [self.dir], [], [], None
        self.up_output = self.last_down = self.started = None
        run.register(self)

    # The vast poller and the instance list of Cluster: they use only name, dir, secrets, timeline and _poller.
    start_poller, stop_poller, instances = Cluster.start_poller, Cluster.stop_poller, Cluster.instances

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
        done = self.shell(f"source .venv/bin/activate && ray-vast up cluster.yaml --profile {ADMIN_PROFILE}", timeout=2400)
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
    livekit.end_cluster left. A suite run in parallel on the same account would show here too."""
    session = boto3.Session(profile_name=profile)
    kept = [f"ray-vast-{name}" for name in skip]

    def ours(name):
        return name.startswith("ray-vast-rvtest-") and not any(name == k or name.startswith(k + "-") for k in kept)

    left = []
    for region in regions:
        ec2 = session.client("ec2", region_name=region)
        tag = [{"Name": "tag-key", "Values": [commands.TAG_KEY]}]
        for r in ec2.describe_instances(Filters=tag + [{"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped",
                                                                                                    "shutting-down"]}])["Reservations"]:
            for i in r["Instances"]:
                cluster = next((t["Value"] for t in i.get("Tags", []) if t["Key"] == commands.TAG_KEY), "")
                if ours(f"ray-vast-{cluster}"):
                    left.append(f"EC2 instance {i['InstanceId']} ({cluster}, {region})")
        for v in ec2.describe_volumes(Filters=tag)["Volumes"]:
            cluster = next((t["Value"] for t in v.get("Tags", []) if t["Key"] == commands.TAG_KEY), "")
            if ours(f"ray-vast-{cluster}"):
                left.append(f"volume {v['VolumeId']} ({cluster}, {region})")
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


def sweep_label(prefixes):
    """Destroy every vast instance whose label starts with one of `prefixes`; list again until 0. Returns the log lines."""
    client, lines = vast(), []
    for _ in range(60):
        found = [i for i in client.list_instances() if isinstance(i.get("label"), str) and i["label"].startswith(tuple(prefixes))]
        lines.append(f"{time.strftime('%FT%TZ', time.gmtime())} {len(found)} left")
        if not found:
            return lines
        for i in found:
            client.destroy_instance(i["id"])
            lines.append(f"destroyed {i['id']} ({i['label']})")
        time.sleep(5)
    raise RuntimeError("sweep: instances still listed after 60 rounds")
