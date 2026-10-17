"""A live cluster for the suite: the `ray-vast` CLI in a private HOME, SSH to the head, a tunnel, jobs, and the AWS and vast
checks. Every command output goes to `<dir>/cli.log`. Secrets (the cluster token, join tokens) stay in memory."""
import json
import os
import secrets
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

import livekit

BIN = Path(sys.executable).parent
RAY_VAST = [str(BIN / "ray-vast")]
REPO = Path(__file__).resolve().parents[3]
MANUAL = REPO / "tests" / "manual"
ADMIN_PROFILE = os.environ.get("RAYVAST_LIVE_PROFILE", "personal")


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
    def __init__(self, fixture, config, out_dir, home, profile=ADMIN_PROFILE, credentials_file=None):
        self.fixture = fixture
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
        if os.environ.get("RAYVAST_LIVE_RUN") and os.environ.get("RAYVAST_LIVE_RESULTS"):  # in a run: the end-of-run check finds it
            livekit.note_opened(Path(os.environ["RAYVAST_LIVE_RESULTS"]) / "_session" / "runs" / os.environ["RAYVAST_LIVE_RUN"],
                                {"name": self.name, "region": self.region, "kept": False, "yaml": str(self.yaml), "home": str(self.home)})
        aws_dir = Path.home() / ".aws"
        self.env = {**os.environ, "HOME": str(self.home), "VAST_API_KEY": vast_key(), "PYTHONUNBUFFERED": "1",
                    "AWS_CONFIG_FILE": os.environ.get("AWS_CONFIG_FILE", str(aws_dir / "config")),
                    "AWS_SHARED_CREDENTIALS_FILE": str(credentials_file or aws_dir / "credentials"),
                    "PATH": f"{BIN}:{os.environ['PATH']}"}
        self.env.pop("AWS_PROFILE", None)
        self.secrets = []  # the cluster token and the join tokens, for the secret scan; never written
        self.up_output = None
        self.kept = False  # --keep-cluster: the run does not end it (conftest.group_lifetime)
        self.timeline = []
        self._poller = None

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
        done = self.cli("up", self.yaml, "--profile", self.profile)
        self.up_output = {"returncode": done.returncode, "stdout": done.stdout, "stderr": done.stderr, "wall_s": round(time.time() - started, 1)}
        if done.returncode != 0:
            raise RuntimeError(f"ray-vast up {self.name} failed (exit {done.returncode}): {(done.stderr or done.stdout)[-800:]}")
        self.collect_token()
        return self

    def down(self, *extra, env=None):
        return self.cli("down", self.yaml, *extra, env=env)

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

    def monitor_log(self, start_line=1):
        return self.head_exec(f"tail -n +{start_line} /tmp/ray/session_latest/logs/monitor.log", timeout=120).splitlines()

    def log_epoch(self, line):
        """The epoch of a monitor.log line, in the time zone of the head container (livekit.log_epoch)."""
        if getattr(self, "_tz", None) is None:
            self._tz = self.head_exec("date +%z", timeout=120).strip()
        return livekit.log_epoch(line, self._tz)

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

    def job(self, env, working_dir, entrypoint, runtime_env=None, timeout=1800, out_dir=None, name="job", no_wait=False):
        """`ray job submit --working-dir` (the normal workflow), wait for the end, stop it at the timeout. Returns
        (status, log). The submit command and the log go to `out_dir`."""
        out_dir = Path(out_dir or self.dir)
        sid = f"{name.replace('.', '-').replace('[', '-').replace(']', '').lower()}-{secrets.token_hex(3)}"
        command = ["submit", "--address", env["RAY_ADDRESS"], "--submission-id", sid, "--no-wait"]
        if working_dir:
            command += ["--working-dir", str(working_dir)]
        if runtime_env:
            command += ["--runtime-env-json", json.dumps(runtime_env)]
        command += ["--", *entrypoint]
        (out_dir / f"{name}_submit.txt").write_text("ray job " + " ".join(command) + "\n")
        done = subprocess.run([str(BIN / "ray"), "job", *command], env=env, capture_output=True, text=True, timeout=600)
        if done.returncode != 0:
            (out_dir / f"{name}.log").write_text(done.stdout + done.stderr)
            return "SUBMIT_FAILED", done.stdout + done.stderr
        if no_wait:
            return sid, ""
        status = self.job_wait(env, sid, timeout)
        log = self.job_logs(env, sid)
        (out_dir / f"{name}.log").write_text(log)
        return status, log

    def job_status(self, env, sid):
        script = ("import sys; from ray.job_submission import JobSubmissionClient; "
                  "print('STATUS', JobSubmissionClient(sys.argv[1]).get_job_status(sys.argv[2]).value)")
        try:
            done = subprocess.run([sys.executable, "-c", script, env["RAY_ADDRESS"], sid], env=env, capture_output=True, text=True,
                                  timeout=120)
        except subprocess.TimeoutExpired:
            return "UNKNOWN"
        found = [line.split()[1] for line in done.stdout.splitlines() if line.startswith("STATUS ")]
        return found[-1] if found else "UNKNOWN"

    def job_wait(self, env, sid, timeout):
        deadline = time.time() + timeout
        status = self.job_status(env, sid)
        while status not in ("SUCCEEDED", "FAILED", "STOPPED") and time.time() < deadline:
            time.sleep(10)
            status = self.job_status(env, sid)
        if status not in ("SUCCEEDED", "FAILED", "STOPPED"):
            subprocess.run([str(BIN / "ray"), "job", "stop", "--address", env["RAY_ADDRESS"], sid], env=env, capture_output=True, timeout=120)
            status = f"TIMEOUT({self.job_status(env, sid)})"
        return status

    def job_logs(self, env, sid):
        done = subprocess.run([str(BIN / "ray"), "job", "logs", "--address", env["RAY_ADDRESS"], sid], env=env,
                              capture_output=True, text=True, timeout=300)
        return done.stdout + done.stderr

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
