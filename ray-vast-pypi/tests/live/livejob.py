"""The jobs of the live suite: the only module that submits, waits for, stops and reads a Ray job (unit-tested in
tests/unit/test_live_job.py). `run()` takes the job's files (or a working dir), the entrypoint, the env vars, the secrets
to redact, a timeout and the result prefix; it returns a `Job`: the status, the parsed result line, the log, and the
handle to wait for or stop the job.

The result line of every job script: `<PREFIX> <id> <word> <json>`, the last one counts. The prefix is `<FAMILY>_RESULT`
(C9_RESULT, C10_RESULT, LB_RESULT), and the caller names it. A data line (one or more rows): `<PREFIX> <json>`
(`Job.lines`)."""
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

BIN = Path(sys.executable).parent
ENDED = ("SUCCEEDED", "FAILED", "STOPPED")
NO_RESULT, BAD_RESULT, SUBMIT_FAILED = "NO_RESULT", "BAD_RESULT", "SUBMIT_FAILED"


class RayCli:
    """The `ray job` adapter: the Ray CLI of this venv, against `env["RAY_ADDRESS"]` (a dashboard tunnel)."""

    STATUS = ("import sys; from ray.job_submission import JobSubmissionClient; "
              "print('STATUS', JobSubmissionClient(sys.argv[1]).get_job_status(sys.argv[2]).value)")

    def submit(self, env, command):
        return subprocess.run([str(BIN / "ray"), "job", *command], env=env, capture_output=True, text=True, timeout=600)

    def status(self, env, sid):
        try:
            done = subprocess.run([sys.executable, "-c", self.STATUS, env["RAY_ADDRESS"], sid], env=env, capture_output=True,
                                  text=True, timeout=120)
        except subprocess.TimeoutExpired:
            return "UNKNOWN"
        found = [line.split()[1] for line in done.stdout.splitlines() if line.startswith("STATUS ")]
        return found[-1] if found else "UNKNOWN"

    def logs(self, env, sid):
        done = subprocess.run([str(BIN / "ray"), "job", "logs", "--address", env["RAY_ADDRESS"], sid], env=env, capture_output=True,
                              text=True, timeout=300)
        return done.stdout + done.stderr

    def stop(self, env, sid):
        return subprocess.run([str(BIN / "ray"), "job", "stop", "--address", env["RAY_ADDRESS"], sid], env=env, capture_output=True,
                              text=True, timeout=120)


def workdir(*files):
    """A new temp working directory for `ray job submit` with a copy of these files."""
    d = Path(tempfile.mkdtemp(prefix="rvjob-"))
    for f in files:
        shutil.copy(f, d / Path(f).name)
    return d


class Job:
    """One submitted job, the handle to wait for or stop it. `status`, `word`, `data` and `log` are set by `wait()`.
    Each text that leaves the Job (the log, its file, the result, the stop output) has the secrets as `<redacted>`."""

    def __init__(self, env, ray, sid, out_dir, name, prefix, secrets, deadline, poll_s):
        self.env, self.ray, self.sid, self.out_dir, self.name, self.prefix = env, ray, sid, out_dir, name, prefix
        self.secrets, self.deadline, self.poll_s = [s for s in secrets if s], deadline, poll_s
        self.status, self.word, self.data, self.log = None, None, {}, ""

    def redact(self, text):
        for secret in self.secrets:
            text = text.replace(secret, "<redacted>")
        return text

    def status_now(self):
        """The status now (PENDING, RUNNING, SUCCEEDED, FAILED, STOPPED, or UNKNOWN when the dashboard does not answer)."""
        return self.ray.status(self.env, self.sid)

    def logs(self):
        """The log now (redacted)."""
        return self.redact(self.ray.logs(self.env, self.sid))

    def stop(self):
        """`ray job stop`. Returns its CompletedProcess (output redacted)."""
        done = self.ray.stop(self.env, self.sid)
        return subprocess.CompletedProcess(done.args, done.returncode, self.redact(done.stdout or ""), self.redact(done.stderr or ""))

    def wait(self, timeout=None):
        """Wait for the end: until the job's own timeout, or `timeout` s from now. A job that still runs then is stopped, and
        its status is TIMEOUT(<status after the stop>). Then the log goes to `<out_dir>/<name>.log` and the result line is
        parsed. Returns the Job."""
        deadline = self.deadline if timeout is None else time.time() + timeout
        status = self.status_now()
        while status not in ENDED and time.time() < deadline:
            time.sleep(self.poll_s)
            status = self.status_now()
        if status not in ENDED:
            self.stop()
            status = f"TIMEOUT({self.status_now()})"
        self.status = status
        self._ended(self.logs())
        return self

    def lines(self, prefix):
        """The JSON of each data line `<prefix> <json>` of the log."""
        return [json.loads(x.split(" ", 1)[1]) for x in self.log.splitlines() if x.startswith(prefix + " ")]

    def _ended(self, log):
        self.log = log
        (self.out_dir / f"{self.name}.log").write_text(log)
        if not self.prefix:
            return
        found = [x for x in log.splitlines() if x.startswith(self.prefix + " ")]
        if not found:
            self.word, self.data = NO_RESULT, {"job_status": self.status, "log_tail": log.splitlines()[-10:]}
            return
        try:
            _, _, word, payload = found[-1].split(" ", 3)
            self.word, self.data = word, json.loads(payload)
        except ValueError as e:  # a short line, or JSON cut off (a job killed while it printed)
            self.word, self.data = BAD_RESULT, {"line": found[-1][:300], "error": str(e)[:200]}


def run(env, entrypoint, *, out_dir, name="job", files=(), working_dir=None, env_vars=None, runtime_env=None, secrets=(),
        sink=None, prefix=None, timeout=1800, wait=True, ray=None, poll_s=10):
    """Submit one job and (with `wait`) wait for its end; stop it at `timeout` s. Returns the Job.
    `env`: the env of a dashboard tunnel (Cluster.tunnel). `files` go into a new working dir; or `working_dir` is used as
    it is; neither: no working dir. `env_vars` join `runtime_env["env_vars"]`. `secrets` (for example an S3 secret key in
    `env_vars`) go to `sink` (the run's secret scan) and are redacted in every file and text of the job. `prefix`: the
    result line to parse (`Job.word`, `Job.data`). With wait=False the caller calls `Job.wait()` (or `Job.stop()`)."""
    ray = ray or RayCli()
    out_dir = Path(out_dir)
    for secret in secrets:
        if sink and secret:
            sink(secret)
    sid = f"{name.replace('.', '-').replace('[', '-').replace(']', '').lower()}-{os.urandom(3).hex()}"
    if files:
        working_dir = workdir(*files)
    if env_vars:
        runtime_env = dict(runtime_env or {}, env_vars={**(runtime_env or {}).get("env_vars", {}), **env_vars})
    command = ["submit", "--address", env["RAY_ADDRESS"], "--submission-id", sid, "--no-wait"]
    if working_dir:
        command += ["--working-dir", str(working_dir)]
    if runtime_env:
        command += ["--runtime-env-json", json.dumps(runtime_env)]
    command += ["--", *entrypoint]
    job = Job(env, ray, sid, out_dir, name, prefix, secrets, time.time() + timeout, poll_s)
    (out_dir / f"{name}_submit.txt").write_text(job.redact("ray job " + shlex.join(command)) + "\n")
    done = ray.submit(env, command)
    if done.returncode != 0:
        job.status = SUBMIT_FAILED
        job._ended(job.redact((done.stdout or "") + (done.stderr or "")))
        return job
    return job.wait() if wait else job
