"""The jobs of the live suite: the only module that submits, waits for, stops and reads a Ray job (unit-tested in
tests/test_live_suite_jobs.py). `run()` takes the entrypoint, the job's working dir (`workdir(*files)` makes one from
files), the runtime env (its env vars inside it), the secrets to redact, a timeout and the result prefix; it returns a
`Job`: the status, the parsed result line, the log, and the handle to wait for or stop the job.

The result line of every job script: `<PREFIX> <id> <word> <json>`, the last one counts. The prefix is `JOB_RESULT` for
the scripts in jobs/, and the caller names it. A data line (one or more rows): `<PREFIX> <json>`
(`Job.lines`)."""
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

from liverecords import AWS_KEY_ID
from liverun import wait_until

BIN = Path(sys.executable).parent
ENDED = ("SUCCEEDED", "FAILED", "STOPPED")
NO_RESULT, BAD_RESULT, SUBMIT_FAILED = "NO_RESULT", "BAD_RESULT", "SUBMIT_FAILED"


class RayCli:
    """The `ray job` adapter: the Ray CLI of this venv, against `env["RAY_ADDRESS"]` (a dashboard tunnel). A job's status
    is asked again every `poll_s` s."""

    poll_s = 10
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

    def progress(self, env, sid):
        """`task_progress` of the job's tasks (the dashboard's task summary, filtered by the job's Ray job id), or None
        while the job has no Ray job id yet. The token of the tunnel env goes only into the request header."""
        token_path = env.get("RAY_AUTH_TOKEN_PATH")
        headers = {"Authorization": "Bearer " + Path(token_path).read_text().strip()} if token_path else {}

        def get(path):
            with urllib.request.urlopen(urllib.request.Request(env["RAY_ADDRESS"] + path, headers=headers), timeout=60) as r:
                return json.loads(r.read())
        job_id = get(f"/api/jobs/{sid}").get("job_id")
        if not job_id:
            return None
        return task_progress(get(f"/api/v0/tasks/summarize?filter_keys=job_id&filter_predicates=%3D&filter_values={job_id}"))


def task_progress(summary):
    """The progress of a job from the dashboard's task summary (`/api/v0/tasks/summarize`): the sorted (name, state,
    count) of its ended (FINISHED, FAILED) tasks and actor creations. Actor calls do not count: Serve's health checks are
    actor calls that end also while the job hangs. None (unknown) when the summary is cut (more tasks than the API gives)."""
    result = summary["data"]["result"]
    if result["num_after_truncation"] < result["total"]:
        return None
    rows = result["result"]["node_id_to_summary"]["cluster"]["summary"]
    return tuple(sorted((name, state, count) for name, row in rows.items() if row["type"] != "ACTOR_TASK"
                        for state, count in row["state_counts"].items() if state in ("FINISHED", "FAILED")))


HANG_QUIET_S = 120  # a "many" run with the WARNING and no progress for this long has proven its hang


class HangRule:
    """The early end of a "many" run of the process-limit pairs: its hang is proven when the head's process-limit WARNING for its node has
    appeared (`warning()` gives the line, or None) AND the job made no progress for `quiet_s` (`progress()` gives a value
    that changes when the job makes progress: `Job.progress`). A call samples both and gives the proof (the WARNING, the
    quiet seconds, the last progress), or None. A progress sample that fails or is None (unknown) is no proof: the quiet
    time starts again. Without the WARNING there is no proof: then the job's own guard (600 s) ends the run."""

    def __init__(self, warning, progress, quiet_s=HANG_QUIET_S, clock=time):
        self.warning, self.progress, self.quiet_s, self.clock = warning, progress, quiet_s, clock
        self.last, self.since = None, clock.time()

    def __call__(self):
        now = self.clock.time()
        try:
            value = self.progress()
        except Exception:  # noqa: BLE001  (one failed sample is no proof)
            value = None
        if value is None:  # unknown: the quiet time starts again
            self.since = now
            return None
        if value != self.last:
            self.last, self.since = value, now
            return None
        if now - self.since < self.quiet_s:
            return None
        line = self.warning()
        return {"warning": line, "quiet_s": round(now - self.since, 1), "progress": [list(x) for x in value]} if line else None


def workdir(*files):
    """A new temp working directory for `ray job submit` with a copy of these files (a directory is copied whole)."""
    d = Path(tempfile.mkdtemp(prefix="rvjob-"))
    for f in files:
        if Path(f).is_dir():
            shutil.copytree(f, d / Path(f).name)
        else:
            shutil.copy(f, d / Path(f).name)
    return d


class Job:
    """One submitted job, the handle to wait for or stop it. `status`, `word`, `data` and `log` are set by `wait()`.
    Each text that leaves the Job (the log, its file, the result, the stop output) has the secrets as `<redacted>`."""

    def __init__(self, env, ray, sid, out_dir, name, prefix, secrets, deadline):
        self.env, self.ray, self.sid, self.out_dir, self.name, self.prefix = env, ray, sid, out_dir, name, prefix
        self.secrets, self.deadline = [s for s in secrets if s], deadline
        self.status, self.word, self.data, self.log = None, None, {}, ""

    def redact(self, text):
        """The text with each secret and each AWS access key ID (also one that the caller did not name) as <redacted>."""
        for secret in self.secrets:
            text = text.replace(secret, "<redacted>")
        return re.sub(AWS_KEY_ID, "<redacted>", text)

    def status_now(self):
        """The status now (PENDING, RUNNING, SUCCEEDED, FAILED, STOPPED, or UNKNOWN when the dashboard does not answer)."""
        return self.ray.status(self.env, self.sid)

    def logs(self):
        """The log now (redacted)."""
        return self.redact(self.ray.logs(self.env, self.sid))

    def progress(self):
        """The job's progress now (`task_progress`), or None (unknown) for a job with no Ray job id yet."""
        return self.ray.progress(self.env, self.sid)

    def stop(self):
        """`ray job stop`. Returns its CompletedProcess (output redacted)."""
        done = self.ray.stop(self.env, self.sid)
        return subprocess.CompletedProcess(done.args, done.returncode, self.redact(done.stdout or ""), self.redact(done.stderr or ""))

    def wait(self, timeout=None, hung=None):
        """Wait for the end: until the job's own timeout, or `timeout` s from now. A job that still runs then is stopped, and
        its status is TIMEOUT(<status after the stop>). `hung` (a HangRule) is asked at each status poll while the job
        runs: when it gives a proof, the job is stopped at once, its status is HUNG(<status after the stop>), and its result
        is HUNG with {"stopped_early": proof}. Then the log goes to `<out_dir>/<name>.log` and the result line is parsed.
        Returns the Job."""
        left = (self.deadline if timeout is None else time.time() + timeout) - time.time()

        def look():
            status = self.status_now()
            return status, (hung() if hung and status not in ENDED else None)
        waited = wait_until(look, left, done=lambda seen: seen[0] in ENDED or seen[1] is not None, poll_s=self.ray.poll_s)
        status, proof = waited.seen or (None, None)
        if proof is not None:
            self.stop()
            status = f"HUNG({self.status_now()})"
        elif not waited:
            self.stop()
            status = f"TIMEOUT({self.status_now()})"
        self.status = status
        self._ended(self.logs())
        if proof is not None:
            self.word, self.data = "HUNG", {"stopped_early": proof}
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


def run(env, entrypoint, *, out_dir, name="job", working_dir=None, runtime_env=None, secrets=(), prefix=None, timeout=1800,
        wait=True, ray=None):
    """Submit one job and (with `wait`) wait for its end; stop it at `timeout` s. Returns the Job.
    `env`: the env of a dashboard tunnel (Cluster.tunnel). `working_dir`: the job's files (`workdir(*files)`), or none.
    `runtime_env`: the job's runtime env, with its env vars in `env_vars`. It goes to `ray job submit` in a temp file of
    mode 600 (`--runtime-env`), never on the command line (`ps` shows a command line to every user); the file is deleted
    after the submit. `secrets` (for example an S3 secret key in the env vars) are redacted in every file and text of the
    job. `prefix`: the result line to parse (`Job.word`, `Job.data`). With wait=False the caller calls `Job.wait()` (or
    `Job.stop()`). `ray`: the `ray job` adapter (RayCli)."""
    ray = ray or RayCli()
    out_dir = Path(out_dir)
    sid = f"{name.replace('.', '-').replace('[', '-').replace(']', '').lower()}-{os.urandom(3).hex()}"
    command = ["submit", "--address", env["RAY_ADDRESS"], "--submission-id", sid, "--no-wait"]
    if working_dir:
        command += ["--working-dir", str(working_dir)]
    env_file = None
    try:  # the env file holds secrets: it goes also when a write fails
        if runtime_env:
            fd, env_file = tempfile.mkstemp(prefix="rvjob-env-", suffix=".yaml")  # mode 600; JSON is YAML
            with os.fdopen(fd, "w") as f:
                json.dump(runtime_env, f)
            command += ["--runtime-env", env_file]
        command += ["--", *entrypoint]
        job = Job(env, ray, sid, out_dir, name, prefix, secrets, time.time() + timeout)
        submitted = f"ray job {shlex.join(command)}\n" + (f"runtime env: {json.dumps(runtime_env)}\n" if runtime_env else "")
        (out_dir / f"{name}_submit.txt").write_text(job.redact(submitted))
        done = ray.submit(env, command)
    finally:
        if env_file:
            os.unlink(env_file)
    if done.returncode != 0:
        job.status = SUBMIT_FAILED
        job._ended(job.redact((done.stdout or "") + (done.stderr or "")))
        return job
    return job.wait() if wait else job
