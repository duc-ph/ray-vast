"""The job module of the live suite (`tests/live/livejob.py`): submit, wait, stop, the result line and the redaction,
through its interface, with a fake `ray job` adapter in place of the Ray CLI."""
import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "live"))  # the live suite imports its modules by their own names
import livejob  # noqa: E402

ENV = {"RAY_ADDRESS": "http://127.0.0.1:1"}


class FakeRay:
    """The `ray job` adapter of livejob, with no cluster: each status call gives the next of `statuses` (the last one
    repeats); `stop` makes the status STOPPED. It keeps each submit command, the working dir and the runtime env file
    (its text and mode) that it saw. No wait between two status calls (`poll_s` 0)."""

    poll_s = 0

    def __init__(self, statuses=("SUCCEEDED",), log="", submit_exit=0, submit_output="", stop_output="Job stopped\n"):
        self.statuses, self.log, self.submit_exit, self.submit_output = list(statuses), log, submit_exit, submit_output
        self.stop_output, self.commands, self.stopped, self.working_dir = stop_output, [], 0, None
        self.runtime_env_file = None

    def submit(self, env, command):
        self.commands.append(command)
        if "--working-dir" in command:
            wd = Path(command[command.index("--working-dir") + 1])
            self.working_dir = {p.name: p.read_text() for p in wd.iterdir()}
        if "--runtime-env" in command:
            path = Path(command[command.index("--runtime-env") + 1])
            self.runtime_env_file = {"path": path, "text": path.read_text(), "mode": oct(path.stat().st_mode & 0o777)}
        return subprocess.CompletedProcess(command, self.submit_exit, self.submit_output, "")

    def status(self, env, sid):
        if self.stopped:
            return "STOPPED"
        return self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]

    def logs(self, env, sid):
        return self.log

    def stop(self, env, sid):
        self.stopped += 1
        return subprocess.CompletedProcess(["stop"], 0, self.stop_output, "")


def test_a_job_that_succeeds_gives_its_status_its_result_line_and_its_log(tmp_path):
    script = tmp_path / "work.py"
    script.write_text("print('hi')\n")
    ray = FakeRay(statuses=("PENDING", "RUNNING", "SUCCEEDED"),
                  log='starting\nLB_RESULT core FAIL {"n": 1}\nLB_RESULT core PASS {"n": 2, "nodes": ["a"]}\nend\n')

    job = livejob.run(ENV, ["python", "work.py"], working_dir=livejob.workdir(script), out_dir=tmp_path, name="core",
                      prefix="LB_RESULT", ray=ray)

    assert job.status == "SUCCEEDED"
    assert (job.word, job.data) == ("PASS", {"n": 2, "nodes": ["a"]})  # the last result line counts
    assert ray.working_dir == {"work.py": "print('hi')\n"}
    assert (tmp_path / "core.log").read_text() == job.log == ray.log
    assert "ray job submit" in (tmp_path / "core_submit.txt").read_text()


def test_a_failed_job_without_a_result_line_gives_no_result_with_the_status_and_the_log_tail(tmp_path):
    ray = FakeRay(statuses=("RUNNING", "FAILED"), log="".join(f"line {i}\n" for i in range(30)) + "Traceback: boom\n")

    job = livejob.run(ENV, ["python", "-c", "x"], working_dir=tmp_path, out_dir=tmp_path, name="c9_x", prefix="C9_RESULT",
                      ray=ray)

    assert job.status == "FAILED"
    assert job.word == livejob.NO_RESULT
    assert job.data == {"job_status": "FAILED", "log_tail": [f"line {i}" for i in range(21, 30)] + ["Traceback: boom"]}


def test_a_result_line_that_does_not_parse_gives_bad_result_with_the_line(tmp_path):
    for line in ('LB_RESULT tune PASS {"results": 6', "LB_RESULT tune"):
        job = livejob.run(ENV, ["python", "x.py"], out_dir=tmp_path, name="tune", prefix="LB_RESULT", ray=FakeRay(log=line + "\n"))

        assert job.status == "SUCCEEDED"
        assert job.word == livejob.BAD_RESULT
        assert job.data["line"] == line and job.data["error"]


def test_a_job_still_running_at_its_timeout_is_stopped(tmp_path):
    ray = FakeRay(statuses=("RUNNING",), log="tick\n")

    job = livejob.run(ENV, ["python", "sleeper.py"], out_dir=tmp_path, name="sleeper", timeout=0, ray=ray)

    assert ray.stopped == 1
    assert job.status == "TIMEOUT(STOPPED)"
    assert (tmp_path / "sleeper.log").read_text() == "tick\n"


def test_the_secrets_go_to_the_job_but_into_no_file_log_result_or_command_line(tmp_path):
    """The runtime env (with an S3 secret key in its env vars) goes to `ray job submit` in a file of mode 600, never on the
    command line (`ps` shows it to every user of the machine). The file is gone after the submit."""
    secret = "wJalrXUtnFEMI/K7MDENG"
    ray = FakeRay(log=f"env AWS_SECRET_ACCESS_KEY={secret}\nLB_RESULT data FAIL " + json.dumps({"error": f"denied for {secret}"}) + "\n")
    runtime_env = {"pip": ["boto3"], "env_vars": {"AWS_SECRET_ACCESS_KEY": secret, "LB_OUT": "s3://b/out"}}

    job = livejob.run(ENV, ["python", "x.py"], out_dir=tmp_path, name="data", prefix="LB_RESULT", runtime_env=runtime_env,
                      secrets=[secret], ray=ray)

    assert not [arg for arg in ray.commands[0] if secret in arg]
    assert json.loads(ray.runtime_env_file["text"]) == runtime_env and ray.runtime_env_file["mode"] == "0o600"
    assert not ray.runtime_env_file["path"].exists()
    assert not [p.name for p in tmp_path.iterdir() if secret in p.read_text()]
    submitted = (tmp_path / "data_submit.txt").read_text()
    assert "<redacted>" in submitted and "s3://b/out" in submitted
    assert secret not in job.log and job.data == {"error": "denied for <redacted>"}
    assert secret not in job.stop().stdout + job.logs()


def test_an_aws_access_key_id_is_redacted_in_every_file_and_text_of_the_job_like_a_secret_key(tmp_path):
    """The runtime env of an S3 job has the IAM test user's access key ID next to its secret key. A key ID (AKIA... for a
    user key, ASIA... for a session key) is redacted too, also when the caller did not name it as a secret."""
    key_id, session_id = "AKIA" + "EXAMPLEKEYID0001", "ASIA" + "EXAMPLEKEYID0002"
    ray = FakeRay(log=f"AWS_ACCESS_KEY_ID={key_id}\nLB_RESULT data FAIL " + json.dumps({"error": f"denied for {session_id}"}) + "\n")

    job = livejob.run(ENV, ["python", "x.py"], out_dir=tmp_path, name="data", prefix="LB_RESULT",
                      runtime_env={"env_vars": {"AWS_ACCESS_KEY_ID": key_id, "LB_OUT": "s3://b/out"}}, ray=ray)

    assert json.loads(ray.runtime_env_file["text"])["env_vars"]["AWS_ACCESS_KEY_ID"] == key_id  # the job gets it
    assert not [p.name for p in tmp_path.iterdir() if key_id in p.read_text() or session_id in p.read_text()]
    assert "AWS_ACCESS_KEY_ID=<redacted>" in job.log and job.data == {"error": "denied for <redacted>"}
    assert "<redacted>" in (tmp_path / "data_submit.txt").read_text()


def test_a_runtime_env_that_cannot_be_written_leaves_no_file_with_its_secrets(tmp_path, monkeypatch):
    """A runtime env with a value that JSON cannot hold fails the run, and its half-written file of mode 600 is gone."""
    monkeypatch.setattr(livejob.tempfile, "tempdir", str(tmp_path))
    runtime_env = {"env_vars": {"AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG", "BAD": object()}}

    with pytest.raises(TypeError):
        livejob.run(ENV, ["python", "x.py"], out_dir=tmp_path / "out", name="data", prefix="LB_RESULT",
                    runtime_env=runtime_env, ray=FakeRay())

    assert not list(tmp_path.glob("rvjob-env-*"))


def test_a_job_without_wait_is_a_handle_that_the_caller_stops_then_waits_for(tmp_path):
    ray = FakeRay(statuses=("RUNNING",), log='HOLD_READY 1.0\nNODE_PROBE {"node_type": "gpu", "ports": 14}\nNODE_PROBE {"node_type": "gpu", "ports": 20}\n')

    job = livejob.run(ENV, ["python", "hold.py", "4"], out_dir=tmp_path, name="hold", wait=False, ray=ray)
    assert job.status is None and job.status_now() == "RUNNING" and "HOLD_READY" in job.logs() and ray.stopped == 0

    assert job.stop().returncode == 0
    assert job.wait(timeout=0).status == "STOPPED"
    assert job.lines("NODE_PROBE") == [{"node_type": "gpu", "ports": 14}, {"node_type": "gpu", "ports": 20}]


def test_a_submit_that_fails_gives_submit_failed_and_the_redacted_output(tmp_path):
    ray = FakeRay(submit_exit=1, submit_output="error: bad runtime env {'AWS_SECRET_ACCESS_KEY': 'sekrit'}\n")

    job = livejob.run(ENV, ["python", "x.py"], out_dir=tmp_path, name="x", prefix="LB_RESULT", secrets=["sekrit"], ray=ray)

    assert job.status == livejob.SUBMIT_FAILED and job.word == livejob.NO_RESULT
    assert "bad runtime env" in job.log and "sekrit" not in job.log
    assert (tmp_path / "x.log").read_text() == job.log


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


WARNING = "WARNING worker 1 (instance 7, ip 192.0.2.1) is at its process limit: 0 refused forks/threads (+0), worker ports in use 58 of 58"


def test_a_many_run_proves_its_hang_only_with_the_warning_and_2_minutes_without_progress():
    """The early end of a "many" run (LB11): the head's process-limit WARNING for the node has appeared AND the job's
    progress did not change for 120 s. A change of the progress starts the 120 s again; a progress sample that fails or
    is unknown (None) is no proof of a hang (it starts the 120 s again too); without the WARNING the run never ends early."""
    clock, warning, progress = FakeClock(), [None], [(("f", "FINISHED", 3),)]

    def sample():
        if progress[0] == "error":
            raise OSError("the dashboard did not answer")
        return progress[0]

    rule = livejob.HangRule(lambda: warning[0], sample, clock=clock)
    assert rule() is None
    clock.sleep(300)
    assert rule() is None  # no progress for 300 s, but no WARNING: the 600 s guard of the job decides
    warning[0] = WARNING
    progress[0] = (("f", "FINISHED", 4),)
    assert rule() is None  # progress now
    clock.sleep(119)
    assert rule() is None
    progress[0] = "error"
    clock.sleep(1)
    assert rule() is None  # a failed sample: no proof
    progress[0] = (("f", "FINISHED", 4),)
    clock.sleep(60)
    progress[0] = None
    assert rule() is None  # an unknown sample (a cut summary): no proof
    progress[0] = (("f", "FINISHED", 4),)
    clock.sleep(119)
    assert rule() is None  # 119 s since the unknown sample
    clock.sleep(1)
    proof = rule()
    assert proof == {"warning": WARNING, "quiet_s": 120.0, "progress": [["f", "FINISHED", 4]]}


def test_a_job_whose_hang_is_proven_is_stopped_at_once_and_ends_hung_with_the_proof(tmp_path):
    proof = {"warning": WARNING, "quiet_s": 120.0, "progress": []}
    answers = iter([None, None, proof])
    ray = FakeRay(statuses=("RUNNING",), log="C10_PARAMS {'trials': 62}\n")

    job = livejob.run(ENV, ["python", "c10_tune.py", "many"], out_dir=tmp_path, name="tune.many", prefix="C10_RESULT",
                      wait=False, ray=ray).wait(hung=lambda: next(answers))

    assert ray.stopped == 1
    assert job.status == "HUNG(STOPPED)"
    assert (job.word, job.data) == ("HUNG", {"stopped_early": proof})
    assert (tmp_path / "tune.many.log").read_text() == ray.log


def test_a_job_that_ends_by_itself_is_not_stopped_by_the_hang_rule(tmp_path):
    ray = FakeRay(statuses=("RUNNING", "SUCCEEDED"), log='C10_RESULT C10.core COMPLETED {"done": 62}\n')
    calls = []

    job = livejob.run(ENV, ["python", "c10_core.py", "many"], out_dir=tmp_path, name="core.many", prefix="C10_RESULT",
                      wait=False, ray=ray).wait(hung=lambda: calls.append(1))

    assert ray.stopped == 0 and job.status == "SUCCEEDED" and (job.word, job.data) == ("COMPLETED", {"done": 62})
    assert calls == [1]  # asked only while the job ran


def _summary(rows, total=None, kept=None):
    """The answer of the dashboard's `/api/v0/tasks/summarize` (Ray 2.59, read from a local Ray), with these rows."""
    n = sum(sum(r[2].values()) for r in rows)
    return {"result": True, "msg": "", "data": {"result": {
        "total": n if total is None else total, "num_after_truncation": n if kept is None else kept, "num_filtered": n,
        "result": {"node_id_to_summary": {"cluster": {"summary": {
            name: {"func_or_class_name": name, "type": kind, "state_counts": counts} for name, kind, counts in rows},
            "summary_by": "func_name"}}}}}}


def test_the_progress_of_a_job_counts_its_ended_tasks_and_actor_starts_but_not_its_actor_calls():
    """Serve's health checks are actor calls: they end every few seconds also while the job hangs. So only the ended
    tasks and actor creations count. A cut summary (more tasks than the API gives) is unknown (None)."""
    rows = [("evaluate", "NORMAL_TASK", {"FINISHED": 5, "PENDING_NODE_ASSIGNMENT": 3}),
            ("Replica.__init__", "ACTOR_CREATION_TASK", {"FINISHED": 2, "FAILED": 1, "PENDING_CREATION": 7}),
            ("Replica.check_health", "ACTOR_TASK", {"FINISHED": 40})]
    before = livejob.task_progress(_summary(rows))
    rows[2][2]["FINISHED"] = 41
    rows[0][2]["PENDING_NODE_ASSIGNMENT"] = 4

    assert before == (("Replica.__init__", "FAILED", 1), ("Replica.__init__", "FINISHED", 2), ("evaluate", "FINISHED", 5))
    assert livejob.task_progress(_summary(rows)) == before  # a health check and a new pending task are no progress
    rows[0][2]["FINISHED"] = 6
    assert livejob.task_progress(_summary(rows)) != before
    assert livejob.task_progress(_summary(rows, total=20000, kept=10000)) is None
