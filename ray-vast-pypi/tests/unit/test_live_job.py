"""The job module of the live suite (`tests/live/livejob.py`): submit, wait, stop, the result line and the redaction,
through its interface, with a fake `ray job` adapter in place of the Ray CLI."""
import json
import subprocess
import sys
from pathlib import Path

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
