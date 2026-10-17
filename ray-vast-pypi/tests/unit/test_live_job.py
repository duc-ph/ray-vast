"""The job module of the live suite (`tests/live/livejob.py`): submit, wait, stop, the result line and the redaction,
through its interface, with a fake `ray job` adapter in place of the Ray CLI."""
import json
import subprocess
from pathlib import Path

from live import livejob

ENV = {"RAY_ADDRESS": "http://127.0.0.1:1"}


class FakeRay:
    """The `ray job` adapter of livejob, with no cluster: each status call gives the next of `statuses` (the last one
    repeats); `stop` makes the status STOPPED. It keeps each submit command and the working dir that it saw."""

    def __init__(self, statuses=("SUCCEEDED",), log="", submit_exit=0, submit_output="", stop_output="Job stopped\n"):
        self.statuses, self.log, self.submit_exit, self.submit_output = list(statuses), log, submit_exit, submit_output
        self.stop_output, self.commands, self.stopped, self.working_dir = stop_output, [], 0, None

    def submit(self, env, command):
        self.commands.append(command)
        if "--working-dir" in command:
            wd = Path(command[command.index("--working-dir") + 1])
            self.working_dir = {p.name: p.read_text() for p in wd.iterdir()}
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

    job = livejob.run(ENV, ["python", "work.py"], files=[script], out_dir=tmp_path, name="core", prefix="LB_RESULT",
                      ray=ray, poll_s=0)

    assert job.status == "SUCCEEDED"
    assert (job.word, job.data) == ("PASS", {"n": 2, "nodes": ["a"]})  # the last result line counts
    assert ray.working_dir == {"work.py": "print('hi')\n"}
    assert (tmp_path / "core.log").read_text() == job.log == ray.log
    assert "ray job submit" in (tmp_path / "core_submit.txt").read_text()


def test_a_failed_job_without_a_result_line_gives_no_result_with_the_status_and_the_log_tail(tmp_path):
    ray = FakeRay(statuses=("RUNNING", "FAILED"), log="".join(f"line {i}\n" for i in range(30)) + "Traceback: boom\n")

    job = livejob.run(ENV, ["python", "-c", "x"], working_dir=tmp_path, out_dir=tmp_path, name="c9_x", prefix="C9_RESULT",
                      ray=ray, poll_s=0)

    assert job.status == "FAILED"
    assert job.word == livejob.NO_RESULT
    assert job.data == {"job_status": "FAILED", "log_tail": [f"line {i}" for i in range(21, 30)] + ["Traceback: boom"]}


def test_a_result_line_that_does_not_parse_gives_bad_result_with_the_line(tmp_path):
    for line in ('LB_RESULT tune PASS {"results": 6', "LB_RESULT tune"):
        job = livejob.run(ENV, ["python", "x.py"], out_dir=tmp_path, name="tune", prefix="LB_RESULT", ray=FakeRay(log=line + "\n"),
                          poll_s=0)

        assert job.status == "SUCCEEDED"
        assert job.word == livejob.BAD_RESULT
        assert job.data["line"] == line and job.data["error"]


def test_a_job_still_running_at_its_timeout_is_stopped(tmp_path):
    ray = FakeRay(statuses=("RUNNING",), log="tick\n")

    job = livejob.run(ENV, ["python", "sleeper.py"], out_dir=tmp_path, name="sleeper", timeout=0, ray=ray, poll_s=0)

    assert ray.stopped == 1
    assert job.status == "TIMEOUT(STOPPED)"
    assert (tmp_path / "sleeper.log").read_text() == "tick\n"


def test_the_secrets_go_to_the_job_and_the_sink_but_into_no_file_log_or_result(tmp_path):
    secret = "wJalrXUtnFEMI/K7MDENG"
    ray = FakeRay(log=f"env AWS_SECRET_ACCESS_KEY={secret}\nLB_RESULT data FAIL " + json.dumps({"error": f"denied for {secret}"}) + "\n")
    sink = []

    job = livejob.run(ENV, ["python", "x.py"], out_dir=tmp_path, name="data", prefix="LB_RESULT", runtime_env={"pip": ["boto3"]},
                      env_vars={"AWS_SECRET_ACCESS_KEY": secret, "LB_OUT": "s3://b/out"}, secrets=[secret], sink=sink.append,
                      ray=ray, poll_s=0)

    runtime_env = json.loads(ray.commands[0][ray.commands[0].index("--runtime-env-json") + 1])
    assert runtime_env == {"pip": ["boto3"], "env_vars": {"AWS_SECRET_ACCESS_KEY": secret, "LB_OUT": "s3://b/out"}}
    assert sink == [secret]
    assert not [p.name for p in tmp_path.iterdir() if secret in p.read_text()]
    assert "<redacted>" in (tmp_path / "data_submit.txt").read_text() and "s3://b/out" in (tmp_path / "data_submit.txt").read_text()
    assert secret not in job.log and job.data == {"error": "denied for <redacted>"}
    assert secret not in job.stop().stdout + job.logs()


def test_a_job_without_wait_is_a_handle_that_the_caller_stops_then_waits_for(tmp_path):
    ray = FakeRay(statuses=("RUNNING",), log='HOLD_READY 1.0\nNODE_PROBE {"node_type": "gpu", "ports": 14}\nNODE_PROBE {"node_type": "gpu", "ports": 20}\n')

    job = livejob.run(ENV, ["python", "hold.py", "4"], out_dir=tmp_path, name="hold", wait=False, ray=ray, poll_s=0)
    assert job.status is None and job.status_now() == "RUNNING" and "HOLD_READY" in job.logs() and ray.stopped == 0

    assert job.stop().returncode == 0
    assert job.wait(timeout=0).status == "STOPPED"
    assert job.lines("NODE_PROBE") == [{"node_type": "gpu", "ports": 14}, {"node_type": "gpu", "ports": 20}]


def test_only_the_job_module_stops_a_job_and_parses_result_lines():
    """No other module of the live suite stops a job by itself or parses a result or probe line. (A scenario may run
    `ray job list`, `ray job logs --follow` or a bare `ray job submit` as the claim that it tests.)"""
    import re
    live = Path(livejob.__file__).parent
    raw = re.compile(r'"job", "stop"|\.split\(" ", 3\)|startswith\("(?:NODE_PROBE|C10_PROBE|\w+_RESULT)|BATTERY_JSON \(')
    found = {p.name: raw.findall(p.read_text()) for p in sorted(live.glob("*.py")) if p.name != "livejob.py"}
    assert {name: f for name, f in found.items() if f} == {}


def test_a_submit_that_fails_gives_submit_failed_and_the_redacted_output(tmp_path):
    ray = FakeRay(submit_exit=1, submit_output="error: bad runtime env {'AWS_SECRET_ACCESS_KEY': 'sekrit'}\n")

    job = livejob.run(ENV, ["python", "x.py"], out_dir=tmp_path, name="x", prefix="LB_RESULT", secrets=["sekrit"], ray=ray, poll_s=0)

    assert job.status == livejob.SUBMIT_FAILED and job.word == livejob.NO_RESULT
    assert "bad runtime env" in job.log and "sekrit" not in job.log
    assert (tmp_path / "x.log").read_text() == job.log
