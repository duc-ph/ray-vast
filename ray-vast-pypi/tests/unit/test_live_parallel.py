"""The parallel live suite: the real `tests/live/conftest.py` runs a small suite under pytest-xdist, with a fake
`livecloud` module, the one factory of the cluster kinds and the cloud guards (no cloud call). The fake clusters have the
cluster interface that livecloud.py writes down. Each fake `up`, `down`, test and end-of-run sweep writes one line to
`events.log` with its process id and time, so the test sees which process did what, and when."""
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

LIVE = Path(__file__).resolve().parents[1] / "live"

FAKE_CLOUD = '''
import json, os, time
from pathlib import Path
REPO = Path(__file__).parent


def event(*parts):
    with open(Path(os.environ["MARK_DIR"]) / "events.log", "a") as f:
        f.write(" ".join(map(str, parts)) + f" pid={os.getpid()} t={time.time():.3f}\\n")


class Done:
    stdout, stderr, returncode = "left: nothing\\n", "", 0


class Poller:
    def __init__(self, timeline=()):
        self.timeline = list(timeline)

    def start(self, period=10):
        pass

    def stop(self):
        pass


class FakeCluster:
    """A cluster of the livecloud interface with no cloud: its state file is the only thing that `up` makes."""
    head_instance_type = "t3.large"

    def __init__(self, run, name, out_dir, home, kept=False, timeline=()):
        self.name, self.dir, self.home, self.kept = name, Path(out_dir), Path(home), kept
        self.dir.mkdir(parents=True, exist_ok=True)
        self.yaml, self.region, self.secrets, self.up_output, self.last_down = self.dir / "cluster.yaml", "us-west-2", [], None, None
        self.records, self.poller = [self.dir], Poller(timeline)
        self.state_file = self.home / ".ray-vast" / f"{self.name}.json"
        run.register(self)  # as every cluster kind

    @property
    def timeline(self):
        return self.poller.timeline

    @property
    def state(self):
        return json.loads(self.state_file.read_text()) if self.state_file.exists() else {}

    def up(self):
        event("UP", self.name)
        time.sleep(0.5)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text("{\\"head_ip\\": \\"192.0.2.1\\"}")
        self.up_output, self.last_down = {}, None
        return self

    def down(self, *args, **kwargs):
        event("DOWN", self.name)
        self.state_file.unlink(missing_ok=True)
        done = Done()
        if os.environ.get("DOWN_LEAVES") and not self.name.endswith("-keep"):
            done.stdout = "left: security group sg-1\\n"
        self.last_down = done
        return done

    def instances(self):
        return []

    def head_runs(self):
        return bool(self.state)

    def collect_token(self):
        return "token"


class LibrariesCluster(FakeCluster):
    def keep_one_worker(self, record):
        event("KEEP_ONE", self.name)

    def end_checks(self, record, done):
        event("END_CHECKS", self.name, "kept" if done is None else "ended")
        return []


class RestrictedUser:
    def __init__(self, tmp_dir):
        self.profile, self.credentials_file, self.secret = "ray-vast-livetest", Path(tmp_dir) / "credentials", "user-secret"

    def reuse(self):
        return False

    def create(self):
        event("USER_CREATE")

    def delete(self):
        event("USER_DELETE")


def suite_cluster(run, config, out_dir, home, kept=False, user=None):
    return FakeCluster(run, config["cluster_name"], out_dir, home, kept)


def libraries_cluster(run, group_dir, home, host, suffix, kept):
    return LibrariesCluster(run, f"rvtest-lib-{host.lower()}-{suffix}", group_dir, home, kept)


def release_cluster(run, out_dir, home, region, version):
    """The release cluster: the user's YAML verbatim, so its name is not a suite name. Its poller saw instance 7."""
    return FakeCluster(run, "gpu-24gb", out_dir, home, timeline=[{"t": 1.0, "instances": [{"id": 7}]}])


def restricted_user(out_dir, tmp_dir, date):
    return RestrictedUser(tmp_dir)


def aws_leftovers(name, region):
    event("AWSCHECK", name)
    return []


def credit_sample():
    raise RuntimeError("no vast in this test")


def invoices():
    return []


def aws_account_leftovers(regions, skip):
    event("ACCOUNTCHECK", *regions)
    return []


def sweep_label(labels, prefixes=(), ids=None):
    event("SWEEP", *labels, *[f"prefix={p}" for p in prefixes], *([] if ids is None else [f"ids={','.join(map(str, ids))}"]))
    return ["0 left"]


def vast_key():
    return "fake-vast-key"
'''

SUITE = '''
import os
import time
import pytest
from livecloud import event


def leak(record):
    if os.environ.get("LEAK"):
        record.evidence("leak.txt", "fake-vast-key")


@pytest.mark.exclusive_account
def test_X01_account_wide(record):
    time.sleep(2)
    event("X01_END")


def test_A03_own_cluster(record, new_cluster, tmp_path, request):
    if request.config.getoption("--keep-cluster"):
        pytest.skip("this fake makes a cluster that is never kept")
    own = new_cluster({"cluster_name": "rvtest-own-1a2b"}, record.dir / "a", tmp_path / "home").up()
    event("RUN", "A03", own.name)


def test_A10_denials(record, restricted_user):
    event("RUN", "A10", "-")
    leak(record)


def test_A04_exists(record, aws_only):
    event("RUN", "A04", aws_only.name)
    time.sleep(1)


def test_A05_network(record, aws_only):
    event("RUN", "A05", aws_only.name)


def test_L01_up(record, lifecycle):
    event("RUN", "L01", lifecycle.name)
    time.sleep(1)


def test_L02_scale(record, lifecycle):
    event("RUN", "L02", lifecycle.name)
    leak(record)


@pytest.mark.parametrize("where", ["H1", "H3"])
def test_LB00_probe(record, libraries, where):
    event("RUN", f"LB00[{where}]", libraries.name)
    time.sleep(1)


@pytest.mark.host("H3")
def test_LB01_jobs(record, libraries):
    event("RUN", "LB01", libraries.name)
    leak(record)


@pytest.mark.host("H1")
def test_LB11_pairs(record, libraries):
    event("RUN", "LB11", libraries.name)


@pytest.mark.box_disk
def test_F17_build(record):
    event("RUN", "F17", "-")
    time.sleep(0.5)


@pytest.mark.box_disk
def test_F20_snippets(record):
    event("RUN", "F20", "-")


@pytest.mark.parametrize("n", range(6))
def test_F01_free(record, n):
    event("RUN", f"F01[{n}]", "-")
    time.sleep(0.5)
'''


def _suite(tmp_path):
    """The real conftest and the real modules of tests/live (linked), the fake livecloud, and the small suite."""
    suite = tmp_path / "suite"
    (suite / "clusters").mkdir(parents=True)
    shutil.copy(LIVE / "conftest.py", suite / "conftest.py")
    for path in LIVE.glob("*.py"):
        if not path.name.startswith("test_") and path.name not in ("conftest.py", "livecloud.py"):
            (suite / path.name).symlink_to(path)
    (suite / "jobs").symlink_to(LIVE / "jobs")
    for name in ("aws_only.yaml", "lifecycle.yaml"):
        shutil.copy(LIVE / "clusters" / name, suite / "clusters" / name)
    (suite / "livecloud.py").write_text(FAKE_CLOUD)
    (suite / "test_suite.py").write_text(SUITE)
    return suite


def _run(tmp_path, suite, *args, extra_env=None):
    """One run of the suite. Returns (exit code, output, events: [(kind, [fields], pid, t)])."""
    marks = tmp_path / "marks"
    marks.mkdir(exist_ok=True)
    (marks / "events.log").unlink(missing_ok=True)
    env = {k: v for k, v in os.environ.items() if not k.startswith(("RAYVAST_LIVE_", "PYTEST_XDIST"))}
    env.update(HOME=str(tmp_path / "home"), MARK_DIR=str(marks), RAYVAST_LIVE_RESULTS=str(tmp_path / "results"), **(extra_env or {}))
    done = subprocess.run([sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-o", "addopts=", "-q", *args, str(suite)],
                          cwd=suite, env=env, capture_output=True, text=True, timeout=300)
    events = []
    for line in ((marks / "events.log").read_text() if (marks / "events.log").exists() else "").splitlines():
        parts = line.split()
        events.append((parts[0], parts[1:-2], int(parts[-2].split("=")[1]), float(parts[-1].split("=")[1])))
    return done.returncode, done.stdout + done.stderr, events


CLUSTERS = ("rvtest-aws", "rvtest-life", "rvtest-lib-h1", "rvtest-lib-h3")
TEST_LABEL = "ray-vast-test-" + time.strftime("%Y%m%d", time.gmtime())


@pytest.mark.slow
@pytest.mark.parametrize("workers", [[], ["-n", "2"], ["-n", "4"]], ids=["one process", "2 xdist workers", "4 xdist workers"])
def test_each_cluster_is_up_and_ended_once_in_the_process_of_its_tests_and_the_run_wide_guards_run_once_after_every_end(tmp_path, workers):
    code, out, events = _run(tmp_path, _suite(tmp_path), *workers)
    assert code == 0, out
    ups = {e[1][0]: e for e in events if e[0] == "UP"}
    downs = [e for e in events if e[0] == "DOWN"]
    assert sorted(n.rsplit("-", 1)[0] for n in ups) == sorted(CLUSTERS + ("rvtest-own",)) and len(ups) == len([e for e in events if e[0] == "UP"])
    assert sorted(e[1][0] for e in downs) == sorted(ups)                      # one down for each up
    for kind, (scenario, name), pid, _ in [e for e in events if e[0] == "RUN" and e[1][1] != "-"]:
        assert pid == ups[name][2], (scenario, name)                            # the tests of a cluster run in its process
    exclusive_end = next(e[3] for e in events if e[0] == "X01_END")
    assert all(e[3] >= exclusive_end for e in ups.values())                    # every `up` (also a scenario's own) after `exclusive_account`
    a10 = next(e for e in events if e[0] == "RUN" and e[1][0] == "A10")
    assert a10[3] >= exclusive_end                                              # the restricted user too (A02 deletes the boundary policy)
    box = [e for e in events if e[0] == "RUN" and e[1][0] in ("F17", "F20")]
    assert len(box) == 2 and len({e[2] for e in box}) == 1                      # `box_disk`: one process, one after the other
    assert [e[0] for e in events].count("USER_CREATE") == 1 and [e[0] for e in events].count("USER_DELETE") == 1
    for name, (_, _, pid, _) in ups.items():  # liverun.end_cluster, in the process of the cluster: down, its sweep, its AWS check
        assert [(e[0], e[1]) for e in events if e[2] == pid and name in " ".join(e[1]) and e[0] in ("DOWN", "SWEEP", "AWSCHECK")] == [
            ("DOWN", [name]), ("SWEEP", [f"ray-vast:{name}"]), ("AWSCHECK", [name])], name
    final = [e for e in events if e[0] in ("SWEEP", "ACCOUNTCHECK") and not any(f.startswith("ray-vast:") for f in e[1])]
    assert [(e[0], e[1][0][:21]) for e in final] == [("SWEEP", "prefix=ray-vast-test-"), ("ACCOUNTCHECK", "us-west-2")]  # once, the test label only
    assert min(e[3] for e in final) >= max(e[3] for e in events if e[0] == "AWSCHECK")  # after every cluster ended
    if workers:
        assert {e[2] for e in final}.isdisjoint({e[2] for e in events if e[0] in ("RUN", "UP")})  # the controller, not a worker
        assert len({e[2] for n, e in ups.items() if not n.startswith("rvtest-own")}) == int(workers[1])  # the clusters ran in parallel


@pytest.mark.slow
def test_each_cluster_group_ends_with_its_teardown_checks_and_a_down_that_leaves_something_fails_the_run(tmp_path):
    suite = _suite(tmp_path)
    code, out, events = _run(tmp_path, suite, "-n", "4")
    assert code == 0, out
    groups = ("aws_only", "lifecycle", "libraries-h1", "libraries-h3")
    for group in groups:
        teardown = json.loads((tmp_path / "results" / "_session" / group / "teardown" / "result.json").read_text())
        assert teardown["outcome"] == "passed" and [(c["name"], c["ok"]) for c in teardown["checks"]] == [
            ("the cluster ended cleanly (liverun.end_cluster)", True)], (group, teardown)
    libraries = [e for e in events if e[0] in ("KEEP_ONE", "DOWN", "END_CHECKS", "SWEEP") and e[1][0].removeprefix("ray-vast:").startswith("rvtest-lib-")]
    for name in {e[1][0] for e in libraries if e[0] == "DOWN"}:
        assert [e[0] for e in libraries if name in e[1][0]] == ["KEEP_ONE", "DOWN", "END_CHECKS", "SWEEP"]  # one worker runs at the `down`

    code, out, events = _run(tmp_path, suite, "-n", "4", extra_env={"DOWN_LEAVES": "1"})
    assert code != 0 and "teardown of lifecycle" in out, out
    teardown = json.loads((tmp_path / "results" / "_session" / "lifecycle" / "teardown" / "result.json").read_text())
    assert teardown["outcome"] == "failed" and not teardown["checks"][0]["ok"]
    assert "down left something (exit 0): left: security group sg-1" in teardown["checks"][0]["detail"]


@pytest.mark.slow
def test_a_kept_cluster_is_reused_never_ended_or_swept_and_a_run_without_the_option_ends_it_first(tmp_path):
    suite = _suite(tmp_path)
    code, out, events = _run(tmp_path, suite, "-n", "4", "--keep-cluster")
    assert code == 0, out
    assert sorted(e[1][0] for e in events if e[0] == "UP") == sorted(f"{c}-keep" for c in CLUSTERS)
    assert not [e for e in events if e[0] in ("DOWN", "AWSCHECK")]
    assert [e[1] for e in events if e[0] == "SWEEP"] == [[f"prefix={TEST_LABEL}"]]  # the final sweep, the test label only
    assert all(f"KEPT CLUSTER {c}-keep still runs" in out for c in CLUSTERS), out

    code, out, events = _run(tmp_path, suite, "-n", "4", "--keep-cluster")
    assert code == 0, out
    assert not [e for e in events if e[0] in ("UP", "DOWN")]                    # reused: the heads still run

    code, out, events = _run(tmp_path, suite, "-n", "4")
    assert code == 0, out
    downs = [e[1][0] for e in events if e[0] == "DOWN"]
    fresh = [e[1][0] for e in events if e[0] == "UP"]
    assert sorted(downs) == sorted([f"{c}-keep" for c in CLUSTERS] + fresh) and not [n for n in fresh if n.endswith("-keep")]
    assert sorted(e[1][0] for e in events if e[0] == "AWSCHECK") == sorted(downs)  # the ended kept clusters are checked too
    assert "KEPT CLUSTER" not in out and not (tmp_path / "home" / ".cache" / "ray-vast-live" / "aws_only").exists()


RELEASE_SUITE = '''
from livecloud import event


def test_R01_up(record, release):
    event("RUN", "R01", release.name)


def test_R02_tutorial(record, release):
    event("RUN", "R02", release.name)
    release.down()  # the tutorial ends with `down`
'''


@pytest.mark.slow
def test_a_release_cluster_with_a_foreign_name_joins_the_run_and_is_ended_once_by_the_same_module(tmp_path):
    suite = _suite(tmp_path)
    (suite / "test_release.py").write_text(RELEASE_SUITE)
    code, out, events = _run(tmp_path, suite, "-n", "2", "-k", "R01_up or R02_tutorial", extra_env={"RAYVAST_LIVE_RELEASE": "9.9.9"})
    assert code == 0, out
    of_release = [e[0] for e in events if "gpu-24gb" in e[1] or "ray-vast:gpu-24gb" in e[1]]
    assert of_release == ["UP", "RUN", "RUN", "DOWN", "SWEEP", "AWSCHECK"], out  # R02's `down` is the only one
    # gpu-24gb is not a suite name: its sweep takes only the instances that its poller saw, never the whole label
    assert [e[1] for e in events if e[0] == "SWEEP"] == [["ray-vast:gpu-24gb", "ids=7"], [f"prefix={TEST_LABEL}"]]
    opened = tmp_path / "results" / "_session" / "runs"
    assert [p.name for p in opened.glob("*/opened/*.json")] == ["gpu-24gb.json"]
    assert [p.name for p in opened.glob("*/ended/*.json")] == ["gpu-24gb.json"]


@pytest.mark.slow
def test_a_secret_in_a_record_is_found_once_by_the_end_of_the_cluster_that_owns_the_record_else_by_the_process_scan(tmp_path):
    code, out, events = _run(tmp_path, _suite(tmp_path), "-n", "4", extra_env={"LEAK": "1"})
    assert code != 0
    teardowns = {group: json.loads((tmp_path / "results" / "_session" / group / "teardown" / "result.json").read_text())["checks"][0]
                 for group in ("aws_only", "lifecycle", "libraries-h1", "libraries-h3")}
    found = {group: check["detail"] for group, check in teardowns.items() if not check["ok"]}
    assert sorted(found) == ["libraries-h3", "lifecycle"], teardowns  # LB01 is H3, L02 is lifecycle; never LB in lifecycle
    assert found["lifecycle"].endswith("/results/L02/leak.txt") and found["libraries-h3"].endswith("/results/LB01/leak.txt")
    problems = (next((tmp_path / "results" / "_session" / "runs").glob("*/problems.txt"))).read_text().splitlines()
    assert [p for p in problems if "secret scan" in p and "A10" in p] == ["the secret scan found 1 file(s): A10/leak.txt"]
    assert sum("L02/leak.txt" in p for p in problems) == 1 and sum("LB01/leak.txt" in p for p in problems) == 1  # no second scan


FAST_WAIT = '''
"""A plugin for this test (`-p fastwait`): each wait of the suite (liverun.wait_until) runs on a fake clock, so a wait
of 1800 s ends at once."""
import liverun

real_wait_until = liverun.wait_until


class Clock:
    t = 0.0

    def time(self):
        return self.t

    def sleep(self, s):
        self.t += s


def wait_until(look, timeout, done=bool, poll_s=10, clock=None):
    return real_wait_until(look, timeout, done, poll_s, clock=Clock())


liverun.wait_until = wait_until
'''

LIFECYCLE_CLOUD = '''
from contextlib import contextmanager


class Job:
    status = "RUNNING"

    def logs(self):
        return "" if os.environ.get("HOLD_NEVER_READY") else "HOLD_READY"

    def stop(self):
        event("JOB_STOP")


class LifecycleCluster(FakeCluster):
    @contextmanager
    def tunnel(self):
        yield {}

    def job(self, env, entrypoint, out_dir=None, **options):
        event("JOB", entrypoint[1])
        return Job()

    def instances(self):
        return [{"id": 41, "actual_status": "running"}]  # a worker that never goes


def suite_cluster(run, config, out_dir, home, kept=False, user=None):
    return LifecycleCluster(run, config["cluster_name"], out_dir, home, kept)
'''


@pytest.mark.slow
@pytest.mark.parametrize("hold_ready", [False, True], ids=["the hold never ready", "the workers never gone"])
def test_a_wait_of_the_lifecycle_group_that_times_out_fails_its_test_and_never_passes_silently(tmp_path, hold_ready):
    suite = _suite(tmp_path)
    shutil.copy(LIVE / "test_lifecycle.py", suite / "test_lifecycle.py")
    (suite / "fastwait.py").write_text(FAST_WAIT)
    (suite / "livecloud.py").write_text(FAKE_CLOUD + LIFECYCLE_CLOUD)
    code, out, events = _run(tmp_path, suite, "-p", "fastwait", "-k", "L10_head_loss",
                             extra_env={} if hold_ready else {"HOLD_NEVER_READY": "1"})
    assert code != 0, out
    record = tmp_path / "results" / "L10" / "result.json"
    if hold_ready:  # L10 waits for 0 workers before it asks for 4 again
        result = json.loads(record.read_text())
        assert result["outcome"] == "failed" and [c for c in result["checks"] if not c["ok"]] == [
            {"name": "0 workers before the second hold (within 900 s)", "ok": False, "detail": "[41]"}], result
        assert [e[1] for e in events if e[0] == "JOB"] == [["hold.py"]]  # no second hold after the failed wait
    else:  # the hold fixture of L03-L10: no HOLD_READY in 1800 s is an error of each test that uses it
        assert "1 error" in out and "the hold job printed no HOLD_READY in 1800 s" in out, out
        assert not record.exists()  # the module fixture runs before the record: the summary says NOT RUN, never PASS
        assert [e[0] for e in events if e[0] in ("JOB", "JOB_STOP")] == ["JOB", "JOB_STOP"]


LIBRARIES_CLOUD = '''
from contextlib import contextmanager
from types import SimpleNamespace


def row(kind):
    return {"container_id": "41", "machine_id": 9, "node_id": f"node-{kind}", "ports": 18, "ray_cpus": 40, "pids.max": "9728",
            "node_type": kind}


class Job(SimpleNamespace):
    def lines(self, prefix):
        return []


class S3:
    bucket, env_vars, secret = "ray-vast-rvtest-20261017-abcd", {"AWS_ACCESS_KEY_ID": "id"}, "s3-secret"

    def objects(self, prefix):
        return [{"Key": prefix + "part-0.parquet", "Size": 1}]


class Window:
    def lines(self):
        return []

    def events(self, kind=None, instances=None, lines=None):
        return []


class HostCluster(LibrariesCluster):
    """A cluster with no worker at its start (LB00[H1] found no host: its last mismatch was ended). A job runs only
    when a worker runs (else it makes no progress, as in the 2026-10-17 run)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.shape, self.matched = {}, {}

    @contextmanager
    def tunnel(self):
        yield {"RAY_ADDRESS": "http://127.0.0.1:1", "RAY_AUTH_TOKEN_PATH": "/dev/null"}

    def s3(self):
        return S3()

    def window(self, from_start=False):
        return Window()

    def ensure_shape(self, shape):
        event("SHAPE", *[f"{k}={v}" for k, v in shape.items()])
        self.shape = dict(shape)
        return 0.0

    def find(self, env, kind, want, keep, out_dir):
        self.ensure_shape({**keep, kind: 1})
        facts = {"machine_id": 9, "probe": dict(row(kind), machine_id=9)}
        self.matched[kind] = facts
        return facts, [dict(facts, match=True)]

    def probe(self, env, out_dir, name="probe", check=True):
        return [row(kind) for kind, n in self.shape.items() if n]

    def job(self, env, entrypoint, out_dir=None, name="job", **options):
        workers = sum(self.shape.values())
        event("JOB", name, f"workers={workers}")
        if not workers:
            return Job(status="FAILED", word="FAIL", data={"error_class": "ExecutionTimeoutError"}, log="")
        return Job(status="SUCCEEDED", word="PASS", data={}, log="")

    def keep_one_worker(self, record):
        pass

    def end_checks(self, record, done):
        return []


def libraries_cluster(run, group_dir, home, host, suffix, kept):
    return HostCluster(run, f"rvtest-lib-{host.lower()}-{suffix}", group_dir, home, kept)
'''


@pytest.mark.slow
def test_lb03_writes_its_parquet_input_on_the_host_of_the_test_also_when_the_cluster_has_no_worker_yet(tmp_path):
    """LB03[H1] (2026-10-17): the input job ran before the test asked for its host, on a cluster with no worker (LB00[H1]
    had found no host), and made no progress for 1800 s. The real test_libraries.py with a fake libraries cluster."""
    suite = _suite(tmp_path / "pkg" / "tests")  # test_libraries.py reads the docs at parents[2] / "docs"
    (tmp_path / "pkg" / "docs").symlink_to(LIVE.parents[1] / "docs")
    shutil.copy(LIVE / "test_libraries.py", suite / "test_libraries.py")
    (suite / "livecloud.py").write_text(FAKE_CLOUD + LIBRARIES_CLOUD)
    code, out, events = _run(tmp_path, suite, "-k", "LB03 and H1")
    jobs = [e[1] for e in events if e[0] in ("JOB", "SHAPE")]
    assert ["make_parquet", "workers=0"] not in jobs, jobs
    assert jobs[:2] == [["h1=1"], ["make_parquet", "workers=1"]], jobs
    assert code == 0, out


CHECKS_SUITE = '''
def test_A05_checks(record):
    record.check("first claim", False, "no")
    record.check("second claim", True)
    record.evidence("after.txt", "the test went on after the false check")
'''


@pytest.mark.slow
def test_a_false_check_fails_its_test_at_its_end_and_the_test_runs_on_until_then(tmp_path):
    suite = _suite(tmp_path)
    (suite / "test_checks.py").write_text(CHECKS_SUITE)
    code, out, events = _run(tmp_path, suite, "-k", "A05_checks")
    assert code != 0 and "first claim" in out, out
    result = json.loads((tmp_path / "results" / "A05" / "result.json").read_text())
    assert result["outcome"] == "failed" and "first claim" in result["message"], result
    assert (tmp_path / "results" / "A05" / "after.txt").exists()
