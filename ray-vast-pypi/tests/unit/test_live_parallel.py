"""The parallel live suite: the real `tests/live/conftest.py` runs a small suite under pytest-xdist, with fake `cluster`,
`restricted` and `libraries_setup` modules (no cloud call). Each fake `up`, `down`, test and end-of-run sweep writes one
line to `events.log` with its process id and time, so the test sees which process did what, and when."""
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

LIVE = Path(__file__).resolve().parents[1] / "live"

FAKE_CLUSTER = '''
import json, os, time
from pathlib import Path
ADMIN_PROFILE = "personal"
REPO = Path(__file__).parent


def event(*parts):
    with open(Path(os.environ["MARK_DIR"]) / "events.log", "a") as f:
        f.write(" ".join(map(str, parts)) + f" pid={os.getpid()} t={time.time():.3f}\\n")


class Done:
    stdout, stderr, returncode = "left: nothing\\n", "", 0


class Cluster:
    def __init__(self, fixture, config, out_dir, home, profile=None, credentials_file=None):
        self.name, self.config, self.dir, self.home = config["cluster_name"], config, Path(out_dir), Path(home)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.yaml, self.region, self.secrets, self.kept, self.timeline, self.up_output = self.dir / "cluster.yaml", "us-west-2", [], False, [], None
        self.state_file = self.home / ".ray-vast" / f"{self.name}.json"
        import livekit  # as the real Cluster: every cluster object of a run is noted for the end-of-run check
        livekit.note_opened(Path(os.environ["RAYVAST_LIVE_RESULTS"]) / "_session" / "runs" / os.environ["RAYVAST_LIVE_RUN"],
                            {"name": self.name, "region": self.region, "kept": False, "yaml": str(self.yaml), "home": str(self.home)})

    @property
    def state(self):
        return json.loads(self.state_file.read_text()) if self.state_file.exists() else {}

    def up(self):
        event("UP", self.name)
        time.sleep(0.5)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text("{\\"head_ip\\": \\"192.0.2.1\\"}")
        self.up_output = {}
        return self

    def down(self, *args, **kwargs):
        event("DOWN", self.name)
        self.state_file.unlink(missing_ok=True)
        return Done()

    def head_runs(self):
        return bool(self.state)

    def collect_token(self):
        return "token"

    def start_poller(self):
        pass

    def stop_poller(self):
        pass


def aws_leftovers(name, region, profile=ADMIN_PROFILE):
    event("AWSCHECK", name)
    return []


def credit_sample():
    raise RuntimeError("no vast in this test")


def invoices():
    return []


def sweep_label(prefixes):
    event("GUARDS", *prefixes)
    return ["0 left"]


def vast_key():
    return "fake-vast-key"
'''

FAKE_RESTRICTED = '''
from pathlib import Path
from cluster import event


class RestrictedUser:
    def __init__(self, out_dir, tmp_dir, date):
        self.profile, self.credentials_file, self.secret = "ray-vast-livetest", Path(tmp_dir) / "credentials", "user-secret"

    def reuse(self):
        return False

    def create(self):
        event("USER_CREATE")

    def delete(self):
        event("USER_DELETE")
'''

FAKE_LIBRARIES_SETUP = '''
from cluster import Cluster


def make_libraries_cluster(group_dir, home, host, suffix):
    config = {"cluster_name": f"rvtest-lib-{host.lower()}-{suffix}", "available_node_types": {"head": {"node_config": {}}}}
    return Cluster("libraries", config, group_dir, home)
'''

SUITE = '''
import time
import pytest
from cluster import event


def test_A02_boundary(record):
    time.sleep(2)
    event("A02_END")


def test_A03_down_recovery(record, account_gate):
    event("RUN", "A03", "-")


def test_A10_denials(record, restricted_user):
    event("RUN", "A10", "-")


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


@pytest.mark.parametrize("where", ["H1", "H3"])
def test_LB00_probe(record, libraries, where):
    event("RUN", f"LB00[{where}]", libraries.name)
    time.sleep(1)


@pytest.mark.host("H3")
def test_LB01_jobs(record, libraries):
    event("RUN", "LB01", libraries.name)


@pytest.mark.host("H1")
def test_LB12_diagnostics(record, libraries):
    event("RUN", "LB12", libraries.name)


@pytest.mark.parametrize("n", range(6))
def test_F01_free(record, n):
    event("RUN", f"F01[{n}]", "-")
    time.sleep(0.5)
'''


def _suite(tmp_path):
    suite = tmp_path / "suite"
    (suite / "clusters").mkdir(parents=True)
    for name in ("conftest.py", "livekit.py"):
        shutil.copy(LIVE / name, suite / name)
    for name in ("aws_only.yaml", "lifecycle.yaml"):
        shutil.copy(LIVE / "clusters" / name, suite / "clusters" / name)
    (suite / "cluster.py").write_text(FAKE_CLUSTER)
    (suite / "restricted.py").write_text(FAKE_RESTRICTED)
    (suite / "libraries_setup.py").write_text(FAKE_LIBRARIES_SETUP)
    (suite / "test_suite.py").write_text(SUITE)
    return suite


def _run(tmp_path, suite, *args):
    """One run of the suite. Returns (exit code, output, events: [(kind, [fields], pid, t)])."""
    marks = tmp_path / "marks"
    marks.mkdir(exist_ok=True)
    (marks / "events.log").unlink(missing_ok=True)
    env = {k: v for k, v in os.environ.items() if not k.startswith(("RAYVAST_LIVE_", "PYTEST_XDIST"))}
    env.update(HOME=str(tmp_path / "home"), MARK_DIR=str(marks), RAYVAST_LIVE_RESULTS=str(tmp_path / "results"))
    done = subprocess.run([sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-o", "addopts=", "-q", *args, str(suite)],
                          cwd=suite, env=env, capture_output=True, text=True, timeout=300)
    events = []
    for line in (marks / "events.log").read_text().splitlines():
        parts = line.split()
        events.append((parts[0], parts[1:-2], int(parts[-2].split("=")[1]), float(parts[-1].split("=")[1])))
    return done.returncode, done.stdout + done.stderr, events


CLUSTERS = ("rvtest-aws", "rvtest-life", "rvtest-lib-h1", "rvtest-lib-h3")


@pytest.mark.slow
@pytest.mark.parametrize("workers", [[], ["-n", "4"]], ids=["one process", "4 xdist workers"])
def test_each_cluster_is_up_and_down_once_in_the_process_of_its_tests_and_the_guards_run_once_after_every_down(tmp_path, workers):
    code, out, events = _run(tmp_path, _suite(tmp_path), *workers)
    assert code == 0, out
    ups = {e[1][0]: e for e in events if e[0] == "UP"}
    downs = [e for e in events if e[0] == "DOWN"]
    assert sorted(n.rsplit("-", 1)[0] for n in ups) == sorted(CLUSTERS) and len(ups) == len([e for e in events if e[0] == "UP"])
    assert sorted(e[1][0] for e in downs) == sorted(ups)                      # one down for each up
    for kind, (scenario, name), pid, _ in [e for e in events if e[0] == "RUN" and e[1][1] != "-"]:
        assert pid == ups[name][2], (scenario, name)                            # the tests of a cluster run in its process
    a02_end = next(e[3] for e in events if e[0] == "A02_END")
    assert all(e[3] >= a02_end for e in ups.values())                          # every `up` waits for A02
    assert [e[0] for e in events].count("USER_CREATE") == 1 and [e[0] for e in events].count("USER_DELETE") == 1
    guards = [e for e in events if e[0] == "GUARDS"]
    assert len(guards) == 1 and guards[0][3] >= max(e[3] for e in downs)        # once, after every down
    assert sorted(guards[0][1]) == sorted([f"ray-vast-test-{guards[0][1][0][-8:]}"] + [f"ray-vast:{n}" for n in ups])
    if workers:
        assert guards[0][2] not in {e[2] for e in events if e[0] in ("RUN", "UP")}  # the controller, not a worker
        assert len({e[2] for e in ups.values()}) == 4                          # the 4 clusters ran in parallel


@pytest.mark.slow
def test_a_kept_cluster_is_reused_never_ended_or_swept_and_a_run_without_the_option_ends_it_first(tmp_path):
    suite = _suite(tmp_path)
    code, out, events = _run(tmp_path, suite, "-n", "4", "--keep-cluster")
    assert code == 0, out
    assert sorted(e[1][0] for e in events if e[0] == "UP") == sorted(f"{c}-keep" for c in CLUSTERS)
    assert not [e for e in events if e[0] in ("DOWN", "AWSCHECK")]
    assert next(e for e in events if e[0] == "GUARDS")[1] == [next(e for e in events if e[0] == "GUARDS")[1][0]]  # the test label only
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
