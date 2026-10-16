"""The logic of the live suite (`tests/live/`): result records, the summary in test-results.md, spend, host choice,
safety names, the secret scan, and the cluster lifetime. No cloud call: fakes stand in for `up`, `down` and vast."""
import json

import pytest

from live import livekit


def test_a_finished_record_writes_result_json_with_outcome_checks_and_evidence(tmp_path):
    record = livekit.Recorder(tmp_path, "A04", markers=["aws"], fixture="aws_only")
    record.evidence("up.log", "cluster rvtest-x is up")
    record.check("second up stops", True, "exit 1")
    record.finish("passed", "")

    result = json.loads((tmp_path / "A04" / "result.json").read_text())
    assert result["id"] == "A04" and result["outcome"] == "passed" and result["fixture"] == "aws_only"
    assert result["checks"] == [{"name": "second up stops", "ok": True, "detail": "exit 1"}]
    assert (tmp_path / "A04" / "up.log").read_text() == "cluster rvtest-x is up"


def _result(root, scenario, outcome, checks=(), message=""):
    record = livekit.Recorder(root, scenario, markers=["free"], fixture="none")
    for name, ok in checks:
        record.check(name, ok)
    record.finish(outcome, message)


def test_the_summary_has_one_row_per_scenario_and_a_failed_check_or_a_missing_record_is_never_a_pass(tmp_path):
    root = tmp_path / "2026-10-16"
    _result(root, "F01", "passed", [("14 shim tests", True)])
    _result(root, "F02", "passed", [("node alive", False)])
    _result(root, "F03", "failed", message="AssertionError: exit 0")
    table = livekit.summary_table(root, expected=["F01", "F02", "F03", "F04"])

    rows = {line.split("|")[1].strip(): line for line in table.splitlines() if line.startswith("| F")}
    assert "PASS" in rows["F01"]
    assert "FAIL" in rows["F02"] and "node alive" in rows["F02"]
    assert "FAIL" in rows["F03"] and "AssertionError: exit 0" in rows["F03"]
    assert "NOT RUN" in rows["F04"]


def test_writing_the_summary_twice_replaces_the_block_of_that_date_and_keeps_the_rest(tmp_path):
    doc = tmp_path / "test-results.md"
    doc.write_text("# Test results\n\nolder text\n")
    livekit.write_summary(doc, "2026-10-16", "| F01 | PASS |\n")
    livekit.write_summary(doc, "2026-10-16", "| F01 | FAIL |\n")

    text = doc.read_text()
    assert text.startswith("# Test results\n\nolder text\n")
    assert text.count("Live suite 2026-10-16") == 1 and "| F01 | FAIL |" in text and "| F01 | PASS |" not in text


def test_spend_adds_back_the_auto_top_ups_inside_the_window_only():
    invoices = [{"type": "payment", "timestamp": 50.0, "amount": -10.0},   # before the group
                {"type": "payment", "timestamp": 150.0, "amount": -10.0},  # an auto top-up during the group
                {"type": "charge", "timestamp": 160.0, "amount": 3.0}]
    # credit 5.50 -> (spend 2.00, top-up +10) -> 13.50
    assert livekit.vast_spend({"t": 100.0, "credit": 5.50}, {"t": 200.0, "credit": 13.50}, invoices) == {
        "spend": 2.0, "top_ups": 10.0, "credit_before": 5.5, "credit_after": 13.5}


@pytest.mark.parametrize("name", ["ray-vast-rvtest-aws-1a2b", "ray-vast-rvtest-aws-1a2b-3", "ray-vast-livetest-20261016",
                                  "ray-vast-livetest-20261016-s3", "ray-vast-livetest-20261016-s3-h1", "ray-vast-livetest-20261016-s3-gpu2",
                                  "ray-vast-rvtest-20261016-0a1b", "ray-vast-rvtest-lib-h1-keep"])
def test_the_suite_may_delete_its_own_resources(name):
    livekit.assert_ours(name)


@pytest.mark.parametrize("name", ["ray-vast-dev", "ray-vast-devbox", "ray-vast-dev-sg", "badminton", "ray-vast-rvtest-badminton",
                                  "ray-vast-head-boundary", "ray-vast-user", "ray-vast-c9", "ray-vast-livetest-20261016-admin", ""])
def test_the_suite_refuses_to_delete_anything_else(name):
    with pytest.raises(livekit.NotOurs):
        livekit.assert_ours(name)


def test_the_secret_scan_names_each_file_with_a_secret_value_or_pattern_and_never_the_text(tmp_path):
    (tmp_path / "sub").mkdir()
    (tmp_path / "clean.log").write_text("enrolled instance 1 as slot slot-1-abc\nCONTAINER_API_KEY in environ: False\n")
    (tmp_path / "token.log").write_text("x" * 10 + "c0ffee" * 8)
    (tmp_path / "sub" / "key.pem").write_text("-----BEGIN OPENSSH PRIVATE KEY-----\n")
    (tmp_path / "bearer.txt").write_text("Authorization: Bearer abc123\n")
    (tmp_path / "env.txt").write_text("CONTAINER_API_KEY=deadbeef\n")
    (tmp_path / "api.json").write_text('{"instance_api_key": "zz9"}')

    found = livekit.scan_secrets(tmp_path, ["c0ffee" * 8, "", "  "])
    assert sorted(p.relative_to(tmp_path).as_posix() for p in found) == [
        "api.json", "bearer.txt", "env.txt", "sub/key.pem", "token.log"]


def test_find_host_rents_again_with_the_tried_machines_blocked_until_one_matches():
    hosts = iter([{"machine_id": 33638, "pids_max": 7680, "cgroup": "v2", "cpus": 128},
                  {"machine_id": 32816, "pids_max": 1280, "cgroup": "v1", "cpus": 48}])
    rented, ended = [], []

    def rent(blocked):
        rented.append(list(blocked))
        return next(hosts)

    host, tries = livekit.find_host({"pids_max": 1280, "cgroup": "v1", "cpus": 48}, rent, ended.append)
    assert host["machine_id"] == 32816
    assert rented == [[], [33638]] and ended == [{"machine_id": 33638, "pids_max": 7680, "cgroup": "v2", "cpus": 128}]
    assert [t["match"] for t in tries] == [False, True]


def test_find_host_gives_up_after_3_rentals_and_ends_each_mismatch():
    ended = []
    rent = lambda blocked: {"machine_id": 100 + len(blocked), "pids_max": None, "cgroup": "v2", "cpus": 8}
    with pytest.raises(livekit.HostNotFound) as error:
        livekit.find_host({"cgroup": "v1"}, rent, ended.append)
    assert [h["machine_id"] for h in ended] == [100, 101, 102]
    assert "100, 101, 102" in str(error.value)


INNER = '''
import os, sys, time, pytest
import livekit
livekit.sigterm_as_interrupt()
MARK = os.environ["MARK_DIR"]

def up():
    open(os.path.join(MARK, "up"), "w").close()
    if os.environ.get("UP_FAILS"):
        raise RuntimeError("up failed half way")
    return "cluster"

def down():
    open(os.path.join(MARK, "down"), "w").close()

@pytest.fixture(scope="session")
def cluster():
    yield from livekit.lifetime(up, down)

def test_one(cluster):
    open(os.path.join(MARK, "started"), "w").close()
    if os.environ.get("INTERRUPT"):
        raise KeyboardInterrupt
    if os.environ.get("WAIT"):
        time.sleep(60)
'''


def _inner_run(tmp_path, wait=False, **env):
    import os
    import signal
    import subprocess
    import sys
    import time
    (tmp_path / "test_inner.py").write_text(INNER)
    live_dir = os.path.join(os.path.dirname(livekit.__file__))
    run_env = {**os.environ, "PYTHONPATH": live_dir, "MARK_DIR": str(tmp_path), **env}
    proc = subprocess.Popen([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-o", "addopts=", str(tmp_path / "test_inner.py")],
                            cwd=tmp_path, env=run_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)
    if wait:
        for _ in range(300):
            if (tmp_path / "started").exists():
                break
            time.sleep(0.1)
        proc.send_signal(signal.SIGTERM)
    try:
        out = proc.communicate(timeout=120)[0]
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
    return proc.returncode, out


@pytest.mark.slow
@pytest.mark.parametrize("env", [{}, {"UP_FAILS": "1"}, {"INTERRUPT": "1"}], ids=["normal end", "up fails", "Ctrl-C in a test"])
def test_down_runs_once_the_cluster_was_asked_for_also_when_up_fails_or_a_test_is_interrupted(tmp_path, env):
    code, out = _inner_run(tmp_path, **env)
    assert (tmp_path / "up").exists() and (tmp_path / "down").exists(), out


@pytest.mark.slow
def test_sigterm_during_a_test_still_runs_down(tmp_path):
    code, out = _inner_run(tmp_path, wait=True, WAIT="1")
    assert (tmp_path / "started").exists() and (tmp_path / "down").exists(), out


def test_the_matrix_parser_reads_id_markers_and_fixture_of_each_row():
    text = """
| ID | Name | Markers | Fixture | Claims proved | Pass criterion | Origin | Destructive |
|---|---|---|---|---|---|---|---|
| F01 | Shim | free | none | architecture.md:86-97 | 14 tests | C0.1 | no |
| A07 | Boundary | aws, security | aws_only | iam.md:78 | denied | audit B | yes |
| not a row | x | y | z |
"""
    assert livekit.matrix_rows(text) == [{"id": "F01", "markers": ["free"], "fixture": "none"},
                                         {"id": "A07", "markers": ["aws", "security"], "fixture": "aws_only"}]


def test_the_suite_parser_reads_tests_markers_and_cluster_fixture(tmp_path):
    (tmp_path / "test_x.py").write_text('''
import pytest
pytestmark = pytest.mark.aws

@pytest.mark.security
def test_A07_boundary(record, aws_only):
    pass

def test_A02_policy(record, tmp_path):
    pass

test_F08_local = make("F08")
''')
    assert livekit.suite_rows(tmp_path) == [{"id": "A07", "markers": ["aws", "security"], "fixture": "aws_only"},
                                            {"id": "A02", "markers": ["aws"], "fixture": "none"},
                                            {"id": "F08", "markers": ["aws"], "fixture": "none"}]


def test_the_claim_matrix_and_the_live_suite_have_the_same_scenarios_markers_and_fixtures():
    from pathlib import Path
    repo = Path(livekit.__file__).resolve().parents[3]
    matrix = livekit.matrix_rows((repo / "docs" / "test-plan.md").read_text())
    suite = livekit.suite_rows(Path(livekit.__file__).parent)
    assert sorted(r["id"] for r in matrix) == sorted({r["id"] for r in suite})
    by_id = {r["id"]: r for r in suite}
    assert [r for r in matrix if r != by_id[r["id"]]] == []


def test_a_cluster_shape_sets_min_and_max_workers_of_every_worker_type_and_the_pinned_queries():
    config = {"max_workers": 5, "head_node_type": "head", "available_node_types": {
        "head": {"resources": {"CPU": 0}, "node_config": {"region": "us-west-2"}},
        "h1": {"min_workers": 0, "max_workers": 1, "node_config": {"disk_gb": 60, "offer_query": {"machine_id": {"in": [32816]}}}},
        "gpu": {"min_workers": 0, "max_workers": 2, "node_config": {"disk_gb": 60, "offer_query": {"gpu_ram": {"gte": 8000}}}}}}
    shaped = livekit.shaped_config(config, {"gpu": 2}, {"h1": {"machine_id": {"notin": [32816]}, "cpu_cores": {"eq": 48}}})
    types = shaped["available_node_types"]
    assert (types["gpu"]["min_workers"], types["gpu"]["max_workers"]) == (2, 2)
    assert (types["h1"]["min_workers"], types["h1"]["max_workers"]) == (0, 0)
    assert types["h1"]["node_config"] == {"disk_gb": 60, "offer_query": {"machine_id": {"notin": [32816]}, "cpu_cores": {"eq": 48}}}
    assert types["head"] == {"resources": {"CPU": 0}, "node_config": {"region": "us-west-2"}}
    assert shaped["max_workers"] == 2
    assert config["available_node_types"]["h1"]["node_config"]["offer_query"] == {"machine_id": {"in": [32816]}}  # a copy


DOC = """# Ray Core

## Run it

```python
print("not this one")
```

## Settings

```python
@ray.remote(num_cpus=2.4)          # CPUS / B
def child(i):
    ...

pool = [Runner.remote() for _ in range(5)]   # B // 2
```

```python
CPUS = max(n["Resources"]["CPU"] for n in ray.nodes())
```

## Why
"""


def test_a_doc_snippet_is_the_python_block_of_its_section_with_each_substitution_applied_once():
    code = livekit.doc_snippet(DOC, "## Settings", 0, [("num_cpus=2.4", "num_cpus=4.8"), ("range(5)", "range(3)")])
    assert code == ('@ray.remote(num_cpus=4.8)          # CPUS / B\ndef child(i):\n    ...\n\n'
                    'pool = [Runner.remote() for _ in range(3)]   # B // 2\n')
    assert livekit.doc_snippet(DOC, "## Settings", 1, []) == 'CPUS = max(n["Resources"]["CPU"] for n in ray.nodes())\n'


@pytest.mark.parametrize("old", ["num_cpus=9", "\n"])
def test_a_substitution_that_is_not_in_the_snippet_exactly_once_is_an_error(old):
    with pytest.raises(ValueError, match="exactly once"):
        livekit.doc_snippet(DOC, "## Settings", 0, [(old, "x")])


@pytest.mark.parametrize("probe, facts", [
    ({"pids.max": "1280", "cgroup": "v1", "ray_cpus": 48.0}, {"pids_max": 1280, "cgroup": "v1", "cpus": 48, "limited": True}),
    ({"pids.max": "max", "cgroup": "v2", "ray_cpus": 15.0}, {"pids_max": None, "cgroup": "v2", "cpus": 15, "limited": False}),
    ({"cgroup": "v1", "ray_cpus": 24.0}, {"pids_max": None, "cgroup": "v1", "cpus": 24, "limited": False}),
])
def test_the_host_facts_of_a_node_probe_are_what_find_host_compares(probe, facts):
    assert livekit.host_facts(probe, 32816) == dict(facts, machine_id=32816)


def test_a_second_run_of_a_scenario_keeps_the_earlier_records_as_run1_and_run2(tmp_path):
    for outcome in ("failed", "failed", "passed"):
        record = livekit.Recorder(tmp_path, "A05", markers=["aws"], fixture="aws_only")
        record.evidence("log.txt", outcome)
        record.finish(outcome, "")
    assert json.loads((tmp_path / "A05" / "result.json").read_text())["outcome"] == "passed"
    assert (tmp_path / "A05.run1" / "result.json").exists() and (tmp_path / "A05.run2" / "log.txt").read_text() == "failed"
    assert livekit.summary_table(tmp_path, ["A05"]).count("| A05") == 1


@pytest.mark.parametrize("scenario, fixtures, host, group", [
    ("A04", ["record", "aws_only", "tmp_path"], None, "aws_only"),
    ("A02", ["record", "tmp_path"], None, "aws_only"),           # deletes the boundary: before the aws_only `up`
    ("L03", ["record", "lifecycle", "session_env"], None, "lifecycle"),
    ("A10", ["record", "restricted_user"], None, "lifecycle"),   # one restricted IAM user for each run
    ("LB11[H1]", ["record", "libraries", "env"], "H1", "libraries-h1"),
    ("LB07", ["record", "libraries", "env", "s3"], "gpu2", "libraries-gpu2"),
    ("R01", ["record", "release"], None, "release"),
    ("F18", ["record", "dist"], None, "dist"),                   # F17 and F18 share one wheel build
    ("F20", ["record", "tmp_path"], None, "dist"),               # after F18: each fills about 1.5 GB of the box disk
    ("F08", ["record"], None, None),
    ("A03", ["record", "tmp_path", "account_gate"], None, None),
])
def test_each_cluster_and_the_tests_that_share_its_setup_get_one_xdist_group(scenario, fixtures, host, group):
    assert livekit.xdist_group(scenario, fixtures, host) == group


def _opened(run_dir, name, kept, region="us-west-2"):
    livekit.note_opened(run_dir, {"name": name, "region": region, "kept": kept, "yaml": f"/r/{name}.yaml", "home": f"/h/{name}"})


def test_the_end_of_a_run_sweeps_and_checks_only_the_clusters_that_are_not_kept_and_names_the_kept_ones(tmp_path):
    run_dir = tmp_path / "run"
    _opened(run_dir, "rvtest-aws-1a2b", kept=False)
    _opened(run_dir, "rvtest-life-keep", kept=True, region="us-east-2")
    (run_dir / "scan_gw1.txt").write_text("L03/probe.log\n")
    swept, checked = [], []

    def sweep(prefixes):
        swept.append(prefixes)
        return ["0 left"]

    def aws_check(name, region):
        checked.append((name, region))
        return ["volume vol-1"] if name == "rvtest-aws-1a2b" else []

    problems, kept = livekit.end_of_run(run_dir, "ray-vast-test-20261016", sweep, aws_check, lambda: [])
    assert swept == [["ray-vast-test-20261016", "ray-vast:rvtest-aws-1a2b"]]
    assert checked == [("rvtest-aws-1a2b", "us-west-2")]
    assert problems == ["AWS left for rvtest-aws-1a2b: ['volume vol-1']", "the secret scan found 1 file(s): L03/probe.log"]
    assert len(kept) == 1 and "KEPT CLUSTER rvtest-life-keep" in kept[0] and "ray-vast down /r/rvtest-life-keep.yaml" in kept[0]


def test_the_end_of_a_run_with_a_failed_sweep_is_a_problem_and_the_aws_checks_still_run(tmp_path):
    run_dir = tmp_path / "run"
    _opened(run_dir, "rvtest-aws-1a2b", kept=False)

    def sweep(prefixes):
        raise RuntimeError("vast is down")

    checked = []
    problems, kept = livekit.end_of_run(run_dir, "ray-vast-test-20261016", sweep, lambda n, r: checked.append(n) or [],
                                        lambda: ["F17/x.txt"])
    assert problems == ["vast sweep failed: RuntimeError: vast is down", "the secret scan found 1 file(s): F17/x.txt"]
    assert checked == ["rvtest-aws-1a2b"] and kept == []


def test_the_vast_spend_of_one_cluster_comes_from_its_timeline_and_a_failed_sample_keeps_the_last_known_instances():
    rows = [{"t": 0.0, "instances": [{"id": 1, "dph_total": 0.36}, {"id": 2, "dph_total": 0.36}]},
            {"t": 1800.0, "error": "ConnectionError"},                  # still 2 instances
            {"t": 3600.0, "instances": [{"id": 1, "dph_total": 0.36}]},
            {"t": 7200.0, "instances": []}]
    assert livekit.timeline_spend(rows) == 0.72 + 0.36
    assert livekit.timeline_spend([]) == 0.0


def test_every_cluster_object_of_a_run_is_noted_for_the_end_of_run_check_also_one_without_a_cluster_fixture(tmp_path, monkeypatch):
    import sys
    from pathlib import Path
    monkeypatch.syspath_prepend(str(Path(livekit.__file__).parent))
    monkeypatch.setenv("RAYVAST_LIVE_RESULTS", str(tmp_path / "results"))
    monkeypatch.setenv("RAYVAST_LIVE_RUN", "run1")
    monkeypatch.setenv("VAST_API_KEY", "fake-key")
    sys.modules.pop("cluster", None)
    import cluster
    config = {"cluster_name": "rvtest-a03b-1a2b", "head_node_type": "head",
              "available_node_types": {"head": {"node_config": {"region": "us-west-2"}}}}
    made = cluster.Cluster("none", config, tmp_path / "out", tmp_path / "home")
    noted = json.loads((tmp_path / "results" / "_session" / "runs" / "run1" / "opened" / "rvtest-a03b-1a2b.json").read_text())
    assert noted == {"name": "rvtest-a03b-1a2b", "region": "us-west-2", "kept": False, "yaml": str(made.yaml), "home": str(tmp_path / "home")}


def test_a_check_detail_with_line_breaks_stays_on_one_row_of_the_summary(tmp_path):
    record = livekit.Recorder(tmp_path, "A03", markers=["aws"], fixture="none")
    record.check("up was stopped", True, "Traceback\n  File x\nKeyboardInterrupt\n")
    record.finish("passed", "")
    rows = [line for line in livekit.summary_table(tmp_path, ["A03"]).splitlines() if line.startswith("| A03")]
    assert len(rows) == 1 and "KeyboardInterrupt" in rows[0] and rows[0].endswith("`" + tmp_path.name + "/A03/` |")
