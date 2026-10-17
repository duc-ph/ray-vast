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


def test_find_host_takes_a_lower_bound_for_a_number_of_the_probe():
    hosts = iter([{"machine_id": 32816, "pids_max": 1280, "cgroup": "v1", "cpus": 5, "limited": True},
                  {"machine_id": 36376, "pids_max": 9728, "cgroup": "v1", "cpus": 38, "limited": True}])
    host, tries = livekit.find_host({"cpus": {"gte": 32}, "limited": True}, lambda blocked: next(hosts), lambda probe: None)
    assert host["machine_id"] == 36376 and [t["match"] for t in tries] == [False, True]


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


def test_the_matrix_parser_reads_id_markers_and_fixture_of_each_row_of_the_matrix_tables_only():
    text = """
| ID | Name | Markers | Fixture | Claims proved | Pass criterion | Origin | Destructive |
|---|---|---|---|---|---|---|---|
| F01 | Shim | free | none | architecture.md:86-97 | 14 tests | C0.1 | no |
| A07 | Boundary | aws, security | aws_only | iam.md:78 | denied | audit B | yes |
| not a row | x | y | z |

| ID | Name | Claims | Proof now |
|---|---|---|---|
| A04 | Cluster already exists | cli.md:51 | F03 |
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


@pytest.mark.parametrize("markers, fixtures, host, group", [
    (["aws"], ["record", "aws_only", "tmp_path"], None, "aws_only"),
    (["aws", "exclusive_account"], ["record", "new_cluster", "tmp_path"], None, "exclusive_account"),
    (["lifecycle"], ["record", "lifecycle", "session_env"], None, "lifecycle"),
    (["aws"], ["record", "restricted_user"], None, "lifecycle"),        # one restricted IAM user for each run
    (["libraries"], ["record", "libraries", "env"], "H1", "libraries-h1"),
    (["libraries"], ["record", "libraries", "env", "s3"], "gpu2", "libraries-gpu2"),
    (["release"], ["record", "release"], None, "release"),
    (["box_disk", "free"], ["record", "dist"], None, "box_disk"),
    (["box_disk", "free"], ["record", "tmp_path"], None, "box_disk"),
    (["free"], ["record", "dist"], None, None),                          # a fixture alone is no declared need
    (["free"], ["record"], None, None),
    (["aws", "failure"], ["record", "new_cluster", "tmp_path"], None, None),
])
def test_each_cluster_and_each_declared_need_gets_one_xdist_group(markers, fixtures, host, group):
    assert livekit.xdist_group(markers, fixtures, host) == group


@pytest.mark.parametrize("markers, fixtures", [
    (["exclusive_account"], ["record", "aws_only"]),   # it would wait for itself: its cluster `up` waits for it
    (["box_disk"], ["record", "lifecycle"]),
    ([], ["record", "aws_only", "restricted_user"]),
])
def test_a_test_with_two_groups_is_an_error(markers, fixtures):
    with pytest.raises(ValueError, match="two xdist groups"):
        livekit.xdist_group(markers, fixtures, None)


def test_the_wait_for_tests_returns_once_each_has_ended_and_names_the_missing_ones_at_its_deadline(tmp_path):
    run = livekit.Run(tmp_path / "results", "run1")
    other = livekit.Run(tmp_path / "results", "run1")  # another process of the same run
    run.wait_ended([], timeout_s=0)
    other.mark_ended("X01")
    with pytest.raises(TimeoutError, match=r"did not end in 0.2 s: X02$"):
        run.wait_ended(["X01", "X02"], timeout_s=0.2, poll_s=0.05)
    other.mark_ended("X02[H1]")
    other.mark_ended("X02")
    run.wait_ended(["X01", "X02", "X02[H1]"], timeout_s=0)


def test_no_scenario_id_of_the_matrix_is_in_the_infrastructure_modules():
    """The infrastructure knows declared needs (markers, fixtures), never a scenario: a scenario ID is only in the test
    names, the matrix and the records."""
    import re
    from pathlib import Path
    live = Path(livekit.__file__).parent
    ids = {r["id"] for r in livekit.matrix_rows((live.parents[2] / "docs" / "test-plan.md").read_text())}
    assert len(ids) > 40
    found = {name: sorted(set(re.findall(r"\b(?:" + "|".join(sorted(ids)) + r")\b", (live / name).read_text())))
             for name in ("conftest.py", "livekit.py", "cluster.py", "libraries_setup.py", "restricted.py", "summarize.py")}
    assert {name: f for name, f in found.items() if f} == {}


def _opened(run, name, kept, region="us-west-2"):
    from types import SimpleNamespace
    run.register(SimpleNamespace(name=name, region=region, kept=kept, yaml=f"/r/{name}.yaml", home=f"/h/{name}", secrets=[]))


def _ended(run, name, problems, kept=False):
    (run.dir / "ended").mkdir(parents=True, exist_ok=True)
    (run.dir / "ended" / f"{name}.json").write_text(json.dumps({"name": name, "kept": kept, "problems": problems}))


def test_the_end_of_a_run_does_only_the_run_wide_guards_and_sums_up_the_problems_of_each_ended_cluster(tmp_path):
    run = livekit.Run(tmp_path, "run1")
    _opened(run, "rvtest-aws-1a2b", kept=False)
    _opened(run, "gpu-24gb", kept=False)
    _opened(run, "rvtest-life-keep", kept=True, region="us-east-2")
    _ended(run, "rvtest-aws-1a2b", ["AWS left: volume vol-1"])
    _ended(run, "gpu-24gb", [])
    _ended(run, "rvtest-life-keep", [], kept=True)
    (run.dir / "scan_gw1.txt").write_text("A10/denials.txt\n")  # a record that no cluster owns
    swept, checked = [], []

    def sweep(prefixes):
        swept.append(prefixes)
        return ["0 left"]

    def account_check(regions, skip):
        checked.append((regions, skip))
        return ["security group sg-9 (ray-vast-rvtest-old-0000-head)"]

    problems, kept = livekit.end_of_run(run.dir, "ray-vast-test-20261016", sweep, account_check)
    assert swept == [["ray-vast-test-20261016"]]                       # each ended cluster had its own sweep
    assert checked == [(["us-east-2", "us-west-2"], ["rvtest-life-keep"])]
    assert problems == ["rvtest-aws-1a2b: AWS left: volume vol-1",
                        "AWS account check: security group sg-9 (ray-vast-rvtest-old-0000-head)",
                        "the secret scan found 1 file(s): A10/denials.txt"]
    assert len(kept) == 1 and "KEPT CLUSTER rvtest-life-keep" in kept[0] and "ray-vast down /r/rvtest-life-keep.yaml" in kept[0]


def test_a_cluster_that_joined_the_run_but_never_ended_is_a_problem_and_the_final_sweep_takes_its_label(tmp_path):
    run = livekit.Run(tmp_path, "run1")
    _opened(run, "rvtest-lib-h1-1a2b", kept=False)  # its worker process died before the teardown

    def sweep(prefixes):
        assert prefixes == ["ray-vast-test-20261016", "ray-vast:rvtest-lib-h1-1a2b"]
        raise RuntimeError("vast is down")

    problems, kept = livekit.end_of_run(run.dir, "ray-vast-test-20261016", sweep, lambda regions, skip: [])
    assert problems == ["vast sweep failed: RuntimeError: vast is down",
                        "rvtest-lib-h1-1a2b joined the run but did not end: run HOME=/h/rvtest-lib-h1-1a2b ray-vast down "
                        "/r/rvtest-lib-h1-1a2b.yaml"]
    assert kept == []


def test_the_vast_spend_of_one_cluster_comes_from_its_timeline_and_a_failed_sample_keeps_the_last_known_instances():
    rows = [{"t": 0.0, "instances": [{"id": 1, "dph_total": 0.36}, {"id": 2, "dph_total": 0.36}]},
            {"t": 1800.0, "error": "ConnectionError"},                  # still 2 instances
            {"t": 3600.0, "instances": [{"id": 1, "dph_total": 0.36}]},
            {"t": 7200.0, "instances": []}]
    assert livekit.timeline_spend(rows) == 0.72 + 0.36
    assert livekit.timeline_spend([]) == 0.0


def test_the_run_identity_is_set_by_the_first_process_and_every_later_process_of_the_run_inherits_it(tmp_path):
    environ = {}
    first = livekit.Run.of_process(environ, tmp_path / "results", "120000-11")
    worker = livekit.Run.of_process(dict(environ), tmp_path / "other", "120005-12")  # an xdist worker: the controller's env
    assert first.dir == worker.dir == tmp_path / "results" / "_session" / "runs" / "120000-11"
    assert first.results == worker.results == tmp_path / "results"


def test_a_record_of_the_run_goes_under_its_record_root_and_marks_the_run_as_live(tmp_path):
    run = livekit.Run(tmp_path / "results", "run1")
    assert not run.live
    record = run.record("A05", markers=["aws"], fixture="aws_only")
    record.finish("passed", "")
    assert (tmp_path / "results" / "A05" / "result.json").exists() and run.live


def _cluster_of(run, tmp_path, monkeypatch, **kwargs):
    import sys
    from pathlib import Path
    monkeypatch.syspath_prepend(str(Path(livekit.__file__).parent))
    monkeypatch.setenv("VAST_API_KEY", "fake-key")
    sys.modules.pop("cluster", None)
    import cluster
    config = {"cluster_name": "rvtest-a03b-1a2b", "head_node_type": "head",
              "available_node_types": {"head": {"node_config": {"region": "us-west-2"}}}}
    return cluster.Cluster(run, "none", config, tmp_path / "out", tmp_path / "home", **kwargs)


@pytest.mark.parametrize("kept", [False, True])
def test_a_cluster_joins_its_run_once_when_it_is_made_with_whether_the_run_keeps_it(tmp_path, monkeypatch, kept):
    run = livekit.Run(tmp_path / "results", "run1")
    made = _cluster_of(run, tmp_path, monkeypatch, kept=kept)
    opened = list((run.dir / "opened").glob("*.json"))
    assert [p.name for p in opened] == ["rvtest-a03b-1a2b.json"] and made.kept is kept
    assert json.loads(opened[0].read_text()) == {"name": "rvtest-a03b-1a2b", "region": "us-west-2", "kept": kept,
                                                 "yaml": str(made.yaml), "home": str(tmp_path / "home")}


def test_the_secret_sink_of_a_run_holds_its_own_secrets_and_the_later_secrets_of_each_cluster(tmp_path, monkeypatch):
    run = livekit.Run(tmp_path / "results", "run1")
    made = _cluster_of(run, tmp_path, monkeypatch)
    run.secret("iam-user-secret")
    made.secrets.append("cluster-token")  # the token and the join tokens arrive after `up`
    assert sorted(run.secret_values()) == ["cluster-token", "iam-user-secret"]


def test_a_check_detail_with_line_breaks_stays_on_one_row_of_the_summary(tmp_path):
    record = livekit.Recorder(tmp_path, "A03", markers=["aws"], fixture="none")
    record.check("up was stopped", True, "Traceback\n  File x\nKeyboardInterrupt\n")
    record.finish("passed", "")
    rows = [line for line in livekit.summary_table(tmp_path, ["A03"]).splitlines() if line.startswith("| A03")]
    assert len(rows) == 1 and "KeyboardInterrupt" in rows[0] and rows[0].endswith("`" + tmp_path.name + "/A03/` |")


def test_log_epoch_reads_a_monitor_log_time_in_the_time_zone_of_the_head_container():
    # The rayproject/ray images set TZ=America/Los_Angeles: 17:20:40 PDT (-0700) is 00:20:40 UTC the next day.
    line = "2026-10-16 17:20:40,978\tINFO node_provider.py:485 -- Launching 1 nodes of type h1."
    assert livekit.log_epoch(line, "-0700") == 1792196440.978
    assert livekit.log_epoch(line, "+0000") == 1792171240.978
    assert livekit.log_epoch("no time here", "-0700") is None


class _Report:
    def __init__(self, outcome, longrepr=None):
        self.failed, self.skipped, self.passed = outcome == "failed", outcome == "skipped", outcome == "passed"
        self.longrepr = longrepr


def test_a_test_without_a_call_report_is_not_recorded_as_passed():
    # Ctrl-C or SIGTERM during the test: pytest makes no call report, and the record must not say "passed".
    assert livekit.outcome_of({"setup": _Report("passed")})[0] == "failed"
    assert livekit.outcome_of({"setup": _Report("passed"), "call": _Report("passed")}) == ("passed", "")
    assert livekit.outcome_of({"setup": _Report("skipped", ("f", 1, "Skipped: kept")), "call": None})[0] == "skipped"


class _Done:
    def __init__(self, stdout="left: nothing\n", returncode=0):
        self.stdout, self.stderr, self.returncode = stdout, "", returncode


class _FakeCluster:
    """A cluster at the seam of `livekit.end_cluster`: its `down` outputs come from `downs`, one for each call."""

    def __init__(self, run, tmp_path, name="rvtest-life-1a2b", kept=False, downs=None, last_down=None):
        self.name, self.region, self.kept, self.last_down = name, "us-east-2", kept, last_down
        self.yaml, self.home, self.secrets = tmp_path / f"{name}.yaml", tmp_path / "home", []
        self.records = [tmp_path / "results" / "_session" / name]
        self.records[0].mkdir(parents=True)
        self.downs, self.down_calls = list(downs or [_Done()]), 0
        run.register(self)

    def down(self):
        self.down_calls += 1
        self.last_down = self.downs.pop(0) if self.downs else _Done()
        return self.last_down


def _guards():
    """A fake vast sweep and AWS check that log their calls. `left` sets what each one finds."""
    calls = {"sweep": [], "aws": [], "destroyed": [], "left": []}

    def sweep(prefixes):
        calls["sweep"].append(prefixes)
        return ["0 left"] if not calls["destroyed"] else ["1 left", *calls["destroyed"], "0 left"]

    def aws_check(name, region):
        calls["aws"].append((name, region))
        return calls["left"]
    return calls, sweep, aws_check


def test_a_clean_cluster_ends_with_one_down_its_label_sweep_its_aws_check_and_no_problem(tmp_path):
    run = livekit.Run(tmp_path / "results", "run1")
    cluster = _FakeCluster(run, tmp_path)
    calls, sweep, aws_check = _guards()
    assert livekit.end_cluster(run, cluster, sweep, aws_check) == []
    assert cluster.down_calls == 1
    assert calls["sweep"] == [["ray-vast:rvtest-life-1a2b"]] and calls["aws"] == [("rvtest-life-1a2b", "us-east-2")]
    ended = json.loads((run.dir / "ended" / "rvtest-life-1a2b.json").read_text())
    assert ended["problems"] == [] and ended["kept"] is False


def test_a_down_that_leaves_something_is_a_problem_and_down_runs_once_more(tmp_path):
    run = livekit.Run(tmp_path / "results", "run1")
    cluster = _FakeCluster(run, tmp_path, downs=[_Done("deleted key pair x\nleft: security group sg-1\n", 1), _Done()])
    calls, sweep, aws_check = _guards()
    assert livekit.end_cluster(run, cluster, sweep, aws_check) == [
        "down left something (exit 1): deleted key pair x left: security group sg-1"]
    assert cluster.down_calls == 2


def test_an_instance_that_the_sweep_still_finds_and_a_resource_that_aws_still_has_are_problems(tmp_path):
    run = livekit.Run(tmp_path / "results", "run1")
    cluster = _FakeCluster(run, tmp_path)
    calls, sweep, aws_check = _guards()
    calls["destroyed"].append("destroyed 41 (ray-vast:rvtest-life-1a2b)")
    calls["left"].append("volume vol-1")
    assert livekit.end_cluster(run, cluster, sweep, aws_check) == [
        "vast: 1 instance(s) with the cluster label after down: destroyed 41 (ray-vast:rvtest-life-1a2b)", "AWS left: volume vol-1"]


def test_the_secret_scan_of_a_cluster_reads_its_own_records_only_never_a_directory_with_a_similar_name(tmp_path):
    run = livekit.Run(tmp_path / "results", "run1")
    cluster = _FakeCluster(run, tmp_path)
    for scenario in ("L03", "LB03"):  # an `L*` glob matched both (the 2026-10-17 bug)
        (tmp_path / "results" / scenario).mkdir(parents=True)
        (tmp_path / "results" / scenario / "probe.log").write_text("token: cluster-token-value\n")
    cluster.records.append(tmp_path / "results" / "L03")
    cluster.secrets.append("cluster-token-value")  # it arrives after `up`
    calls, sweep, aws_check = _guards()
    assert livekit.end_cluster(run, cluster, sweep, aws_check) == [
        f"the secret scan found 1 file(s): {tmp_path / 'results' / 'L03' / 'probe.log'}"]


def test_a_kept_cluster_gets_no_down_no_sweep_and_no_aws_check_but_its_records_are_scanned(tmp_path):
    run = livekit.Run(tmp_path / "results", "run1")
    cluster = _FakeCluster(run, tmp_path, name="rvtest-life-keep", kept=True)
    (cluster.records[0] / "cli.log").write_text("-----BEGIN RSA PRIVATE KEY-----\n")
    calls, sweep, aws_check = _guards()
    problems = livekit.end_cluster(run, cluster, sweep, aws_check, after_down=lambda done: [f"S3 after down {done}"])
    assert problems == ["S3 after down None", f"the secret scan found 1 file(s): {cluster.records[0] / 'cli.log'}"]
    assert cluster.down_calls == 0 and calls["sweep"] == [] and calls["aws"] == []
    assert json.loads((run.dir / "ended" / "rvtest-life-keep.json").read_text())["kept"] is True


def test_a_cluster_that_a_scenario_already_ended_cleanly_gets_no_second_down_but_still_its_sweep_and_aws_check(tmp_path):
    run = livekit.Run(tmp_path / "results", "run1")
    cluster = _FakeCluster(run, tmp_path, name="gpu-24gb", last_down=_Done())  # R02 ran the tutorial's `down`
    calls, sweep, aws_check = _guards()
    seen = []
    assert livekit.end_cluster(run, cluster, sweep, aws_check, after_down=lambda done: seen.append(done) or []) == []
    assert cluster.down_calls == 0 and seen == [cluster.last_down]
    assert calls["sweep"] == [["ray-vast:gpu-24gb"]] and calls["aws"] == [("gpu-24gb", "us-east-2")]


def test_a_cluster_whose_up_failed_half_way_still_gets_the_sweep_and_the_aws_check_when_down_or_vast_fails(tmp_path):
    run = livekit.Run(tmp_path / "results", "run1")
    cluster = _FakeCluster(run, tmp_path)

    def down():
        raise TimeoutError("ray-vast down: 1800 s")
    cluster.down = down

    def sweep(prefixes):
        raise ConnectionError("vast is down")
    checked = []
    problems = livekit.end_cluster(run, cluster, sweep, lambda name, region: checked.append(name) or ["key pair ray-vast-x"])
    assert problems == ["down failed: TimeoutError: ray-vast down: 1800 s", "vast sweep failed: ConnectionError: vast is down",
                        "AWS left: key pair ray-vast-x"]
    assert checked == ["rvtest-life-1a2b"]
    assert json.loads((run.dir / "ended" / "rvtest-life-1a2b.json").read_text())["problems"] == problems


def test_the_process_scan_reads_each_record_of_the_process_that_no_cluster_owns(tmp_path):
    run = livekit.Run(tmp_path / "results", "run1")
    run.secret("iam-user-secret")
    run.record("A10", ["aws"], "none").evidence("denials.txt", "iam-user-secret")
    (run.group_dir("restricted_user") / "user.log").write_text("iam-user-secret")
    cluster = _FakeCluster(run, tmp_path)
    owned = run.record("L03", ["lifecycle"], "lifecycle")
    owned.evidence("probe.log", "iam-user-secret")  # scanned when its cluster ends, not again here
    cluster.records.append(owned.dir)
    run.record("F01", ["free"], "none").evidence("ok.txt", "nothing secret")
    assert run.scan_unowned() == ["A10/denials.txt", "_session/restricted_user/user.log"]


def test_in_lb11_refused_forks_are_data_so_a_tuned_run_with_refused_forks_still_passes(monkeypatch):
    from pathlib import Path
    c10 = Path(livekit.__file__).resolve().parents[3] / "tests" / "manual" / "c10"
    if not (c10 / "run_c10.py").exists():
        pytest.skip("the C10 runner is in the repository, not in the package tree (F18 copies the package tree only)")
    monkeypatch.syspath_prepend(str(c10))
    from run_c10 import verdict
    pre = {"node_id": "n1", "pids.events": "max 0", "pids.max": "1280"}
    post = {"node_id": "n1", "pids.events": "max 7", "pids.max": "1280"}
    warnings = ["WARNING worker 41 is at its process limit: 7 refused forks/threads (+7), pids 1280 of 1280"]
    assert verdict("tuned", {"kind": "COMPLETED"}, pre, post, warnings)[0] == "FAIL"  # the C10 runner's own rule
    assert verdict("tuned", {"kind": "COMPLETED"}, pre, post, warnings, refused_fails=False) == (
        "PASS", ["completed, node alive, refused forks 7 (data, not a pass criterion)"])


def test_a_run_without_a_cluster_calls_no_cloud_guard_but_still_sums_up_the_process_scans(tmp_path):
    run = livekit.Run(tmp_path, "run1")
    run.record("F01", ["free"], "none")
    (run.dir / "scan_gw0.txt").write_text("F17/x.txt\n")

    def no_cloud(*args):
        raise AssertionError("a free run calls no cloud API")
    assert livekit.end_of_run(run.dir, "ray-vast-test-20261016", no_cloud, no_cloud) == (
        ["the secret scan found 1 file(s): F17/x.txt"], [])
