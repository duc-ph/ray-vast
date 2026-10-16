"""The live suite (docs/test-plan.md, "Live suite"). One test for each scenario of the claim matrix; its name starts with
`test_<ID>_`. Markers select groups: `pytest tests/live -m "aws or lifecycle"` starts only the clusters that those tests use.

- Every test writes `archive/results/<UTC date>/<ID>/result.json` (the `record` fixture, autouse), so no pass exists
  without a record. `python tests/live/summarize.py` writes the table into docs/test-results.md.
- The cluster fixtures (`aws_only`, `lifecycle`, `libraries`) are session-scoped: `up` once, `down` at the end of the
  session, also after a failure, Ctrl-C or SIGTERM. The vast spend of each group goes to `_session/<fixture>/spend.json`.
- At the end of the session: a vast label sweep to 0 (the clusters of the session and `ray-vast-test-<date>`), the AWS
  leftover check of each cluster, and a secret scan of the results directory.
Environment: RAYVAST_LIVE_PROFILE (admin AWS profile, default `personal`), RAYVAST_LIVE_RESULTS (default
<repo>/archive/results/<UTC date>). The vast key comes from VAST_API_KEY or ~/.config/vastai/vast_api_key.
"""
import json
import os
import secrets
import time
from pathlib import Path

import pytest
import yaml

import livekit
from cluster import ADMIN_PROFILE, REPO, Cluster, aws_leftovers, credit_sample, invoices, sweep_label

livekit.sigterm_as_interrupt()

DATE = time.strftime("%Y-%m-%d", time.gmtime())
TEST_LABEL = f"ray-vast-test-{DATE.replace('-', '')}"
RESULTS = Path(os.environ.get("RAYVAST_LIVE_RESULTS", REPO / "archive" / "results" / DATE))
SESSION = RESULTS / "_session"
CLUSTERS = Path(__file__).parent / "clusters"
GROUP_MARKERS = ("free", "aws", "lifecycle", "failure", "security", "limits", "libraries", "release")
CLUSTER_FIXTURES = ("aws_only", "lifecycle", "libraries", "release")
FILE_ORDER = ["test_free.py", "test_aws.py", "test_lifecycle.py", "test_libraries.py", "test_release.py"]
EC2_PRICE = {"t3.large": 0.0832, "t3.medium": 0.0416}  # $/h, us regions on-demand
EXTRA_SECRETS = []  # the secret access key of the restricted user, for the scan
OPENED = []  # every cluster of the session: the final sweep, AWS check and secret scan use it


def pytest_configure(config):
    for name in GROUP_MARKERS:
        config.addinivalue_line("markers", f"{name}: live suite group (docs/test-plan.md, claim matrix)")


@pytest.hookimpl(tryfirst=True)  # before the -m selection, so the default run deselects the live tests
def pytest_collection_modifyitems(config, items):
    here = Path(__file__).parent
    ours = [i for i in items if Path(i.fspath).parent == here]
    for item in ours:
        item.add_marker(pytest.mark.live)
        if item.get_closest_marker("timeout") is None:
            item.add_marker(pytest.mark.timeout(0))  # a live scenario runs for minutes to hours; its own waits have limits
    config.stash[RAN_LIVE] = bool(ours)
    rank = {name: n for n, name in enumerate(FILE_ORDER)}
    items.sort(key=lambda i: rank.get(Path(i.fspath).name, -1) if Path(i.fspath).parent == here else -1)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    item.stash.setdefault(REPORTS, {})[report.when] = report


REPORTS = pytest.StashKey[dict]()
RAN_LIVE = pytest.StashKey[bool]()


def scenario_id(node):
    """`test_F01_shim_rules[H1]` gives `F01[H1]`."""
    name = node.name
    base, _, param = name.partition("[")
    return base.split("_")[1] + (f"[{param}" if param else "")


@pytest.fixture(autouse=True)
def record(request):
    node = request.node
    markers = [m.name for m in node.iter_markers() if m.name in GROUP_MARKERS]
    fixture = next((f for f in CLUSTER_FIXTURES if f in request.fixturenames), "none")
    rec = livekit.Recorder(RESULTS, scenario_id(node), markers, fixture)
    yield rec
    reports = node.stash.get(REPORTS, {})
    failed = next((r for r in (reports.get("setup"), reports.get("call")) if r is not None and r.failed), None)
    skipped = next((r for r in (reports.get("setup"), reports.get("call")) if r is not None and r.skipped), None)
    if failed is not None:
        crash = getattr(failed.longrepr, "reprcrash", None)
        rec.finish("failed", crash.message if crash else str(failed.longrepr)[-600:])
    elif skipped is not None:
        rec.finish("skipped", str(skipped.longrepr[-1]) if isinstance(skipped.longrepr, tuple) else "skipped")
    else:
        rec.finish("passed", "")


@pytest.fixture(scope="session")
def results_dir():
    RESULTS.mkdir(parents=True, exist_ok=True)
    return RESULTS


def _load(fixture, suffix=None):
    config = yaml.safe_load((CLUSTERS / f"{fixture}.yaml").read_text())
    config["cluster_name"] = f"{config['cluster_name']}-{suffix or secrets.token_hex(2)}"
    return config


def group_lifetime(fixture, make, tmp_path_factory):
    """`up` a cluster for one group, and `down` it at the end, with the spend of the group."""
    group_dir = SESSION / fixture
    group_dir.mkdir(parents=True, exist_ok=True)
    before = credit_sample()
    started = time.time()
    holder = {}

    def up():
        cluster = make(group_dir, tmp_path_factory.mktemp(f"home-{fixture}"))
        holder["cluster"] = cluster
        OPENED.append(cluster)
        cluster.start_poller()
        return cluster.up()

    def down():
        cluster = holder.get("cluster")
        if cluster is not None:
            done = cluster.down()
            (group_dir / "down_final.log").write_text(done.stdout + done.stderr)
            if "left: nothing" not in done.stdout:  # once more, as the test plan says
                cluster.down()
            cluster.stop_poller()
        time.sleep(30)  # vast bills the last minutes late; the credit settles
        after = credit_sample()
        spend = livekit.vast_spend(before, after, invoices())
        instance_type = (cluster.config["available_node_types"]["head"]["node_config"].get("instance_type", "t3.large")
                         if cluster else "t3.large")
        hours = (time.time() - started) / 3600
        spend.update(group=fixture, cluster=cluster.name if cluster else None, hours=round(hours, 2),
                     ec2_estimate=round(hours * EC2_PRICE.get(instance_type, 0.1), 3))
        (group_dir / "spend.json").write_text(json.dumps(spend, indent=1))

    yield from livekit.lifetime(up, down)


@pytest.fixture(scope="session")
def aws_only(tmp_path_factory, results_dir):
    yield from group_lifetime("aws_only", lambda d, home: Cluster("aws_only", _load("aws_only"), d, home), tmp_path_factory)


@pytest.fixture(scope="session")
def restricted_user(tmp_path_factory, results_dir):
    """The IAM user `ray-vast-livetest-<date>` with only the policy `ray-vast-user`, made with the getting-started
    commands (as the admin), and a credentials file with its profile. Deleted at the end of the session."""
    from restricted import RestrictedUser
    user = RestrictedUser(SESSION / "restricted_user", tmp_path_factory.mktemp("iam"), DATE)

    def create():
        user.create()
        EXTRA_SECRETS.append(user.secret)
        return user
    yield from livekit.lifetime(create, user.delete)


@pytest.fixture(scope="session")
def lifecycle(tmp_path_factory, results_dir, restricted_user):
    def make(d, home):
        return Cluster("lifecycle", _load("lifecycle"), d, home, profile=restricted_user.profile,
                       credentials_file=restricted_user.credentials_file)
    yield from group_lifetime("lifecycle", make, tmp_path_factory)


@pytest.fixture(scope="session")
def libraries(tmp_path_factory, results_dir):
    from libraries_setup import make_libraries_cluster
    yield from group_lifetime("libraries", make_libraries_cluster, tmp_path_factory)


def pytest_sessionfinish(session, exitstatus):
    """The end of every paid session: a vast label sweep to 0, the AWS check, the secret scan."""
    if not RESULTS.exists() or not session.config.stash.get(RAN_LIVE, False) or not session.items:
        return
    SESSION.mkdir(parents=True, exist_ok=True)
    problems = []
    try:
        lines = sweep_label([TEST_LABEL] + [f"ray-vast:{c.name}" for c in OPENED])
        (SESSION / f"sweep_{int(time.time())}.log").write_text("\n".join(lines) + "\n")
    except Exception as e:  # noqa: BLE001 - the report must name it
        problems.append(f"vast sweep failed: {type(e).__name__}: {e}")
    for cluster in OPENED:
        left = aws_leftovers(cluster.name, cluster.region, ADMIN_PROFILE)
        (SESSION / f"aws_check_{cluster.name}.log").write_text(("\n".join(left) or "aws left: nothing") + "\n")
        if left:
            problems.append(f"AWS left for {cluster.name}: {left}")
    secret_values = [s for c in OPENED for s in c.secrets] + EXTRA_SECRETS + [os.environ.get("VAST_API_KEY") or _vast_key_or_empty()]
    found = livekit.scan_secrets(RESULTS, secret_values)
    (SESSION / "secret_scan.txt").write_text("".join(f"{p.relative_to(RESULTS)}\n" for p in found) or "no file holds a secret\n")
    if found:
        problems.append(f"the secret scan found {len(found)} file(s): see _session/secret_scan.txt")
    if problems:
        (SESSION / f"problems_{int(time.time())}.txt").write_text("\n".join(problems) + "\n")
        print("\nLIVE SUITE PROBLEMS:\n" + "\n".join(problems))
        session.exitstatus = 1


def _vast_key_or_empty():
    try:
        from cluster import vast_key
        return vast_key()
    except Exception:  # noqa: BLE001
        return ""
