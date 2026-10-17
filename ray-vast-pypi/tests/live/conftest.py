"""The live suite (docs/test-plan.md, "Live suite"). One test for each scenario of the claim matrix; its name starts with
`test_<ID>_`. Markers select groups: `pytest tests/live -m "aws or lifecycle"` starts only the clusters that those tests use.

- Every test writes `archive/results/<UTC date>/<ID>/result.json` (the `record` fixture, autouse), so no pass exists
  without a record. `python tests/live/summarize.py` writes the table into docs/test-results.md.
- The cluster fixtures (`aws_only`, `lifecycle`, `libraries`) run `up` once and `down` at the end of the session, also
  after a failure, Ctrl-C or SIGTERM. `libraries` is one cluster for each host (H1, H2, H3, gpu2).
- Parallel runs: `pytest tests/live -n 12` (pytest-xdist; this file makes it `--dist loadgroup`). Each cluster and the
  tests that share its setup are one xdist group (`livekit.xdist_group`), so one worker process owns the cluster: one `up`,
  one `down`. Other tests go to any worker. The clusters wait for A02 (it deletes the account boundary) when it runs.
- `--keep-cluster`: a cluster is named `<name>-keep`, its HOME is ~/.cache/ray-vast-live/<group>; a later run reuses it
  while its head runs, and no `down` runs at the end (the scenarios that end the cluster skip). A run without the option
  ends a kept cluster of its groups first, then makes a fresh one.
- At the end of the run, once, after every worker ended: a vast label sweep to 0 (the clusters of the run that are not
  kept, and `ray-vast-test-<date>`), the AWS leftover check of each of them, and a secret scan of the results directory
  (`livekit.end_of_run`; each worker scans for its own secret values first).
Environment: RAYVAST_LIVE_PROFILE (admin AWS profile, default `personal`), RAYVAST_LIVE_RESULTS (default
<repo>/archive/results/<UTC date>). The vast key comes from VAST_API_KEY or ~/.config/vastai/vast_api_key.
"""
import contextlib
import json
import os
import secrets
import shutil
import time
from pathlib import Path

import pytest
import yaml

import livekit
from cluster import ADMIN_PROFILE, REPO, Cluster, aws_leftovers, credit_sample, invoices, sweep_label, vast_key

livekit.sigterm_as_interrupt()

DATE = time.strftime("%Y-%m-%d", time.gmtime())
TEST_LABEL = f"ray-vast-test-{DATE.replace('-', '')}"
# The controller sets these once; the xdist workers that it starts inherit them, so all processes of a run agree.
RESULTS = Path(os.environ.setdefault("RAYVAST_LIVE_RESULTS", str(REPO / "archive" / "results" / DATE)))
RUN_DIR = RESULTS / "_session" / "runs" / os.environ.setdefault("RAYVAST_LIVE_RUN", f"{time.strftime('%H%M%S', time.gmtime())}-{os.getpid()}")
SESSION = RESULTS / "_session"
CLUSTERS = Path(__file__).parent / "clusters"
KEEP_HOME = Path.home() / ".cache" / "ray-vast-live"  # the HOME of each kept cluster (its state file and SSH key)
GROUP_MARKERS = ("free", "aws", "lifecycle", "failure", "security", "limits", "libraries", "release")
CLUSTER_FIXTURES = ("aws_only", "lifecycle", "libraries", "release")
FILE_ORDER = ["test_free.py", "test_aws.py", "test_lifecycle.py", "test_libraries.py", "test_release.py"]
EC2_PRICE = {"t3.large": 0.0832, "t3.medium": 0.0416}  # $/h, us regions on-demand
A02_WAIT_S = 1200
EXTRA_SECRETS = []  # the secret access keys of the IAM test users, for the scan
OPENED = []  # the clusters of this process: its secret values go to the scan

REPORTS = pytest.StashKey[dict]()
A02_SELECTED = pytest.StashKey[bool]()
END_LINES = pytest.StashKey[list]()
CREDIT_BEFORE = pytest.StashKey[dict]()


def is_worker(config):
    return hasattr(config, "workerinput")


def pytest_addoption(parser):
    parser.addoption("--keep-cluster", action="store_true", default=False,
                     help="live suite: reuse a running cluster named <name>-keep, and do not end it at the end of the run")


def pytest_configure(config):
    for name in GROUP_MARKERS:
        config.addinivalue_line("markers", f"{name}: live suite group (docs/test-plan.md, claim matrix)")
    config.addinivalue_line("markers", "host(name): the libraries host of a test (H1, H2, H3 or gpu2): its cluster and xdist group")
    # pytest -n N: --dist loadgroup, so the groups keep each cluster in one worker. A worker parses the command line again
    # (dist "load"), and xdist adds the group to the test ids only when its own `loadgroup` option is set.
    if is_worker(config):
        config.option.loadgroup = True
    elif config.getoption("dist", "no") == "load":
        config.option.dist = "loadgroup"
    elif config.getoption("dist", "no") not in ("no", "loadgroup"):
        raise pytest.UsageError(f"the live suite runs with --dist loadgroup, not --dist {config.getoption('dist')}")


@pytest.hookimpl(tryfirst=True)  # before the -m selection (the default run deselects the live tests) and before xdist reads the groups
def pytest_collection_modifyitems(config, items):
    here = Path(__file__).parent
    ours = [i for i in items if Path(i.fspath).parent == here]
    for item in ours:
        item.add_marker(pytest.mark.live)
        if item.get_closest_marker("timeout") is None:
            item.add_marker(pytest.mark.timeout(0))  # a live scenario runs for minutes to hours; its own waits have limits
        group = livekit.xdist_group(scenario_id(item).split("[")[0], item.fixturenames, livekit.host_of(item))
        if group:
            item.add_marker(pytest.mark.xdist_group(group))
    rank = {name: n for n, name in enumerate(FILE_ORDER)}
    items.sort(key=lambda i: rank.get(Path(i.fspath).name, -1) if Path(i.fspath).parent == here else -1)


def pytest_collection_finish(session):
    session.config.stash[A02_SELECTED] = any(Path(i.fspath).parent == Path(__file__).parent and scenario_id(i) == "A02"
                                             for i in session.items)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    item.stash.setdefault(REPORTS, {})[report.when] = report


def scenario_id(node):
    """`test_F01_shim_rules[H1]` gives `F01[H1]`."""
    name = node.name
    base, _, param = name.partition("[")
    return base.split("_")[1] + (f"[{param}" if param else "")


@pytest.fixture(autouse=True)
def record(request):
    node = request.node
    RUN_DIR.mkdir(parents=True, exist_ok=True)  # marks the run as live: the end-of-run guards run
    markers = [m.name for m in node.iter_markers() if m.name in GROUP_MARKERS]
    fixture = next((f for f in CLUSTER_FIXTURES if f in request.fixturenames), "none")
    rec = livekit.Recorder(RESULTS, scenario_id(node), markers, fixture)
    yield rec
    rec.finish(*livekit.outcome_of(node.stash.get(REPORTS, {})))
    if scenario_id(node) == "A02":
        (RUN_DIR / "A02.done").touch()


def wait_for_a02(config):
    """A02 needs an account with no ray-vast role, and it deletes the boundary for a moment. In an xdist run its group
    starts at once; every other `up` of the run waits here until it is done (at most A02_WAIT_S)."""
    if not is_worker(config) or not config.stash.get(A02_SELECTED, False):
        return  # one process: A02 is the first test of test_aws.py, before every cluster
    deadline = time.time() + A02_WAIT_S
    while not (RUN_DIR / "A02.done").exists() and time.time() < deadline:
        time.sleep(5)


@pytest.fixture
def account_gate(request):
    """For a test that makes a ray-vast role without a cluster fixture (A03, A11, A13)."""
    wait_for_a02(request.config)


@pytest.fixture(scope="session")
def results_dir():
    RESULTS.mkdir(parents=True, exist_ok=True)
    return RESULTS


def _load(fixture, suffix):
    config = yaml.safe_load((CLUSTERS / f"{fixture}.yaml").read_text())
    config["cluster_name"] = f"{config['cluster_name']}-{suffix}"
    return config


def group_lifetime(group, make, request, tmp_path_factory):
    """`up` the cluster of one group (`make(group_dir, home, suffix)` gives the Cluster), and `down` it at the end, with
    the vast spend of its instances (from its timeline). With --keep-cluster: the suffix is `keep`, HOME is
    KEEP_HOME/<group>, a running cluster is reused, and there is no `down`."""
    keep = request.config.getoption("--keep-cluster")
    group_dir = SESSION / group
    group_dir.mkdir(parents=True, exist_ok=True)
    kept_home = KEEP_HOME / group
    started = time.time()
    holder = {}

    def up():
        wait_for_a02(request.config)
        if not keep and (kept_home / ".ray-vast").exists():  # end the kept cluster of an earlier run first
            old = make(group_dir / "ended_kept", kept_home, "keep")  # Cluster() notes it for the end-of-run check
            old.down()
            shutil.rmtree(kept_home)
        home = kept_home if keep else tmp_path_factory.mktemp(f"home-{group}")
        cluster = make(group_dir, home, "keep" if keep else secrets.token_hex(2))
        cluster.kept = keep
        holder["cluster"] = cluster
        OPENED.append(cluster)
        livekit.note_opened(RUN_DIR, {"name": cluster.name, "region": cluster.region, "kept": keep, "yaml": str(cluster.yaml),
                                      "home": str(home)})  # again, with `kept`
        cluster.start_poller()
        if keep and cluster.head_runs():
            cluster.collect_token()
            with open(group_dir / "cli.log", "a") as f:
                f.write(f"\n=== {time.strftime('%FT%TZ', time.gmtime())} --keep-cluster: reused the running cluster {cluster.name}\n")
            return cluster
        if keep and cluster.state:  # a kept cluster whose head is gone: clean it, then a new `up`
            cluster.down()
        return cluster.up()

    def down():
        cluster = holder.get("cluster")
        if cluster is not None and not keep:
            done = cluster.down()
            (group_dir / "down_final.log").write_text(done.stdout + done.stderr)
            if "left: nothing" not in done.stdout:  # once more, as the test plan says
                cluster.down()
        if cluster is not None:
            cluster.stop_poller()
            instance_type = cluster.config["available_node_types"]["head"]["node_config"].get("instance_type", "t3.large")
            hours = (time.time() - started) / 3600
            (group_dir / "spend.json").write_text(json.dumps({
                "group": group, "cluster": cluster.name, "kept": keep, "vast_estimate": livekit.timeline_spend(cluster.timeline),
                "hours": round(hours, 2), "ec2_estimate": round(hours * EC2_PRICE.get(instance_type, 0.1), 3)}, indent=1))

    yield from livekit.lifetime(up, down)


@pytest.fixture(scope="session")
def aws_only(request, tmp_path_factory, results_dir):
    yield from group_lifetime("aws_only", lambda d, home, sfx: Cluster("aws_only", _load("aws_only", sfx), d, home),
                              request, tmp_path_factory)


@pytest.fixture(scope="session")
def restricted_user(request, tmp_path_factory, results_dir):
    """The IAM user `ray-vast-livetest-<date>` with only the policy `ray-vast-user`, made with the getting-started
    commands (as the admin), and a credentials file with its profile. Deleted at the end of the session (kept and reused
    with --keep-cluster)."""
    from restricted import RestrictedUser
    keep = request.config.getoption("--keep-cluster")
    user = RestrictedUser(SESSION / "restricted_user", KEEP_HOME / "restricted_user" if keep else tmp_path_factory.mktemp("iam"), DATE)

    def create():
        if not (keep and user.reuse()):
            user.create()
        EXTRA_SECRETS.append(user.secret)
        return user
    yield from livekit.lifetime(create, (lambda: None) if keep else user.delete)


@pytest.fixture(scope="session")
def lifecycle(request, tmp_path_factory, results_dir, restricted_user):
    def make(d, home, sfx):
        return Cluster("lifecycle", _load("lifecycle", sfx), d, home, profile=restricted_user.profile,
                       credentials_file=restricted_user.credentials_file)
    yield from group_lifetime("lifecycle", make, request, tmp_path_factory)


@pytest.fixture(scope="session")
def library_clusters(request, tmp_path_factory, results_dir):
    """One libraries cluster for each host (H1, H2, H3, gpu2), each made at the first test of its host. A failed `up` is
    not tried again in the run: the later tests of that host fail with the same error."""
    from libraries_setup import make_libraries_cluster
    with contextlib.ExitStack() as stack:
        made = {}

        def get(host):
            if host not in made:
                try:
                    made[host] = stack.enter_context(contextlib.contextmanager(group_lifetime)(
                        f"libraries-{host.lower()}", lambda d, home, sfx: make_libraries_cluster(d, home, host, sfx), request,
                        tmp_path_factory))
                except Exception as e:  # noqa: BLE001 - kept for the later tests of the host
                    made[host] = e
            if isinstance(made[host], Exception):
                raise made[host]
            return made[host]
        yield get


@pytest.fixture
def libraries(request, library_clusters):
    """The libraries cluster of the test's host (`livekit.host_of`)."""
    return library_clusters(livekit.host_of(request.node))


def pytest_sessionstart(session):
    if not is_worker(session.config):
        try:  # the vast spend of the whole run (all groups): the credit before and after
            session.config.stash[CREDIT_BEFORE] = credit_sample()
        except Exception:  # noqa: BLE001 - no key or no network: no run spend
            pass


def pytest_sessionfinish(session, exitstatus):
    """Each process: a scan of the results for its own secret values. The controller (the only process without xdist),
    after every worker ended: the end-of-run guards (livekit.end_of_run) and the vast spend of the run."""
    config = session.config
    if not RUN_DIR.exists():
        return  # no live test ran
    values = [s for c in OPENED for s in c.secrets] + EXTRA_SECRETS
    if values:
        found = livekit.scan_secrets(RESULTS, values)
        worker = config.workerinput["workerid"] if is_worker(config) else "main"
        (RUN_DIR / f"scan_{worker}.txt").write_text("".join(f"{p.relative_to(RESULTS)}\n" for p in found))
    if is_worker(config):
        return
    key = os.environ.get("VAST_API_KEY") or vast_key_or_empty()
    problems, kept = livekit.end_of_run(RUN_DIR, TEST_LABEL, sweep_label, lambda name, region: aws_leftovers(name, region, ADMIN_PROFILE),
                                        lambda: [p.relative_to(RESULTS) for p in livekit.scan_secrets(RESULTS, [key])])
    before = config.stash.get(CREDIT_BEFORE, None)
    if before is not None and (RUN_DIR / "opened").exists():  # a run without a cluster rents nothing
        time.sleep(30)  # vast bills the last minutes late; the credit settles
        after = credit_sample()
        (RUN_DIR / "spend.json").write_text(json.dumps(livekit.vast_spend(before, after, invoices()), indent=1))
    if problems:
        (RUN_DIR / "problems.txt").write_text("\n".join(problems) + "\n")
        session.exitstatus = 1
    config.stash[END_LINES] = kept + [f"LIVE SUITE PROBLEM: {p}" for p in problems]


def pytest_terminal_summary(terminalreporter, config):
    for line in config.stash.get(END_LINES, []):
        terminalreporter.write_line(line, red=True, bold=True)


def vast_key_or_empty():
    try:
        return vast_key()
    except Exception:  # noqa: BLE001
        return ""
