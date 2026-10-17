"""The live suite (docs/test-plan.md, "Live suite"). One test for each scenario of the claim matrix; its name starts with
`test_<ID>_`. Markers select groups: `pytest tests/live -m "aws or lifecycle"` starts only the clusters that those tests use.

- Every test writes `archive/results/<UTC date>/<ID>/result.json` (the `record` fixture, autouse), so no pass exists
  without a record. `python tests/live/summarize.py` writes the table into docs/test-results.md.
- Checks: `record.check(...)` records a claim and the test goes on; a false check fails the test at its end
  (`pytest_pyfunc_call`). `record.require(...)` stops the test at once when the next steps need the claim.
- The run context (`liverun.Run`, the `run` fixture) is passed in explicitly: the run identity, the record root, the
  secret sink and the clusters of this process. Each cluster joins it when it is made. The record of a test belongs to
  the cluster of the test (`Run.record(owner=...)`): the end of that cluster scans it.
- The cluster kinds, the restricted IAM user and the cloud guards come from one factory module: livecloud.py (it also
  writes down the cluster interface).
- The cluster fixtures (`aws_only`, `lifecycle`, `libraries`, `release`) run `up` once and end the cluster at the end of
  the session, also after a failure, Ctrl-C or SIGTERM (`group_lifetime`). `libraries` is one cluster for each host (H1, H1b,
  H2, H2b, H3, gpu2, H4, H5). `release` is the tutorial's cluster (a foreign name, the plain CLI). A scenario makes its own clusters
  with the `new_cluster` factory; they end at the end of the test.
- One module ends every cluster: `liverun.end_cluster` (its `down` unless kept or already clean, its vast label sweep, its
  AWS check, the secret scan of its own records). The result of a group goes to `_session/<group>/teardown/result.json`
  (for libraries also the destroyed worker and the S3 cleanup); a problem fails the run.
- Declared needs: a test states a need with a marker of `liveschedule.NEEDS`, never the kit with a scenario ID.
  `exclusive_account`: the test runs first (in one process before every other test; in an xdist run its group is the
  first work unit), and every cluster `up` of the run and the restricted IAM user wait until each such test ended
  (`after_exclusive_account`).
  `box_disk`: the tests share one process (one after the other).
- Parallel runs: `pytest tests/live -n 12` (pytest-xdist; this file makes it `--dist loadgroup`, in the collection order).
  Each cluster and the tests that share its setup are one xdist group, and so is each need (`liveschedule.xdist_group`),
  so one worker process owns the cluster: one `up`, one `down`. Other tests go to any worker.
- `--keep-cluster` (`keep_or_fresh` decides; the rest reads `cluster.kept`): a cluster is named `<name>-keep`, its HOME is
  ~/.cache/ray-vast-live/<group>; a later run reuses it while its head runs, and no `down` runs at the end (the
  scenarios that end the cluster skip). A run without the option ends a kept cluster of its groups first, then makes a
  fresh one.
- At the end of the run, once, after every worker ended, only the run-wide guards (`liverun.end_of_run`): a label sweep
  of `ray-vast-test-<date>` (and of a suite cluster that did not end), the account-wide AWS check of the suite names, and the
  summary. Each process first scans the records that no cluster owns (`Run.scan_unowned`).
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

import livecloud as cloud
import liverecords
import liverun
import liveschedule

liverun.sigterm_as_interrupt()

DATE = time.strftime("%Y-%m-%d", time.gmtime())
TEST_LABEL = f"ray-vast-test-{DATE.replace('-', '')}"
CLUSTERS = Path(__file__).parent / "clusters"
KEEP_HOME = Path.home() / ".cache" / "ray-vast-live"  # the HOME of each kept cluster (its state file and SSH key)
# In one process a session cluster lives until the end of the session: the files without a cluster or with a cheap one first.
FILE_ORDER = ["test_free.py", "test_aws.py", "test_lifecycle.py", "test_libraries.py", "test_release.py"]
EC2_PRICE = {"t3.large": 0.0832, "t3.medium": 0.0416}  # $/h, us regions on-demand
EXCLUSIVE_WAIT_S = 1200  # the longest that a cluster `up` waits for the `exclusive_account` tests; then it fails

RUN = pytest.StashKey[liverun.Run]()
KEEP = pytest.StashKey[bool]()  # --keep-cluster
REPORTS = pytest.StashKey[dict]()
RECORD = pytest.StashKey[liverecords.Recorder]()
EXCLUSIVE = pytest.StashKey[list]()  # the record names of the selected `exclusive_account` tests
END_LINES = pytest.StashKey[list]()
CREDIT_BEFORE = pytest.StashKey[dict]()


def is_worker(config):
    return hasattr(config, "workerinput")


def pytest_addoption(parser):
    parser.addoption("--keep-cluster", action="store_true", default=False,
                     help="live suite: reuse a running cluster named <name>-keep, and do not end it at the end of the run")


def pytest_configure(config):
    config.stash[RUN] = liverun.Run.of_process(os.environ, cloud.REPO / "archive" / "results" / DATE,
                                               f"{time.strftime('%H%M%S', time.gmtime())}-{os.getpid()}")
    config.stash[KEEP] = config.getoption("--keep-cluster")
    for name in liveschedule.GROUP_MARKERS:
        config.addinivalue_line("markers", f"{name}: live suite group (docs/test-plan.md, claim matrix)")
    for name, meaning in liveschedule.NEEDS.items():
        config.addinivalue_line("markers", f"{name}: {meaning}")
    config.addinivalue_line("markers", "host(name): the libraries host of a test (H1, H1b, H2, H2b, H3, gpu2, H4 or H5): its cluster and xdist group")
    # pytest -n N: --dist loadgroup, so the groups keep each cluster in one worker. A worker parses the command line again
    # (dist "load"), and xdist adds the group to the test ids only when its own `loadgroup` option is set. No reorder by
    # group size: xdist then gives out the groups in the collection order, so the `exclusive_account` group is the first
    # work unit of the first worker (else it can wait behind a cluster whose `up` waits for it).
    config.option.loadscopereorder = False
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
        try:
            group = liveschedule.xdist_group([m.name for m in item.iter_markers()], item.fixturenames, liveschedule.host_of(item))
        except ValueError as e:
            raise pytest.UsageError(f"{item.nodeid}: {e}") from None
        if group:
            item.add_marker(pytest.mark.xdist_group(group))
    rank = {name: n for n, name in enumerate(FILE_ORDER)}
    items.sort(key=lambda i: (not exclusive_account(i), rank.get(Path(i.fspath).name, -1) if Path(i.fspath).parent == here else -1))


def exclusive_account(item):
    return Path(item.fspath).parent == Path(__file__).parent and item.get_closest_marker("exclusive_account") is not None


def pytest_collection_finish(session):
    session.config.stash[EXCLUSIVE] = [scenario_id(i) for i in session.items if exclusive_account(i)]


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item, nextitem):
    yield
    if exclusive_account(item):  # also after a failure or a skip: the barrier needs only its end
        item.config.stash[RUN].mark_ended(scenario_id(item))


def after_exclusive_account(config):
    """Every cluster `up` (of a group, or a scenario's own) and the restricted IAM user wait here until each selected
    `exclusive_account` test ended, in any process of the run (at most EXCLUSIVE_WAIT_S, then a TimeoutError). In one
    process they ran first, so it returns at once."""
    config.stash[RUN].wait_ended(config.stash.get(EXCLUSIVE, []), EXCLUSIVE_WAIT_S)


@pytest.hookimpl(wrapper=True)
def pytest_pyfunc_call(pyfuncitem):
    """The end of a test body: a false `record.check` fails the test here (the test ran on after it)."""
    result = yield
    record = pyfuncitem.stash.get(RECORD, None)
    failed = record.failed_checks() if record is not None else []
    if failed:
        raise AssertionError(f"{len(failed)} check(s) failed: {'; '.join(failed)}")
    return result


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    item.stash.setdefault(REPORTS, {})[report.when] = report


def scenario_id(node):
    """`test_X01_some_name[H1]` gives `X01[H1]`."""
    name = node.name
    base, _, param = name.partition("[")
    return base.split("_")[1] + (f"[{param}" if param else "")


@pytest.fixture(scope="session")
def run(request):
    """The run context (liverun.Run) of this process. The vast key is one of its secrets (the scans look for it)."""
    run = request.config.stash[RUN]
    run.secret(vast_key_or_empty())
    return run


def end(run, cluster, after_down=None):
    """liverun.end_cluster with the real guards: the vast label sweep and the AWS check as the admin."""
    return liverun.end_cluster(run, cluster, cloud.sweep_label, cloud.aws_leftovers, after_down)


@pytest.fixture(autouse=True)
def record(request, run):
    """The record of the test. It belongs to the cluster of the test (its cluster fixture, `up` first), so the end of that
    cluster scans it."""
    node = request.node
    markers = [m.name for m in node.iter_markers() if m.name in liveschedule.GROUP_MARKERS]
    fixture = liveschedule.cluster_fixture(request.fixturenames)
    owner = request.getfixturevalue(fixture) if fixture != "none" else None
    rec = run.record(scenario_id(node), markers, fixture, owner=owner)
    node.stash[RECORD] = rec
    yield rec
    rec.finish(*liverecords.outcome_of(node.stash.get(REPORTS, {})))


@pytest.fixture
def new_cluster(request, run, record):
    """The factory of a scenario's own clusters: `new_cluster(config, out_dir, home)` gives a suite cluster of the run,
    after the `exclusive_account` tests (not in such a test itself). At the end of the test, `end` ends each one (also
    after a failure); a problem is a failed check of the scenario."""
    made = []

    def make(config, out_dir, home):
        if not exclusive_account(request.node):
            after_exclusive_account(request.config)
        made.append(cloud.suite_cluster(run, config, out_dir, home))
        return made[-1]
    yield make
    problems = [f"{c.name}: {p}" for c in made for p in end(run, c)]
    record.require("each cluster of the scenario ended cleanly (liverun.end_cluster)", not problems, "; ".join(problems))


def _load(fixture, suffix):
    config = yaml.safe_load((CLUSTERS / f"{fixture}.yaml").read_text())
    config["cluster_name"] = f"{config['cluster_name']}-{suffix}"
    return config


def keep_or_fresh(config, run, group, group_dir, make, tmp_path_factory):
    """The one place of the --keep-cluster decision for the cluster of a group (`make(group_dir, home, suffix, kept)`).
    With the option: the cluster `<name>-keep` in KEEP_HOME/<group>; while its head runs the run reuses it (no `up`); when
    its head is gone, `down` cleans what is left. Without it: a kept cluster of an earlier run in KEEP_HOME/<group> ends
    first, then a fresh cluster with a random suffix in a temp HOME. Returns (the cluster, True when it still needs its
    `up`, the problems of the ended kept cluster)."""
    kept_home = KEEP_HOME / group
    if config.stash[KEEP]:
        cluster = make(group_dir, kept_home, "keep", True)
        if cluster.head_runs():
            cluster.collect_token()
            with open(group_dir / "cli.log", "a") as f:
                f.write(f"\n=== {time.strftime('%FT%TZ', time.gmtime())} --keep-cluster: reused the running cluster {cluster.name}\n")
            return cluster, False, []
        if cluster.state:  # a kept cluster whose head is gone: clean it, then a new `up`
            cluster.down()
        return cluster, True, []
    problems = []
    if (kept_home / ".ray-vast").exists():
        old = make(group_dir / "ended_kept", kept_home, "keep", False)  # not kept now
        problems = [f"the kept cluster {old.name} of an earlier run: {p}" for p in end(run, old)]
        shutil.rmtree(kept_home)
    return make(group_dir, tmp_path_factory.mktemp(f"home-{group}"), secrets.token_hex(2), False), True, problems


def group_lifetime(run, group, make, request, tmp_path_factory, before_down=None, end_checks=None):
    """`up` the cluster of one group (`make(group_dir, home, suffix, kept)` gives the cluster, of a livecloud kind; the
    reuse of a kept one: `keep_or_fresh`), and end it with `end` when the group ends. The teardown record
    `<group_dir>/teardown/result.json` has one check: the problems of the end (none: the cluster ended clean). It also
    writes the vast spend of the instances (from the timeline) and the EC2 estimate to `<group_dir>/spend.json`.
    `before_down(cluster, record)` runs before the end (not for a kept cluster); `end_checks(cluster, record, done)`
    gives more problems after the `down` (`done` is None for a kept cluster). A problem fails the run (an error at the
    teardown)."""
    group_dir = run.group_dir(group)
    started = time.time()
    holder = {"problems": []}

    def up():
        after_exclusive_account(request.config)
        cluster, needs_up, holder["problems"] = keep_or_fresh(request.config, run, group, group_dir, make, tmp_path_factory)
        holder["cluster"] = cluster
        cluster.poller.start()
        return cluster.up() if needs_up else cluster

    def down():
        cluster = holder.get("cluster")
        if cluster is None:
            return
        record = liverecords.Recorder(group_dir, "teardown", [], group)
        problems = list(holder["problems"])
        try:
            if before_down and not cluster.kept:
                try:
                    before_down(cluster, record)
                except Exception as e:  # noqa: BLE001 - the cluster still ends
                    problems.append(f"before the down: {type(e).__name__}: {e}")
            problems += end(run, cluster, (lambda done: end_checks(cluster, record, done)) if end_checks else None)
        finally:
            cluster.poller.stop()
            hours = (time.time() - started) / 3600
            (group_dir / "spend.json").write_text(json.dumps({
                "group": group, "cluster": cluster.name, "kept": cluster.kept, "vast_estimate": liverecords.timeline_spend(cluster.timeline),
                "hours": round(hours, 2), "ec2_estimate": round(hours * EC2_PRICE.get(cluster.head_instance_type, 0.1), 3)}, indent=1))
        record.check("the cluster ended cleanly (liverun.end_cluster)", not problems, "; ".join(problems))
        record.finish("failed" if problems else "passed", "; ".join(problems))
        assert not problems, f"teardown of {group}: {problems}"

    yield from liverun.lifetime(up, down)


@pytest.fixture(scope="session")
def aws_only(request, tmp_path_factory, run):
    yield from group_lifetime(run, "aws_only", lambda d, home, sfx, kept: cloud.suite_cluster(run, _load("aws_only", sfx), d, home, kept),
                              request, tmp_path_factory)


@pytest.fixture(scope="session")
def restricted_user(request, tmp_path_factory, run):
    """The IAM user `ray-vast-livetest-<date>` with only the policy `ray-vast-user`, made with the getting-started
    commands (as the admin), and a credentials file with its profile. Deleted at the end of the session (kept and reused
    with --keep-cluster). It waits for the `exclusive_account` tests, as every cluster `up` does: they change the
    account policies that the user's tests check."""
    keep = request.config.stash[KEEP]
    user = cloud.restricted_user(run.group_dir("restricted_user"), KEEP_HOME / "restricted_user" if keep else tmp_path_factory.mktemp("iam"),
                                 DATE)

    def create():
        after_exclusive_account(request.config)
        if not (keep and user.reuse()):
            user.create()
        run.secret(user.secret)
        return user
    yield from liverun.lifetime(create, (lambda: None) if keep else user.delete)


@pytest.fixture(scope="session")
def lifecycle(request, tmp_path_factory, run, restricted_user):
    yield from group_lifetime(run, "lifecycle", lambda d, home, sfx, kept: cloud.suite_cluster(
        run, _load("lifecycle", sfx), d, home, kept, user=restricted_user), request, tmp_path_factory)


@pytest.fixture(scope="session")
def library_clusters(request, tmp_path_factory, run):
    """One libraries cluster (libraries_setup.LibrariesCluster) for each host (H1, H1b, H2, H2b, H3, gpu2, H4, H5), each made at the first
    test of its host. A failed `up` is not tried again in the run: the later tests of that host fail with the same error.
    Before its `down`, one worker runs (so `down` destroys one); after it, its S3 test bucket and IAM user go."""
    with contextlib.ExitStack() as stack:
        made = {}

        def get(host):
            if host not in made:
                try:
                    made[host] = stack.enter_context(contextlib.contextmanager(group_lifetime)(
                        run, f"libraries-{host.lower()}", lambda d, home, sfx, kept: cloud.libraries_cluster(run, d, home, host, sfx, kept),
                        request, tmp_path_factory, before_down=lambda c, record: c.keep_one_worker(record),
                        end_checks=lambda c, record, done: c.end_checks(record, done)))
                except Exception as e:  # noqa: BLE001 - kept for the later tests of the host
                    made[host] = e
            if isinstance(made[host], Exception):
                raise made[host]
            return made[host]
        yield get


@pytest.fixture(scope="session")
def release(request, tmp_path_factory, run):
    """The release cluster (livecloud.release_cluster): the getting-started tutorial from a clean venv with
    ray-vast from PyPI. Only with RAYVAST_LIVE_RELEASE=<version> (after the publish) and without --keep-cluster (the
    tutorial ends its cluster). RAYVAST_LIVE_RELEASE_REGION changes the region (default us-west-2)."""
    version = os.environ.get("RAYVAST_LIVE_RELEASE")
    if not version:
        pytest.skip("set RAYVAST_LIVE_RELEASE=<version> after the release is on PyPI")
    if request.config.stash[KEEP]:
        pytest.skip("the release tutorial ends its cluster: run it without --keep-cluster")
    region = os.environ.get("RAYVAST_LIVE_RELEASE_REGION", "us-west-2")
    yield from group_lifetime(run, "release", lambda d, home, sfx, kept: cloud.release_cluster(run, d, home, region, version), request,
                              tmp_path_factory)


@pytest.fixture
def libraries(request, library_clusters):
    """The libraries cluster of the test's host (`liveschedule.host_of`)."""
    return library_clusters(liveschedule.host_of(request.node))


def pytest_sessionstart(session):
    if not is_worker(session.config) and not session.config.option.collectonly:  # a collection (liveschedule.suite_rows) rents nothing
        try:  # the vast spend of the whole run (all groups): the credit before and after
            session.config.stash[CREDIT_BEFORE] = cloud.credit_sample()
        except Exception:  # noqa: BLE001 - no key or no network: no run spend
            pass


def pytest_sessionfinish(session, exitstatus):
    """Each process: the secret scan of its records that no cluster owns (Run.scan_unowned). The controller (the only process without xdist),
    after every worker ended: the end-of-run guards (liverun.end_of_run) and the vast spend of the run."""
    config = session.config
    run = config.stash[RUN]
    if not run.live:
        return  # no live test ran
    worker = config.workerinput["workerid"] if is_worker(config) else "main"
    (run.dir / f"scan_{worker}.txt").write_text("".join(f"{p}\n" for p in run.scan_unowned()))
    if is_worker(config):
        return
    problems, kept = liverun.end_of_run(run.dir, TEST_LABEL, cloud.sweep_label, cloud.aws_account_leftovers)
    before = config.stash.get(CREDIT_BEFORE, None)
    if before is not None and (run.dir / "opened").exists():  # a run without a cluster rents nothing
        time.sleep(30)  # vast bills the last minutes late; the credit settles
        after = cloud.credit_sample()
        (run.dir / "spend.json").write_text(json.dumps(liverecords.vast_spend(before, after, cloud.invoices()), indent=1))
    if problems:
        (run.dir / "problems.txt").write_text("\n".join(problems) + "\n")
        session.exitstatus = 1
    config.stash[END_LINES] = kept + [f"LIVE SUITE PROBLEM: {p}" for p in problems]


def pytest_terminal_summary(terminalreporter, config):
    for line in config.stash.get(END_LINES, []):
        terminalreporter.write_line(line, red=True, bold=True)


def vast_key_or_empty():
    try:
        return cloud.vast_key()
    except Exception:  # noqa: BLE001
        return ""
