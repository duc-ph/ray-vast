"""The live suite (docs/test-plan.md, "Live suite"). One test for each scenario of the claim matrix; its name starts with
`test_<ID>_`. Markers select groups: `pytest tests/live -m "aws or lifecycle"` starts only the clusters that those tests use.

- Every test writes `archive/results/<UTC date>/<ID>/result.json` (the `record` fixture, autouse), so no pass exists
  without a record. `python tests/live/summarize.py` writes the table into docs/test-results.md.
- The run context (`livekit.Run`, the `run` fixture) is passed in explicitly: the run identity, the record root, the
  secret sink and the clusters of this process. Each Cluster joins it when it is made.
- The cluster fixtures (`aws_only`, `lifecycle`, `libraries`, `release`) run `up` once and end the cluster at the end of
  the session, also after a failure, Ctrl-C or SIGTERM (`group_lifetime`). `libraries` is one cluster for each host (H1,
  H2, H3, gpu2). `release` is the tutorial's cluster (cluster.ReleaseCluster: a foreign name, the plain CLI). A scenario
  makes its own clusters with the `new_cluster` factory; they end at the end of the test.
- One module ends every cluster: `livekit.end_cluster` (its `down` unless kept or already clean, its vast label sweep, its
  AWS check, the secret scan of its own records). The result of a group goes to `_session/<group>/teardown/result.json`
  (for libraries also the destroyed worker and the S3 cleanup); a problem fails the run.
- Declared needs: a test states a need with a marker of `livekit.NEEDS`, never the kit with a scenario ID.
  `exclusive_account`: the test runs first (in one process before every other test; in an xdist run its group is the
  first work unit), and every cluster `up` of the run and the restricted IAM user wait until each such test ended
  (`after_exclusive_account`).
  `box_disk`: the tests share one process (one after the other).
- Parallel runs: `pytest tests/live -n 12` (pytest-xdist; this file makes it `--dist loadgroup`, in the collection order).
  Each cluster and the tests that share its setup are one xdist group, and so is each need (`livekit.xdist_group`), so
  one worker process owns the cluster: one `up`, one `down`. Other tests go to any worker.
- `--keep-cluster`: a cluster is named `<name>-keep`, its HOME is ~/.cache/ray-vast-live/<group>; a later run reuses it
  while its head runs, and no `down` runs at the end (the scenarios that end the cluster skip). A run without the option
  ends a kept cluster of its groups first, then makes a fresh one.
- At the end of the run, once, after every worker ended, only the run-wide guards (`livekit.end_of_run`): a label sweep
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

import livekit
from cluster import (ADMIN_PROFILE, REPO, Cluster, aws_account_leftovers, aws_leftovers, credit_sample, invoices, sweep_label,
                     vast_key)

livekit.sigterm_as_interrupt()

DATE = time.strftime("%Y-%m-%d", time.gmtime())
TEST_LABEL = f"ray-vast-test-{DATE.replace('-', '')}"
CLUSTERS = Path(__file__).parent / "clusters"
KEEP_HOME = Path.home() / ".cache" / "ray-vast-live"  # the HOME of each kept cluster (its state file and SSH key)
# In one process a session cluster lives until the end of the session: the files without a cluster or with a cheap one first.
FILE_ORDER = ["test_free.py", "test_aws.py", "test_lifecycle.py", "test_libraries.py", "test_release.py"]
EC2_PRICE = {"t3.large": 0.0832, "t3.medium": 0.0416}  # $/h, us regions on-demand
EXCLUSIVE_WAIT_S = 1200  # the longest that a cluster `up` waits for the `exclusive_account` tests; then it fails

RUN = pytest.StashKey[livekit.Run]()
REPORTS = pytest.StashKey[dict]()
EXCLUSIVE = pytest.StashKey[list]()  # the record names of the selected `exclusive_account` tests
END_LINES = pytest.StashKey[list]()
CREDIT_BEFORE = pytest.StashKey[dict]()


def is_worker(config):
    return hasattr(config, "workerinput")


def pytest_addoption(parser):
    parser.addoption("--keep-cluster", action="store_true", default=False,
                     help="live suite: reuse a running cluster named <name>-keep, and do not end it at the end of the run")


def pytest_configure(config):
    config.stash[RUN] = livekit.Run.of_process(os.environ, REPO / "archive" / "results" / DATE,
                                               f"{time.strftime('%H%M%S', time.gmtime())}-{os.getpid()}")
    for name in livekit.GROUP_MARKERS:
        config.addinivalue_line("markers", f"{name}: live suite group (docs/test-plan.md, claim matrix)")
    for name, meaning in livekit.NEEDS.items():
        config.addinivalue_line("markers", f"{name}: {meaning}")
    config.addinivalue_line("markers", "host(name): the libraries host of a test (H1, H2, H3 or gpu2): its cluster and xdist group")
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
            group = livekit.xdist_group([m.name for m in item.iter_markers()], item.fixturenames, livekit.host_of(item))
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
    """The run context (livekit.Run) of this process. The vast key is one of its secrets (the scans look for it)."""
    run = request.config.stash[RUN]
    run.secret(vast_key_or_empty())
    return run


def end(run, cluster, after_down=None):
    """livekit.end_cluster with the real guards: the vast label sweep and the AWS check as the admin."""
    return livekit.end_cluster(run, cluster, sweep_label, lambda name, region: aws_leftovers(name, region, ADMIN_PROFILE), after_down)


@pytest.fixture(autouse=True)
def record(request, run):
    node = request.node
    markers = [m.name for m in node.iter_markers() if m.name in livekit.GROUP_MARKERS]
    rec = run.record(scenario_id(node), markers, livekit.cluster_fixture(request.fixturenames))
    yield rec
    for name in livekit.CLUSTER_FIXTURES:  # the record belongs to the cluster of the test: the end of that cluster scans it
        if node.funcargs.get(name) is not None:
            node.funcargs[name].records.append(rec.dir)
    rec.finish(*livekit.outcome_of(node.stash.get(REPORTS, {})))


@pytest.fixture
def new_cluster(request, run, record):
    """The factory of a scenario's own clusters: `new_cluster(config, out_dir, home)` gives a Cluster of the run, after
    the `exclusive_account` tests (not in such a test itself). At the end of the test, `end` ends each one (also after a
    failure); a problem is a failed check of the scenario."""
    made = []

    def make(config, out_dir, home):
        if not exclusive_account(request.node):
            after_exclusive_account(request.config)
        made.append(Cluster(run, config, out_dir, home))
        return made[-1]
    yield make
    problems = [f"{c.name}: {p}" for c in made for p in end(run, c)]
    record.check("each cluster of the scenario ended cleanly (livekit.end_cluster)", not problems, "; ".join(problems))
    assert not problems, problems


def _load(fixture, suffix):
    config = yaml.safe_load((CLUSTERS / f"{fixture}.yaml").read_text())
    config["cluster_name"] = f"{config['cluster_name']}-{suffix}"
    return config


def group_lifetime(run, group, make, request, tmp_path_factory, before_down=None, end_checks=None):
    """`up` the cluster of one group (`make(group_dir, home, suffix, kept)` gives the cluster: a Cluster or a
    ReleaseCluster), and end it with `end` when the group ends. The teardown record `<group_dir>/teardown/result.json` has
    one check: the problems of the end (none: the cluster ended clean). It also writes the vast spend of the instances
    (from the timeline) to `<group_dir>/spend.json`. `before_down(cluster, record)` runs before the end (not for a kept
    cluster); `end_checks(cluster, record, done)` gives more problems after the `down` (`done` is None for a kept
    cluster). A problem fails the run (an error at the teardown). With --keep-cluster: the suffix is `keep`, HOME is
    KEEP_HOME/<group>, a running cluster is reused, and there is no `down`."""
    keep = request.config.getoption("--keep-cluster")
    group_dir = run.group_dir(group)
    kept_home = KEEP_HOME / group
    started = time.time()
    holder = {"problems": []}

    def up():
        after_exclusive_account(request.config)
        if not keep and (kept_home / ".ray-vast").exists():  # end the kept cluster of an earlier run first
            old = make(group_dir / "ended_kept", kept_home, "keep", False)  # not kept now
            holder["problems"] += [f"the kept cluster {old.name} of an earlier run: {p}" for p in end(run, old)]
            shutil.rmtree(kept_home)
        home = kept_home if keep else tmp_path_factory.mktemp(f"home-{group}")
        cluster = make(group_dir, home, "keep" if keep else secrets.token_hex(2), keep)
        holder["cluster"] = cluster
        cluster.start_poller()
        if cluster.kept and cluster.head_runs():
            cluster.collect_token()
            with open(group_dir / "cli.log", "a") as f:
                f.write(f"\n=== {time.strftime('%FT%TZ', time.gmtime())} --keep-cluster: reused the running cluster {cluster.name}\n")
            return cluster
        if cluster.kept and cluster.state:  # a kept cluster whose head is gone: clean it, then a new `up`
            cluster.down()
        return cluster.up()

    def down():
        cluster = holder.get("cluster")
        if cluster is None:
            return
        record = livekit.Recorder(group_dir, "teardown", [], group)
        problems = list(holder["problems"])
        try:
            if before_down and not cluster.kept:
                try:
                    before_down(cluster, record)
                except Exception as e:  # noqa: BLE001 - the cluster still ends
                    problems.append(f"before the down: {type(e).__name__}: {e}")
            problems += end(run, cluster, (lambda done: end_checks(cluster, record, done)) if end_checks else None)
        finally:
            cluster.stop_poller()
            head = (cluster.config.get("available_node_types") or {}).get("head", {})
            instance_type = head.get("node_config", {}).get("instance_type", "t3.large")
            hours = (time.time() - started) / 3600
            (group_dir / "spend.json").write_text(json.dumps({
                "group": group, "cluster": cluster.name, "kept": cluster.kept, "vast_estimate": livekit.timeline_spend(cluster.timeline),
                "hours": round(hours, 2), "ec2_estimate": round(hours * EC2_PRICE.get(instance_type, 0.1), 3)}, indent=1))
        record.check("the cluster ended cleanly (livekit.end_cluster)", not problems, "; ".join(problems))
        record.finish("failed" if problems else "passed", "; ".join(problems))
        assert not problems, f"teardown of {group}: {problems}"

    yield from livekit.lifetime(up, down)


@pytest.fixture(scope="session")
def aws_only(request, tmp_path_factory, run):
    yield from group_lifetime(run, "aws_only", lambda d, home, sfx, kept: Cluster(run, _load("aws_only", sfx), d, home, kept),
                              request, tmp_path_factory)


@pytest.fixture(scope="session")
def restricted_user(request, tmp_path_factory, run):
    """The IAM user `ray-vast-livetest-<date>` with only the policy `ray-vast-user`, made with the getting-started
    commands (as the admin), and a credentials file with its profile. Deleted at the end of the session (kept and reused
    with --keep-cluster). It waits for the `exclusive_account` tests, as every cluster `up` does: they change the
    account policies that the user's tests check."""
    from restricted import RestrictedUser
    keep = request.config.getoption("--keep-cluster")
    user = RestrictedUser(run.group_dir("restricted_user"), KEEP_HOME / "restricted_user" if keep else tmp_path_factory.mktemp("iam"), DATE)

    def create():
        after_exclusive_account(request.config)
        if not (keep and user.reuse()):
            user.create()
        run.secret(user.secret)
        return user
    yield from livekit.lifetime(create, (lambda: None) if keep else user.delete)


@pytest.fixture(scope="session")
def lifecycle(request, tmp_path_factory, run, restricted_user):
    def make(d, home, sfx, kept):
        return Cluster(run, _load("lifecycle", sfx), d, home, kept, profile=restricted_user.profile,
                       credentials_file=restricted_user.credentials_file)
    yield from group_lifetime(run, "lifecycle", make, request, tmp_path_factory)


@pytest.fixture(scope="session")
def library_clusters(request, tmp_path_factory, run):
    """One libraries cluster (libraries_setup.LibrariesCluster) for each host (H1, H2, H3, gpu2), each made at the first
    test of its host. A failed `up` is not tried again in the run: the later tests of that host fail with the same error.
    Before its `down`, one worker runs (so `down` destroys one); after it, its S3 test bucket and IAM user go."""
    from libraries_setup import LibrariesCluster
    with contextlib.ExitStack() as stack:
        made = {}

        def get(host):
            if host not in made:
                try:
                    made[host] = stack.enter_context(contextlib.contextmanager(group_lifetime)(
                        run, f"libraries-{host.lower()}", lambda d, home, sfx, kept: LibrariesCluster(run, d, home, host, sfx, kept),
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
    """The release cluster (cluster.ReleaseCluster): the getting-started tutorial from a clean venv with
    ray-vast from PyPI. Only with RAYVAST_LIVE_RELEASE=<version> (after the publish) and without --keep-cluster (the
    tutorial ends its cluster). RAYVAST_LIVE_RELEASE_REGION changes the region (default us-west-2)."""
    version = os.environ.get("RAYVAST_LIVE_RELEASE")
    if not version:
        pytest.skip("set RAYVAST_LIVE_RELEASE=<version> after the release is on PyPI")
    if request.config.getoption("--keep-cluster"):
        pytest.skip("the release tutorial ends its cluster: run it without --keep-cluster")
    from cluster import ReleaseCluster
    region = os.environ.get("RAYVAST_LIVE_RELEASE_REGION", "us-west-2")
    yield from group_lifetime(run, "release", lambda d, home, sfx, kept: ReleaseCluster(run, d, home, region, version), request,
                              tmp_path_factory)


@pytest.fixture
def libraries(request, library_clusters):
    """The libraries cluster of the test's host (`livekit.host_of`)."""
    return library_clusters(livekit.host_of(request.node))


def pytest_sessionstart(session):
    if not is_worker(session.config) and not session.config.option.collectonly:  # a collection (livekit.suite_rows) rents nothing
        try:  # the vast spend of the whole run (all groups): the credit before and after
            session.config.stash[CREDIT_BEFORE] = credit_sample()
        except Exception:  # noqa: BLE001 - no key or no network: no run spend
            pass


def pytest_sessionfinish(session, exitstatus):
    """Each process: the secret scan of its records that no cluster owns (Run.scan_unowned). The controller (the only process without xdist),
    after every worker ended: the end-of-run guards (livekit.end_of_run) and the vast spend of the run."""
    config = session.config
    run = config.stash[RUN]
    if not run.live:
        return  # no live test ran
    worker = config.workerinput["workerid"] if is_worker(config) else "main"
    (run.dir / f"scan_{worker}.txt").write_text("".join(f"{p}\n" for p in run.scan_unowned()))
    if is_worker(config):
        return
    problems, kept = livekit.end_of_run(run.dir, TEST_LABEL, sweep_label,
                                        lambda regions, skip: aws_account_leftovers(regions, skip, ADMIN_PROFILE))
    before = config.stash.get(CREDIT_BEFORE, None)
    if before is not None and (run.dir / "opened").exists():  # a run without a cluster rents nothing
        time.sleep(30)  # vast bills the last minutes late; the credit settles
        after = credit_sample()
        (run.dir / "spend.json").write_text(json.dumps(livekit.vast_spend(before, after, invoices()), indent=1))
    if problems:
        (run.dir / "problems.txt").write_text("\n".join(problems) + "\n")
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
