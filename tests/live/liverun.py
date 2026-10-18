"""One live-suite run (unit-tested in tests/test_live_suite_kit.py): the run context (`Run`), the names that the run may
touch, the lifetime of a cluster fixture, the end of each cluster (`end_cluster`) and of the run (`end_of_run`), and the
one wait of the suite (`wait_until`)."""
import json
import re
import signal
import time
from dataclasses import dataclass
from pathlib import Path

from liverecords import Recorder, scan_secrets


class NotOurs(Exception):
    """The suite was about to change an AWS or vast resource that it did not make."""


OUR_NAME = re.compile(r"^ray-vast-(rvtest-[a-z0-9-]+|livetest-\d{8}(-s3(-[a-z0-9-]+)?)?)$")


def assert_ours(name):
    """Every name that the suite deletes passes here first. Only the suite's clusters and S3 buckets (`rvtest-*`) and
    its IAM test users (`livetest-<date>`, `livetest-<date>-s3-<host>`) are ours; never the operator machine, its key pair, or the two account
    policies."""
    if not OUR_NAME.match(name or ""):
        raise NotOurs(f"refusing to touch {name!r}: not made by the live suite")


def is_suite_cluster(name):
    """True for a cluster name that only the suite makes (`rvtest-*`). The release cluster `gpu-24gb` is not one."""
    return name.startswith("rvtest-") and OUR_NAME.match(f"ray-vast-{name}") is not None


def lifetime(up, down):
    """The body of a cluster fixture: `yield from lifetime(up, down)`. `down` runs when the fixture ends, when `up` fails
    half way, and on Ctrl-C (pytest tears the session fixtures down after a KeyboardInterrupt)."""
    try:
        yield up()
    finally:
        down()


def sigterm_as_interrupt():
    """Make SIGTERM act like Ctrl-C, so a killed run still tears its clusters down."""
    def handler(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}")
    signal.signal(signal.SIGTERM, handler)


@dataclass
class Wait:
    """The end of a wait: `ok` (also its truth value), the seconds it took, the last value that `look` gave (None if it
    gave none), and the last error of `look` (None if it raised none)."""
    ok: bool
    elapsed: float
    seen: object
    error: str | None = None

    def __bool__(self):
        return self.ok


def wait_until(look, timeout, done=bool, poll_s=10, clock=time):
    """Call `look()` until `done(seen)` is true or `timeout` s passed, every `poll_s` s (never past the timeout). `look`
    runs at least once. A `look` (or `done`) that raises gives `error` (`<error type>: <message>`) and the wait goes on:
    one failed sample must not end it. Returns a Wait."""
    start, seen, error = clock.time(), None, None
    while True:
        try:
            value = look()
            seen = value
            if done(value):
                return Wait(True, round(clock.time() - start, 1), seen, error)
        except Exception as e:  # noqa: BLE001
            error = f"{type(e).__name__}: {str(e)[:200]}"
        left = timeout - (clock.time() - start)
        if left <= 0:
            return Wait(False, round(clock.time() - start, 1), seen, error)
        clock.sleep(min(poll_s, left))


class Run:
    """The context of one live-suite run, passed in explicitly: the `run` fixture gives it to a scenario, and each cluster
    takes it as its first argument. It holds the run identity (`dir`: `<results>/_session/runs/<run id>`), the record
    root (`results`), the secret sink of this process, and the clusters of this process.
    `register(cluster)` is the only way a cluster joins the run: each cluster kind calls it when it is made, before any `up`."""

    def __init__(self, results, run_id):
        self.results = Path(results)
        self.dir = self.results / "_session" / "runs" / run_id
        self._secrets = []
        self._clusters = []
        self._dirs = []  # every record and group directory that this process made

    @classmethod
    def of_process(cls, environ, default_results, default_run_id):
        """The run of this process. The first process (the xdist controller) sets RAYVAST_LIVE_RESULTS and
        RAYVAST_LIVE_RUN in `environ`; the xdist workers inherit them, so all processes of a run agree."""
        return cls(environ.setdefault("RAYVAST_LIVE_RESULTS", str(default_results)),
                   environ.setdefault("RAYVAST_LIVE_RUN", default_run_id))

    @property
    def live(self):
        """True when a scenario of this run started (the end-of-run guards run then)."""
        return self.dir.exists()

    def record(self, scenario, fixture, owner=None):
        """A new record of the run. `owner`: the cluster of the test; its end scans the record (else the process scan)."""
        self.dir.mkdir(parents=True, exist_ok=True)
        record = Recorder(self.results, scenario, fixture)
        self._dirs.append(record.dir)
        if owner is not None:
            owner.records.append(record.dir)
        return record

    def group_dir(self, group):
        """`<results>/_session/<group>`: the data of one cluster group."""
        path = self.results / "_session" / group
        path.mkdir(parents=True, exist_ok=True)
        self._dirs.append(path)
        return path

    def secret(self, value):
        """A secret value that the scan of this process looks for (for example an IAM test user's secret key)."""
        self._secrets.append(value)

    def secret_values(self):
        """The secrets of the run and the secrets of each cluster now (its token and join tokens arrive after `up`)."""
        return self._secrets + [s for c in self._clusters for s in c.secrets]

    def scan_unowned(self):
        """The secret scan of the records and group directories of this process that no cluster of the process owns
        (`end_cluster` scans those). A file in a directory that a cluster owns is not reported here, also when the
        directory is inside an unowned record (a scenario's own cluster records `<record>/a`). Returns the paths that hold
        a secret, relative to `results`."""
        owned = [Path(d) for c in self._clusters for d in c.records]

        def unowned(path):
            return not any(path == o or o in path.parents for o in owned)
        values = self.secret_values()
        return sorted({str(p.relative_to(self.results)) for d in dict.fromkeys(self._dirs) if unowned(d) and d.exists()
                       for p in scan_secrets(d, values) if unowned(p)})

    def register(self, cluster):
        """Write `<dir>/opened/<name>.json` ({name, region, kept, yaml, home}), so the end of the run checks the cluster,
        also after a failed `up` or in another worker process."""
        path = self.dir / "opened" / f"{cluster.name}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"name": cluster.name, "region": cluster.region, "kept": cluster.kept, "yaml": str(cluster.yaml),
                                    "home": str(cluster.home)}))
        self._clusters.append(cluster)

    def mark_ended(self, test):
        """`<dir>/ended_tests/<test>`: the test ended (passed, failed or skipped), seen by every process of the run."""
        path = self.dir / "ended_tests" / test
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()

    def wait_ended(self, tests, timeout_s, poll_s=5):
        """Wait until each test of `tests` ended in some process of the run (`mark_ended`). Raises TimeoutError with the
        missing tests after `timeout_s`."""
        deadline = time.time() + timeout_s
        while True:
            missing = [t for t in tests if not (self.dir / "ended_tests" / t).exists()]
            if not missing:
                return
            if time.time() >= deadline:
                raise TimeoutError(f"these tests did not end in {timeout_s} s: {', '.join(missing)}")
            time.sleep(poll_s)


def _clean(done):
    return done.returncode == 0 and "left: nothing" in done.stdout


def end_cluster(run, cluster, sweep, aws_check, after_down=None):
    """End one cluster cleanly: the only place in the suite that does. Every cluster of the run comes here once (a group
    cluster at the end of its group, a scenario's own cluster at the end of its test, the release cluster).
    1. `down`, unless the cluster is kept, or its last `down` (a scenario's own `down`) exited 0 with
       `left: nothing`. A `down` that leaves something is a problem, and it runs once more.
    2. `after_down(done)` (the checks of the group; `done` is the last `down`, None for a kept cluster) gives more problems.
    3. Not kept: the vast label sweep of this cluster (an instance that it still finds is a problem) and the AWS check of
       this cluster in its region. The sweep of a suite cluster (`is_suite_cluster`) takes every instance with its exact
       label. The sweep of another cluster (the release cluster `gpu-24gb`) takes only the instances that the run saw for
       it (`cluster.timeline`), because a user can have a cluster of that name on the same account.
    4. The secret scan of this cluster's records only (`cluster.records`: its own directories, not a name glob), with the
       secret values of the run.
    The cluster: the interface of livecloud.py (`name`, `region`, `kept`, `last_down`, `records`, `timeline`, `down()`).
    `sweep(labels, ids=None)` (cluster.sweep_label) gives log lines (`destroyed <id> (<label>)` for each
    destroyed instance); `aws_check(name, region)` gives the resources left. Writes `<run.dir>/ended/<name>.json`. Returns the problems (empty: the cluster ended clean)."""
    problems = []
    done = cluster.last_down
    if not cluster.kept and (done is None or not _clean(done)):
        try:
            done = cluster.down()
            if not _clean(done):
                problems.append(f"down left something (exit {done.returncode}): {' '.join(done.stdout.split()[-12:])}")
                done = cluster.down()
        except Exception as e:  # noqa: BLE001 - the sweep and the AWS check still run
            problems.append(f"down failed: {type(e).__name__}: {e}")
    if after_down:
        try:
            problems += after_down(None if cluster.kept else done)
        except Exception as e:  # noqa: BLE001 - the sweep and the AWS check still run
            problems.append(f"the checks after down failed: {type(e).__name__}: {e}")
    if not cluster.kept:
        try:
            labels = [f"ray-vast:{cluster.name}"]
            if is_suite_cluster(cluster.name):
                swept = sweep(labels)
            else:  # a user can have a cluster of this name on the same account: only the instances that this run saw
                swept = sweep(labels, ids=sorted({i["id"] for row in cluster.timeline for i in row.get("instances", [])}))
            destroyed = [line for line in swept if line.startswith("destroyed ")]
            if destroyed:
                problems.append(f"vast: {len(destroyed)} instance(s) with the cluster label after down: {', '.join(destroyed)}")
        except Exception as e:  # noqa: BLE001
            problems.append(f"vast sweep failed: {type(e).__name__}: {e}")
        try:
            left = aws_check(cluster.name, cluster.region)
            if left:
                problems.append(f"AWS left: {', '.join(left)}")
        except Exception as e:  # noqa: BLE001
            problems.append(f"AWS check failed: {type(e).__name__}: {e}")
    values = run.secret_values()
    found = sorted({str(p) for d in cluster.records if Path(d).exists() for p in scan_secrets(d, values)})
    if found:
        problems.append(f"the secret scan found {len(found)} file(s): {', '.join(found)}")
    path = run.dir / "ended" / f"{cluster.name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"name": cluster.name, "kept": cluster.kept, "problems": problems}, indent=1))
    return problems


def end_of_run(run_dir, test_label, sweep, account_check):
    """The run-wide guards, once, after every worker process ended (so no worker still uses a cluster). Each cluster was
    already ended by `end_cluster` (its own `down`, sweep, AWS check and secret scan); this does not repeat that work:
    - a final vast label sweep, `sweep(labels, prefixes=[test_label])`: the test label as a prefix, and the exact label of
      each suite cluster that joined the run but did not end (its worker died). A cluster that is not a suite cluster
      (`is_suite_cluster`) gets no label sweep here: the run reports it as not ended;
    - the account-wide AWS check `account_check(regions, skip)`: resources with a suite name in each region of the run,
      except the kept clusters (`skip`);
    - the summary: the problems of each ended cluster, each cluster that did not end, and the files that each process
      scan found (`scan_<worker>.txt`: the records of that process that no cluster owns).
    A run without a cluster calls neither cloud guard. Returns (problems, one KEPT line for each kept cluster). The logs go
    to `run_dir`."""
    run_dir = Path(run_dir)
    opened = [json.loads(p.read_text()) for p in sorted((run_dir / "opened").glob("*.json"))]
    ended = {c["name"]: c for c in (json.loads(p.read_text()) for p in sorted((run_dir / "ended").glob("*.json")))}
    not_ended = [c for c in opened if c["name"] not in ended]
    problems, left = [], []
    if opened:  # a run without a cluster (the free group) calls no cloud API
        try:
            lines = sweep([f"ray-vast:{c['name']}" for c in not_ended if not c["kept"] and is_suite_cluster(c["name"])],
                          prefixes=[test_label])
            (run_dir / "sweep.log").write_text("\n".join(lines) + "\n")
        except Exception as e:  # noqa: BLE001 - the report must name it
            problems.append(f"vast sweep failed: {type(e).__name__}: {e}")
        try:
            left = account_check(sorted({c["region"] for c in opened} | {"us-west-2"}), [c["name"] for c in opened if c["kept"]])
        except Exception as e:  # noqa: BLE001
            left = [f"failed: {type(e).__name__}: {e}"]
        (run_dir / "aws_account_check.log").write_text(("\n".join(left) or "aws left: nothing") + "\n")
    for c in opened:
        problems += [f"{c['name']}: {p}" for p in ended.get(c["name"], {}).get("problems", [])]
    problems += [f"AWS account check: {x}" for x in left]
    problems += [f"{c['name']} joined the run but did not end: run HOME={c['home']} ray-vast down {c['yaml']}" for c in not_ended]
    found = sorted({line for p in run_dir.glob("scan_*.txt") for line in p.read_text().split()})
    (run_dir / "secret_scan.txt").write_text("".join(f"{p}\n" for p in found) or "no file holds a secret\n")
    if found:
        problems.append(f"the secret scan found {len(found)} file(s): {', '.join(found)}")
    kept = [f"KEPT CLUSTER {c['name']} still runs and costs money. End it: HOME={c['home']} ray-vast down {c['yaml']} "
            f"(or run the suite again without --keep-cluster)" for c in opened if c["kept"]]
    return problems, kept
