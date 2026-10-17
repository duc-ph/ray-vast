"""Parts of the live suite that hold logic (unit-tested in tests/unit/test_live_kit.py). The suite itself is in
`conftest.py` and the `test_*.py` files of this directory; the claim matrix is in docs/test-plan.md."""
import json
import re
import signal
import time
from pathlib import Path


class Recorder:
    """The record of one scenario run: `<root>/<scenario>/result.json` plus evidence files in the same directory."""

    def __init__(self, root, scenario, markers, fixture):
        self.dir = Path(root) / scenario
        if (self.dir / "result.json").exists():  # a re-run: keep the earlier record as <scenario>.run<n>
            n = 1
            while (Path(root) / f"{scenario}.run{n}").exists():
                n += 1
            self.dir.rename(Path(root) / f"{scenario}.run{n}")
        self.dir.mkdir(parents=True, exist_ok=True)
        self.result = {"id": scenario, "markers": sorted(markers), "fixture": fixture, "started": time.time(),
                       "checks": [], "data": {}}

    def evidence(self, name, text):
        path = self.dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def check(self, name, ok, detail=""):
        self.result["checks"].append({"name": name, "ok": bool(ok), "detail": str(detail)})
        return ok

    def data(self, key, value):
        self.result["data"][key] = value

    def finish(self, outcome, message):
        ended = time.time()
        self.result.update(outcome=outcome, message=message, ended=ended, wall_s=round(ended - self.result["started"], 1))
        (self.dir / "result.json").write_text(json.dumps(self.result, indent=1, default=str))


def outcome_of(reports):
    """(outcome, message) of one test from its pytest reports ({"setup", "call"}). A test without a call report (Ctrl-C or
    SIGTERM while it ran) is "failed", never "passed"."""
    setup, call = reports.get("setup"), reports.get("call")
    failed = next((r for r in (setup, call) if r is not None and r.failed), None)
    skipped = next((r for r in (setup, call) if r is not None and r.skipped), None)
    if failed is not None:
        crash = getattr(failed.longrepr, "reprcrash", None)
        return "failed", crash.message if crash else str(failed.longrepr)[-600:]
    if skipped is not None:
        return "skipped", str(skipped.longrepr[-1]) if isinstance(skipped.longrepr, tuple) else "skipped"
    if call is None:
        return "failed", "interrupted: the test did not end (no call report)"
    return "passed", ""


def _verdict(result):
    """PASS only for a passed test whose checks are all true. Returns (word, the line that explains it)."""
    failed = [c for c in result.get("checks", []) if not c["ok"]]
    if result.get("outcome") == "skipped":
        return "SKIP", result.get("message", "")
    if result.get("outcome") == "passed" and not failed:
        return "PASS", "; ".join(f"{c['name']}: {c['detail']}" if c["detail"] else c["name"] for c in result["checks"])[:300]
    reason = "; ".join(f"check failed: {c['name']} ({c['detail']})" for c in failed) or result.get("message", "")
    return "FAIL", reason.splitlines()[0][:300] if reason else "failed"


def summary_table(root, expected):
    """A markdown table of the results under `root` (one directory for each run: `<id>` or `<id>[<param>]`).
    An expected scenario with no result.json is NOT RUN."""
    root = Path(root)
    runs = {}
    for path in sorted(root.glob("*/result.json")):
        if ".run" in path.parent.name:  # an earlier run of a scenario (Recorder keeps it); the last run counts
            continue
        result = json.loads(path.read_text())
        runs.setdefault(path.parent.name.split("[")[0], []).append((path.parent.name, result))
    lines = ["| ID | Result | Time (s) | Line | Evidence |", "|---|---|---|---|---|"]
    for scenario in expected:
        for name, result in runs.get(scenario, [(scenario, None)]):
            if result is None:
                lines.append(f"| {name} | NOT RUN | | | |")
                continue
            word, line = _verdict(result)
            line = " ".join(line.replace("|", "/").split())  # one row: no line break, no pipe
            lines.append(f"| {name} | {word} | {result.get('wall_s', '')} | {line} | `{root.name}/{name}/` |")
    return "\n".join(lines) + "\n"


def write_summary(doc, date, table):
    """Put `table` in `doc` in the block of `date` (replace the old block of that date, else add it at the end)."""
    doc = Path(doc)
    start, end = f"<!-- live-suite {date} start -->", f"<!-- live-suite {date} end -->"
    block = f"{start}\n## Live suite {date}\n\nWritten by `tests/live/summarize.py` from `archive/results/{date}/`.\n\n{table}{end}\n"
    text = doc.read_text()
    if start in text:
        before, rest = text.split(start, 1)
        text = before + block + rest.split(end, 1)[1].lstrip("\n")
    else:
        text = text.rstrip("\n") + "\n\n" + block
    doc.write_text(text)


def vast_spend(before, after, invoices):
    """The vast spend between two credit samples ({"t", "credit"}). The account tops itself up (a `payment` invoice
    with a negative amount), so the payments inside the window are added back."""
    top_ups = sum(-i["amount"] for i in invoices if i.get("type") == "payment" and before["t"] <= i.get("timestamp", 0) <= after["t"])
    return {"spend": round(before["credit"] - after["credit"] + top_ups, 4), "top_ups": top_ups,
            "credit_before": before["credit"], "credit_after": after["credit"]}


class NotOurs(Exception):
    """The suite was about to change an AWS or vast resource that it did not make."""


OUR_NAME = re.compile(r"^ray-vast-(rvtest-[a-z0-9-]+|livetest-\d{8}(-s3(-[a-z0-9]+)?)?)$")


def assert_ours(name):
    """Every name that the suite deletes passes here first. Only the suite's clusters and S3 buckets (`rvtest-*`) and
    its IAM test users (`livetest-<date>`, `livetest-<date>-s3-<host>`) are ours; never the dev box, its key pair, the two account
    policies, or anything named badminton."""
    if not OUR_NAME.match(name or "") or "badminton" in name:
        raise NotOurs(f"refusing to touch {name!r}: not made by the live suite")


SECRET_PATTERNS = re.compile(rb'PRIVATE KEY|instance_api_key"?\s*[:=]\s*"?[A-Za-z0-9]|CONTAINER_API_KEY=[A-Za-z0-9]|Bearer [A-Za-z0-9]')


def scan_secrets(root, secrets):
    """The files under `root` that hold one of the secret values (the cluster tokens, the join tokens, the vast key) or a
    secret pattern (test-plan safety rules). Returns paths only: the caller never prints the text."""
    values = [s.strip().encode() for s in secrets if s and s.strip()]
    found = []
    for path in sorted(Path(root).rglob("*")):
        if path.is_file():
            data = path.read_bytes()
            if SECRET_PATTERNS.search(data) or any(v in data for v in values):
                found.append(path)
    return found


class HostNotFound(Exception):
    pass


HOST_RENTALS_MAX = 3


def find_host(want, rent, end):
    """Rent a host until its probe matches `want` (for example {"cpus": {"gte": 32}, "cgroup": "v1"}: a value is the exact
    value of that fact, or {"gte": n} for a number of at least n).
    `rent(blocked)` rents one host, never on a blocked machine, and returns its probe (with `machine_id`). `end(probe)`
    ends a host that does not match. vast can give the same machine again, so each tried machine is blocked.
    Returns (the matching probe, the list of tries); raises HostNotFound after HOST_RENTALS_MAX rentals."""
    blocked, tries = [], []
    for _ in range(HOST_RENTALS_MAX):
        probe = rent(list(blocked))
        match = all(probe.get(key) is not None and probe[key] >= value["gte"] if isinstance(value, dict) else probe.get(key) == value
                    for key, value in want.items())
        tries.append({**probe, "match": match})
        if match:
            return probe, tries
        end(probe)
        blocked.append(probe["machine_id"])
    raise HostNotFound(f"no host matched {want} in {HOST_RENTALS_MAX} rentals (machines {', '.join(map(str, blocked))})")


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


def check(record, name, ok, detail=""):
    """Record a check, then assert it: the test stops at the first false check."""
    record.check(name, ok, detail)
    assert ok, f"{name}: {detail}"


CLUSTER_FIXTURES = ("aws_only", "lifecycle", "libraries", "release")
ROW = re.compile(r"^\|\s*([A-Z]+\d\d)\s*\|[^|]*\|([^|]*)\|([^|]*)\|")


def matrix_rows(text):
    """The scenarios of the claim matrix (docs/test-plan.md): [{id, markers, fixture}] in the order of the tables. Only the
    tables with the matrix header (`| ID | Name | Markers | Fixture |`) count; another table ends at its first non-table line."""
    rows, in_matrix = [], False
    for line in text.splitlines():
        if not line.startswith("|"):
            in_matrix = False
        elif line.startswith("| ID | Name | Markers | Fixture |"):
            in_matrix = True
        m = ROW.match(line)
        if in_matrix and m and m.group(3).strip():
            rows.append({"id": m.group(1), "markers": sorted(x.strip() for x in m.group(2).split(",") if x.strip()),
                         "fixture": m.group(3).strip().split()[0]})
    return rows


def suite_rows(directory):
    """The scenarios of the live suite: each `test_<ID>_...` function (or assignment) in `directory/test_*.py`, with its
    markers (the module `pytestmark` and its decorators) and its cluster fixture (`none` without one)."""
    import ast
    rows = []
    for path in sorted(Path(directory).glob("test_*.py")):
        tree = ast.parse(path.read_text())
        module_marks = []
        for node in tree.body:
            if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "pytestmark" for t in node.targets):
                values = node.value.elts if isinstance(node.value, (ast.List, ast.Tuple)) else [node.value]
                module_marks = [v.attr for v in values if isinstance(v, ast.Attribute)]
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and re.match(r"test_[A-Z]+\d\d_", node.name):
                marks = [d.attr for d in node.decorator_list if isinstance(d, ast.Attribute) and isinstance(d.value, ast.Attribute)
                         and d.value.attr == "mark"]
                args = [a.arg for a in node.args.args]
                fixture = next((f for f in CLUSTER_FIXTURES if f in args), "none")
                rows.append({"id": node.name.split("_")[1], "markers": sorted(set(module_marks + marks)), "fixture": fixture})
            elif isinstance(node, ast.Assign) and len(node.targets) == 1 and re.match(r"test_[A-Z]+\d\d_", getattr(node.targets[0], "id", "")):
                rows.append({"id": node.targets[0].id.split("_")[1], "markers": sorted(module_marks), "fixture": "none"})
    return rows


def shaped_config(config, shape, queries=None):
    """The head's cluster YAML for one cluster shape ({node type: workers}): every worker type gets min_workers =
    max_workers = its count (0 if not named), so the autoscaler starts exactly these workers and no other. `queries`
    ({node type: offer_query}) replaces the offer query of a type (a host pin or a block). Returns a copy."""
    import copy
    config = copy.deepcopy(config)
    for name, node_type in config["available_node_types"].items():
        if name == config["head_node_type"]:
            continue
        node_type["min_workers"] = node_type["max_workers"] = shape.get(name, 0)
        if name in (queries or {}):
            node_type["node_config"]["offer_query"] = queries[name]
    config["max_workers"] = sum(shape.values())
    return config


def doc_snippet(markdown, heading, index, subs):
    """The `index`-th ```python block in the section `heading` of a user doc, with each (old, new) substitution applied.
    Each `old` must occur exactly once in the block, so a doc change makes the test fail, not test other code."""
    section = markdown.split("\n" + heading + "\n", 1)[1].split("\n## ", 1)[0]
    code = re.findall(r"```python\n(.*?)```", section, re.S)[index]
    for old, new in subs:
        if code.count(old) != 1:
            raise ValueError(f"{old!r} is in the snippet {code.count(old)} times, not exactly once")
        code = code.replace(old, new)
    return code


def host_facts(probe, machine_id):
    """What `find_host` compares, from one NODE_PROBE row (jobs/node_probe.py): pids.max (None for "max" or no file),
    the cgroup version, the Ray CPUs, and whether the container has a process limit."""
    value = probe.get("pids.max")
    pids_max = int(value) if value and value.isdigit() else None
    return {"machine_id": machine_id, "pids_max": pids_max, "cgroup": probe.get("cgroup"), "cpus": int(probe["ray_cpus"]),
            "limited": pids_max is not None}


def xdist_group(scenario, fixturenames, host=None):
    """The xdist group of one test, for `pytest -n N --dist loadgroup` (conftest forces loadgroup). xdist runs all tests
    of a group in one worker process, so a session fixture of that group runs `up` once and `down` once. A02 joins the
    aws_only group (it deletes the account boundary, so it runs first there; the other clusters wait for it). A test with
    the restricted IAM user joins lifecycle (one user for each run). F17 and F18 share one wheel build; F20 joins them,
    because F18 and F20 each fill about 1.5 GB of the dev box disk in a container (pip installs). Each libraries host
    (H1, H2, H3, gpu2) is its own cluster and group. None: any worker."""
    if "aws_only" in fixturenames or scenario == "A02":
        return "aws_only"
    if "lifecycle" in fixturenames or "restricted_user" in fixturenames:
        return "lifecycle"
    if "libraries" in fixturenames:
        return f"libraries-{host.lower()}"
    if "dist" in fixturenames or scenario == "F20":
        return "dist"
    return "release" if "release" in fixturenames else None


class Run:
    """The context of one live-suite run, passed in explicitly: the `run` fixture gives it to a scenario, and each Cluster
    takes it as its first argument. It holds the run identity (`dir`: `<results>/_session/runs/<run id>`), the record
    root (`results`), the secret sink of this process, and the clusters of this process.
    `register(cluster)` is the only way a cluster joins the run: Cluster.__init__ calls it, before any `up`."""

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

    def record(self, scenario, markers, fixture):
        self.dir.mkdir(parents=True, exist_ok=True)
        record = Recorder(self.results, scenario, markers, fixture)
        self._dirs.append(record.dir)
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
        (`end_cluster` scans those). Returns the paths that hold a secret, relative to `results`."""
        owned = [Path(d) for c in self._clusters for d in c.records]
        dirs = [d for d in dict.fromkeys(self._dirs) if not any(d == o or o in d.parents for o in owned)]
        values = self.secret_values()
        return sorted({str(p.relative_to(self.results)) for d in dirs if d.exists() for p in scan_secrets(d, values)})

    def register(self, cluster):
        """Write `<dir>/opened/<name>.json` ({name, region, kept, yaml, home}), so the end of the run checks the cluster,
        also after a failed `up` or in another worker process."""
        path = self.dir / "opened" / f"{cluster.name}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"name": cluster.name, "region": cluster.region, "kept": cluster.kept, "yaml": str(cluster.yaml),
                                    "home": str(cluster.home)}))
        self._clusters.append(cluster)


def _clean(done):
    return done.returncode == 0 and "left: nothing" in done.stdout


def end_cluster(run, cluster, sweep, aws_check, after_down=None):
    """End one cluster cleanly: the only place in the suite that does. Every cluster of the run comes here once (a group
    cluster at the end of its group, a scenario's own cluster at the end of its test, the release cluster).
    1. `down`, unless the cluster is kept, or its last `down` (a scenario's, for example A01, L10, R02) exited 0 with
       `left: nothing`. A `down` that leaves something is a problem, and it runs once more.
    2. `after_down(done)` (the checks of the group; `done` is the last `down`, None for a kept cluster) gives more problems.
    3. Not kept: the vast label sweep of this cluster (an instance that it still finds is a problem) and the AWS check of
       this cluster in its region.
    4. The secret scan of this cluster's records only (`cluster.records`: its own directories, not a name glob), with the
       secret values of the run.
    The cluster: `name`, `region`, `kept`, `last_down`, `records`, `down()` (returns returncode, stdout, stderr).
    `sweep(prefixes)` gives log lines (`destroyed <id> (<label>)` for each destroyed instance); `aws_check(name, region)`
    gives the resources left. Writes `<run.dir>/ended/<name>.json`. Returns the problems (empty: the cluster ended clean)."""
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
            destroyed = [line for line in sweep([f"ray-vast:{cluster.name}"]) if line.startswith("destroyed ")]
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
    - a final vast label sweep of the test label and of each cluster that joined the run but did not end (its worker died);
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
            lines = sweep([test_label] + [f"ray-vast:{c['name']}" for c in not_ended if not c["kept"]])
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


def timeline_spend(rows):
    """The vast spend of one cluster in $, from its timeline (Cluster.start_poller): each sample's instances cost their
    dph_total until the next sample. A failed sample keeps the instances of the last good one. In a parallel run the
    credit of the account moves for all clusters at once, so each group uses this estimate."""
    total, last = 0.0, []
    for row, after in zip(rows, rows[1:]):
        last = row.get("instances", last)
        total += sum(i.get("dph_total") or 0 for i in last) * (after["t"] - row["t"]) / 3600
    return round(total, 4)


LOG_TIME = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),(\d{3})")


def log_epoch(line, tz):
    """The epoch of a monitor.log line. Python logging writes the local time of the head container, and the rayproject/ray
    images set TZ=America/Los_Angeles, so `tz` is the container's `date +%z` (for example "-0700"). None without a time."""
    from datetime import datetime
    m = LOG_TIME.match(line)
    return datetime.strptime(f"{m.group(1)}.{m.group(2)} {tz}", "%Y-%m-%d %H:%M:%S.%f %z").timestamp() if m else None


def host_of(item):
    """The libraries host of a test item: its `where` parameter or its `host` marker (None for other tests)."""
    params = getattr(item, "callspec", None)
    if params is not None and "where" in params.params:
        return params.params["where"]
    marker = item.get_closest_marker("host")
    return marker.args[0] if marker else None
