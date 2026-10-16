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
            line = line.replace("|", "/")
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


OUR_NAME = re.compile(r"^ray-vast-(rvtest-[a-z0-9-]+|livetest-\d{8}(-s3)?)$")


def assert_ours(name):
    """Every name that the suite deletes passes here first. Only the suite's clusters and S3 buckets (`rvtest-*`) and
    its IAM test users (`livetest-<date>`, `livetest-<date>-s3`) are ours; never the dev box, its key pair, the two account
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
    """Rent a host until its probe matches `want` (for example {"pids_max": 1280, "cgroup": "v1", "cpus": 48}).
    `rent(blocked)` rents one host, never on a blocked machine, and returns its probe (with `machine_id`). `end(probe)`
    ends a host that does not match. vast can give the same machine again, so each tried machine is blocked.
    Returns (the matching probe, the list of tries); raises HostNotFound after HOST_RENTALS_MAX rentals."""
    blocked, tries = [], []
    for _ in range(HOST_RENTALS_MAX):
        probe = rent(list(blocked))
        match = all(probe.get(key) == value for key, value in want.items())
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
    """The scenarios of the claim matrix (docs/test-plan.md): [{id, markers, fixture}] in the order of the tables."""
    rows = []
    for line in text.splitlines():
        m = ROW.match(line)
        if m and m.group(3).strip():
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
