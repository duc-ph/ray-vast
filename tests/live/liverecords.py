"""The records of the live suite (unit-tested in tests/test_live_suite_kit.py): one Recorder for each test run, the
outcome of a test, the summary table of a run, the spend tables and the secret scan of the records."""
import json
import re
import time
from pathlib import Path


class Recorder:
    """The record of one test run: `<root>/<scenario>/result.json` plus evidence files in the same directory.
    Two verbs for a claim: `check` records it and the test goes on (the conftest fails the test at its end when a check
    is false); `require` records it and stops the test at once when it is false (the next steps need it)."""

    def __init__(self, root, scenario, fixture):
        self.dir = Path(root) / scenario
        if (self.dir / "result.json").exists():  # a re-run: keep the earlier record as <scenario>.run<n>
            n = 1
            while (Path(root) / f"{scenario}.run{n}").exists():
                n += 1
            self.dir.rename(Path(root) / f"{scenario}.run{n}")
        self.dir.mkdir(parents=True, exist_ok=True)
        self.result = {"id": scenario, "fixture": fixture, "started": time.time(),
                       "checks": [], "data": {}}

    def evidence(self, name, text):
        path = self.dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def check(self, name, ok, detail=""):
        """Record one check. Returns `ok`."""
        self.result["checks"].append({"name": name, "ok": bool(ok), "detail": str(detail)})
        return ok

    def require(self, name, ok, detail=""):
        """Record one check; a false one stops the test at once (AssertionError)."""
        assert self.check(name, ok, detail), f"{name}: {detail}"

    def failed_checks(self):
        """The names of the false checks."""
        return [c["name"] for c in self.result["checks"] if not c["ok"]]

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


def summary_table(root, expected=None):
    """A markdown table of the results under `root` (one directory for each run: `<name>` or `<name>[<param>]`).
    `expected`: the test names to list, in this order (default: every record under `root`); one with no result.json is
    NOT RUN."""
    root = Path(root)
    runs = {}
    for path in sorted(root.glob("*/result.json")):
        if re.search(r"\.run\d+$", path.parent.name):  # an earlier run of a scenario (Recorder keeps it); the last run counts
            continue
        result = json.loads(path.read_text())
        runs.setdefault(path.parent.name.split("[")[0], []).append((path.parent.name, result))
    lines = ["| Test | Result | Time (s) | Line | Evidence |", "|---|---|---|---|---|"]
    for scenario in runs if expected is None else expected:
        for name, result in runs.get(scenario, [(scenario, None)]):
            if result is None:
                lines.append(f"| {name} | NOT RUN | | | |")
                continue
            word, line = _verdict(result)
            line = " ".join(line.replace("|", "/").split())  # one row: no line break, no pipe
            lines.append(f"| {name} | {word} | {result.get('wall_s', '')} | {line} | `{root.name}/{name}/` |")
    return "\n".join(lines) + "\n"


def spend_tables(root):
    """The markdown spend tables of the run records under `root` ("" without a spend record): one row for each cluster
    group (`_session/<group>/spend.json`, written by the group teardown) and one for each run
    (`_session/runs/<run>/spend.json`, the credit before and after)."""
    root, text = Path(root), ""
    groups = []
    for path in sorted((root / "_session").glob("*/spend.json")):
        s = json.loads(path.read_text())
        groups.append(f"| {s['group']} | {s['cluster']} | ${s['vast_estimate']:.4f} | {s['hours']} h | "
                      f"${s['ec2_estimate']:.2f} |")
    if groups:
        text += ("\nSpend for each cluster group (vast: the dph_total of its instances over time, from its timeline; EC2: an "
                 "estimate from the hours):\n\n| Group | Cluster | vast | Hours | EC2 |\n|---|---|---|---|---|\n" + "\n".join(groups) + "\n")
    runs = []
    for path in sorted((root / "_session" / "runs").glob("*/spend.json")):
        s = json.loads(path.read_text())
        runs.append(f"| {path.parent.name} | ${s['spend']:.4f} | ${s['top_ups']:.2f} |")
    if runs:
        text += ("\nvast spend of each run (the credit before and after, plus the auto top-ups in between):\n\n"
                 "| Run | vast | Top-ups |\n|---|---|---|\n" + "\n".join(runs) + "\n")
    return text


def vast_spend(before, after, invoices):
    """The vast spend between two credit samples ({"t", "credit"}). The account tops itself up (a `payment` invoice
    with a negative amount), so the payments inside the window are added back."""
    top_ups = sum(-i["amount"] for i in invoices if i.get("type") == "payment" and before["t"] <= i.get("timestamp", 0) <= after["t"])
    return {"spend": round(before["credit"] - after["credit"] + top_ups, 4), "top_ups": top_ups,
            "credit_before": before["credit"], "credit_after": after["credit"]}


def timeline_spend(rows):
    """The vast spend of one cluster in $, from its timeline (livepoller.Poller): each sample's instances cost their
    dph_total until the next sample. A failed sample keeps the instances of the last good one. In a parallel run the
    credit of the account moves for all clusters at once, so each group uses this estimate."""
    total, last = 0.0, []
    for row, after in zip(rows, rows[1:]):
        last = row.get("instances", last)
        total += sum(i.get("dph_total") or 0 for i in last) * (after["t"] - row["t"]) / 3600
    return round(total, 4)


AWS_KEY_ID = r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"  # an AWS access key ID (AKIA: a user key, ASIA: a session key)
SECRET_PATTERNS = re.compile(rb'PRIVATE KEY|instance_api_key"?\s*[:=]\s*"?[A-Za-z0-9]|CONTAINER_API_KEY=[A-Za-z0-9]|Bearer [A-Za-z0-9]|'
                             + AWS_KEY_ID.encode())


def scan_secrets(root, secrets):
    """The files under `root` that hold one of the secret values (the cluster tokens, the join tokens, the vast key) or a
    secret pattern (the suite's safety rule). Returns paths only: the caller never prints the text."""
    values = [s.strip().encode() for s in secrets if s and s.strip()]
    found = []
    for path in sorted(Path(root).rglob("*")):
        if path.is_file():
            data = path.read_bytes()
            if SECRET_PATTERNS.search(data) or any(v in data for v in values):
                found.append(path)
    return found
