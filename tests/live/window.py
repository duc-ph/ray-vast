"""The observation window of the live suite: a test opens a window on a cluster, acts, and then asks what happened since
the window opened. All the head log regexes and the time-zone logic of the suite are here.

The head adapter (a `cluster.Cluster`, or a fake in tests/test_live_suite_monitor_log.py): `monitor_log(start_line)`,
`monitor_log_length()`, `head_time_zone()`, `instances()` (the vast listing of the cluster now) and `timeline` (the
cluster's poller samples, livepoller.Poller)."""
import ast
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from zoneinfo import ZoneInfo

from ray_vast.provider import FAILED_STATUSES, PENDING_STATUSES

# Python logging writes the local time of the head container (the rayproject/ray images set TZ=America/Los_Angeles).
LINE = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),(\d{3})\s+(\w+) (\S+) -- (.*)$")
# Each kind: the start of the message of its line (provider.py, head_server.py, Ray's node_provider.py and util.py).
KINDS = {
    "rented": re.compile(r"create (?P<count>\d+) x (?P<node_type>\S+): rented (?P<rented>\d+) candidates"),
    "enrolled": re.compile(r"enrolled instance (?P<instance>\d+) \(machine (?P<machine>\d+), ip (?P<ip>[\d.]+)\) into (?P<slot>\S+)"),
    "ending": re.compile(r"ending instance (?P<instance>\d+) \(machine (?P<machine>\S+), ip (?P<ip>\S+)\): (?P<reason>.*)"),
    "refused": re.compile(r"enroll refused for instance (?P<instance>\d+) from (?P<ip>\S+): (?P<reason>.*)"),
    "destroy_request": re.compile(r"worker (?P<instance>\d+) asks to be destroyed: (?P<reason>.*)"),
    "process_limit": re.compile(r"worker (?P<slot>\S+) \(instance (?P<instance>\d+), ip (?P<ip>\S+)\) is at its process limit: "
                                r"(?P<refused>\d+) refused forks/threads \(\+(?P<refused_new>\d+)\), pids (?P<pids>\d+) of (?P<pids_max>\d+), "
                                r"worker ports in use (?P<ports_in_use>\d+) of (?P<ports_total>\d+)\. (?P<advice>.*?) See (?P<docs>\S+)$"),
    "unknown_status": re.compile(r"reconcile: unknown vast actual_status (?P<status>.*) \(treated as pending\)"),
    "launch_failed": re.compile(r"Failed to launch (?P<count>\d+) nodes of type (?P<node_type>\S+): (?P<reason>.*)"),
    "legacy_field": re.compile(r"Dropping the empty legacy field (?P<name>\w+)\."),
}
# The cause of an `ending` line, from the start of its reason (the reasons of provider.py).
CAUSES = {"its request has no free slot left": "no_free_slot", "terminated by the autoscaler": "autoscaler",
          "not running with an IP after ": "pull_limit", "instance is gone from vast": "gone_from_vast",
          "not enrolled ": "enroll_timeout"}


def _value(text):
    """A field of a line: digits are an int, a Python repr (`'its request ...'`) is its string."""
    if text.isdigit():
        return int(text)
    if len(text) >= 2 and text[0] == text[-1] and text[0] in "'\"":
        try:
            return ast.literal_eval(text)
        except (ValueError, SyntaxError):
            pass
    return text


@dataclass
class Event:
    """One head log line of a known kind: its head-clock epoch `t`, the vast instance (an int, None when the line names
    none), the reason (None when the kind has none), the other fields of the line, and the line."""
    kind: str
    t: float
    line: str
    instance: int | None = None
    reason: str | None = None
    fields: dict = field(default_factory=dict)

    @property
    def cause(self):
        """For an `ending`: no_free_slot, autoscaler, pull_limit, gone_from_vast, enroll_timeout or other."""
        return next((c for start, c in CAUSES.items() if (self.reason or "").startswith(start)), "other")


def events(lines, zone, kind=None, instances=None):
    """The events of these head log lines, in log order; `zone`: the time zone of the head (a name or a ZoneInfo); `kind`:
    only that kind; `instances`: only the events of these vast instance IDs. A line without a log time is no event."""
    zone = zone if isinstance(zone, ZoneInfo) else ZoneInfo(zone.lstrip(":") or "UTC")
    wanted = None if instances is None else {int(i) for i in instances}
    found = []
    for line in lines:
        m = LINE.match(line)
        if not m:
            continue
        for name, pattern in KINDS.items():
            hit = pattern.match(m.group(5)) if kind in (None, name) else None
            if not hit:
                continue
            values = {k: _value(v) for k, v in hit.groupdict().items()}
            instance = values.pop("instance", None)
            if wanted is not None and instance not in wanted:
                continue
            t = datetime.strptime(f"{m.group(1)}.{m.group(2)}", "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=zone).timestamp()
            found.append(Event(name, round(t, 3), line, instance, values.pop("reason", None), values))
    return found


class Window:
    """Opened on a head: from now on (or `from_start`: from the first line of the head log)."""

    def __init__(self, head, clock=time, from_start=False):
        self.head, self.clock = head, clock
        self.opened = clock.time()
        self.start_line = 1 if from_start else head.monitor_log_length() + 1
        self.zone = head.head_time_zone()
        self._listings = []

    def lines(self):
        """The head log lines since the window opened."""
        return self.head.monitor_log(self.start_line)

    def events(self, kind=None, instances=None, lines=None):
        """The events since the window opened (module `events`, in the head's time zone); `lines`: read these head log
        lines instead (for example the output of a documented grep)."""
        return events(self.lines() if lines is None else lines, self.zone, kind, instances)

    # --- vast ----------------------------------------------------------------------------------------------------------
    def vast_now(self):
        """The vast instances of the cluster now. The listing is also a sample of `vast_seen`."""
        listing = self.head.instances()
        self._listings.append({"t": round(self.clock.time(), 1), "instances": listing})
        return listing

    def _samples(self):
        """The poller samples from 5 s before the window opened (a sample in the same poll period counts) and the listings
        of `vast_now`, in time order. A failed poller sample (no `instances`) is no sample."""
        rows = [r for r in self.head.timeline if "instances" in r and r["t"] >= self.opened - 5] + self._listings
        return sorted(rows, key=lambda r: r["t"])

    def vast_seen(self):
        """{instance ID: [(t, status), ...]}: each status change of each instance that the window saw, in time order. The
        status `gone`: a later sample did not list the instance."""
        changes = {}
        for row in self._samples():
            listed = {i["id"]: i.get("actual_status") for i in row["instances"]}
            for iid, status in listed.items():
                if not changes.get(iid) or changes[iid][-1][1] != status:
                    changes.setdefault(iid, []).append((row["t"], status))
            for iid, seen in changes.items():
                if iid not in listed and seen[-1][1] != "gone":
                    seen.append((row["t"], "gone"))
        return changes

    def enroll_seconds(self):
        """{instance ID: s}: the time of its `enrolled` line minus its first sample with the status `running` (an instance
        with only one of the two is not in it)."""
        running = {iid: next(t for t, s in seen if s == "running") for iid, seen in self.vast_seen().items()
                   if any(s == "running" for _, s in seen)}
        enrolled = {e.instance: e.t for e in self.events("enrolled")}
        return {iid: round(t - running[iid], 1) for iid, t in enrolled.items() if iid in running}


def phase(status):
    """The phase of a vast `actual_status` as the provider treats it: running, pending (the provider waits), failed (the
    provider ends the instance) or unknown (the provider logs it once and waits)."""
    if status == "running":
        return "running"
    if status in PENDING_STATUSES:
        return "pending"
    return "failed" if status in FAILED_STATUSES else "unknown"
