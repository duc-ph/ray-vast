"""The process-limit pairs of the library tests (unit-tested in tests/test_live_suite_kit.py). A pair is two runs of one
library on one worker with more CPUs than processes at once: the "tuned" run uses the settings of the library page and
must complete; the "many" run uses Ray's defaults and must fail the documented way (it hangs, errors or goes on with
fewer processes), with the head's process-limit WARNING and no node crash. `verdict` decides one run from the result
line of the job (jobs/paircommon.py) and the node limits (jobs/node_limits.py) before and after it."""
import re

# The head's process-limit WARNING line (head_server.py), for example: `worker 2 (instance 55900080, ip 203.0.113.7) is at its
# process limit: 3 refused forks/threads (+3), pids 1190 of 1280, worker ports in use 25 of 25. ...`.
WARNING_PATTERN = re.compile(r"WARNING.*is at its process limit")
REFUSED_PATTERN = re.compile(r"refused forks/threads \(\+[1-9]")  # refusals rose: a tuned run must not have one
PIDS_LIMIT_MAX = 3328  # a "many" run is INCONCLUSIVE on a node with no pids limit or a higher one
TORCH_ENV = {"pip": {"packages": ["torch==2.7.1"], "pip_check": False}, "config": {"setup_timeout_seconds": 1800}}


def events_max(limits):
    """The 'max N' counter of pids.events (forks that the pids limit refused), or None."""
    found = re.search(r"max (\d+)", (limits or {}).get("pids.events") or "")
    return int(found.group(1)) if found else None


def pids_max(limits):
    value = (limits or {}).get("pids.max")
    return int(value) if value and value.isdigit() else None


def verdict(kind, job, pre, post, warnings, limit_max=PIDS_LIMIT_MAX):
    """Decide one run. kind: tuned, default or many. A default run (Ray's defaults on a worker with CPU <= N) is a
    tuned run that must also have no process-limit WARNING line at all. job: {"kind": COMPLETED|HUNG|ERROR|DEGRADED|None,
    ...}. pre and post: the node limits of the target node before and after (None if it is gone). warnings: the head
    WARNING lines for the node (None = not checked). limit_max: a many run on a node with a higher pids.max is
    INCONCLUSIVE. A refused fork fails a tuned run (the advice is wrong); in a many run it is data. Returns (status,
    reasons)."""
    alive = post is not None and "error" not in post and post.get("node_id") == (pre or {}).get("node_id")
    before, after = events_max(pre), events_max(post)
    refused = None if before is None or after is None else after - before
    limit = pids_max(pre)
    if kind == "many":
        if limit is None or limit > limit_max:
            return "INCONCLUSIVE", [f"pids.max of the node is {limit or 'max'}, not <= {limit_max}"]
        reasons = []
        if job.get("kind") not in ("HUNG", "ERROR", "DEGRADED"):  # DEGRADED: it went on with fewer processes than it asked for
            reasons.append(f"the job did not fail ({job.get('kind')}): no advice is needed at this size")
        if not alive:
            reasons.append("the node crashed or left the cluster")
        no_port = (post or {}).get("no_port_errors", 0) - (pre or {}).get("no_port_errors", 0)
        if warnings is not None and not any(WARNING_PATTERN.search(w) for w in warnings):
            reasons.append("no head pids/ports WARNING line for the node")
            if no_port <= 0 and not refused:  # say why: the job waited for something else, for example CPUs
                reasons.append("no 'No available ports' line and no refused fork: the job hung for another reason "
                               "(CPUs?), not at the process limit")
        if warnings is None and no_port <= 0:  # no head log: Ray's own "No available ports" lines are the evidence
            reasons.append("no 'No available ports' line in the worker-*.err logs of the node")
        return ("FAIL" if reasons else "PASS"), reasons or [
            f"{job.get('kind')}, warning {'seen' if warnings else 'not checked'}, node alive, refused forks {refused}, "
            f"'No available ports' lines {no_port}"]
    reasons = []
    if job.get("kind") != "COMPLETED":
        reasons.append(f"the job did not complete ({job.get('kind')})")
    if not alive:
        reasons.append("the node crashed or left the cluster")
    if refused:
        reasons.append(f"pids.events: {refused} refused forks")
    if warnings and any(REFUSED_PATTERN.search(w) for w in warnings):
        reasons.append("a head WARNING line with new refused forks or thread starts")
    if kind == "default" and warnings and any(WARNING_PATTERN.search(w) for w in warnings):
        reasons.append("a head process-limit WARNING line for the node")
    return ("FAIL" if reasons else "PASS"), reasons or ["completed, node alive, no refused fork"]
