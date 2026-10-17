# Live suite, group libraries (LB11, a measurement): refused forks. One task on the target node (hard node affinity, num_cpus=0)
# starts sleeping threads until the container refuses one (EAGAIN: `can't start new thread`). It holds them for
# LB_HOLD_S seconds (the worker's heartbeat reports the refused count to the head), then lets them end.
# Safety: the task starts no thread on a node without a pids limit (pids.max "max" or no file), and it stops at
# pids.max threads in any case.
# Prints `LB_RESULT forks PASS|FAIL <json>`. Usage: python lb_forks.py <node id>   Env: LB_HOLD_S (default 90).
import json
import os
import sys
import threading
import time

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

NODE = sys.argv[1]
HOLD_S = float(os.environ.get("LB_HOLD_S", "90"))
ray.init(address=os.environ.get("RAY_ADDRESS", "auto"))


@ray.remote(num_cpus=0, max_retries=0)
def bomb(hold_s):
    def read(name):
        for base in ("/sys/fs/cgroup", "/sys/fs/cgroup/pids"):
            try:
                return open(f"{base}/{name}").read().strip()
            except OSError:
                continue
        return None
    limit = read("pids.max")
    out = {"pids.max": limit, "events_before": read("pids.events"), "current_before": read("pids.current")}
    if not limit or not limit.isdigit():
        return dict(out, error="no pids limit: no thread started")
    release, threads = threading.Event(), []
    try:
        while len(threads) < int(limit):
            thread = threading.Thread(target=release.wait, daemon=True)
            thread.start()
            threads.append(thread)
    except RuntimeError as e:  # `can't start new thread`: the pids limit refused it
        out["refused"] = str(e)
    out.update(started=len(threads), current_at_peak=read("pids.current"), events_at_peak=read("pids.events"))
    time.sleep(hold_s)
    release.set()
    for thread in threads:
        thread.join(timeout=5)
    out.update(events_after=read("pids.events"), current_after=read("pids.current"))
    return out


try:
    out = ray.get(bomb.options(scheduling_strategy=NodeAffinitySchedulingStrategy(NODE, soft=False)).remote(HOLD_S), timeout=900)
    status = "PASS" if out.get("refused") else "FAIL"
except Exception as e:  # the node can die under the bomb: that is one of the documented outcomes
    out, status = {"error_class": type(e).__name__, "error_head": str(e).splitlines()[:6]}, "FAIL"
print(f"LB_RESULT forks {status} " + json.dumps(out), flush=True)
sys.exit(0 if status == "PASS" else 1)
