# Live suite, group libraries (LB09): Serve autoscaling with the serve.md settings: `max_replicas` = B x workers,
# `num_cpus` = CPUS / B, no max_replicas_per_node. A load of concurrent DeploymentHandle calls (2 s each) for LB_LOAD_S
# seconds makes Serve scale out. Every 5 s the job counts the ALIVE replica actors on each node.
# PASS: Serve scaled out above 1 replica, never above max_replicas, and never above B replicas on one node.
# Prints `LB_RESULT serve_autoscale PASS|FAIL <json>`.   Env: LB_B, LB_CPUS, LB_WORKERS, LB_LOAD_S (default 150).
import collections
import json
import os
import sys
import threading
import time

import ray
from ray import serve

B, CPUS, WORKERS = int(os.environ["LB_B"]), float(os.environ["LB_CPUS"]), int(os.environ["LB_WORKERS"])
LOAD_S = float(os.environ.get("LB_LOAD_S", "150"))
MAX = B * WORKERS
t0 = time.time()
ray.init(address=os.environ.get("RAY_ADDRESS", "auto"))


@serve.deployment(autoscaling_config={"min_replicas": 1, "max_replicas": MAX, "target_ongoing_requests": 1},
                  ray_actor_options={"num_cpus": round(CPUS / B, 3)})
class Slow:
    def __call__(self, i):
        time.sleep(2)
        return i


def replicas_per_node():
    from ray.util.state import list_actors
    actors = list_actors(address=ray.get_runtime_context().gcs_address, filters=[("state", "=", "ALIVE")], limit=10000,
                         raise_on_missing_output=False)
    return dict(collections.Counter(a.node_id[:8] for a in actors if a.class_name.startswith("ServeReplica:lbauto:Slow")))


samples, stop = [], threading.Event()


def sample():
    while not stop.is_set():
        try:
            samples.append({"t": round(time.time() - t0, 1), "per_node": replicas_per_node()})
        except Exception as e:  # one bad sample must not end the job
            samples.append({"t": round(time.time() - t0, 1), "error": type(e).__name__})
        stop.wait(5)


data = {"B": B, "CPUS": CPUS, "workers": WORKERS, "max_replicas": MAX, "num_cpus": round(CPUS / B, 3)}
try:
    handle = serve.run(Slow.bind(), name="lbauto", route_prefix=None)
    threading.Thread(target=sample, daemon=True).start()
    answered, i = 0, 0
    while time.time() - t0 < LOAD_S:
        batch = [handle.remote(i + k) for k in range(4 * MAX)]
        i += len(batch)
        answered += sum(1 for r in batch if r.result(timeout_s=300) is not None)
    stop.set()
    serve.delete("lbauto")
except Exception as e:
    print("LB_RESULT serve_autoscale FAIL " + json.dumps(dict(data, error_class=type(e).__name__, error_head=str(e).splitlines()[:8],
                                                             samples=samples[-5:])), flush=True)
    sys.exit(1)
totals = [sum(s.get("per_node", {}).values()) for s in samples]
per_node_max = max((max(s["per_node"].values()) for s in samples if s.get("per_node")), default=0)
data.update(calls=i, answered=answered, peak_replicas=max(totals, default=0), per_node_max=per_node_max, samples=samples[::3])
ok = 1 < data["peak_replicas"] <= MAX and per_node_max <= B and answered == i
print(f"LB_RESULT serve_autoscale {'PASS' if ok else 'FAIL'} " + json.dumps(data), flush=True)
sys.exit(0 if ok else 1)
