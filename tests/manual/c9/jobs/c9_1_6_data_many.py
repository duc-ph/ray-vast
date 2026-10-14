# C9.1.6: many small tasks: 400 blocks, map_batches with num_cpus=0.05, so Ray could run far more than 58 tasks on one node.
# Each task sleeps 1 s and returns its node and pid. Records the distinct worker processes per node (each needs one of the
# 58 worker ports) and the time. PASS: all 400 blocks finish.
import collections
import os
import time

import numpy as np
import ray

from c9common import SMALL, error_info, result, short, start, wait_for_nodes

ID = "C9.1.6"
BLOCKS = 40 if SMALL else 400
start(ID, 1800)
wait_for_nodes(ID)


def slow(batch):
    t = time.time()
    time.sleep(1)
    return {"node": np.array([ray.get_runtime_context().get_node_id()]), "pid": np.array([os.getpid()]),
            "start": np.array([t]), "end": np.array([time.time()])}


try:
    t = time.time()
    rows = ray.data.range(BLOCKS * 10, override_num_blocks=BLOCKS).map_batches(slow, batch_size=None, num_cpus=0.05).take_all()
    seconds = time.time() - t
except Exception as e:
    result(ID, "FAIL", error_info(e))
pids = collections.defaultdict(set)
for r in rows:
    pids[short(r["node"])].add(int(r["pid"]))
events = sorted([(r["start"], 1) for r in rows] + [(r["end"], -1) for r in rows])
running = peak = 0
for _, step in events:
    running += step
    peak = max(peak, running)
data = {"blocks": BLOCKS, "tasks": len(rows), "seconds": round(seconds, 1), "processes_per_node": {k: len(v) for k, v in pids.items()},
        "peak_concurrent_tasks": peak}
result(ID, "PASS" if len(rows) == BLOCKS else "FAIL", data)
