# C9.0.2: the working directory reaches every worker. 4 tasks pinned to each worker node (NodeAffinity, hard); each reads
# data/hello.txt and tells where it runs. (Phase 2 change: 8 SPREAD tasks all ran in one warm worker process on one node,
# because SPREAD is best effort and the tasks are short.)
# PASS: the file text is right in every task, and the tasks ran on >= C9_MIN_NODES nodes.
import collections

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from c9common import MIN_NODES, error_info, node_info, result, short, start, wait_for_nodes

ID = "C9.0.2"
start(ID, 600)
nodes = wait_for_nodes(ID)
expected = open("data/hello.txt").read()


@ray.remote(num_cpus=1)
def probe(i):
    import os
    with open("data/hello.txt") as f:
        text = f.read()
    return dict(node_info(), index=i, file_ok=text == expected, cwd=os.getcwd())


try:
    rows = ray.get([probe.options(scheduling_strategy=NodeAffinitySchedulingStrategy(n, soft=False)).remote(i)
                    for i, n in enumerate(list(nodes) * 4)], timeout=600)
except Exception as e:
    result(ID, "FAIL", error_info(e))
for row in rows:
    print(row, flush=True)
nodes = collections.Counter(short(r["node_id"]) for r in rows)
data = {"tasks_per_node": dict(nodes), "ips": sorted({r["ip"] for r in rows}), "file_ok": all(r["file_ok"] for r in rows),
        "cwd_example": rows[0]["cwd"]}
result(ID, "PASS" if data["file_ok"] and len(nodes) >= MIN_NODES else "FAIL", data)
