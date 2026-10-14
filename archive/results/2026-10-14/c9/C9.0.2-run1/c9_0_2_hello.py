# C9.0.2: the working directory reaches every worker. 8 SPREAD tasks; each reads data/hello.txt and tells where it runs.
# PASS: the file text is right in every task, and the tasks ran on >= C9_MIN_NODES nodes.
import collections

import ray

from c9common import MIN_NODES, error_info, node_info, result, short, start, wait_for_nodes

ID = "C9.0.2"
start(ID, 600)
wait_for_nodes(ID)
expected = open("data/hello.txt").read()


@ray.remote(num_cpus=1, scheduling_strategy="SPREAD")
def probe(i):
    import os
    with open("data/hello.txt") as f:
        text = f.read()
    return dict(node_info(), index=i, file_ok=text == expected, cwd=os.getcwd())


try:
    rows = ray.get([probe.remote(i) for i in range(8)], timeout=300)
except Exception as e:
    result(ID, "FAIL", error_info(e))
for row in rows:
    print(row, flush=True)
nodes = collections.Counter(short(r["node_id"]) for r in rows)
data = {"tasks_per_node": dict(nodes), "ips": sorted({r["ip"] for r in rows}), "file_ok": all(r["file_ok"] for r in rows),
        "cwd_example": rows[0]["cwd"]}
result(ID, "PASS" if data["file_ok"] and len(nodes) >= MIN_NODES else "FAIL", data)
