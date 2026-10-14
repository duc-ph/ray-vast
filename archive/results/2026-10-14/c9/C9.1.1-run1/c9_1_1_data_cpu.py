# C9.1.1: Ray Data CPU pipeline: range -> map_batches (numpy) -> filter -> count / take.
# PASS: the count is right, and the tasks ran on >= C9_MIN_NODES nodes (Ray state API).
import time

import ray

from c9common import MIN_NODES, SMALL, error_info, job_task_nodes, result, start, wait_for_nodes

ID = "C9.1.1"
N = 100_000 if SMALL else 10_000_000
start(ID, 1200)
wait_for_nodes(ID)


def double(batch):
    batch["id"] = batch["id"] * 2
    return batch


try:
    t = time.time()
    ds = ray.data.range(N).map_batches(double, batch_format="numpy").filter(lambda row: row["id"] % 3 == 0)
    count = ds.count()
    first = ds.take(3)
    seconds = time.time() - t
except Exception as e:
    result(ID, "FAIL", error_info(e))
expected = len(range(0, 2 * N, 6))  # 2*i with i in [0, N) and 2*i divisible by 3
nodes = job_task_nodes()
print(ds.stats(), flush=True)
data = {"rows": N, "count": count, "expected": expected, "first": first, "seconds": round(seconds, 2),
        "rows_per_s": round(2 * N / seconds), "task_nodes": nodes}
ok = count == expected and "error" not in nodes and len(nodes) >= MIN_NODES
result(ID, "PASS" if ok else "FAIL", data)
