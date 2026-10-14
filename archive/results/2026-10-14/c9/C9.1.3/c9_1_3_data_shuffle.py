# C9.1.3: all-to-all operations: random_shuffle and groupby(...).count() on the C9.1.2 dataset (same size rule).
# Blocks move between the workers. PASS: the shuffle keeps all rows, and the 16 group counts add up to the row count.
import os
import time

import ray

from c9common import SMALL, error_info, job_task_nodes, result, start, wait_for_nodes

ID = "C9.1.3"
start(ID, 2400)
nodes = wait_for_nodes(ID)
store_gb = min(v["object_store_gb"] for v in nodes.values())
target_gb = 0.02 if SMALL else min(1.5 * store_gb, float(os.environ.get("C9_MAX_GB", "3")))
ROW_BYTES = 256 * 1024 * 8
rows = max(10, int(target_gb * 1e9 / ROW_BYTES))


def add_key(batch):
    batch["key"] = batch["data"][:, 0, 0] % 16
    return batch


try:
    ds = ray.data.range_tensor(rows, shape=(256, 1024), override_num_blocks=max(8, rows // 50)).map_batches(add_key)
    t = time.time()
    shuffled = ds.random_shuffle().materialize()
    shuffled_rows = shuffled.count()
    t_shuffle = time.time() - t
    t = time.time()
    groups = ds.groupby("key").count().take_all()
    t_group = time.time() - t
except Exception as e:
    result(ID, "FAIL", error_info(e))
counts = {int(g["key"]): int(g["count()"]) for g in groups}
data = {"rows": rows, "dataset_gb": round(rows * ROW_BYTES / 1e9, 2), "shuffled_rows": shuffled_rows,
        "shuffle_s": round(t_shuffle, 1), "groupby_s": round(t_group, 1), "groups": len(counts),
        "group_total": sum(counts.values()), "task_nodes": job_task_nodes()}
ok = shuffled_rows == rows and data["group_total"] == rows and len(counts) == min(16, rows)
result(ID, "PASS" if ok else "FAIL", data)
