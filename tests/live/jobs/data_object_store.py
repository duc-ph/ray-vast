# A dataset of about 1.5 x the object store of one worker (capped by JOB_MAX_GB, default 3) moves between the nodes:
# range_tensor -> map_batches -> materialize -> iter_batches in the driver (the driver runs on the head).
# Records the time, the size and the spilled bytes (memory_summary). PASS: all rows arrive.
import os
import re
import time

import numpy as np
import ray

from jobcommon import SMALL, error_info, job_task_nodes, result, start, wait_for_nodes

ID = "data_object_store"
start(ID, 2400)
nodes = wait_for_nodes(ID)
store_gb = min(v["object_store_gb"] for v in nodes.values())
target_gb = 0.02 if SMALL else min(1.5 * store_gb, float(os.environ.get("JOB_MAX_GB", "3")))
ROW_BYTES = 256 * 1024 * 8  # one int64 tensor (256, 1024) = 2 MiB
rows = max(10, int(target_gb * 1e9 / ROW_BYTES))


def add_one(batch):
    batch["data"] = batch["data"] + 1
    return batch


try:
    t = time.time()
    ds = ray.data.range_tensor(rows, shape=(256, 1024), override_num_blocks=max(8, rows // 50)).map_batches(add_one)
    ds = ds.materialize()
    t_mat = time.time() - t
    got_rows, got_bytes, checksum = 0, 0, 0
    for batch in ds.iter_batches(batch_size=16, batch_format="numpy"):
        got_rows += len(batch["data"])
        got_bytes += batch["data"].nbytes
        checksum += int(batch["data"][:, 0, 0].sum())
    t_all = time.time() - t
except Exception as e:
    result(ID, "FAIL", error_info(e))
try:
    summary = ray._private.internal_api.memory_summary(address=ray.get_runtime_context().gcs_address, stats_only=True)
except Exception as e:
    summary = f"memory_summary failed: {type(e).__name__}: {e}"
print(summary, flush=True)
spilled = re.findall(r"Spilled (\d+) MiB", summary)
data = {"worker_object_store_gb": store_gb, "dataset_gb": round(rows * ROW_BYTES / 1e9, 2), "rows": rows, "got_rows": got_rows,
        "materialize_s": round(t_mat, 1), "total_s": round(t_all, 1), "driver_mb_per_s": round(got_bytes / 1e6 / max(t_all - t_mat, 1e-3), 1),
        "spilled_mib": [int(s) for s in spilled], "task_nodes": job_task_nodes(),
        "checksum_ok": checksum == sum(i + 1 for i in range(rows))}
result(ID, "PASS" if got_rows == rows and data["checksum_ok"] else "FAIL", data)
