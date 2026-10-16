# Live suite, group libraries (LB04): the C9 Data shuffles with the settings of docs/libraries/data.md at the host's B.
#   shuffle: C9.1.3 (random_shuffle and groupby().count() of about 3 GB).
#   gpu:     C9.1.4 (GPU batch inference with a callable class, then groupby("node").count()). Needs torch.
# The data.md settings: max_hash_shuffle_aggregators = B // 4, execution_options.resource_limits cpu = B // 4, read
# concurrency = B // 4, map TaskPoolStrategy(size=B // 4) (an actor pool for the callable class), override_num_blocks = B // 2.
# The C9 scripts set none of them (C10 phase B: both failed with `Failed to startup worker after retrying 5 times`).
# Prints `C9_RESULT <id> PASS|FAIL <json>` (c9common). Usage: python lb_data_tuned.py shuffle|gpu   Env: LB_B (default 10).
import os
import sys
import time

import numpy as np
import ray
from ray.data import DataContext, ExecutionResources, TaskPoolStrategy

from c9common import GPU, MIN_NODES, SMALL, error_info, job_task_nodes, result, short, start, wait_for_nodes

MODE = sys.argv[1]
ID = {"shuffle": "LB04.shuffle", "gpu": "LB04.gpu"}[MODE]
B = int(os.environ.get("LB_B", "10"))
start(ID, 2400)
nodes = wait_for_nodes(ID, gpus_per_node=GPU if MODE == "gpu" else 0)
ctx = DataContext.get_current()
ctx.max_hash_shuffle_aggregators = max(1, B // 4)
ctx.execution_options.resource_limits = ExecutionResources(cpu=max(1, B // 4))
settings = {"B": B, "max_hash_shuffle_aggregators": max(1, B // 4), "resource_limits_cpu": max(1, B // 4),
            "read_concurrency": max(1, B // 4), "override_num_blocks": max(1, B // 2)}
print(f"LB_SETTINGS {settings}", flush=True)

if MODE == "shuffle":
    store_gb = min(v["object_store_gb"] for v in nodes.values())
    target_gb = 0.02 if SMALL else min(1.5 * store_gb, float(os.environ.get("C9_MAX_GB", "3")))
    ROW_BYTES = 256 * 1024 * 8
    rows = max(10, int(target_gb * 1e9 / ROW_BYTES))

    def add_key(batch):
        batch["key"] = batch["data"][:, 0, 0] % 16
        return batch

    try:
        ds = ray.data.range_tensor(rows, shape=(256, 1024), override_num_blocks=settings["override_num_blocks"],
                                   concurrency=settings["read_concurrency"])
        ds = ds.map_batches(add_key, compute=TaskPoolStrategy(size=max(1, B // 4)))
        t = time.time()
        shuffled_rows = ds.random_shuffle().materialize().count()
        t_shuffle = time.time() - t
        t = time.time()
        groups = ds.groupby("key").count().take_all()
        t_group = time.time() - t
    except Exception as e:
        result(ID, "FAIL", dict(error_info(e), settings=settings))
    counts = {int(g["key"]): int(g["count()"]) for g in groups}
    data = {"settings": settings, "rows": rows, "dataset_gb": round(rows * ROW_BYTES / 1e9, 2), "shuffled_rows": shuffled_rows,
            "shuffle_s": round(t_shuffle, 1), "groupby_s": round(t_group, 1), "groups": len(counts),
            "group_total": sum(counts.values()), "task_nodes": job_task_nodes()}
    result(ID, "PASS" if shuffled_rows == rows and data["group_total"] == rows and len(counts) == min(16, rows) else "FAIL", data)

ROWS = 2_000 if SMALL else 200_000


class Scorer:
    def __init__(self):
        import torch
        self.torch = torch
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        torch.manual_seed(0)
        self.model = torch.nn.Sequential(torch.nn.Linear(256, 1024), torch.nn.ReLU(), torch.nn.Linear(1024, 10)).to(self.device).eval()
        self.node = ray.get_runtime_context().get_node_id()

    def __call__(self, batch):
        with self.torch.inference_mode():
            x = self.torch.as_tensor(batch["data"], dtype=self.torch.float32, device=self.device)
            pred = self.model(x).argmax(dim=1).cpu().numpy()
        n = len(pred)
        return {"pred": pred, "node": np.array([self.node] * n), "device": np.array([self.device] * n)}


try:
    ds = ray.data.range_tensor(ROWS, shape=(256,), override_num_blocks=settings["override_num_blocks"],
                               concurrency=settings["read_concurrency"])
    t = time.time()
    out = ds.map_batches(Scorer, concurrency=MIN_NODES, num_gpus=GPU, batch_size=4096).select_columns(["node", "device"]).materialize()
    seconds = time.time() - t
    scored = out.count()
    per_node = out.groupby("node").count().take_all()
    devices = sorted(out.unique("device"))
except Exception as e:
    result(ID, "FAIL", dict(error_info(e), settings=settings))
data = {"settings": settings, "rows": ROWS, "scored": scored, "seconds": round(seconds, 1), "rows_per_s": round(ROWS / seconds),
        "rows_per_node": {short(r["node"]): r["count()"] for r in per_node}, "devices": devices}
ok = scored == ROWS and len(per_node) >= MIN_NODES and devices == (["cuda"] if GPU else ["cpu"])
result(ID, "PASS" if ok else "FAIL", data)
