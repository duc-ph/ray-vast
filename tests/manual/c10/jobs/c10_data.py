# C10.data: a Ray Data pipeline: range -> map_batches (num_cpus=0.05) -> groupby(key).count().
#   many:  Ray Data defaults. The hash shuffle starts min(cluster CPUs, 128) aggregator actors, one worker process each, and
#          the map tasks get up to CPUs / 0.05 processes: more processes than worker ports. The aggregators hold the ports,
#          and the tasks that feed them never get a process: the job hangs.
#   tuned: budget = ports - 4. The processes of all operators together stay <= budget, a quarter each for: the shuffle
#          aggregators (DataContext.max_hash_shuffle_aggregators), the read tasks (concurrency), the map tasks
#          (compute=TaskPoolStrategy(size=...)), and the shuffle tasks (execution_options.resource_limits CPU; they take 1 CPU).
#   tuned2: tuned, and override_num_blocks = budget // 2 (added in the live run, 2026-10-16: the hash shuffle starts one
#          `_shuffle_block` task (1 CPU) for each input block that arrives, and resource_limits does not cap these tasks; on
#          a 24-CPU node 16 of them ran at once and took all 14 ports. Fewer blocks give fewer shuffle tasks at once.)
# Usage: python c10_data.py many|tuned|tuned2
import time

import numpy as np
import ray
from ray.data import DataContext, ExecutionResources

from c10common import PROGRESS, SCALE, error_info, finish, start, target

ID = "C10.data"
mode = start(ID, 900)
cpus, ports, budget = target()
BLOCKS = max(20, int(400 * SCALE)) if mode != "tuned2" else max(1, budget // 2)
ROWS = max(20, int(400 * SCALE)) * 1000  # the same rows in every mode


def add_key(batch):
    time.sleep(0.05)
    batch["key"] = batch["id"] % 16
    return batch


params = {"blocks": BLOCKS, "rows": ROWS, "map_num_cpus": 0.05}
if mode in ("tuned", "tuned2"):
    context = DataContext.get_current()
    context.max_hash_shuffle_aggregators = budget // 4
    context.execution_options.resource_limits = ExecutionResources(cpu=budget // 4)
    params.update(max_hash_shuffle_aggregators=budget // 4, resource_limits_cpu=budget // 4, read_concurrency=budget // 4,
                  map_concurrency=budget // 4)
print(f"C10_PARAMS {params}", flush=True)
PROGRESS.update(params, stage="groupby")
try:
    t = time.time()
    tuned = mode in ("tuned", "tuned2")
    ds = ray.data.range(ROWS, override_num_blocks=BLOCKS, **({"concurrency": budget // 4} if tuned else {}))
    ds = ds.map_batches(add_key, num_cpus=0.05, **({"compute": ray.data.TaskPoolStrategy(size=budget // 4)} if tuned else {}))
    groups = ds.groupby("key").count().take_all()
    seconds = time.time() - t
except Exception as e:
    finish(ID, mode, "ERROR", dict(error_info(e), progress=dict(PROGRESS)))
total = int(np.sum([g["count()"] for g in groups]))
kind = "COMPLETED" if total == ROWS and len(groups) == 16 else "ERROR"
finish(ID, mode, kind, dict(params, groups=len(groups), total=total, seconds=round(seconds, 1)))
