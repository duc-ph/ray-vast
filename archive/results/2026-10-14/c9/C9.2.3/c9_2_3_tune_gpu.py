# C9.2.3: 2 Tune trials with resources {"cpu": 1, "gpu": 1}; each gets its own GPU, on a different worker.
# Each trial lasts about 20 s, so both run at the same time. PASS: 2 results, 2 nodes, 1 GPU id in each trial.
import time

import ray
from ray import tune

from c9common import GPU, MIN_NODES, SMALL, error_info, result, short, start, wait_for_nodes

ID = "C9.2.3"
start(ID, 1200)
wait_for_nodes(ID, gpus_per_node=GPU)


def trainable(config):
    node = ray.get_runtime_context().get_node_id()
    gpu_ids = [str(g) for g in ray.get_gpu_ids()]
    for step in range(4 if SMALL else 10):
        time.sleep(2)
        tune.report({"step": step, "node": node, "gpu_ids": ",".join(gpu_ids), "t": time.time()})


try:
    grid = tune.Tuner(tune.with_resources(trainable, {"cpu": 1, "gpu": GPU}), param_space={"i": tune.grid_search(list(range(MIN_NODES)))},
                      tune_config=tune.TuneConfig(metric="step", mode="max")).fit()
except Exception as e:
    result(ID, "FAIL", error_info(e))
trials = [{"node": short(r.metrics.get("node")), "gpu_ids": r.metrics.get("gpu_ids")} for r in grid if r.metrics]
data = {"results": len(grid), "errors": grid.num_errors, "trials": trials}
ok = (len(grid) == MIN_NODES and grid.num_errors == 0 and len({t["node"] for t in trials}) == MIN_NODES
      and all(len(t["gpu_ids"].split(",")) == 1 and t["gpu_ids"] for t in trials))
result(ID, "PASS" if ok else "FAIL", data)
