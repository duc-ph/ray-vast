# C9.2.2: Ray Tune with ASHAScheduler: 8 trials, max_t 20; bad trials stop early.
# PASS: 8 results without errors, and at least one trial stopped before max_t.
import time

import ray
from ray import tune
from ray.tune.schedulers import ASHAScheduler

from c9common import SMALL, error_info, result, short, start, wait_for_nodes

ID = "C9.2.2"
MAX_T = 8 if SMALL else 20
start(ID, 1200)
wait_for_nodes(ID)


def trainable(config):
    node = ray.get_runtime_context().get_node_id()
    for step in range(1, MAX_T + 1):
        time.sleep(0.3)
        tune.report({"loss": config["lr_bad"] + 1.0 / step, "node": node})


try:
    t = time.time()
    grid = tune.Tuner(tune.with_resources(trainable, {"cpu": 1}), param_space={"lr_bad": tune.grid_search([0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7])},
                      tune_config=tune.TuneConfig(metric="loss", mode="min",
                                                  scheduler=ASHAScheduler(max_t=MAX_T, grace_period=2, reduction_factor=2))).fit()
    seconds = time.time() - t
except Exception as e:
    result(ID, "FAIL", error_info(e))
iterations = sorted(r.metrics.get("training_iteration", 0) for r in grid)
data = {"results": len(grid), "errors": grid.num_errors, "iterations": iterations, "seconds": round(seconds, 1),
        "trial_nodes": sorted({short(r.metrics.get("node")) for r in grid if r.metrics})}
result(ID, "PASS" if len(grid) == 8 and grid.num_errors == 0 and min(iterations) < MAX_T else "FAIL", data)
