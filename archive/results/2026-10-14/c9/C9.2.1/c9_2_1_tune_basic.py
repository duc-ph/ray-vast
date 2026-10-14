# C9.2.1: Ray Tune function trainable, 8 trials (num_samples=8), 1 CPU each, a short loop that reports a metric.
# PASS: the ResultGrid has 8 results without errors, and the trials ran on >= C9_MIN_NODES nodes.
# Phase 2 change: with 1 CPU each, Ray's scheduler packed all 8 trials on one node (correct Ray behavior: the workers have
# 13-48 CPUs). Each trial now asks for the CPU count of the smallest worker node, so one node cannot hold all trials.
import time

import ray
from ray import tune

from c9common import MIN_NODES, SMALL, error_info, result, short, start, wait_for_nodes

ID = "C9.2.1"
STEPS = 3 if SMALL else 10
start(ID, 1200)
nodes = wait_for_nodes(ID)
TRIAL_CPU = 1 if SMALL else min(n["CPU"] for n in nodes.values())


def trainable(config):
    node = ray.get_runtime_context().get_node_id()
    for step in range(STEPS):
        time.sleep(0.5)
        tune.report({"score": config["x"] * step, "step": step, "node": node})


try:
    t = time.time()
    grid = tune.Tuner(tune.with_resources(trainable, {"cpu": TRIAL_CPU}), param_space={"x": tune.uniform(0, 1)},
                      tune_config=tune.TuneConfig(num_samples=8, metric="score", mode="max")).fit()
    seconds = time.time() - t
except Exception as e:
    result(ID, "FAIL", error_info(e))
nodes = sorted({short(r.metrics.get("node")) for r in grid if r.metrics})
data = {"trial_cpu": TRIAL_CPU, "results": len(grid), "errors": grid.num_errors, "seconds": round(seconds, 1), "trial_nodes": nodes,
        "best_score": round(grid.get_best_result().metrics["score"], 3), "storage": grid.experiment_path}
result(ID, "PASS" if len(grid) == 8 and grid.num_errors == 0 and len(nodes) >= MIN_NODES else "FAIL", data)
