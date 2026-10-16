# C10.tune: Ray Tune trials that each run their evaluation as a Ray task (a common pattern: parallel evaluation).
# Each trial has a placement group of 2 bundles of min(0.1, cpus / (2 * trials)) CPU: one for the trial, one for its task.
#   many:  max(ports + 4, 60) trials, all at once (Tune's default: as many as the CPUs allow). Each trial actor holds one
#          worker process; the trials take all worker ports, and their evaluation tasks never get a process: the job hangs.
#   tuned: TuneConfig(max_concurrent_trials=budget // 2) (budget = ports - 4): one process for each trial plus one for its
#          task. The same trials run in waves and complete.
# Usage: python c10_tune.py many|tuned
import time

import ray
from ray import tune

from c10common import PROGRESS, SCALE, error_info, finish, start, target

ID = "C10.tune"
mode = start(ID, 600)
cpus, ports, budget = target()
TRIALS = max(ports + 4, int(60 * SCALE))  # 2 processes for each trial: more than the ports at any size
STEPS = 3


@ray.remote(num_cpus=0)  # it runs in the trial's placement group (its second bundle)
def evaluate(x, step):
    time.sleep(0.5)
    return x * step


def trainable(config):
    for step in range(STEPS):
        tune.report({"score": ray.get(evaluate.remote(config["x"], step)), "step": step})


concurrent = budget // 2 if mode == "tuned" else 0  # 0: Tune's default, no limit
bundle = min(0.1, cpus / (2 * TRIALS))
params = {"trials": TRIALS, "bundle_cpu": round(bundle, 4), "max_concurrent_trials": concurrent or None}
print(f"C10_PARAMS {params}", flush=True)
PROGRESS.update(params)
try:
    t = time.time()
    resources = tune.PlacementGroupFactory([{"CPU": bundle}, {"CPU": bundle}])  # the trial and its evaluation task
    grid = tune.Tuner(tune.with_resources(trainable, resources), param_space={"x": tune.uniform(0, 1)},
                      tune_config=tune.TuneConfig(num_samples=TRIALS, metric="score", mode="max",
                                                  max_concurrent_trials=concurrent or None)).fit()
    seconds = time.time() - t
except Exception as e:
    finish(ID, mode, "ERROR", dict(error_info(e), progress=dict(PROGRESS)))
kind = "COMPLETED" if len(grid) == TRIALS and grid.num_errors == 0 else "ERROR"
finish(ID, mode, kind, dict(params, results=len(grid), errors=grid.num_errors, seconds=round(seconds, 1)))
