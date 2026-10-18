# Tune checkpoints with the default storage_path (a local path on the head; no shared storage).
# Records what Ray 2.59 does on a multi-node cluster: an error, a warning, or checkpoints that stay on the workers.
# PASS: fit works and every trial's last checkpoint is readable from the driver (on the head).
# LIMIT: Ray refuses the setup, or the checkpoints are not on the head. The JSON tells which.
# Set JOB_STORAGE (for example s3://bucket/prefix, with credentials in env_vars) to repeat it with shared storage.
import os
import tempfile
import time

import ray
from ray import tune
from ray.tune import Checkpoint

from jobcommon import SMALL, error_info, result, short, start, storage_settings, wait_for_nodes

ID = "tune_checkpoint"
start(ID, 1200)
wait_for_nodes(ID)


def trainable(config):
    node = ray.get_runtime_context().get_node_id()
    for step in range(3):
        time.sleep(0.5)
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "state.txt"), "w") as f:
                f.write(f"{config['i']} {step} {node}")
            tune.report({"step": step, "node": node}, checkpoint=Checkpoint.from_directory(d))


storage, storage_fs = storage_settings()
try:
    grid = tune.Tuner(tune.with_resources(trainable, {"cpu": 1}), param_space={"i": tune.grid_search(list(range(2 if SMALL else 4)))},
                      run_config=tune.RunConfig(name="tune_checkpoint", storage_path=storage, storage_filesystem=storage_fs),
                      tune_config=tune.TuneConfig(metric="step", mode="max")).fit()
except Exception as e:
    result(ID, "LIMIT", dict(error_info(e), storage=storage))
trials = []
for r in grid:
    row = {"node": short((r.metrics or {}).get("node")), "error": str(r.error)[:300] if r.error else None,
           "checkpoint": r.checkpoint.path if r.checkpoint else None}
    if r.checkpoint:
        try:
            with r.checkpoint.as_directory() as d:
                row["readable_on_driver"] = os.path.exists(os.path.join(d, "state.txt"))
        except Exception as e:
            row["readable_on_driver"] = False
            row["read_error"] = error_info(e)["error_head"][:2]
    trials.append(row)
data = {"storage": storage or "default", "experiment_path": grid.experiment_path, "errors": grid.num_errors, "trials": trials}
ok = grid.num_errors == 0 and all(t.get("readable_on_driver") for t in trials)
result(ID, "PASS" if ok else "LIMIT", data)
