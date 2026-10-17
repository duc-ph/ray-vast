# Live suite, group libraries: run one snippet of a user doc (docs/libraries/*.md) and check what it did.
# The test copies the snippet from the doc (libraries_setup.doc_snippet: only the B and CPUS values, the bucket names and the
# recorded `...` bodies change) into `snippet.py` in the working dir. This script defines what the snippet needs and does
# not define itself (the prelude), runs the snippet with exec() as the job's main code, then checks the result
# (the epilogue). It prints one last line: `LB_RESULT <lib> PASS|FAIL <json>`, and exits 0 on PASS.
# Usage: python lb_snippet.py core|data|tune|train|serve|rllib_runners|rllib_learners
# Env: LB_GUARD_S (wall-clock guard, default 1800), LB_OUT (data: the output path), LB_ROWS (data: the input rows),
#      LB_ITEMS (core: the work items).
import collections
import json
import os
import sys
import threading
import time
import traceback

import ray

LIB = sys.argv[1]
T0 = time.time()
_lock = threading.Lock()


def result(status, data):
    with _lock:
        print(f"LB_RESULT {LIB} {status} " + json.dumps(dict(data, elapsed_s=round(time.time() - T0, 1)), default=str, sort_keys=True),
              flush=True)
        sys.stderr.flush()
        os._exit(0 if status == "PASS" else 1)


def guard():
    time.sleep(float(os.environ.get("LB_GUARD_S", "1800")))
    result("FAIL", {"error": "HUNG: the wall-clock guard fired"})


def actor_nodes(prefix):
    """{short node id: count} of the ALIVE actors whose class name starts with prefix."""
    from ray.util.state import list_actors
    actors = list_actors(address=ray.get_runtime_context().gcs_address, filters=[("state", "=", "ALIVE")], limit=10000,
                         raise_on_missing_output=False)
    return dict(collections.Counter(a.node_id[:8] for a in actors if a.class_name.startswith(prefix)))


threading.Thread(target=guard, daemon=True).start()
ray.init(address=os.environ.get("RAY_ADDRESS", "auto"))
snippet = open("snippet.py").read()
print(f"LB_START {LIB} {T0:.1f}\n--- snippet.py ---\n{snippet}--- end ---", flush=True)

# --- prelude: the names that the snippet uses but does not define ---
if LIB == "data":
    def my_fn(batch):
        return batch
elif LIB == "tune":
    import tempfile
    from ray import tune

    def trainable(config):
        node = ray.get_runtime_context().get_node_id()
        for step in range(3):
            time.sleep(0.5)
            with tempfile.TemporaryDirectory() as d:
                with open(os.path.join(d, "state.txt"), "w") as f:
                    f.write(f"{config['x']} {step}")
                tune.report({"score": config["x"] * step, "node": node}, checkpoint=tune.Checkpoint.from_directory(d))
elif LIB == "train":
    import torch

    torch.manual_seed(0)
    _x = torch.randn(20_000, 32)
    dataset = torch.utils.data.TensorDataset(_x, _x @ torch.randn(32, 1))

    def train_body(loader):
        """The body of the snippet's train_func (its `...`): DDP on the GPUs, 2 epochs, a checkpoint at each epoch."""
        import tempfile
        import ray.train
        import ray.train.torch
        loader = ray.train.torch.prepare_data_loader(loader)
        model = ray.train.torch.prepare_model(torch.nn.Sequential(torch.nn.Linear(32, 64), torch.nn.ReLU(), torch.nn.Linear(64, 1)))
        optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
        rank = ray.train.get_context().get_world_rank()
        for epoch in range(2):
            for xb, yb in loader:
                loss = torch.nn.functional.mse_loss(model(xb), yb)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
            print("LB_RANK " + json.dumps({"rank": rank, "epoch": epoch, "device": str(ray.train.torch.get_device()),
                                           "gpu_ids": [str(g) for g in ray.get_gpu_ids()],
                                           "node": ray.get_runtime_context().get_node_id()[:8]}), flush=True)
            with tempfile.TemporaryDirectory() as d:
                if rank == 0:
                    torch.save(model.state_dict(), os.path.join(d, "model.pt"))
                ray.train.report({"epoch": epoch, "loss": loss.item()},
                                 checkpoint=ray.train.Checkpoint.from_directory(d) if rank == 0 else None)
elif LIB == "rllib_learners":
    from ray.rllib.algorithms.ppo import PPOConfig  # the learner snippet uses the PPOConfig import of the first snippet

# --- the snippet ---
try:
    exec(compile(snippet, "snippet.py", "exec"), globals())
except Exception as e:
    result("FAIL", {"stage": "snippet", "error_class": type(e).__name__, "error_head": str(e).splitlines()[:12],
                    "traceback_tail": traceback.format_exception(e)[-3:]})

# --- epilogue: check what the snippet did ---
try:
    if LIB == "core":
        items = int(os.environ.get("LB_ITEMS", "100"))
        t = time.time()
        answers = ray.get([pool[i % len(pool)].work.remote(i) for i in range(items)], timeout=1200)
        data = {"CPUS": CPUS, "pool": len(pool), "items": items, "answers": len(answers), "seconds": round(time.time() - t, 1),
                "runner_nodes": actor_nodes("Runner")}
        result("PASS" if len(answers) == items else "FAIL", data)
    elif LIB == "data":
        out = ray.data.read_parquet(os.environ["LB_OUT"])
        groups = out.take_all()
        total = sum(int(g["count()"]) for g in groups)
        data = {"groups": len(groups), "rows_counted": total, "rows_expected": int(os.environ["LB_ROWS"])}
        result("PASS" if total == data["rows_expected"] else "FAIL", data)
    elif LIB == "tune":
        grid = tuner.get_results()
        nodes = sorted({(r.metrics or {}).get("node", "")[:8] for r in grid})
        data = {"results": len(grid), "errors": grid.num_errors, "trial_nodes": nodes, "experiment_path": grid.experiment_path,
                "checkpoints": sum(1 for r in grid if r.checkpoint)}
        result("PASS" if grid.num_errors == 0 and data["checkpoints"] == len(grid) else "FAIL", data)
    elif LIB == "train":
        result("PASS", {"fit": "returned"})  # trainer.fit() raises when the run fails; the test reads LB_RANK and S3
    elif LIB == "serve":
        result("PASS", {"replica_nodes": actor_nodes("ServeReplica:default:Model")})
    elif LIB == "rllib_runners":
        first = algo.train()  # the snippet ran the first iteration; this is the second
        data = {"iterations": first.get("training_iteration"), "return": (first.get("env_runners") or {}).get("episode_return_mean"),
                "runner_nodes": actor_nodes("SingleAgentEnvRunner")}
        algo.stop()
        result("PASS" if data["iterations"] == 2 else "FAIL", data)
    elif LIB == "rllib_learners":
        metrics = (grid[0].metrics or {}) if len(grid) else {}
        data = {"errors": grid.num_errors, "iterations": metrics.get("training_iteration")}
        result("PASS" if grid.num_errors == 0 and data["iterations"] == 2 else "FAIL", data)
    result("FAIL", {"error": f"unknown lib {LIB}"})
except Exception as e:
    result("FAIL", {"stage": "epilogue", "error_class": type(e).__name__, "error_head": str(e).splitlines()[:12],
                    "traceback_tail": traceback.format_exception(e)[-3:]})
