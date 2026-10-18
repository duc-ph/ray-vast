# Shared Ray Train (PyTorch) run of the Train jobs. Needs torch (pairs.TORCH_ENV).
# A small MLP on synthetic data, 2 epochs, DDP through ray.train.torch.prepare_model. Rank 0 gathers the node id of every
# rank with all_gather_object (a real collective over the backend) and reports it. FailureConfig(max_failures=0): the
# first error ends the run. TorchConfig(timeout_s=180): a process group that cannot connect fails after 3 min.
# Train V2 keeps in Result.metrics only the metrics of a checkpoint report, so every rank also sends its metrics to a small
# collector actor of the driver.
import os
import re
import tempfile
import time

import ray
import ray.train
import ray.train.torch
from ray.train import Checkpoint, FailureConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchConfig, TorchTrainer

from jobcommon import SMALL, error_info, result, short, storage_settings

NETWORK_ERRORS = re.compile(r"(?i)timed? ?out|connect|DistNetworkError|DistStoreError|socket|NCCL|gloo|unreachable|refused")
STORAGE_ERRORS = re.compile(r"(?i)storage|persistent|shared file ?system|checkpoint")


@ray.remote(num_cpus=0)
class Collector:
    def __init__(self):
        self.rows = []

    def put(self, row):
        self.rows.append(row)

    def get(self):
        return self.rows


def train_func(config):
    import torch
    import torch.distributed as dist

    context = ray.train.get_context()
    rank = context.get_world_rank()
    node = ray.get_runtime_context().get_node_id()
    device = ray.train.torch.get_device()
    print(f"JOB_TRAIN rank={rank} node={node} device={device} backend={dist.get_backend() if dist.is_initialized() else None}", flush=True)
    nodes = [None] * context.get_world_size()
    dist.all_gather_object(nodes, node)

    torch.manual_seed(0)
    x = torch.randn(2_000 if config["small"] else 50_000, 64)
    y = x @ torch.randn(64, 1)
    loader = ray.train.torch.prepare_data_loader(
        torch.utils.data.DataLoader(torch.utils.data.TensorDataset(x, y), batch_size=256, shuffle=True))
    model = ray.train.torch.prepare_model(torch.nn.Sequential(torch.nn.Linear(64, 128), torch.nn.ReLU(), torch.nn.Linear(128, 1)))
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
    for epoch in range(2):
        t = time.time()
        total = 0.0
        for xb, yb in loader:
            loss = torch.nn.functional.mse_loss(model(xb), yb)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            total += loss.item()
        metrics = {"epoch": epoch, "loss": total / len(loader), "epoch_s": time.time() - t, "rank_nodes": nodes, "device": str(device),
                   "backend": dist.get_backend()}
        ray.get(config["collector"].put.remote(dict(metrics, rank=rank, node=node)))
        if config["checkpoint"] and rank == 0:
            with tempfile.TemporaryDirectory() as d:
                torch.save(model.state_dict(), os.path.join(d, "model.pt"))
                ray.train.report(metrics, checkpoint=Checkpoint.from_directory(d))
        else:
            ray.train.report(metrics)


def run(test_id, num_workers, use_gpu, backend, strategy, checkpoint, limit_errors):
    """Run the trainer and print the result line. An error that matches `limit_errors` is a LIMIT, other errors a FAIL."""
    t = time.time()
    collector = Collector.remote()
    try:
        trainer = TorchTrainer(
            train_func, train_loop_config={"small": SMALL, "checkpoint": checkpoint, "collector": collector},
            scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu, placement_strategy=strategy),
            torch_config=TorchConfig(backend=backend, timeout_s=180),
            run_config=RunConfig(name=f"job_{test_id}_{int(t)}", storage_path=storage_settings()[0], storage_filesystem=storage_settings()[1],
                                 failure_config=FailureConfig(max_failures=0)))
        res = trainer.fit()
    except Exception as e:
        info = error_info(e)
        text = "\n".join(info["error_head"] + info["traceback_tail"])
        status = "LIMIT" if limit_errors and limit_errors.search(text) else "FAIL"
        rows = ray.get(collector.get.remote(), timeout=30)
        result(test_id, status, dict(info, seconds_to_error=round(time.time() - t, 1), num_workers=num_workers, use_gpu=use_gpu,
                                     backend=backend, strategy=strategy, epochs_reported=len(rows)))
    rows = ray.get(collector.get.remote(), timeout=30)
    last = [r for r in rows if r["epoch"] == 1]
    data = {"seconds": round(time.time() - t, 1), "num_workers": num_workers, "strategy": strategy,
            "backend": sorted({r["backend"] for r in rows}), "devices": sorted({r["device"] for r in rows}),
            "rank_nodes": [short(n) for n in (rows[0]["rank_nodes"] if rows else [])], "reports": len(rows),
            "loss_last": [round(r["loss"], 4) for r in last], "epoch_s": [round(r["epoch_s"], 2) for r in last],
            "error": str(res.error)[:500] if res.error else None,
            "checkpoint": res.checkpoint.path if res.checkpoint else None, "storage": os.environ.get("JOB_STORAGE") or "default"}
    ok = res.error is None and len(last) == num_workers and (not checkpoint or res.checkpoint is not None)
    result(test_id, "PASS" if ok else "FAIL", data)
