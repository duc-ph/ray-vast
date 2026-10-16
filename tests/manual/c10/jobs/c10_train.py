# C10.train: Ray Train (PyTorch, gloo, CPU) on one node: DDP of a small MLP, 1 epoch. Each Train worker reads its data with
# a torch DataLoader with 2 worker processes (forked children: they use pids, not worker ports). Needs torch.
#   many:  num_workers = max(ports + 4, 40 * C10_SCALE), at most cpus - 1, 1 CPU each: more Train workers than worker ports.
#          Some workers never start; the worker group start times out or hangs.
#   tuned: num_workers = budget // 2 (budget = ports - 4): a Train worker plus the Train controller and helpers stay in the
#          port budget, and the DataLoader children stay in the pids budget.
# No checkpoint: a checkpoint needs shared storage on a multi-node cluster (C9.3.1).
# Usage: python c10_train.py many|tuned
import time

import ray.train
import ray.train.torch
from ray.train import FailureConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchConfig, TorchTrainer

from c10common import PROGRESS, SCALE, error_info, finish, start, target

ID = "C10.train"
mode = start(ID, 900)
cpus, ports, budget = target()
if mode == "many":
    workers = int(min(cpus - 1, max(ports + 4, 40 * SCALE)))
else:
    workers = max(1, budget // 2)


def train_func(config):
    import torch
    torch.manual_seed(0)
    x = torch.randn(2_000, 32)
    y = x @ torch.randn(32, 1)
    loader = ray.train.torch.prepare_data_loader(torch.utils.data.DataLoader(
        torch.utils.data.TensorDataset(x, y), batch_size=128, shuffle=True, num_workers=2))
    model = ray.train.torch.prepare_model(torch.nn.Sequential(torch.nn.Linear(32, 64), torch.nn.ReLU(), torch.nn.Linear(64, 1)))
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
    total = 0.0
    for xb, yb in loader:
        loss = torch.nn.functional.mse_loss(model(xb), yb)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        total += loss.item()
    ray.train.report({"loss": total / len(loader)})


params = {"num_workers": workers, "cpu_per_worker": 1, "dataloader_workers": 2}
print(f"C10_PARAMS {params}", flush=True)
PROGRESS.update(params)
try:
    t = time.time()
    res = TorchTrainer(train_func, scaling_config=ScalingConfig(num_workers=workers, use_gpu=False, resources_per_worker={"CPU": 1}),
                       torch_config=TorchConfig(backend="gloo", timeout_s=300),
                       run_config=RunConfig(name=f"c10_train_{mode}_{int(t)}", failure_config=FailureConfig(max_failures=0))).fit()
    seconds = time.time() - t
except Exception as e:
    finish(ID, mode, "ERROR", dict(error_info(e), progress=dict(PROGRESS)))
kind = "COMPLETED" if res.error is None and "loss" in (res.metrics or {}) else "ERROR"
finish(ID, mode, kind, dict(params, loss=(res.metrics or {}).get("loss"), error=str(res.error)[:500] if res.error else None,
                            seconds=round(seconds, 1)))
