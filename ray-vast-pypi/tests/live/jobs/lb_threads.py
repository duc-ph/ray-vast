# Live suite, group libraries (LB15, a measurement): the threads of each process on a worker whose container sees many
# more CPUs than its CPU quota. Thread pools size themselves by the visible CPUs (Arrow, torch, OpenMP), not by the quota.
# A sampler actor on the target node (num_cpus=0, hard node affinity) reads every process of the container every 0.5 s:
# its threads (/proc/<pid>/task), its title (argv[0], for example `ray::MapBatches(add_key)`) and its parent; and the
# cgroup's pids.current and pids.events. Then one workload runs with Ray's defaults (one process for each CPU):
#   data:  read_parquet (files that a task wrote on the node) -> map_batches -> groupby(key).count()
#   train: TorchTrainer, 4 workers (1 CPU each), each with a torch DataLoader with 2 worker processes, 1 epoch
#   rllib: PPO on CartPole-v1, 4 env runners (1 CPU each), 1 iteration
#   task:  one task for each CPU that imports pyarrow and torch and uses both (a matmul, a pyarrow compute)
# Mode `fixed`: the same workload with the fixes of rule 2 in every worker process (lb_threads_fix.setup, the
# worker_process_setup_hook of ray.init): pyarrow.set_cpu_count / set_io_thread_count, torch.set_num_threads /
# set_num_interop_threads, all LB_FIX_THREADS (default 2); the caller also sets OMP_NUM_THREADS in the job's env_vars.
# Prints `LB_RESULT threads-<workload>-<mode> PASS|FAIL <json>`: PASS when the workload completed and the sampler has
# samples. The data: each process kind with its process count and its most threads, the most threads of one process,
# the Ray worker processes and their children above 60 threads (`over_60`), the pids.current peak and the refused forks (pids.events) of the run.
# Usage: python lb_threads.py data|train|rllib|task default|fixed   Env: LB_NODE (the target node id), LB_FIX_THREADS.
import json
import os
import re
import sys
import threading
import time

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

WORKLOAD, MODE = sys.argv[1], sys.argv[2]
FIX = int(os.environ.get("LB_FIX_THREADS", "2"))
LIMIT = 60  # rule 2: keep each process under about 60 threads
FIXED = ("ray::DashboardAgent", "ray::RuntimeEnvAgent")  # the fixed Ray processes of a node: not the user's processes
if MODE == "fixed":
    from lb_threads_fix import setup
    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"), runtime_env={"worker_process_setup_hook": setup})
else:
    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"))
NODE = os.environ.get("LB_NODE") or next(n["NodeID"] for n in ray.nodes() if n["Alive"] and n["Resources"].get("CPU", 0) > 0)
CPUS = int(next(n["Resources"]["CPU"] for n in ray.nodes() if n["NodeID"] == NODE))
ON_NODE = NodeAffinitySchedulingStrategy(NODE, soft=False)


def pids_file(name):
    for base in ("/sys/fs/cgroup", "/sys/fs/cgroup/pids"):  # cgroup v2, then v1
        try:
            return open(f"{base}/{name}").read().strip()
        except OSError:
            continue
    return None


def refused(events):
    found = re.search(r"max (\d+)", events or "")
    return int(found.group(1)) if found else None


@ray.remote(num_cpus=0, max_concurrency=2)
class Sampler:
    def __init__(self):
        self.stop, self.procs, self.samples, self.peak = threading.Event(), {}, 0, 0
        self.events_before = pids_file("pids.events")
        threading.Thread(target=self.loop, daemon=True).start()

    def loop(self):
        me = os.getpid()
        while not self.stop.is_set():
            for pid in filter(str.isdigit, os.listdir("/proc")):
                try:
                    threads = len(os.listdir(f"/proc/{pid}/task"))
                    title = open(f"/proc/{pid}/cmdline", "rb").read().split(b"\0")[0].decode(errors="replace")
                    ppid = int(re.search(r"PPid:\s+(\d+)", open(f"/proc/{pid}/status").read()).group(1))
                except (OSError, AttributeError):
                    continue
                if int(pid) == me:
                    continue
                kind = title if title.startswith("ray::") else os.path.basename(title.split()[0]) if title else "?"
                old = self.procs.get(pid)
                self.procs[pid] = {"kind": kind[:80], "ppid": ppid, "threads": max(threads, old["threads"] if old else 0)}
            current = pids_file("pids.current")
            self.peak = max(self.peak, int(current) if current and current.isdigit() else 0)
            self.samples += 1
            time.sleep(0.5)

    def ready(self):
        return self.samples

    def result(self):
        self.stop.set()
        time.sleep(1)
        by_pid = self.procs
        for p in by_pid.values():  # a child of a Ray worker process (a DataLoader worker, a vector env) keeps its parent's title
            parent = by_pid.get(str(p["ppid"]))
            if parent and parent["kind"].startswith("ray::"):
                p["kind"] = "child of " + parent["kind"]
        kinds = {}
        for p in by_pid.values():
            k = kinds.setdefault(p["kind"], {"processes": 0, "max_threads": 0})
            k["processes"] += 1
            k["max_threads"] = max(k["max_threads"], p["threads"])
        before, after = refused(self.events_before), refused(pids_file("pids.events"))
        return {"samples": self.samples, "kinds": dict(sorted(kinds.items(), key=lambda kv: -kv[1]["max_threads"])),
                "max_threads_one_process": max((p["threads"] for p in by_pid.values()), default=0),
                "over_60": sorted((p["kind"], p["threads"]) for p in by_pid.values() if p["threads"] > LIMIT and
                                  p["kind"].startswith(("ray::", "child of ray::")) and not p["kind"].startswith(FIXED)),
                "pids_current_peak": self.peak, "pids.max": pids_file("pids.max"),
                "refused_forks": None if before is None or after is None else after - before,
                "cpu_count": os.cpu_count(), "affinity": len(os.sched_getaffinity(0))}


def data():
    @ray.remote(num_cpus=0)
    def write(path, files, rows):
        import numpy as np
        import pyarrow as pa
        import pyarrow.parquet as pq
        os.makedirs(path, exist_ok=True)
        for i in range(files):
            ids = np.arange(i * rows, (i + 1) * rows)
            pq.write_table(pa.table({"id": ids, "value": np.random.rand(rows)}), f"{path}/part-{i:03d}.parquet")
        return files * rows
    path = "/tmp/lb15/in"  # on the target node: the only node with CPUs, so the read tasks run there
    rows = ray.get(write.options(scheduling_strategy=ON_NODE).remote(path, 4 * CPUS, 25_000))

    def add_key(batch):
        batch["key"] = batch["id"] % 16
        return batch
    groups = ray.data.read_parquet(path).map_batches(add_key).groupby("key").count().take_all()
    total = sum(g["count()"] for g in groups)
    return {"rows": rows, "groups": len(groups), "total": total, "ok": total == rows and len(groups) == 16}


def train():
    from ray.train import RunConfig, ScalingConfig
    from ray.train.torch import TorchConfig, TorchTrainer

    def train_func(config):
        import torch
        import ray.train.torch as rt
        if config["fixed"]:
            torch.set_num_threads(config["fix"])
        x = torch.randn(4_000, 64)
        y = x @ torch.randn(64, 1)
        loader = rt.prepare_data_loader(torch.utils.data.DataLoader(torch.utils.data.TensorDataset(x, y), batch_size=64,
                                                                    shuffle=True, num_workers=2))
        model = rt.prepare_model(torch.nn.Sequential(torch.nn.Linear(64, 256), torch.nn.ReLU(), torch.nn.Linear(256, 1)))
        optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
        for xb, yb in loader:
            loss = torch.nn.functional.mse_loss(model(xb), yb)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
        ray.train.report({"loss": loss.item()})
    res = TorchTrainer(train_func, train_loop_config={"fixed": MODE == "fixed", "fix": FIX},
                       scaling_config=ScalingConfig(num_workers=4, use_gpu=False),
                       torch_config=TorchConfig(backend="gloo", timeout_s=300), run_config=RunConfig(name=f"lb15_{MODE}_{int(time.time())}")).fit()
    return {"num_workers": 4, "dataloader_workers": 2, "error": str(res.error)[:300] if res.error else None, "ok": res.error is None}


def rllib():
    from ray.rllib.algorithms.ppo import PPOConfig
    algo = (PPOConfig().environment("CartPole-v1").env_runners(num_env_runners=4)
            .training(train_batch_size_per_learner=2000, minibatch_size=250, num_epochs=1)).build_algo()
    out = algo.train()
    algo.stop()
    ret = out.get("env_runners", {}).get("episode_return_mean")
    return {"num_env_runners": 4, "episode_return_mean": ret, "ok": ret is not None}


def task():
    @ray.remote
    def use_both(fixed, fix):
        import pyarrow as pa
        import pyarrow.compute as pc
        import torch
        if fixed:
            torch.set_num_threads(fix)
        a = torch.randn(1024, 1024)
        for _ in range(20):
            a = (a @ a).tanh()
        total = pc.sum(pa.array(range(2_000_000))).as_py()
        time.sleep(5)  # the sampler sees the pools at their size
        return {"threads": len(os.listdir("/proc/self/task")), "torch_threads": torch.get_num_threads(),
                "arrow_cpu_count": pa.cpu_count(), "arrow_io_threads": pa.io_thread_count(), "sum_ok": total == 1999999000000}
    rows = ray.get([use_both.options(scheduling_strategy=ON_NODE).remote(MODE == "fixed", FIX) for _ in range(CPUS)])
    return {"tasks": len(rows), "per_task": rows[0], "max_task_threads": max(r["threads"] for r in rows), "ok": all(r["sum_ok"] for r in rows)}


sampler = Sampler.options(scheduling_strategy=ON_NODE).remote()
ray.get(sampler.ready.remote(), timeout=300)
time.sleep(3)
t = time.time()
try:
    out = {"data": data, "train": train, "rllib": rllib, "task": task}[WORKLOAD]()
except Exception as e:  # noqa: BLE001  (the measurement still counts)
    out = {"ok": False, "error_class": type(e).__name__, "error_head": str(e).splitlines()[:8]}
out["seconds"] = round(time.time() - t, 1)
measured = ray.get(sampler.result.remote(), timeout=120)
status = "PASS" if out.get("ok") and measured["samples"] > 0 else "FAIL"
print(f"LB_RESULT threads-{WORKLOAD}-{MODE} {status} " + json.dumps({"workload": out, "mode": MODE, "fix_threads": FIX if MODE == "fixed" else None,
                                                                    "cpus": CPUS, **measured}, default=str), flush=True)
sys.exit(0 if status == "PASS" else 1)
