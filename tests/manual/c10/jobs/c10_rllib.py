# C10.rllib: RLlib PPO on CartPole-v1 (new API stack, torch). Needs torch (runtime_env pip); the image has gymnasium.
# Modes:
#   many:  one node: num_env_runners = max(ports + 4, 40 * C10_SCALE), at most cpus - 1, 1 env per runner. Each env runner
#          is one worker process: more than the worker ports. Some runners never start; build or train hangs or fails.
#   tuned: one node: the same number of environments on 2 env runners (num_envs_per_env_runner = that number / 2).
#   r1:    functional, PASS expected: 4 env runners on 2 nodes (num_cpus_per_env_runner = half of the smallest node's
#          CPUs, so one node cannot hold all 4), 2 training iterations.
#   r2:    functional, PASS expected: 2 learners with 1 GPU each on ONE node with 2 GPUs (starts the gpu2 node type).
#   r3:    functional, LIMIT expected: 2 learners with 1 GPU each on 2 nodes. RLlib's learners use torch.distributed
#          (Ray Train's TorchConfig, rendezvous on a random port): the same NAT limit as multi-node Ray Train (C9.3.2).
#          RLlib does not expose the rendezvous timeout (TorchConfig default 1800 s), so a guard that fires while the
#          learners are on 2 nodes in the build stage is a LIMIT.
# C10_GPU=0: r2 and r3 ask for 0 GPUs (a local check on CPU).
# Usage: python c10_rllib.py many|tuned|r1|r2|r3
import collections
import os
import re
import time

import ray

from c10common import PROGRESS, SCALE, error_info, finish, result, start, target, wait_for_nodes, worker_nodes

ID = "C10.rllib"
GPU = float(os.environ.get("C10_GPU", "1"))
MIN_NODES = int(os.environ.get("C10_MIN_NODES", "2"))
NETWORK_ERRORS = re.compile(r"(?i)timed? ?out|connect|DistNetworkError|DistStoreError|socket|NCCL|gloo|unreachable|refused")


def actor_nodes(word):
    """{short node id: count} of the ALIVE actors whose class name contains word (Ray state API)."""
    from ray.util.state import list_actors
    actors = list_actors(address=ray.get_runtime_context().gcs_address, filters=[("state", "=", "ALIVE")], limit=10000,
                         raise_on_missing_output=False)
    return dict(collections.Counter(a.node_id[:8] for a in actors if word in a.class_name))


def r3_guard(guard_s):
    learners = actor_nodes("RayTrainWorker")
    status = "LIMIT" if PROGRESS.get("stage") == "build" and len(learners) >= 2 else "FAIL"
    result(f"{ID}.R3", status, {"error": f"no answer in {guard_s} s at stage {PROGRESS.get('stage')}", "learner_nodes": learners})


mode = os.sys.argv[1] if len(os.sys.argv) > 1 else "many"
start(ID if mode in ("many", "tuned") else f"{ID}.{mode.upper()}", 900 if mode in ("many", "tuned") else 1500,
      on_hang=r3_guard if mode == "r3" else None)
from ray.rllib.algorithms.ppo import PPOConfig  # noqa: E402  (after ray.init: the import is slow)

config = PPOConfig().environment("CartPole-v1").training(train_batch_size_per_learner=2000, minibatch_size=250, num_epochs=1)
if mode in ("many", "tuned"):
    cpus, ports, budget = target()
    envs = int(min(cpus - 1, max(ports + 4, 40 * SCALE)))
    runners, per_runner = (envs, 1) if mode == "many" else (2, -(-envs // 2))
    config = config.env_runners(num_env_runners=runners, num_envs_per_env_runner=per_runner, num_cpus_per_env_runner=1)
    params = {"num_env_runners": runners, "num_envs_per_env_runner": per_runner, "environments": runners * per_runner}
elif mode == "r1":
    nodes = wait_for_nodes(f"{ID}.R1", MIN_NODES)
    runner_cpu = max(1, int(min(n["CPU"] for n in nodes.values()) // 2)) if MIN_NODES >= 2 else 1
    config = config.env_runners(num_env_runners=4, num_cpus_per_env_runner=runner_cpu)
    params = {"num_env_runners": 4, "num_cpus_per_env_runner": runner_cpu}
else:
    nodes = wait_for_nodes(f"{ID}.{mode.upper()}", 1 if mode == "r2" else MIN_NODES, gpus_per_node=GPU * (2 if mode == "r2" else 1))
    config = config.env_runners(num_env_runners=2).learners(num_learners=2, num_gpus_per_learner=GPU)
    params = {"num_learners": 2, "num_gpus_per_learner": GPU, "num_env_runners": 2}
print(f"C10_PARAMS {params}", flush=True)
PROGRESS.update(params, stage="build")
try:
    t = time.time()
    algo = config.build_algo()
    PROGRESS["stage"] = "train"
    learner_nodes, runner_nodes = actor_nodes("RayTrainWorker"), actor_nodes("EnvRunner")
    returns = []
    for _ in range(1 if mode in ("many", "tuned") else 2):
        out = algo.train()
        returns.append(out.get("env_runners", {}).get("episode_return_mean"))
    seconds = time.time() - t
    algo.stop()
except Exception as e:
    info = dict(error_info(e), progress=dict(PROGRESS))
    if mode in ("many", "tuned"):
        finish(ID, mode, "ERROR", info)
    text = "\n".join(info["error_head"] + info["traceback_tail"])
    result(f"{ID}.{mode.upper()}", "LIMIT" if mode == "r3" and NETWORK_ERRORS.search(text) else "FAIL", info)
data = dict(params, returns=returns, seconds=round(seconds, 1), learner_nodes=learner_nodes, runner_nodes=runner_nodes,
            worker_nodes=len(worker_nodes()))
if mode in ("many", "tuned"):
    finish(ID, mode, "COMPLETED" if returns[-1] is not None else "ERROR", data)
ok = returns[-1] is not None
if mode == "r1":
    ok = ok and len(runner_nodes) >= MIN_NODES
elif mode == "r2":
    ok = ok and len(learner_nodes) == 1 and sum(learner_nodes.values()) == 2
else:  # r3 that worked: unexpected on vast (a LIMIT is expected), PASS in a local check on one node
    ok = ok and sum(learner_nodes.values()) == 2
result(f"{ID}.{mode.upper()}", "PASS" if ok else "FAIL", data)
