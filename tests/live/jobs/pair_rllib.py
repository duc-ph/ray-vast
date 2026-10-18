# Process-limit pair, RLlib: PPO on CartPole-v1 (new API stack, torch). Needs torch (runtime_env pip); the image has gymnasium.
# Modes:
#   many:  one node: num_env_runners = max(ports + 4, 40 * JOB_SCALE), 1 env per runner, CPU per runner
#          min(1, (cpus - 1) / runners), so they fit the CPUs of any host. Each env runner
#          is one worker process: more than the worker ports. Some runners never start; build or train hangs or fails,
#          or RLlib trains with the runners that started (kind DEGRADED).
#   tuned: one node: the same number of environments on 2 env runners (num_envs_per_env_runner = that number / 2).
#   default: one node: num_env_runners = cpus with the default num_cpus_per_env_runner (1) and 1 env per runner.
#   spread:   functional, PASS expected: 4 env runners on 2 nodes (num_cpus_per_env_runner = half of the smallest node's
#          CPUs, so one node cannot hold all 4), 2 training iterations.
#   learners: functional, PASS expected: 2 learners with 1 GPU each on ONE node with 2 GPUs (starts the gpu2 node type).
#   learners_split: functional, LIMIT expected: 2 learners with 1 GPU each on 2 nodes. RLlib's learners use torch.distributed
#          (Ray Train's TorchConfig, rendezvous on a random port): the same NAT limit as multi-node Ray Train.
#          RLlib does not expose the rendezvous timeout (TorchConfig default 1800 s), so a guard that fires while the
#          learners are on 2 nodes in the build stage is a LIMIT.
# JOB_GPU=0: learners and learners_split ask for 0 GPUs (a local check on CPU).
# Usage: python pair_rllib.py many|tuned|default|spread|learners|learners_split
import collections
import os
import re
import time

import ray

from paircommon import PROGRESS, SCALE, error_info, finish, result, start, target, wait_for_nodes, worker_nodes

ID = "pair_rllib"
GPU = float(os.environ.get("JOB_GPU", "1"))
MIN_NODES = int(os.environ.get("JOB_MIN_NODES", "2"))
NETWORK_ERRORS = re.compile(r"(?i)timed? ?out|connect|DistNetworkError|DistStoreError|socket|NCCL|gloo|unreachable|refused")


def actor_nodes(word):
    """{short node id: count} of the ALIVE actors whose class name contains word (Ray state API)."""
    from ray.util.state import list_actors
    actors = list_actors(address=ray.get_runtime_context().gcs_address, filters=[("state", "=", "ALIVE")], limit=10000,
                         raise_on_missing_output=False)
    return dict(collections.Counter(a.node_id[:8] for a in actors if word in a.class_name))


def split_guard(guard_s):
    learners = actor_nodes("_WrappedExecutable")  # the learner actors (Ray Train worker group)
    status = "LIMIT" if PROGRESS.get("stage") == "build" and len(learners) >= 2 else "FAIL"
    result(f"{ID}.LEARNERS_SPLIT", status, {"error": f"no answer in {guard_s} s at stage {PROGRESS.get('stage')}", "learner_nodes": learners})


mode = os.sys.argv[1] if len(os.sys.argv) > 1 else "many"
start(ID if mode in ("many", "tuned", "default") else f"{ID}.{mode.upper()}", 900 if mode in ("many", "tuned", "default") else 1500,
      on_hang=split_guard if mode == "learners_split" else None)
from ray.rllib.algorithms.ppo import PPOConfig  # noqa: E402  (after ray.init: the import is slow)

config = PPOConfig().environment("CartPole-v1").training(train_batch_size_per_learner=2000, minibatch_size=250, num_epochs=1)
if mode == "default":
    cpus, ports, budget = target()
    config = config.env_runners(num_env_runners=int(cpus))
    params = {"num_env_runners": int(cpus), "num_envs_per_env_runner": 1, "environments": int(cpus), "num_cpus_per_env_runner": "default 1"}
elif mode in ("many", "tuned"):
    cpus, ports, budget = target()
    envs = int(max(ports + 4, 40 * SCALE))
    runners, per_runner = (envs, 1) if mode == "many" else (2, -(-envs // 2))
    runner_cpu = round(min(1, (cpus - 1) / runners), 4)
    config = config.env_runners(num_env_runners=runners, num_envs_per_env_runner=per_runner, num_cpus_per_env_runner=runner_cpu)
    params = {"num_env_runners": runners, "num_envs_per_env_runner": per_runner, "environments": runners * per_runner,
              "num_cpus_per_env_runner": runner_cpu}
elif mode == "spread":
    nodes = wait_for_nodes(f"{ID}.SPREAD", MIN_NODES)
    runner_cpu = max(1, int(min(n["CPU"] for n in nodes.values()) // 2)) if MIN_NODES >= 2 else 1
    config = config.env_runners(num_env_runners=4, num_cpus_per_env_runner=runner_cpu)
    params = {"num_env_runners": 4, "num_cpus_per_env_runner": runner_cpu}
elif mode == "learners":
    # The first learners run waited for a 2-GPU node, but nothing asked for one, so the autoscaler never
    # started `gpu2`. Now Tune runs PPO with the algorithm's own placement group, STRICT_PACK: 2 learner bundles {GPU 1, CPU 1}
    # and the driver bundle on ONE node. Only `gpu2` (2 GPUs) can hold it, so the autoscaler starts it. No remote env
    # runners (the gpu2 type declares 4 CPUs).
    config = (config.env_runners(num_env_runners=0).learners(num_learners=2, num_gpus_per_learner=GPU, num_cpus_per_learner=1)
              .resources(placement_strategy="STRICT_PACK"))
    params = {"num_learners": 2, "num_gpus_per_learner": GPU, "num_env_runners": 0, "placement_strategy": "STRICT_PACK", "via": "Tune"}
    print(f"JOB_PARAMS {params}", flush=True)
    import threading
    from ray import tune
    seen = {}

    def watch():  # the learner actors live inside the trial: sample their nodes while it runs
        while True:
            try:
                now = actor_nodes("_WrappedExecutable")
                if sum(now.values()) >= sum(seen.values()):
                    seen.clear()
                    seen.update(now)
            except Exception:
                pass
            time.sleep(5)
    threading.Thread(target=watch, daemon=True).start()
    try:
        t = time.time()
        grid = tune.Tuner(config.algo_class, param_space=config, run_config=tune.RunConfig(
            stop={"training_iteration": 2},
            # Tune saved a checkpoint at the end, and Ray refused the local storage on a multi-node cluster
            # (the default storage limit). This mode tests the learners, so no checkpoint.
            checkpoint_config=tune.CheckpointConfig(checkpoint_at_end=False, checkpoint_frequency=0))).fit()
        seconds = time.time() - t
    except Exception as e:
        result(f"{ID}.LEARNERS", "FAIL", dict(error_info(e), learner_nodes=dict(seen)))
    metrics = (grid[0].metrics or {}) if len(grid) else {}
    data = dict(params, seconds=round(seconds, 1), learner_nodes=dict(seen), errors=grid.num_errors,
                iterations=metrics.get("training_iteration"), returns=(metrics.get("env_runners") or {}).get("episode_return_mean"))
    ok = grid.num_errors == 0 and metrics.get("training_iteration") == 2 and len(seen) == 1 and sum(seen.values()) == 2
    result(f"{ID}.LEARNERS", "PASS" if ok else "FAIL", data)
else:
    nodes = wait_for_nodes(f"{ID}.LEARNERS_SPLIT", MIN_NODES, gpus_per_node=GPU)
    config = config.env_runners(num_env_runners=2).learners(num_learners=2, num_gpus_per_learner=GPU)
    params = {"num_learners": 2, "num_gpus_per_learner": GPU, "num_env_runners": 2}
print(f"JOB_PARAMS {params}", flush=True)
PROGRESS.update(params, stage="build")
try:
    t = time.time()
    algo = config.build_algo()
    PROGRESS["stage"] = "train"
    group = getattr(algo.learner_group, "_backend_executor", None)  # None: one local learner in the driver
    learner_nodes = dict(collections.Counter(group.worker_group.execute(lambda: ray.get_runtime_context().get_node_id()[:8]))) if group else {}
    runner_nodes = actor_nodes("EnvRunner")
    returns = []
    for _ in range(1 if mode in ("many", "tuned", "default") else 2):
        out = algo.train()
        returns.append(out.get("env_runners", {}).get("episode_return_mean"))
    seconds = time.time() - t
    algo.stop()
except Exception as e:
    info = dict(error_info(e), progress=dict(PROGRESS))
    if mode in ("many", "tuned", "default"):
        finish(ID, mode, "ERROR", info)
    text = "\n".join(info["error_head"] + info["traceback_tail"])
    result(f"{ID}.{mode.upper()}", "LIMIT" if mode == "learners_split" and NETWORK_ERRORS.search(text) else "FAIL", info)
data = dict(params, returns=returns, seconds=round(seconds, 1), learner_nodes=learner_nodes, runner_nodes=runner_nodes,
            worker_nodes=len(worker_nodes()))
if mode in ("many", "tuned", "default"):
    # DEGRADED: RLlib trained with fewer env runners than it asked for (58 of 62 on 58 worker ports).
    started = sum(runner_nodes.values())
    kind = "ERROR" if returns[-1] is None else ("DEGRADED" if started < params["num_env_runners"] else "COMPLETED")
    finish(ID, mode, kind, dict(data, env_runners_started=started))
ok = returns[-1] is not None
if mode == "spread":
    ok = ok and len(runner_nodes) >= MIN_NODES
else:  # learners_split that worked: unexpected on vast (a LIMIT is expected), PASS in a local check on one node (JOB_MIN_NODES=1)
    ok = ok and sum(learner_nodes.values()) == 2 and len(learner_nodes) >= min(2, MIN_NODES)
result(f"{ID}.{mode.upper()}", "PASS" if ok else "FAIL", data)
