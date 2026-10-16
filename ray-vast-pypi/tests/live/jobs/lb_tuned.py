# Live suite, group libraries (LB11): the "tuned" half of the C10 pairs for Tune, Train and RLlib, with the values of the
# user docs at the host's B and CPUS (the C10 scripts use older values for these three; core, data and serve use
# c10_core.py tuned, c10_data.py tuned2 and c10_serve.py full, which match the docs). The load is the same as the
# matching "many" run of C10:
#   tune:  max(ports + 4, 60) trials, each with resources {"cpu": CPUS / B}, and each trial runs its evaluation as a Ray
#          task with num_cpus = CPUS / B (tune.md: "and num_cpus of each task that a trial starts");
#          TuneConfig(max_concurrent_trials = B // 2).
#   train: TorchTrainer (gloo, CPU) with num_workers = B // 2, resources_per_worker {"CPU": CPUS / B},
#          placement_strategy "STRICT_PACK", DataLoader num_workers 2 (train.md). Needs torch.
#   rllib: PPO, num_env_runners = B // 2, num_cpus_per_env_runner = CPUS / B, the max(ports + 4, 40) environments of
#          the many run spread over the runners (rllib.md: more environments per runner, not more runners). Needs torch.
# Prints `C10_JOB <id> COMPLETED|HUNG|ERROR <json>` (c10common), so run_c10.verdict() decides.
# Usage: python lb_tuned.py tune|train|rllib   Env: C10_PORTS, C10_CPUS (the target node), C10_GUARD_S, C10_SCALE.
import sys
import time

import ray

from c10common import PROGRESS, SCALE, error_info, finish, start, target

LIB = sys.argv[1]
ID = f"LB11.{LIB}"
start(ID, {"tune": 600, "train": 900, "rllib": 900}[LIB])
cpus, ports, budget = target()
share = round(cpus / budget, 4)
try:
    t = time.time()
    if LIB == "tune":
        from ray import tune

        trials = max(ports + 4, int(60 * SCALE))

        @ray.remote(num_cpus=share)
        def evaluate(x, step):
            time.sleep(0.5)
            return x * step

        def trainable(config):
            for step in range(3):
                tune.report({"score": ray.get(evaluate.remote(config["x"], step))})

        params = {"trials": trials, "trial_cpu": share, "task_cpu": share, "max_concurrent_trials": budget // 2}
        print(f"C10_PARAMS {params}", flush=True)
        PROGRESS.update(params)
        grid = tune.Tuner(tune.with_resources(trainable, {"cpu": share}), param_space={"x": tune.uniform(0, 1)},
                          tune_config=tune.TuneConfig(num_samples=trials, metric="score", mode="max",
                                                      max_concurrent_trials=budget // 2)).fit()
        kind = "COMPLETED" if len(grid) == trials and grid.num_errors == 0 else "ERROR"
        finish(ID, "tuned", kind, dict(params, results=len(grid), errors=grid.num_errors, seconds=round(time.time() - t, 1)))
    elif LIB == "train":
        import ray.train
        import ray.train.torch
        from ray.train import FailureConfig, RunConfig, ScalingConfig
        from ray.train.torch import TorchConfig, TorchTrainer

        workers = max(1, budget // 2)

        def train_func(config):
            import torch
            torch.manual_seed(0)
            x = torch.randn(2_000, 32)
            loader = ray.train.torch.prepare_data_loader(torch.utils.data.DataLoader(
                torch.utils.data.TensorDataset(x, x @ torch.randn(32, 1)), batch_size=128, shuffle=True, num_workers=2))
            model = ray.train.torch.prepare_model(torch.nn.Sequential(torch.nn.Linear(32, 64), torch.nn.ReLU(), torch.nn.Linear(64, 1)))
            optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
            for xb, yb in loader:
                loss = torch.nn.functional.mse_loss(model(xb), yb)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
            ray.train.report({"loss": loss.item()})

        params = {"num_workers": workers, "cpu_per_worker": share, "placement_strategy": "STRICT_PACK", "dataloader_workers": 2}
        print(f"C10_PARAMS {params}", flush=True)
        PROGRESS.update(params)
        res = TorchTrainer(train_func, scaling_config=ScalingConfig(num_workers=workers, use_gpu=False, resources_per_worker={"CPU": share},
                                                                    placement_strategy="STRICT_PACK"),
                           torch_config=TorchConfig(backend="gloo", timeout_s=300),
                           run_config=RunConfig(name=f"lb11_train_{int(t)}", failure_config=FailureConfig(max_failures=0))).fit()
        finish(ID, "tuned", "COMPLETED" if res.error is None else "ERROR",
               dict(params, error=str(res.error)[:500] if res.error else None, seconds=round(time.time() - t, 1)))
    else:
        from ray.rllib.algorithms.ppo import PPOConfig

        runners = max(1, budget // 2)
        envs = int(max(ports + 4, 40 * SCALE))
        per_runner = -(-envs // runners)
        config = (PPOConfig().environment("CartPole-v1").training(train_batch_size_per_learner=2000, minibatch_size=250, num_epochs=1)
                  .env_runners(num_env_runners=runners, num_envs_per_env_runner=per_runner, num_cpus_per_env_runner=share))
        params = {"num_env_runners": runners, "num_envs_per_env_runner": per_runner, "num_cpus_per_env_runner": share}
        print(f"C10_PARAMS {params}", flush=True)
        PROGRESS.update(params, stage="build")
        algo = config.build_algo()
        PROGRESS["stage"] = "train"
        out = algo.train()
        algo.stop()
        returns = (out.get("env_runners") or {}).get("episode_return_mean")
        finish(ID, "tuned", "COMPLETED" if returns is not None else "ERROR", dict(params, returns=returns, seconds=round(time.time() - t, 1)))
except Exception as e:
    finish(ID, "tuned", "ERROR", dict(error_info(e), progress=dict(PROGRESS)))
