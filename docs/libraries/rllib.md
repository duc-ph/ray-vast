# RLlib

Reinforcement learning with env runners and learners.

## Status

- **One worker ✅:** PPO with env runners works. PPO with 2 GPU learners on one worker works.
- **Many workers 🟨:** Env runners on many workers work. Learners on 2 or more workers fail behind the NAT of vast. Put all learners on one worker.

Submit your work with `ray job submit`. Install PyTorch in the job (`--runtime-env-json '{"pip": ["torch==2.7.1"]}'`), or build it into the image. Read [Submit jobs](jobs.md).

## What is different on vast

- Put all learners on one worker. For 2 learners on one worker, use a node type with `GPU: 2`, for example [`gpu-80gb.yaml`](https://github.com/duc-ph/ray-vast/blob/main/src/ray_vast/examples/gpu-80gb.yaml).
- Each worker runs at most *N* Ray processes at the same time. Each env runner is one process. Read [Many tasks or actors](many-tasks.md).

## When you need the settings

Use `STRICT_PACK` for learners always. Use the CPU values only in these cases:

- You set `num_cpus_per_env_runner` below 1.
- `num_env_runners` on one worker is more than *N*.

## Settings

`CPU` is the `CPU` of the node type in your cluster YAML. *N* is the number of processes at once of the worker. Read it in the [start line](many-tasks.md#the-number-of-processes-at-once).

The values below are an example for *N* = 10 and `CPU` = 24. Use the values of your workers. Env runners:

```python
from ray.rllib.algorithms.ppo import PPOConfig

config = (PPOConfig().environment("CartPole-v1")
          .env_runners(num_env_runners=2,             # 2 (tested); N // 2 or fewer for each worker
                       num_envs_per_env_runner=20,     # more environments here, not more runners
                       num_cpus_per_env_runner=2.4))   # CPU / N
algo = config.build_algo()
algo.train()
```

Learners: put all learners on one worker. Run the algorithm through Tune with `STRICT_PACK`:

```python
from ray import tune

config = (PPOConfig().environment("CartPole-v1")
          .env_runners(num_env_runners=0)
          .learners(num_learners=2, num_gpus_per_learner=1, num_cpus_per_learner=2.4)   # CPU / N
          .resources(placement_strategy="STRICT_PACK"))   # all learners on one worker
tune.Tuner(config.algo_class, param_space=config,
           run_config=tune.RunConfig(
               stop={"training_iteration": 2},
               checkpoint_config=tune.CheckpointConfig(checkpoint_at_end=False))).fit()
```

We tested this code with `num_cpus_per_learner=1`. For checkpoints, add `storage_path="s3://my-bucket/experiments"` to `RunConfig`, as for [Ray Tune](tune.md#settings).

| Setting | Value (*N* = 10, `CPU` = 24) | Formula |
|---|---|---|
| `num_cpus_per_env_runner`, `num_cpus_per_learner` | 2.4 | `CPU` / *N* |
| `num_env_runners` | 2 (tested) | *N* // 2 or fewer for each worker |
| `num_envs_per_env_runner` | 20 | The environments you need / `num_env_runners` |
| `placement_strategy` for learners | `"STRICT_PACK"` | All learners on one worker |
| `num_learners` | 2 | At most the GPUs of one worker |
| `RunConfig(storage_path=...)` (through Tune, for checkpoints) | `s3://...` | A cloud bucket |

- More environments in each env runner give the same samples with fewer processes.
- RLlib can go on with fewer env runners than you asked for.

## Threads

- A vector environment that starts subprocesses (for example a `gymnasium` async vector environment) makes each environment a child process of an env runner. These processes count against the process limit.
- PyTorch uses `OMP_NUM_THREADS` for its intra-op threads in each env runner and learner, and Ray sets it. Each env runner (PPO, CartPole) had 27 threads on a container that saw 128 CPUs.

Read [Threads](many-tasks.md#threads).

## What does not work

| What | Do this instead |
|---|---|
| Many env runners with 1 environment each. In a test, 40 env runners on one worker with 14 worker ports hung in the build. On a worker with 58 worker ports, a job asked for 62 env runners. Only 58 started, and RLlib trained with them after about 6 minutes. | Use *N* // 2 env runners or fewer, and raise `num_envs_per_env_runner`. |
| Learners on 2 or more workers. The build stage hangs (`[c10d] The server socket ... has timed out`). Read [No multi-node Ray Train](../reference/limits.md#no-multi-node-ray-train). | Put all learners on one worker with many GPUs (`STRICT_PACK`). |
| Tune checkpoints with the default `storage_path` (`Unable to set up cluster storage`). | Set `storage_path` to a cloud bucket, and give the workers credentials. Read [Submit jobs](jobs.md#pass-credentials-to-the-job). |

## If it fails

1. **The head's `monitor.log`.** Look for the `is at its process limit` WARNING. It names the worker and shows the worker ports in use.

    ```bash
    ray-vast attach cluster.yaml --profile my-aws-profile
    # in the shell on the head:
    sudo docker exec ray-vast-head grep "process limit" /tmp/ray/session_latest/logs/monitor.log
    ```

2. **The job log.** A build that never ends shows no training iteration.

    ```bash
    ray job logs <job id>
    ```

3. **The worker logs.** Open the dashboard (http://127.0.0.1:8265) and the **Logs** tab. Choose the worker. Look for `No available ports` in its `worker-*.err` files.
