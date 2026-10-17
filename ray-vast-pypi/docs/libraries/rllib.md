# RLlib

Reinforcement learning with env runners and learners.

## Status

- **One worker ✅:** PPO with env runners and with 2 GPU learners on one worker works.
- **Many workers 🟨:** Env runners on many workers work, but learners on 2 or more workers fail behind the NAT of vast.

## Run it

Use an example cluster YAML, for example [`gpu-24gb.yaml`](https://github.com/duc-ph/ray-vast/blob/main/ray-vast-pypi/src/ray_vast/examples/gpu-24gb.yaml). For 2 learners on one worker, use a node type with `GPU: 2`, for example [`gpu-80gb.yaml`](https://github.com/duc-ph/ray-vast/blob/main/ray-vast-pypi/src/ray_vast/examples/gpu-80gb.yaml). Set the region of the head (`node_config.region` of the head node type).

```bash
ray-vast up cluster.yaml --profile my-aws-profile
ray-vast dashboard cluster.yaml --profile my-aws-profile   # terminal 1: keep it open
```

In terminal 2:

```bash
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=~/.ray-vast/<cluster_name>/token   # the line that dashboard printed
export RAY_ADDRESS=http://127.0.0.1:8265
ray job submit --working-dir . \
  --runtime-env-json '{"pip": ["torch==2.7.1"]}' \
  -- python rl_job.py
```

When you finish:

```bash
ray-vast down cluster.yaml --profile my-aws-profile
```

## Settings

B is the number of worker processes that your work can use on one worker. B = worker ports − 4. The 4 ports stay free for the processes that Ray's libraries start next to your work (the Serve proxy, the Data and Train helpers).
The start line of each worker shows its port count, for example `worker ports 14` (in the vast console, or `vastai logs <instance id>`).
Hosts with `pids.max` 1280 give 14 worker ports, so B = 10. Most workers in our tests had 14. A host with a lower limit gives fewer ports, so read the start line. If you cannot read it, use B = 10.

CPUS is the number of Ray CPUs of one worker. It is the `CPU` of the node type in your cluster YAML. [Ray Core](core.md#settings) shows how to get CPUS in your driver.

The rule for every library: each process that your code starts (a task, an actor, a replica, a trial, a Train worker or an env runner) asks for `num_cpus` = CPUS / B. Then Ray puts at most B of them on one worker. The library settings below come on top of this rule.

The values below are for B = 10 and CPUS = 24. Env runners:

```python
from ray.rllib.algorithms.ppo import PPOConfig

config = (PPOConfig().environment("CartPole-v1")
          .env_runners(num_env_runners=2,             # 2 (tested); B // 2 or fewer for each worker
                       num_envs_per_env_runner=20,     # more environments here, not more runners
                       num_cpus_per_env_runner=2.4))   # CPUS / B
algo = config.build_algo()
algo.train()
```

Learners: put all learners on one worker. Run the algorithm through Tune with `STRICT_PACK`:

```python
from ray import tune

config = (PPOConfig().environment("CartPole-v1")
          .env_runners(num_env_runners=0)
          .learners(num_learners=2, num_gpus_per_learner=1, num_cpus_per_learner=2.4)   # CPUS / B
          .resources(placement_strategy="STRICT_PACK"))   # all learners on one worker
tune.Tuner(config.algo_class, param_space=config,
           run_config=tune.RunConfig(
               stop={"training_iteration": 2},
               checkpoint_config=tune.CheckpointConfig(checkpoint_at_end=False))).fit()
```

We tested this code with `num_cpus_per_learner=1`. For checkpoints, add `storage_path="s3://my-bucket/experiments"` to `RunConfig`, as for [Ray Tune](tune.md#settings).

| Setting | Value (B = 10, CPUS = 24) | Formula |
|---|---|---|
| `num_cpus_per_env_runner`, `num_cpus_per_learner` | 2.4 | CPUS / B |
| `num_env_runners` | 2 (tested) | B // 2 or fewer for each worker |
| `num_envs_per_env_runner` | 20 | The environments you need / `num_env_runners` |
| `placement_strategy` for learners | `"STRICT_PACK"` | All learners on one worker |
| `num_learners` | 2 | At most the GPUs of one worker |
| `RunConfig(storage_path=...)` (through Tune, for checkpoints) | `s3://...` | A cloud bucket |

## Why

ray-vast gives each worker as many worker ports as its process limit (`pids.max`) allows, usually 14.
Each Ray actor, task or replica needs a port.
Too many at the same time make the work hang, or fail after 5 worker-start retries, but they do not crash the worker.
More environments in each env runner give the same samples with fewer processes.
Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts).

## What does not work

| What | Do this instead |
|---|---|
| Many env runners with 1 environment each. In a test, 40 env runners on one worker with 14 ports hung in the build. | Use B // 2 env runners or fewer, and raise `num_envs_per_env_runner`. |
| Learners on 2 or more workers. The build stage hangs (`[c10d] The server socket ... has timed out`). Read [Limits](../reference/limits.md#no-multi-node-ray-train). | Put all learners on one worker with many GPUs (`STRICT_PACK`). |
| Tune checkpoints with the default `storage_path` (`Unable to set up cluster storage`). | Set `storage_path` to a cloud bucket, and give the workers credentials. Read [Ray Jobs](jobs.md#pass-credentials-to-the-job). |

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
