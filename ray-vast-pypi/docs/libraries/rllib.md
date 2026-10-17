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

## Do you need the process settings?

Read [The process rules](../reference/limits.md#the-process-rules) first. That section defines B and CPUS, gives B for each host size, and gives rule 1 and rule 2. B is the number of worker processes that your work can use on one worker. B = worker ports − 4.

### Rule 1: processes

Rule 1 is in [Limits](../reference/limits.md#rule-1-processes). For RLlib, you also need the [settings](#settings) in these cases:

- You set `num_cpus_per_env_runner` below 1.
- `num_env_runners` on one worker is more than B.

### Rule 2: threads and child processes

Rule 2 is in [Limits](../reference/limits.md#rule-2-threads-and-child-processes). For RLlib, look at these:

- Vector environments that start subprocesses (for example a `gymnasium` async vector environment). Each environment is then a child process of an env runner.
- The intra-op threads of PyTorch in each env runner and learner.

## Settings

Use these settings only when [rule 1](#rule-1-processes) applies.

CPUS is the number of Ray CPUs of one worker. It is the `CPU` of the node type in your cluster YAML. [Ray Core](core.md#settings) shows how to get CPUS in your driver.

The rule for every library: each process that your code starts (a task, an actor, a replica, a trial, a Train worker or an env runner) asks for `num_cpus` = CPUS / B. Then Ray puts at most B of them on one worker. The library settings below come on top of this rule.

The values below are an example for B = 10 and CPUS = 24: rule 1 applies, because 24 is more than 10. Use the B and CPUS of your workers. Env runners:

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

ray-vast gives each worker as many worker ports as its process limit (`pids.max`) allows: 4 to 58. About half of the hosts give 58.
Each Ray actor, task or replica needs a port.
Too many at the same time make the work hang, or fail after 5 worker-start retries, but they do not crash the worker.
RLlib can also go on with fewer env runners than you asked for.
More environments in each env runner give the same samples with fewer processes.
Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts).

## What does not work

| What | Do this instead |
|---|---|
| Many env runners with 1 environment each. In a test, 40 env runners on one worker with 14 ports hung in the build. In a test on a worker with 58 ports, the job asked for 62 env runners. Only 58 started, and RLlib trained with them after about 6 minutes. | Use B // 2 env runners or fewer, and raise `num_envs_per_env_runner`. |
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
