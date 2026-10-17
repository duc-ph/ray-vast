# Ray Train

Distributed training with PyTorch.

## Status

- **One worker 🟨:** Training on one worker works with 1 GPU and with 2 GPUs (NCCL), but checkpoints need a cloud bucket.
- **Many workers ❌:** PyTorch DDP over 2 or more workers fails, because its connections need ports that the NAT of vast does not open.

## Run it

Use an example cluster YAML with 2 GPUs on one worker, for example [`gpu-80gb.yaml`](https://github.com/duc-ph/ray-vast/blob/main/ray-vast-pypi/src/ray_vast/examples/gpu-80gb.yaml) (`GPU: 2`). Set the region of the head (`node_config.region` of the head node type).

```bash
ray-vast up cluster.yaml --profile my-aws-profile
ray-vast dashboard cluster.yaml --profile my-aws-profile   # terminal 1: keep it open
```

In terminal 2:

```bash
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=~/.ray-vast/<cluster_name>/token   # the line that dashboard printed
export RAY_ADDRESS=http://127.0.0.1:8265
ray job submit --working-dir . \
  --runtime-env-json '{"pip": ["torch==2.7.1"], "env_vars": {"AWS_ACCESS_KEY_ID": "...", "AWS_SECRET_ACCESS_KEY": "..."}}' \
  -- python train.py
```

When you finish:

```bash
ray-vast down cluster.yaml --profile my-aws-profile
```

The workers and the driver (on the head) need write access to the bucket. Read [Ray Jobs](jobs.md#pass-credentials-to-the-job).

## Do you need the process settings?

Read [The process rules](../reference/limits.md#the-process-rules) first. That section defines B and CPUS, gives B for each host size, and gives rule 1 and rule 2. B is the number of worker processes that your work can use on one worker. B = worker ports − 4.

### Rule 1: processes

Rule 1 is in [Limits](../reference/limits.md#rule-1-processes). For Ray Train, you also need the [settings](#settings) in these cases:

- You set `resources_per_worker={"CPU": ...}` below 1.
- `num_workers` is more than B.

### Rule 2: threads and child processes

Rule 2 is in [Limits](../reference/limits.md#rule-2-threads-and-child-processes). For Ray Train, look at these:

- `DataLoader(num_workers=...)`. Each loader worker is a child process of a Train worker. It uses no port, but it counts against the process limit.
- The intra-op threads of PyTorch in each Train worker.

## Settings

Use these settings only when [rule 1](#rule-1-processes) applies.

CPUS is the number of Ray CPUs of one worker. It is the `CPU` of the node type in your cluster YAML. [Ray Core](core.md#settings) shows how to get CPUS in your driver.

The rule for every library: each process that your code starts (a task, an actor, a replica, a trial, a Train worker or an env runner) asks for `num_cpus` = CPUS / B. Then Ray puts at most B of them on one worker. The library settings below come on top of this rule.

The values below are an example for B = 10 and CPUS = 24: rule 1 applies, because 24 is more than 10. Use the B and CPUS of your workers.

```python
import torch
from ray.train import RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer

def train_func(config):
    loader = torch.utils.data.DataLoader(dataset, batch_size=128, num_workers=2)  # small num_workers
    ...

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(
        num_workers=2,                  # at most B // 2 = 5; for GPUs, the GPUs of one worker
        use_gpu=True,
        resources_per_worker={"CPU": 2.4, "GPU": 1},   # CPUS / B
        placement_strategy="STRICT_PACK",   # all Train worker processes on one worker
    ),
    run_config=RunConfig(storage_path="s3://my-bucket/experiments"),
)
trainer.fit()
```

| Setting | Value (B = 10, CPUS = 24) | Formula |
|---|---|---|
| `ScalingConfig(resources_per_worker={"CPU": ...})` | 2.4 | CPUS / B |
| `ScalingConfig(num_workers=...)` | 2 (tested) | B // 2 or fewer, and at most the GPUs of one worker |
| `ScalingConfig(placement_strategy=...)` | `"STRICT_PACK"` | All Train worker processes on one worker |
| `DataLoader(num_workers=...)` | 2 | 2 or fewer. These processes count against the process limit. They do not use ports. |
| `RunConfig(storage_path=...)` | `s3://...` | A cloud bucket |

## Why

ray-vast gives each worker as many worker ports as its process limit (`pids.max`) allows: 4 to 58. About half of the hosts give 58.
Each Ray actor, task or replica needs a port.
Too many at the same time make the work hang, or fail after 5 worker-start retries, but they do not crash the worker.
With `num_workers` = B // 2, the Train worker processes and their helpers fit in the ports.
Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts).

## What does not work

| What | Do this instead |
|---|---|
| PyTorch DDP over 2 or more workers (NCCL or Gloo). The rendezvous uses a random port, and Gloo and NCCL send the container's private address. The job fails after about 3 minutes with `DistNetworkError` or `DistStoreError ... 1/2 clients joined`. Read [Limits](../reference/limits.md#no-multi-node-ray-train). | Use one worker with many GPUs. Make a node type with `GPU: 2` or more. Read [Choose node types and offer queries](../how-to/node-types.md). |
| The default `storage_path`. The error is `A local path on the head node is not accessible by worker nodes`. | Set `storage_path` to a cloud bucket, and give the workers credentials. |
| Anonymous S3 access through pyarrow (`s3://anonymous@...`). S3 refuses its uploads. | Use credentials that can write to the bucket prefix. |

## If it fails

1. **The head's `monitor.log`.** Look for the `is at its process limit` WARNING. It names the worker and shows the worker ports in use.

    ```bash
    ray-vast attach cluster.yaml --profile my-aws-profile
    # in the shell on the head:
    sudo docker exec ray-vast-head grep "process limit" /tmp/ray/session_latest/logs/monitor.log
    ```

2. **The job log.** A worker group that does not start shows no progress.

    ```bash
    ray job logs <job id>
    ```

3. **The worker logs.** Open the dashboard (http://127.0.0.1:8265) and the **Logs** tab. Choose the worker. Look for `No available ports` in its `worker-*.err` files.
