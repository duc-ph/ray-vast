# Ray Train

Distributed training with PyTorch.

## Status

- **One worker 🟨:** Training on one worker works with 1 GPU and with 2 GPUs (NCCL). Checkpoints need a cloud bucket (`storage_path`). The default storage fails.
- **Many workers ❌:** PyTorch DDP over 2 or more workers fails. Its connections need ports that the NAT of vast does not open. Use one worker with many GPUs.

Submit your work with `ray job submit`. The workers and the driver (on the head) need write access to the bucket. Read [Submit jobs](jobs.md#pass-credentials-to-the-job).

## What is different on vast

- Train on one worker. Use a node type with `GPU: 2` or more for more GPUs, for example [`gpu-80gb.yaml`](https://github.com/duc-ph/ray-vast/blob/main/src/ray_vast/examples/gpu-80gb.yaml). Read [No multi-node Ray Train](../reference/limits.md#no-multi-node-ray-train).
- Set `storage_path` to a cloud bucket. Read [Checkpoints need cloud storage](../reference/limits.md#checkpoints-need-cloud-storage).
- Install PyTorch in the job (`--runtime-env-json '{"pip": ["torch==2.7.1"]}'`), or build it into the image.

## When you need the settings

Use the [settings](#settings) for the storage and the placement always. Use the CPU values only in these cases:

- You set `resources_per_worker={"CPU": ...}` below 1.
- `num_workers` is more than *N*.

## Settings

`CPU` is the `CPU` of the node type in your cluster YAML. *N* is the number of processes at once of the worker. Read it in the [start line](many-tasks.md#the-number-of-processes-at-once).

The values below are an example for *N* = 10 and `CPU` = 24. Use the values of your workers.

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
        num_workers=2,                  # at most N // 2 = 5; for GPUs, the GPUs of one worker
        use_gpu=True,
        resources_per_worker={"CPU": 2.4, "GPU": 1},   # CPU / N
        placement_strategy="STRICT_PACK",   # all Train worker processes on one worker
    ),
    run_config=RunConfig(storage_path="s3://my-bucket/experiments"),
)
trainer.fit()
```

| Setting | Value (*N* = 10, `CPU` = 24) | Formula |
|---|---|---|
| `ScalingConfig(resources_per_worker={"CPU": ...})` | 2.4 | `CPU` / *N* |
| `ScalingConfig(num_workers=...)` | 2 (tested) | *N* // 2 or fewer, and at most the GPUs of one worker |
| `ScalingConfig(placement_strategy=...)` | `"STRICT_PACK"` | All Train worker processes on one worker |
| `DataLoader(num_workers=...)` | 2 | 2 or fewer. These processes count against the process limit. |
| `RunConfig(storage_path=...)` | `s3://...` | A cloud bucket |

With `num_workers` = *N* // 2 or fewer, the Train worker processes and their helpers fit on the worker.

## Threads

- Each `DataLoader` worker is a child process of a Train worker. It counts against the process limit of the container.
- PyTorch uses `OMP_NUM_THREADS` for its intra-op threads, and Ray sets it. Each Train worker had 35 threads on a container that saw 128 CPUs. Each `DataLoader` worker had 1.

Read [Threads](many-tasks.md#threads).

## What does not work

| What | Do this instead |
|---|---|
| PyTorch DDP over 2 or more workers (NCCL or Gloo). The rendezvous uses a random port, and Gloo and NCCL send the container's private address. The job fails after about 3 minutes with `DistNetworkError` or `DistStoreError ... 1/2 clients joined`. | Use one worker with many GPUs. Make a node type with `GPU: 2` or more. Read [Choose node types and GPU images](../how-to/node-types.md). |
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

4. **A job with 2 or more GPUs hangs at the first NCCL call.** Read [A multi-GPU job hangs at the first NCCL call](../how-to/troubleshoot.md#a-multi-gpu-job-hangs-at-the-first-nccl-call).
