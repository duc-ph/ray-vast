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

ray-vast gives each worker a fixed number of worker ports. Each Ray worker process (a task, an actor, a replica, a trial, a Train worker or an env runner) needs one port. The process limit of the host (`pids.max`) sets the number of ports. Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts).

B is the number of worker processes that your work can use on one worker. B = worker ports − 4. The 4 ports stay free for the processes that Ray's libraries start next to your work (the Serve proxy, the Data and Train helpers).

### Rule 1: processes

With Ray's defaults, Ray starts about one worker process for each CPU. You need the [settings](#settings) below only in these cases:

- Your node type has more CPUs (`CPU`) than B.
- You set `resources_per_worker={"CPU": ...}` below 1, or `num_workers` is more than B.

B depends on the host. vast gives a container a CPU quota of 0.96 × `cpu_cores_effective`, and a process limit of 256 for each whole core of that quota. The table is for a host with 64 or more direct ports:

| `cpu_cores_effective` of the host | Worker ports | B |
|---|---|---|
| 4 | 6 | 2 |
| 5 to 7 | 10 to 18 | 6 to 14 |
| 8 to 16 | 23 to 57 | 19 to 53 |
| 17 or more | 58 (the maximum) | 54 |

The offer filter is `cpu_cores_effective >= max(CPU, 4)`. So on a host with 64 or more direct ports, a node type with `CPU` from 5 to 54 gets a B of at least `CPU`. A node type with `CPU` 3 or 4 can get a 4-core host with B = 2. A node type with `CPU` above 54 always has more CPUs than B.
A host with fewer direct ports gives fewer worker ports: at most the direct ports minus 6. For example, 20 direct ports give at most 14 worker ports (B = 10).

Read the real value in the start line of the worker (in the vast console, or `vastai logs <instance id>`):

```text
bootstrap: limits: pids.max 1536 (cgroup v2), cpus 6, worker ports 18, soft limit 6, grpc threads 4, startup concurrency 8
```

Here B = 18 − 4 = 14, and `cpus 6` is less than B. So this worker needs no settings for the defaults.

We tested rule 1 on a local worker with the real ray-vast head and worker bootstrap: 6 Ray CPUs, `pids.max` 1536 and 18 worker ports (2026-10-17, scenario F21). The default code (a `TorchTrainer` with 6 workers of 1 CPU each) completed. The head wrote no process-limit warning, and the container refused no thread.

### Rule 2: threads and child processes

The process limit counts threads and child processes too. ray-vast sizes the worker ports for about 60 threads in each worker process. Keep each process under about 60 threads, and keep its child processes few and small. Look at these:

- `DataLoader(num_workers=...)`. Each loader worker is a child process of a Train worker. It uses no port, but it counts against the process limit.
- The intra-op threads of PyTorch in each Train worker.

Thread pools often size themselves by the CPUs that the container sees. A vast container often sees all the CPUs of the host (for example 384), not its CPU quota (for example 46 cores).

- The threads of each library's processes on such a host: _measured in LB15 (not run yet)_.
- The fixes and their effect: _measured in LB15 (not run yet)_. LB15 tests `OMP_NUM_THREADS` in `env_vars`, `pyarrow.set_cpu_count()` and `torch.set_num_threads()`.

We tested rule 2 on the worker of rule 1 (2026-10-17, scenario F22). 6 tasks (one for each CPU) started 200 threads each. The worker had only 13 Ray worker processes for its 18 worker ports. The container reached its limit of 1536 in two runs: 5 and 6 of the 6 tasks got `RuntimeError: can't start new thread`, and the container refused 47 and 48 thread starts. The worker stayed alive in this test, but at the limit the raylet can stop, and then the worker ends.

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
