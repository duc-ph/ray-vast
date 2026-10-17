# Ray Data

Datasets, maps, shuffles, `groupby`, batch inference, reads and writes.

## Status

- **One worker 🟨:** Maps and reads work with the defaults, but `groupby` and other hash shuffles need the settings below and `override_num_blocks`.
- **Many workers 🟨:** Tasks and GPU inference actors run on many workers, but a local output path stays on each worker.

## Run it

Use an example cluster YAML, for example [`gpu-24gb.yaml`](https://github.com/duc-ph/ray-vast/blob/main/ray-vast-pypi/src/ray_vast/examples/gpu-24gb.yaml). Set the region of the head (`node_config.region` of the head node type).

```bash
ray-vast up cluster.yaml --profile my-aws-profile
ray-vast dashboard cluster.yaml --profile my-aws-profile   # terminal 1: keep it open
```

In terminal 2:

```bash
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=~/.ray-vast/<cluster_name>/token   # the line that dashboard printed
export RAY_ADDRESS=http://127.0.0.1:8265
ray job submit --working-dir . \
  --runtime-env-json '{"env_vars": {"AWS_ACCESS_KEY_ID": "...", "AWS_SECRET_ACCESS_KEY": "..."}}' \
  -- python my_pipeline.py
```

When you finish:

```bash
ray-vast down cluster.yaml --profile my-aws-profile
```

The workers need credentials for your bucket. Read [Ray Jobs](jobs.md#pass-credentials-to-the-job).

## Do you need the process settings?

ray-vast gives each worker a fixed number of worker ports. Each Ray worker process (a task, an actor, a replica, a trial, a Train worker or an env runner) needs one port. The process limit of the host (`pids.max`) sets the number of ports. Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts).

B is the number of worker processes that your work can use on one worker. B = worker ports − 4. The 4 ports stay free for the processes that Ray's libraries start next to your work (the Serve proxy, the Data and Train helpers).

### Rule 1: processes

With Ray's defaults, Ray starts about one worker process for each CPU. You need the [settings](#settings) below only in these cases:

- Your node type has more CPUs (`CPU`) than B.
- You set `num_cpus` below 1 on `map_batches`, or you use a hash shuffle (`groupby`, `random_shuffle`) on a cluster with more CPUs than B on one worker. Ray Data starts one shuffle aggregator for each CPU of the cluster.

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

We tested rule 1 on a local worker with the real ray-vast head and worker bootstrap: 6 Ray CPUs, `pids.max` 1536 and 18 worker ports (2026-10-17, scenario F21). The default code (`range`, `map_batches` with the default `num_cpus` and `groupby`) completed. The head wrote no process-limit warning, and the container refused no thread.

### Rule 2: threads and child processes

The process limit counts threads and child processes too. ray-vast sizes the worker ports for about 60 threads in each worker process. Keep each process under about 60 threads, and keep its child processes few and small. Look at these:

- The Arrow thread pools in each read and map task. Arrow sizes its CPU pool by the CPUs that the container sees, unless `OMP_NUM_THREADS` is set.
- The libraries that your map functions import (NumPy, PyTorch).

Thread pools often size themselves by the CPUs that the container sees. A vast container often sees all the CPUs of the host (for example 384), not its CPU quota (for example 46 cores).

- The threads of each library's processes on such a host: _measured in LB15 (not run yet)_.
- The fixes and their effect: _measured in LB15 (not run yet)_. LB15 tests `OMP_NUM_THREADS` in `env_vars`, `pyarrow.set_cpu_count()` and `torch.set_num_threads()`.

We tested rule 2 on the worker of rule 1 (2026-10-17, scenario F22). 6 tasks (one for each CPU) started 200 threads each. The worker had only 13 Ray worker processes for its 18 worker ports. The container reached its limit of 1536 in two runs: 5 and 6 of the 6 tasks got `RuntimeError: can't start new thread`, and the container refused 47 and 48 thread starts. The worker stayed alive in this test, but at the limit the raylet can stop, and then the worker ends.

## Settings

Use these settings only when [rule 1](#rule-1-processes) applies.

CPUS is the number of Ray CPUs of one worker. It is the `CPU` of the node type in your cluster YAML. [Ray Core](core.md#settings) shows how to get CPUS in your driver.

The rule for every library: each process that your code starts (a task, an actor, a replica, a trial, a Train worker or an env runner) asks for `num_cpus` = CPUS / B. Then Ray puts at most B of them on one worker. The library settings below come on top of this rule.

Ray Data starts its own tasks and actors (reads, maps, shuffles, aggregators). Do not set `num_cpus` on them: the settings below limit them. The rule is for the other tasks and actors of your job.

Put this code in your driver script. The values are an example for B = 10. Use the B of your workers.

```python
import ray
from ray.data import DataContext, ExecutionResources, TaskPoolStrategy

ctx = DataContext.get_current()
ctx.max_hash_shuffle_aggregators = 2                                # B // 4
ctx.execution_options.resource_limits = ExecutionResources(cpu=2)   # B // 4

ds = ray.data.read_parquet("s3://my-bucket/in",
                           concurrency=2,             # B // 4
                           override_num_blocks=5)     # B // 2
ds = ds.map_batches(my_fn, compute=TaskPoolStrategy(size=2))   # B // 4
ds.groupby("key").count().write_parquet("s3://my-bucket/out")
```

| Setting | Value (B = 10) | Formula |
|---|---|---|
| `max_hash_shuffle_aggregators` | 2 | B // 4 |
| `execution_options.resource_limits` (`cpu`) | 2 | B // 4 |
| Read `concurrency` | 2 | B // 4 |
| `map_batches(compute=TaskPoolStrategy(size=...))` | 2 | B // 4 |
| `override_num_blocks` of the read (for `groupby` and other shuffles) | 5 | B // 2 |
| Output path | `s3://...` | A cloud bucket |

We tested these settings with `range`, `map_batches` and `groupby` on one worker. The `read_parquet` example uses the same parameters. With few blocks, each block is large, so check the memory of your workers.
These limits are for the full cluster. With more workers, they stay safe. The job then uses only part of the cluster.
Without a hash shuffle (only reads and maps), the defaults worked in our tests.

## Why

ray-vast gives each worker as many worker ports as its process limit (`pids.max`) allows: 4 to 58. About half of the hosts give 58.
Each Ray actor, task or replica needs a port.
Too many at the same time make the work hang, or fail after 5 worker-start retries, but they do not crash the worker.
The B // 4 settings split B between the aggregators, the reads, the maps and the shuffle tasks, and `override_num_blocks` = B // 2 limits the `_shuffle_block` tasks that `resource_limits` does not limit.
Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts).

## What does not work

| What | Do this instead |
|---|---|
| `groupby` or `random_shuffle` with the defaults. Ray Data starts one aggregator for each CPU (up to 128). The job hangs, or fails with `RaySystemError: Failed to startup worker after retrying 5 times ... _shuffle_block`. | Use all the settings above. |
| `resource_limits` and fewer aggregators, without `override_num_blocks`. The `_shuffle_block` tasks do not obey `resource_limits`. On a 24-CPU worker, 16 ran at the same time and took all 14 ports. | Also set `override_num_blocks` = B // 2 on the read. |
| A local output path (for example `write_parquet("/tmp/out")`). The files stay on each worker. The head and your laptop do not see them. | Write to a cloud bucket. |

## If it fails

1. **The head's `monitor.log`.** Look for the `is at its process limit` WARNING. It names the worker and shows the worker ports in use.

    ```bash
    ray-vast attach cluster.yaml --profile my-aws-profile
    # in the shell on the head:
    sudo docker exec ray-vast-head grep "process limit" /tmp/ray/session_latest/logs/monitor.log
    ```

2. **The job log.** Look for `Failed to startup worker after retrying 5 times`, or for a progress bar that does not move.

    ```bash
    ray job logs <job id>
    ```

3. **The worker logs.** Open the dashboard (http://127.0.0.1:8265) and the **Logs** tab. Choose the worker. Look for `No available ports` in its `worker-*.err` files.
