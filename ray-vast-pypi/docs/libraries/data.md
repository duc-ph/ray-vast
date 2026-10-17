# Ray Data

Datasets, maps, shuffles, `groupby`, batch inference, reads and writes.

## Status

- **One worker 🟨:** Reads, maps, `groupby` and GPU batch inference work (LB03, LB04, LB05). Write the output to a cloud bucket. A local output path stays on the worker (C9.1.5).
- **Many workers 🟨:** Tasks and GPU inference actors run on many workers (LB03, LB05). Write the output to a cloud bucket. A local output path stays on each worker (C9.1.5).
- A hash shuffle (`groupby`, `random_shuffle`) needs the [settings](#settings) when rule 1 applies (LB04, LB11). These settings do not lower the mark.

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

Read [The process rules](../reference/limits.md#the-process-rules) first. That section defines B and CPUS, gives B for each host size, and gives rule 1 and rule 2. B is the number of worker processes that your work can use on one worker. B = worker ports − 4.

### Rule 1: processes

Rule 1 is in [Limits](../reference/limits.md#rule-1-processes). For Ray Data, you also need the [settings](#settings) in these cases:

- You set `num_cpus` below 1 on `map_batches`.
- You use a hash shuffle (`groupby`, `random_shuffle`) on a cluster with more CPUs than B on one worker. Ray Data starts one shuffle aggregator for each CPU of the cluster.

### Rule 2: threads and child processes

Rule 2 is in [Limits](../reference/limits.md#rule-2-threads-and-child-processes). For Ray Data, look at these:

- The Arrow thread pools in each read and map task. Arrow sizes its CPU pool by `OMP_NUM_THREADS`, and Ray sets it for each task (1 for a task with 1 CPU). In a test on a container that saw 128 CPUs, each shuffle aggregator of a `groupby` had 37 threads.
- The libraries that your map functions import (NumPy, PyTorch).

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
