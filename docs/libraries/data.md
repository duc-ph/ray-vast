# Ray Data

Datasets, maps, shuffles, `groupby`, batch inference, reads and writes.

## Status

- **One worker 🟨:** Reads, maps, `groupby` and GPU batch inference work. Write the output to a cloud bucket. A local output path stays on the worker.
- **Many workers 🟨:** Tasks and GPU inference actors run on many workers. Write the output to a cloud bucket. A local output path stays on each worker.

Submit your work with `ray job submit`. The workers need credentials for your bucket. Read [Submit jobs](jobs.md#pass-credentials-to-the-job).

## What is different on vast

- Write the output to a cloud bucket, for example `write_parquet("s3://my-bucket/out")`.
- Each worker runs at most *N* Ray processes at the same time. A hash shuffle needs many processes at once. Read [Many tasks or actors](many-tasks.md).

## When you need the settings

Use the [settings](#settings) only in these cases:

- You set `num_cpus` below 1 on `map_batches`.
- You use a hash shuffle (`groupby`, `random_shuffle`) on a cluster with more CPUs than *N*. Ray Data starts one shuffle aggregator for each CPU of the cluster. The aggregators and the shuffle tasks need processes at the same time.

Reads and maps with the defaults need no settings.

## Settings

*N* is the number of processes at once of the worker. Read it in the [start line](many-tasks.md#the-number-of-processes-at-once).

Ray Data starts its own tasks and actors (reads, maps, shuffles, aggregators). Do not set `num_cpus` on them. The settings below limit them.

Put this code in your driver script. The values are an example for *N* = 10. Use the *N* of your workers.

```python
import ray
from ray.data import DataContext, ExecutionResources, TaskPoolStrategy

ctx = DataContext.get_current()
ctx.max_hash_shuffle_aggregators = 2                                # N // 4
ctx.execution_options.resource_limits = ExecutionResources(cpu=2)   # N // 4

ds = ray.data.read_parquet("s3://my-bucket/in",
                           concurrency=2,             # N // 4
                           override_num_blocks=5)     # N // 2
ds = ds.map_batches(my_fn, compute=TaskPoolStrategy(size=2))   # N // 4
ds.groupby("key").count().write_parquet("s3://my-bucket/out")
```

| Setting | Value (*N* = 10) | Formula |
|---|---|---|
| `max_hash_shuffle_aggregators` | 2 | *N* // 4 |
| `execution_options.resource_limits` (`cpu`) | 2 | *N* // 4 |
| Read `concurrency` | 2 | *N* // 4 |
| `map_batches(compute=TaskPoolStrategy(size=...))` | 2 | *N* // 4 |
| `override_num_blocks` of the read (for `groupby` and other shuffles) | 5 | *N* // 2 |
| Output path | `s3://...` | A cloud bucket |

- The *N* // 4 settings split the processes between the aggregators, the reads, the maps and the shuffle tasks.
- `override_num_blocks` = *N* // 2 limits the `_shuffle_block` tasks. `resource_limits` does not limit them.
- We tested these settings with `range`, `map_batches` and `groupby` on one worker. The `read_parquet` example uses the same parameters.
- With few blocks, each block is large. Check the memory of your workers.
- These limits are for the full cluster. With more workers, they stay safe. The job then uses only part of the cluster.

## Threads

- Arrow sizes its CPU thread pool by `OMP_NUM_THREADS`. Ray sets it for each task (1 for a task with 1 CPU). Each shuffle aggregator of a `groupby` had 37 threads on a container that saw 128 CPUs.
- The libraries that your map functions import (NumPy, PyTorch) also start threads.

Read [Threads](many-tasks.md#threads).

## What does not work

| What | Do this instead |
|---|---|
| `groupby` or `random_shuffle` with the defaults, on a cluster with more CPUs than *N*. Ray Data starts one aggregator for each CPU (up to 128). The job hangs, or fails with `RaySystemError: Failed to startup worker after retrying 5 times ... _shuffle_block`. | Use all the settings above. |
| `resource_limits` and fewer aggregators, without `override_num_blocks`. The `_shuffle_block` tasks do not obey `resource_limits`. On a 24-CPU worker, 16 ran at the same time and took all 14 worker ports. | Also set `override_num_blocks` = *N* // 2 on the read. |
| A local output path (for example `write_parquet("/tmp/out")`). The files stay on each worker. The head and your laptop do not see them. | Write to a cloud bucket. |

## If it fails

1. **The head's `monitor.log`.** Look for the `is at its process limit` WARNING. It names the worker and shows the worker ports in use.

    ```bash
    ray-vast attach cluster.yaml
    # in the shell on the head:
    sudo docker exec ray-vast-head grep "process limit" /tmp/ray/session_latest/logs/monitor.log
    ```

2. **The job log.** Look for `Failed to startup worker after retrying 5 times`, or for a progress bar that does not move.

    ```bash
    ray job logs <job id>
    ```

3. **The worker logs.** Open the dashboard (http://127.0.0.1:8265) and the **Logs** tab. Choose the worker. Look for `No available ports` in its `worker-*.err` files.
