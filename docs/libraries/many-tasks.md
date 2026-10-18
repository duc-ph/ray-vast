# Many tasks or actors

A worker on vast runs a limited number of Ray processes at the same time.
This page tells you when this limit matters, and what to do.

## The number of processes at once

Each task, actor, Serve replica, Tune trial, Train worker and RLlib env runner is one Ray process on a worker.
Each worker runs at most *N* of them at the same time. *N* is at most 54.

*N* depends on the host. The worker prints it in its start line:

```text
bootstrap: limits: pids.max 1536 (cgroup v2), cpus 6, processes at once 14, worker ports 18, soft limit 6, grpc threads 4, startup concurrency 8
```

Here *N* is 14. The start line is in the vast console (the log of the instance), or in the output of `vastai logs <instance id>`.

## When you need to do something

With Ray's defaults, Ray starts one process for each CPU of the worker.
The provider rents only hosts that run at least `CPU` processes at once, for a node type with `CPU` up to 54.
So one task for each CPU always fits.

You need the settings of the library pages only in these cases:

- You start many tasks or actors with `num_cpus` below 1.
- You start many actors with the default options. An actor needs 1 CPU to start, but it holds 0 CPUs while it runs. So Ray does not limit the number of these actors on a worker.
- A node type has `CPU` above 54. `ray-vast up` warns about this.
- A library case that its page names. For example, a Ray Tune trial that starts its own tasks.

## What happens when there are too many

The worker does not crash. The work waits:

- A task waits for a free process. The job takes longer.
- An actor that waits can hang. Or it fails with `Failed to startup worker after retrying 5 times`. This error does not name the cause.
- A job that needs more processes at the same time than *N* can hang. For example, a Ray Data `groupby` needs its map tasks and its aggregators at the same time.

The head writes a WARNING in its log when a worker is full. Read [A worker is at its process limit](../how-to/troubleshoot.md#a-worker-is-at-its-process-limit).

## The general fix

Give each process a part of the worker's CPUs: `num_cpus` = `CPU` / *N*.
Then Ray puts at most *N* of them on one worker. `CPU` is the `CPU` of the node type in your cluster YAML.

For example, with `CPU: 24` and *N* = 10, use `num_cpus=2.4`.

Each library page gives the settings for that library: [Ray Core](core.md#settings), [Ray Data](data.md#settings), [Ray Train](train.md#settings), [Ray Tune](tune.md#settings), [Ray Serve](serve.md#settings), [RLlib](rllib.md#settings).

## Threads

The process limit of a vast container also counts threads and child processes. ray-vast leaves room for about 60 threads in each Ray process.

- Keep each process under about 60 threads.
- Give your own thread pools a fixed size, for example `ThreadPoolExecutor(max_workers=4)`. The default of Python is the number of CPUs that the container sees, plus 4.
- A vast container often sees all the CPUs of the host (for example 384), not its CPU share (for example 46 cores). Thread pools that size themselves by the visible CPUs can get too big.
- Ray sets `OMP_NUM_THREADS` for each task and actor. PyTorch and the CPU pool of Arrow use it. So these libraries stay small with Ray's defaults.
- Keep child processes few and small, for example a small `DataLoader` `num_workers`.

We counted the threads of each process on a worker that saw 128 CPUs, with Ray's defaults:

| Process | Threads |
|---|---|
| Ray Data shuffle aggregator (`read_parquet`, `map_batches`, `groupby`) | 37 |
| Ray Train worker (`TorchTrainer`, `DataLoader` with 2 workers) | 35 |
| `DataLoader` worker process | 1 |
| RLlib env runner (PPO, CartPole) | 27 |
| A task that uses PyTorch and pyarrow | 27 |
| An idle Ray worker process | 27 to 29 |
| A Serve replica with a FastAPI ingress | 50 |

So you need no thread fix with Ray's defaults. Look for code that sets a higher thread count, for example `torch.set_num_threads()`.

Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts) for how vast sets the limit.
