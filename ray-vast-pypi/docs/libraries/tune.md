# Ray Tune

Hyperparameter search with trials.

## Status

- **One worker 🟨:** Trials, ASHA and GPU trials work, but checkpoints need a cloud bucket.
- **Many workers 🟨:** Trials run on many workers, but checkpoints need a cloud bucket.

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
  -- python tune_job.py
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
- Your trials ask for less than 1 CPU, or each trial starts Ray tasks of its own.

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

We tested rule 1 on a local worker with the real ray-vast head and worker bootstrap: 6 Ray CPUs, `pids.max` 1536 and 18 worker ports (2026-10-17, scenario F21). The default code (22 trials with the default 1 CPU each (6 at the same time)) completed. The head wrote no process-limit warning, and the container refused no thread.

### Rule 2: threads and child processes

The process limit counts threads and child processes too. ray-vast sizes the worker ports for about 60 threads in each worker process. Keep each process under about 60 threads, and keep its child processes few and small. Look at these:

- Each trial runs your trainable. The rule 2 items of the library in the trainable apply: [Ray Train](train.md#rule-2-threads-and-child-processes) for torch, [Ray Data](data.md#rule-2-threads-and-child-processes) for Arrow.

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
from ray import tune

tuner = tune.Tuner(
    tune.with_resources(trainable, {"cpu": 2.4}),                 # CPUS / B
    param_space={"x": tune.uniform(0, 1)},
    tune_config=tune.TuneConfig(num_samples=60, metric="score", mode="max",
                                max_concurrent_trials=5),        # B // 2
    run_config=tune.RunConfig(storage_path="s3://my-bucket/experiments"),
)
tuner.fit()
```

A trial that starts Ray tasks must also hold the CPUs of these tasks. Ray starts a task of a trial only in the trial's own bundles. If no bundle is free for the task, the task does not start, and Tune shows `No trial resources are available`. Give each trial one bundle for itself and one bundle for each task that it runs at the same time (`tune.PlacementGroupFactory`):

```python
import ray
from ray import tune

@ray.remote(num_cpus=2.4)                                        # CPUS / B
def evaluate(x):
    return x

def trainable(config):
    tune.report({"score": ray.get(evaluate.remote(config["x"]))})

resources = tune.PlacementGroupFactory([{"CPU": 2.4}, {"CPU": 2.4}])   # the trial, and its task
tuner = tune.Tuner(tune.with_resources(trainable, resources), ...)
```

| Setting | Value (B = 10, CPUS = 24) | Formula |
|---|---|---|
| CPUs of each trial (`tune.with_resources`) | 2.4 | CPUS / B |
| A trial that starts tasks: one bundle for the trial and one for each task that runs at the same time (`tune.PlacementGroupFactory`), and `num_cpus` of each task | 2.4 for each bundle and task | CPUS / B |
| `TuneConfig(max_concurrent_trials=...)` | 5 | B // 2 |
| `RunConfig(storage_path=...)` | `s3://...` | A cloud bucket |

`max_concurrent_trials` is for the full cluster. With more workers, it stays safe.

## Why

ray-vast gives each worker as many worker ports as its process limit (`pids.max`) allows: 4 to 58. About half of the hosts give 58.
Each Ray actor, task or replica needs a port.
Too many at the same time make the work hang, or fail after 5 worker-start retries, but they do not crash the worker.
A trial that starts a Ray task needs 2 ports, so `max_concurrent_trials` = B // 2 keeps the trials in the ports.
Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts).

## What does not work

| What | Do this instead |
|---|---|
| Many trials at the same time (the default). In a test, 60 trials on one worker with 14 ports hung. | Set `max_concurrent_trials` = B // 2. |
| A task that a trial starts, with no free bundle in the trial for it. The task does not start: `No trial resources are available`. | Give the trial a bundle for each task (`tune.PlacementGroupFactory`). |
| The default `storage_path`. The error is `Unable to set up cluster storage ... A local path on the head node is not accessible by worker nodes`. | Set `storage_path` to a cloud bucket, and give the workers credentials. |
| Anonymous S3 access through pyarrow (`s3://anonymous@...`). S3 refuses its uploads. | Use credentials that can write to the bucket prefix. |

## If it fails

1. **The head's `monitor.log`.** Look for the `is at its process limit` WARNING. It names the worker and shows the worker ports in use.

    ```bash
    ray-vast attach cluster.yaml --profile my-aws-profile
    # in the shell on the head:
    sudo docker exec ray-vast-head grep "process limit" /tmp/ray/session_latest/logs/monitor.log
    ```

2. **The job log.** Look for trials that stay `RUNNING` with no new results.

    ```bash
    ray job logs <job id>
    ```

3. **The worker logs.** Open the dashboard (http://127.0.0.1:8265) and the **Logs** tab. Choose the worker. Look for `No available ports` in its `worker-*.err` files.
