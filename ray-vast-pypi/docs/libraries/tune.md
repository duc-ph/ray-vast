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

## Settings

B is the number of worker processes that your work can use on one worker. B = worker ports − 4. The 4 ports stay free for the processes that Ray's libraries start next to your work (the Serve proxy, the Data and Train helpers).
The start line of each worker shows its port count, for example `worker ports 14` (in the vast console, or `vastai logs <instance id>`).
Hosts with `pids.max` 1280 give 14 worker ports, so B = 10. Most workers in our tests had 14. A host with a lower limit gives fewer ports, so read the start line. If you cannot read it, use B = 10.

CPUS is the number of Ray CPUs of one worker. It is the `CPU` of the node type in your cluster YAML. [Ray Core](core.md#settings) shows how to get CPUS in your driver.

The rule for every library: each process that your code starts (a task, an actor, a replica, a trial, a Train worker or an env runner) asks for `num_cpus` = CPUS / B. Then Ray puts at most B of them on one worker. The library settings below come on top of this rule.

The values below are for B = 10 and CPUS = 24.

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

ray-vast gives each worker as many worker ports as its process limit (`pids.max`) allows, usually 14.
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
