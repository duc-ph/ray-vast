# Ray Tune

Hyperparameter search with trials.

## Status

- **One worker 🟨:** Trials, ASHA and GPU trials work. Checkpoints need a cloud bucket (`storage_path`). The default storage fails.
- **Many workers 🟨:** Trials and GPU trials run on many workers. Checkpoints need a cloud bucket (`storage_path`).

Submit your work with `ray job submit`. The workers and the driver (on the head) need write access to the bucket. Read [Submit jobs](jobs.md#pass-credentials-to-the-job).

## What is different on vast

- Set `storage_path` to a cloud bucket. Read [Checkpoints need cloud storage](../reference/limits.md#checkpoints-need-cloud-storage).
- Each worker runs at most *N* Ray processes at the same time. A trial that starts its own tasks uses more than one process. Read [Many tasks or actors](many-tasks.md).

## When you need the settings

Use `storage_path` always. Use the CPU values only in these cases:

- Your trials ask for less than 1 CPU.
- Each trial starts Ray tasks of its own.

## Settings

`CPU` is the `CPU` of the node type in your cluster YAML. *N* is the number of processes at once of the worker. Read it in the [start line](many-tasks.md#the-number-of-processes-at-once).

The values below are an example for *N* = 10 and `CPU` = 24. Use the values of your workers.

```python
from ray import tune

tuner = tune.Tuner(
    tune.with_resources(trainable, {"cpu": 2.4}),                 # CPU / N
    param_space={"x": tune.uniform(0, 1)},
    tune_config=tune.TuneConfig(num_samples=60, metric="score", mode="max",
                                max_concurrent_trials=5),        # N // 2
    run_config=tune.RunConfig(storage_path="s3://my-bucket/experiments"),
)
tuner.fit()
```

A trial that starts Ray tasks must also hold the CPUs of these tasks. Ray starts a task of a trial only in the trial's own bundles. If no bundle is free for the task, the task does not start, and Tune shows `No trial resources are available`. Give each trial one bundle for itself and one bundle for each task that it runs at the same time (`tune.PlacementGroupFactory`):

```python
import ray
from ray import tune

@ray.remote(num_cpus=2.4)                                        # CPU / N
def evaluate(x):
    return x

def trainable(config):
    tune.report({"score": ray.get(evaluate.remote(config["x"]))})

resources = tune.PlacementGroupFactory([{"CPU": 2.4}, {"CPU": 2.4}])   # the trial, and its task
tuner = tune.Tuner(tune.with_resources(trainable, resources), ...)
```

| Setting | Value (*N* = 10, `CPU` = 24) | Formula |
|---|---|---|
| CPUs of each trial (`tune.with_resources`) | 2.4 | `CPU` / *N* |
| A trial that starts tasks: one bundle for the trial and one for each task that runs at the same time (`tune.PlacementGroupFactory`), and `num_cpus` of each task | 2.4 for each bundle and task | `CPU` / *N* |
| `TuneConfig(max_concurrent_trials=...)` | 5 | *N* // 2 |
| `RunConfig(storage_path=...)` | `s3://...` | A cloud bucket |

- A trial that starts a Ray task needs 2 processes. So `max_concurrent_trials` = *N* // 2 keeps the trials in the limit.
- `max_concurrent_trials` is for the full cluster. With more workers, it stays safe.

## Threads

Each trial runs your trainable. The thread advice of the library in the trainable applies: [Ray Train](train.md#threads) for PyTorch, [Ray Data](data.md#threads) for Arrow.

## What does not work

| What | Do this instead |
|---|---|
| Many trials at the same time with less than 1 CPU each, or trials that each wait for tasks. In a test, 60 trials on one worker with 14 worker ports hung. | Set `max_concurrent_trials` = *N* // 2. |
| A task that a trial starts, with no free bundle in the trial for it. The task does not start: `No trial resources are available`. | Give the trial a bundle for each task (`tune.PlacementGroupFactory`). |
| The default `storage_path`. The error is `Unable to set up cluster storage ... A local path on the head node is not accessible by worker nodes`. | Set `storage_path` to a cloud bucket, and give the workers credentials. |
| Anonymous S3 access through pyarrow (`s3://anonymous@...`). S3 refuses its uploads. | Use credentials that can write to the bucket prefix. |

## If it fails

1. **The head's `monitor.log`.** Look for the `is at its process limit` WARNING. It names the worker and shows the worker ports in use.

    ```bash
    ray-vast attach cluster.yaml
    # in the shell on the head:
    sudo docker exec ray-vast-head grep "process limit" /tmp/ray/session_latest/logs/monitor.log
    ```

2. **The job log.** Look for trials that stay `RUNNING` with no new results.

    ```bash
    ray job logs <job id>
    ```

3. **The worker logs.** Open the dashboard (http://127.0.0.1:8265) and the **Logs** tab. Choose the worker. Look for `No available ports` in its `worker-*.err` files.
