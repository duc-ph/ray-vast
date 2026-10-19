# Ray Core

Tasks, actors, placement groups and objects.

## Status

- **One worker ✅:** Tasks, actors, actor restarts and placement groups work.
- **Many workers ✅:** Placement groups (`PACK`, `STRICT_SPREAD`) work. Objects of 2 GB move between hosts over the public internet.

Submit your work with `ray job submit`. Read [Submit jobs](jobs.md).

## What is different on vast

- Each worker runs at most *N* Ray processes at the same time. Read [Many tasks or actors](many-tasks.md).
- Write results to cloud storage, not to the disk of a worker. Ray removes idle workers.

## When you need the settings

Use the [settings](#settings) only in these cases:

- You start many tasks or actors with `num_cpus` below 1.
- You start many actors with the default options. Such an actor needs 1 CPU to start, but it holds 0 CPUs while it runs. So Ray does not limit the number of these actors on a worker.
- Your actors each wait for a task of their own.
- The node type has `CPU` above 54.

## Settings

`CPU` is the `CPU` of the node type in your cluster YAML. *N* is the number of processes at once of the worker. Read it in the [start line](many-tasks.md#the-number-of-processes-at-once).

The values below are an example for *N* = 10 and `CPU` = 24. Use the values of your workers.

```python
import ray

@ray.remote(num_cpus=2.4)          # CPU / N
def child(i):
    ...

@ray.remote(num_cpus=2.4)          # CPU / N
class Runner:
    def work(self, i):
        return ray.get(child.remote(i))   # the actor waits for a task: 2 processes

pool = [Runner.remote() for _ in range(5)]   # N // 2, because each call also needs a task
```

| Setting | Value (*N* = 10, `CPU` = 24) | Formula |
|---|---|---|
| `num_cpus` of each task | 2.4 | `CPU` / *N* |
| `num_cpus` of each actor | 2.4 | `CPU` / *N* |
| Actors that wait for a task | 5 | *N* // 2 |

You can get `CPU` in your driver. This needs at least one worker, because the head has `CPU: 0`:

```python
CPUS = max(n["Resources"]["CPU"] for n in ray.nodes()
           if n["Alive"] and n["Resources"].get("CPU", 0) > 0)
```

If your worker types have different `CPU` values, this gives the largest. Then the workers of the smaller types run fewer tasks at the same time.
To keep the workers alike, give all worker types the same `CPU`. Read [Choose node types and GPU images](../how-to/node-types.md).

Ray has no setting for the most actors on a worker. So `num_cpus` is the control. With `num_cpus` = `CPU` / *N*, at most *N* actors fit on a worker.

## Threads

- Give your own thread pools a fixed size, for example `concurrent.futures.ThreadPoolExecutor(max_workers=4)`. The default of Python is the number of CPUs that the container sees, plus 4.
- NumPy, PyTorch and pyarrow start thread pools. Ray sets `OMP_NUM_THREADS` for each task, and these pools use it. A task that used PyTorch and pyarrow had 27 threads on a container that saw 128 CPUs.

Read [Threads](many-tasks.md#threads).

## What does not work

| What | Do this instead |
|---|---|
| Many actors with the default `num_cpus`. An actor needs 1 CPU to start but holds 0 CPUs while it runs, so many actors fit on one worker. | Set `num_cpus` = `CPU` / *N* on each actor. |
| More than *N* // 2 actors that each wait for their own task. The actors take all processes, and the tasks get none. | Use a pool of *N* // 2 actors. |

## If it fails

1. **The head's `monitor.log`.** Look for the `is at its process limit` WARNING. It names the worker and shows the worker ports in use.

    ```bash
    ray-vast attach cluster.yaml
    # in the shell on the head:
    sudo docker exec ray-vast-head grep "process limit" /tmp/ray/session_latest/logs/monitor.log
    ```

2. **The job log.** An actor that got no process hangs, or fails with `Failed to startup worker after retrying 5 times`. This error does not name the cause.

    ```bash
    ray job logs <job id>
    ```

3. **The worker logs.** Open the dashboard (http://127.0.0.1:8265) and the **Logs** tab. Choose the worker. Look for `No available ports` in its `worker-*.err` files.
