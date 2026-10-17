# Ray Core

Tasks, actors, placement groups and objects.

## Status

- **One worker ✅:** Tasks, actors, actor restarts and placement groups work on one worker.
- **Many workers ✅:** Placement groups (`PACK`, `STRICT_SPREAD`) and 2 GB objects between hosts work over the public internet.

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
ray job submit --working-dir . -- python my_job.py
```

When you finish:

```bash
ray-vast down cluster.yaml --profile my-aws-profile
```

Packages, credentials and GPUs: read [Ray Jobs](jobs.md).

## Do you need the process settings?

Read [The process rules](../reference/limits.md#the-process-rules) first. That section defines B and CPUS, gives B for each host size, and gives rule 1 and rule 2. B is the number of worker processes that your work can use on one worker. B = worker ports − 4.

### Rule 1: processes

Rule 1 is in [Limits](../reference/limits.md#rule-1-processes). For Ray Core, you also need the [settings](#settings) in these cases:

- You start many tasks or actors with `num_cpus` below 1.
- You start many actors with the default `num_cpus`. Such an actor needs 1 CPU to start, but it holds 0 CPUs while it runs. So Ray does not limit the number of such actors on a worker.

### Rule 2: threads and child processes

Rule 2 is in [Limits](../reference/limits.md#rule-2-threads-and-child-processes). For Ray Core, look at these:

- Your own thread pools, for example `concurrent.futures.ThreadPoolExecutor`. Give them a fixed `max_workers`. The default of Python is the number of CPUs that the container sees, plus 4.
- The libraries that your tasks import. NumPy, PyTorch and pyarrow start thread pools. Limits has the measured numbers.

## Settings

Use these settings only when [rule 1](#rule-1-processes) applies.

CPUS is the number of Ray CPUs of one worker. It is the `CPU` of the node type in your cluster YAML.

The rule for every library: each process that your code starts (a task, an actor, a replica, a trial, a Train worker or an env runner) asks for `num_cpus` = CPUS / B. Then Ray puts at most B of them on one worker. The library pages add their own settings on top of this rule.

The values below are an example for B = 10 and CPUS = 24: rule 1 applies, because 24 is more than 10. Use the B and CPUS of your workers.

```python
import ray

@ray.remote(num_cpus=2.4)          # CPUS / B
def child(i):
    ...

@ray.remote(num_cpus=2.4)          # CPUS / B
class Runner:
    def work(self, i):
        return ray.get(child.remote(i))   # the actor waits for a task: 2 processes

pool = [Runner.remote() for _ in range(5)]   # B // 2, because each call also needs a task
```

| Setting | Value (B = 10, CPUS = 24) | Formula |
|---|---|---|
| `num_cpus` of each task | 2.4 | CPUS / B |
| `num_cpus` of each actor | 2.4 | CPUS / B |
| Actors that wait for a task | 5 | B // 2 |

You can also get CPUS in your driver. This needs at least one worker, because the head has `CPU: 0`:

```python
CPUS = max(n["Resources"]["CPU"] for n in ray.nodes()
           if n["Alive"] and n["Resources"].get("CPU", 0) > 0)
```

If your worker types have different `CPU` values, this gives the largest. Then the workers of the smaller types run fewer tasks at the same time.
To keep the workers alike, give all worker types the same `CPU`. Read [Choose node types and offer queries](../how-to/node-types.md).

## Why

ray-vast gives each worker as many worker ports as its process limit (`pids.max`) allows: 4 to 58. About half of the hosts give 58.
Each Ray actor, task or replica needs a port.
Too many at the same time make the work hang, or fail after 5 worker-start retries, but they do not crash the worker.
Ray has no setting for the maximum actors on a worker, so `num_cpus` = CPUS / B is the control: at most B fit on a worker, not 48 with the default `num_cpus=1` on a 48-CPU worker.
Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts).

## What does not work

| What | Do this instead |
|---|---|
| Many actors with the default `num_cpus`. An actor needs 1 CPU to start but holds 0 CPUs while it runs, so many actors fit on one worker. | Set `num_cpus` = CPUS / B on each actor. |
| More than B // 2 actors that each wait for their own task. The actors take all ports, and the tasks get none. | Use a pool of B // 2 actors. |

## If it fails

1. **The head's `monitor.log`.** Look for the `is at its process limit` WARNING. It names the worker and shows the worker ports in use.

    ```bash
    ray-vast attach cluster.yaml --profile my-aws-profile
    # in the shell on the head:
    sudo docker exec ray-vast-head grep "process limit" /tmp/ray/session_latest/logs/monitor.log
    ```

2. **The job log.** An actor that got no port hangs, or fails with `Failed to startup worker after retrying 5 times`. This error does not name the port limit.

    ```bash
    ray job logs <job id>
    ```

3. **The worker logs.** Open the dashboard (http://127.0.0.1:8265) and the **Logs** tab. Choose the worker. Look for `No available ports` in its `worker-*.err` files.
