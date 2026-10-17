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

ray-vast gives each worker a fixed number of worker ports. Each Ray worker process (a task, an actor, a replica, a trial, a Train worker or an env runner) needs one port. The process limit of the host (`pids.max`) sets the number of ports. Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts).

B is the number of worker processes that your work can use on one worker. B = worker ports − 4. The 4 ports stay free for the processes that Ray's libraries start next to your work (the Serve proxy, the Data and Train helpers).

### Rule 1: processes

With Ray's defaults, Ray starts about one worker process for each CPU. You need the [settings](#settings) below only in these cases:

- Your node type has more CPUs (`CPU`) than B.
- You start many actors. An actor with the default `num_cpus` needs 1 CPU to start, but it holds 0 CPUs while it runs. So Ray does not limit the number of such actors on a worker.

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

We tested rule 1 on a local worker with the real ray-vast head and worker bootstrap: 6 Ray CPUs, `pids.max` 1536 and 18 worker ports (2026-10-17, scenario F21). The default code (6 actors with the default options, each of which waits for a task with the default `num_cpus`) completed. The head wrote no process-limit warning, and the container refused no thread.

### Rule 2: threads and child processes

The process limit counts threads and child processes too. ray-vast sizes the worker ports for about 60 threads in each worker process. Keep each process under about 60 threads, and keep its child processes few and small. Look at these:

- Your own thread pools, for example `concurrent.futures.ThreadPoolExecutor`. Give them a fixed `max_workers`. The default of Python is the number of CPUs that the container sees, plus 4.
- The libraries that your tasks import. NumPy, PyTorch and pyarrow start thread pools. Read the measured numbers below.

Thread pools often size themselves by the CPUs that the container sees. A vast container often sees all the CPUs of the host (for example 384), not its CPU quota (for example 46 cores).

- The threads of each library's processes on such a host: _measured in LB15 (not run yet)_.
- The fixes and their effect: _measured in LB15 (not run yet)_. LB15 tests `OMP_NUM_THREADS` in `env_vars`, `pyarrow.set_cpu_count()` and `torch.set_num_threads()`.

We tested rule 2 on the worker of rule 1 (2026-10-17, scenario F22). 6 tasks (one for each CPU) started 200 threads each. The worker had only 13 Ray worker processes for its 18 worker ports. The container reached its limit of 1536 in two runs: 5 and 6 of the 6 tasks got `RuntimeError: can't start new thread`, and the container refused 47 and 48 thread starts. The worker stayed alive in this test, but at the limit the raylet can stop, and then the worker ends.

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

You can also get CPUS in your driver (it needs at least one worker; the head has `CPU: 0`):

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
