# Ray Serve

Model serving with replicas, handles and HTTP.

## Status

- **One worker ✅:** Replicas, GPU replicas and `DeploymentHandle` calls work on one worker.
- **Many workers ✅:** Replicas run on many workers, and HTTP works on port 8000 of the head through an SSH tunnel.

## Run it

Use an example cluster YAML, for example [`gpu-24gb.yaml`](https://github.com/duc-ph/ray-vast/blob/main/ray-vast-pypi/src/ray_vast/examples/gpu-24gb.yaml). Set the region of the head (`node_config.region` of the head node type).

```bash
ray-vast up cluster.yaml --profile my-aws-profile
ray-vast dashboard cluster.yaml --profile my-aws-profile   # terminal 1: keep it open
```

In terminal 2, deploy the app with a job. The script calls `serve.run(app, route_prefix="/")`. The app stays up after the job ends (in our test, HTTP calls worked after the job).

```bash
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=~/.ray-vast/<cluster_name>/token   # the line that dashboard printed
export RAY_ADDRESS=http://127.0.0.1:8265
ray job submit --working-dir . -- python serve_app.py
```

In terminal 3, open an SSH tunnel to port 8000 of the head. Use the SSH key that `up` made: `~/.ray-vast/<cluster_name>/ssh_key.pem`. `ray-vast up` prints the head IP.

```bash
ssh -i ~/.ray-vast/<cluster_name>/ssh_key.pem -o UserKnownHostsFile=~/.ray-vast/<cluster_name>.known_hosts -N -L 18000:127.0.0.1:8000 ubuntu@<head ip>
curl http://127.0.0.1:18000/
```

`DeploymentHandle` calls from a job need no tunnel.

When you finish:

```bash
ray-vast down cluster.yaml --profile my-aws-profile
```

## Do you need the process settings?

ray-vast gives each worker a fixed number of worker ports. Each Ray worker process (a task, an actor, a replica, a trial, a Train worker or an env runner) needs one port. The process limit of the host (`pids.max`) sets the number of ports. Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts).

B is the number of worker processes that your work can use on one worker. B = worker ports − 4. The 4 ports stay free for the processes that Ray's libraries start next to your work (the Serve proxy, the Data and Train helpers).

### Rule 1: processes

With Ray's defaults, Ray starts about one worker process for each CPU. You need the [settings](#settings) below only in these cases:

- Your node type has more CPUs (`CPU`) than B.
- Your replicas ask for less than 1 CPU (`ray_actor_options={"num_cpus": ...}`), or `num_replicas` on one worker is more than B.

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

We tested rule 1 on a local worker with the real ray-vast head and worker bootstrap: 6 Ray CPUs, `pids.max` 1536 and 18 worker ports (2026-10-17, scenario F21). The default code (6 replicas with the default `num_cpus` of 1) completed. The head wrote no process-limit warning, and the container refused no thread.

### Rule 2: threads and child processes

The process limit counts threads and child processes too. ray-vast sizes the worker ports for about 60 threads in each worker process. Keep each process under about 60 threads, and keep its child processes few and small. Look at these:

- The thread pools of FastAPI and of your model. A replica with a FastAPI ingress has 50 threads (measured with `RAY_num_grpc_internal_threads=4`, 2026-10-16). Your model's threads come on top.

Thread pools often size themselves by the CPUs that the container sees. A vast container often sees all the CPUs of the host (for example 384), not its CPU quota (for example 46 cores).

- The threads of each library's processes on such a host: _measured in LB15 (not run yet)_.
- The fixes and their effect: _measured in LB15 (not run yet)_. LB15 tests `OMP_NUM_THREADS` in `env_vars`, `pyarrow.set_cpu_count()` and `torch.set_num_threads()`.

We tested rule 2 on the worker of rule 1 (2026-10-17, scenario F22). 6 tasks (one for each CPU) started 200 threads each. The worker had only 13 Ray worker processes for its 18 worker ports. The container reached its limit of 1536 in two runs: 5 and 6 of the 6 tasks got `RuntimeError: can't start new thread`, and the container refused 47 and 48 thread starts. The worker stayed alive in this test, but at the limit the raylet can stop, and then the worker ends.

## Settings

Use these settings only when [rule 1](#rule-1-processes) applies.

CPUS is the number of Ray CPUs of one worker. It is the `CPU` of the node type in your cluster YAML. [Ray Core](core.md#settings) shows how to get CPUS in your driver.

The rule for every library: each process that your code starts (a task, an actor, a replica, a trial, a Train worker or an env runner) asks for `num_cpus` = CPUS / B. Then Ray puts at most B of them on one worker. The library settings below come on top of this rule.

The values below are an example for B = 10 and CPUS = 24, on one worker: rule 1 applies, because 24 is more than 10. Use the B and CPUS of your workers.

```python
from ray import serve

@serve.deployment(
    num_replicas=10,                        # at most B for each worker
    ray_actor_options={"num_cpus": 2.4},    # CPUS / B
)
class Model:
    def __call__(self, request):
        ...

serve.run(Model.bind(), route_prefix="/")
```

| Setting | Value (B = 10, CPUS = 24) | Formula |
|---|---|---|
| `ray_actor_options={"num_cpus": ...}` | 2.4 | CPUS / B |
| `num_replicas` (or `max_replicas` of `autoscaling_config`) | 10 for each worker | B × workers |

With `num_cpus` = CPUS / B, Ray puts at most B replicas on one worker. So you do not need `max_replicas_per_node`.
We tested B replicas with `num_cpus` = CPUS / B on one worker with 14 ports (`pids.max` 1280, a local test with the real ray-vast head and worker, 2026-10-16): 10 replicas started and answered, with no refused fork.

## Why

ray-vast gives each worker as many worker ports as its process limit (`pids.max`) allows: 4 to 58. About half of the hosts give 58.
Each Ray actor, task or replica needs a port.
Too many at the same time make the work hang, or fail after 5 worker-start retries, but they do not crash the worker.
Each worker also runs a Serve proxy. One of the 4 free ports is for it.
A Serve replica with a FastAPI ingress has 50 threads. We measured this with `RAY_num_grpc_internal_threads=4` on a simulated 48-core host (2026-10-16). Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts). ray-vast sizes the worker ports for this, so B replicas stay under the process limit.

## What does not work

| What | Do this instead |
|---|---|
| Many replicas on one worker. In a test, 60 replicas on one worker with 14 ports made `serve.run` hang. | Set `num_cpus` = CPUS / B on each replica. |
| HTTP from your laptop to the head without a tunnel. The security groups do not open port 8000. | Use the SSH tunnel above. |

## If it fails

1. **The head's `monitor.log`.** Look for the `is at its process limit` WARNING. It names the worker and shows the worker ports in use.

    ```bash
    ray-vast attach cluster.yaml --profile my-aws-profile
    # in the shell on the head:
    sudo docker exec ray-vast-head grep "process limit" /tmp/ray/session_latest/logs/monitor.log
    ```

2. **The job log.** `serve.run` does not return when some replicas do not start.

    ```bash
    ray job logs <job id>
    ```

3. **The worker logs.** Open the dashboard (http://127.0.0.1:8265) and the **Logs** tab. Choose the worker. Look for `No available ports` in its `worker-*.err` files.
