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

## Settings

B is the number of worker processes that your work can use on one worker. B = worker ports − 4. The 4 ports stay free for the processes that Ray's libraries start next to your work (the Serve proxy, the Data and Train helpers).
The start line of each worker shows its port count, for example `worker ports 14` (in the vast console, or `vastai logs <instance id>`).
Hosts with `pids.max` 1280 give 14 worker ports, so B = 10. Most workers in our tests had 14. A host with a lower limit gives fewer ports, so read the start line. If you cannot read it, use B = 10.

CPUS is the number of Ray CPUs of one worker. It is the real core count of the host, not the `CPU` of the node type. [Ray Core](core.md#settings) shows how to get CPUS in your driver.

The rule for every library: each process that your code starts (a task, an actor, a replica, a trial, a Train worker or an env runner) asks for `num_cpus` = CPUS / B. Then Ray puts at most B of them on one worker. The library settings below come on top of this rule.

The values below are for B = 10 and CPUS = 24, on one worker.

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

ray-vast gives each worker as many worker ports as its process limit (`pids.max`) allows, usually 14.
Each Ray actor, task or replica needs a port.
Too many at the same time make the work hang, or fail after 5 worker-start retries, but they do not crash the worker.
Each worker also runs a Serve proxy. One of the 4 free ports is for it.
A replica has about 50 threads (read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts)). ray-vast sizes the worker ports for this, so B replicas stay under the process limit.

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
