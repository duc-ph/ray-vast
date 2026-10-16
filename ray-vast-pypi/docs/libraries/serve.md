# Ray Serve

Model serving with replicas, handles and HTTP.

## Status

- **One worker ✅:** Replicas, GPU replicas and `DeploymentHandle` calls work on one worker.
- **Many workers ✅:** Replicas run on many workers, and HTTP works on port 8000 of the head through an SSH tunnel.

## Run it

Use an example cluster YAML, for example [`gpu-24gb.yaml`](https://github.com/duc-ph/ray-vast/blob/main/ray-vast-pypi/src/ray_vast/examples/gpu-24gb.yaml). Add your EC2 key pair (`key_name`, `key_file`) to `provider`.

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

In terminal 3, open an SSH tunnel to port 8000 of the head. Use the key file of your cluster YAML (`key_file`). `ray-vast up` prints the head IP.

```bash
ssh -i ~/my-ec2-key.pem -N -L 18000:127.0.0.1:8000 ubuntu@<head ip>
curl http://127.0.0.1:18000/
```

`DeploymentHandle` calls from a job need no tunnel.

When you finish:

```bash
ray-vast down cluster.yaml --profile my-aws-profile
```

## Settings

B is the number of worker processes that your work can use on one worker. B = worker ports − 4.
The start line of each worker shows its port count, for example `worker ports 14` (in the vast console, or `vastai logs <instance id>`).
Hosts with `pids.max` 1280 give 14 worker ports, so B = 10. Most workers in our tests had 14. A host with a lower limit gives fewer ports, so read the start line. If you cannot read it, use B = 10.

The values below are for B = 10.

```python
from ray import serve

@serve.deployment(
    num_replicas=5,                    # at most B // 2 for each worker
    max_replicas_per_node=5,           # B // 2
    ray_actor_options={"num_cpus": 1},
)
class Model:
    def __call__(self, request):
        ...

serve.run(Model.bind(), route_prefix="/")
```

| Setting | Value (B = 10) | Formula |
|---|---|---|
| `num_replicas` (or `max_replicas` of `autoscaling_config`) | 5 for each worker | B // 2 × workers |
| `max_replicas_per_node` | 5 | B // 2 (not tested; it keeps the limit with many workers) |

## Why

ray-vast gives each worker as many worker ports as its process limit (`pids.max`) allows, usually 14.
Each Ray actor, task or replica needs a port.
Too many at the same time make the work hang, or fail after 5 worker-start retries, but they do not crash the worker.
Each worker also runs a Serve proxy, and a replica has about 50 threads, so B // 2 replicas on each worker keep the ports and the threads under the limit.
Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts).

## What does not work

| What | Do this instead |
|---|---|
| Many replicas on one worker. In a test, 60 replicas on one worker with 14 ports made `serve.run` hang. | Set `num_replicas` and `max_replicas_per_node` to at most B // 2 for each worker. |
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
