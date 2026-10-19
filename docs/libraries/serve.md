# Ray Serve

Model serving with replicas, handles and HTTP.

## Status

- **One worker ✅:** Replicas, GPU replicas and `DeploymentHandle` calls work.
- **Many workers ✅:** Replicas run on many workers. HTTP works on port 8000 of the head through an SSH tunnel.

## Run it

Deploy the app with a job. Read [Submit jobs](jobs.md). The script calls `serve.run(app, route_prefix="/")`. The app stays up after the job ends.

```bash
ray job submit --working-dir . -- python serve_app.py
```

To call the app over HTTP, open an SSH tunnel to port 8000 of the head in another terminal. Use the SSH key that `up` made: `~/.ray-vast/<cluster_name>/ssh_key.pem`. `ray-vast up` prints the head IP.

```bash
ssh -i ~/.ray-vast/<cluster_name>/ssh_key.pem -o UserKnownHostsFile=~/.ray-vast/<cluster_name>.known_hosts -N -L 18000:127.0.0.1:8000 ubuntu@<head ip>
curl http://127.0.0.1:18000/
```

`DeploymentHandle` calls from a job need no tunnel.

## What is different on vast

- The security groups of the head do not open port 8000. Use the SSH tunnel above.
- Each worker runs at most *N* Ray processes at the same time. Each replica is one process. Read [Many tasks or actors](many-tasks.md).

## When you need the settings

Use the [settings](#settings) only in these cases:

- Your replicas ask for less than 1 CPU (`ray_actor_options={"num_cpus": ...}`).
- `num_replicas` on one worker is more than *N*.

## Settings

`CPU` is the `CPU` of the node type in your cluster YAML. *N* is the number of processes at once of the worker. Read it in the [start line](many-tasks.md#the-number-of-processes-at-once).

The values below are an example for *N* = 10 and `CPU` = 24, on one worker. Use the values of your workers.

```python
from ray import serve

@serve.deployment(
    num_replicas=10,                        # at most N for each worker
    ray_actor_options={"num_cpus": 2.4},    # CPU / N
)
class Model:
    def __call__(self, request):
        ...

serve.run(Model.bind(), route_prefix="/")
```

| Setting | Value (*N* = 10, `CPU` = 24) | Formula |
|---|---|---|
| `ray_actor_options={"num_cpus": ...}` | 2.4 | `CPU` / *N* |
| `num_replicas` (or `max_replicas` of `autoscaling_config`) | 10 for each worker | *N* × workers |

- With `num_cpus` = `CPU` / *N*, Ray puts at most *N* replicas on one worker. So you do not need `max_replicas_per_node`.
- Each worker also runs a Serve proxy. *N* already leaves room for it.
- We tested 10 replicas on a worker with *N* = 10. All replicas started and answered.

## Threads

- A replica with a FastAPI ingress has 50 threads. The threads of your model come on top. ray-vast leaves room for this, so *N* replicas stay under the process limit.

Read [Threads](many-tasks.md#threads).

## What does not work

| What | Do this instead |
|---|---|
| Many replicas on one worker. In a test, 60 replicas on one worker with 14 worker ports made `serve.run` hang. | Set `num_cpus` = `CPU` / *N* on each replica. |
| HTTP from your laptop to the head without a tunnel. The security groups do not open port 8000. | Use the SSH tunnel above. |

## If it fails

1. **The head's `monitor.log`.** Look for the `is at its process limit` WARNING. It names the worker and shows the worker ports in use.

    ```bash
    ray-vast attach cluster.yaml
    # in the shell on the head:
    sudo docker exec ray-vast-head grep "process limit" /tmp/ray/session_latest/logs/monitor.log
    ```

2. **The job log.** `serve.run` does not return when some replicas do not start.

    ```bash
    ray job logs <job id>
    ```

3. **The worker logs.** Open the dashboard (http://127.0.0.1:8265) and the **Logs** tab. Choose the worker. Look for `No available ports` in its `worker-*.err` files.
