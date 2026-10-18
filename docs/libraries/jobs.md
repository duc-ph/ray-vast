# Submit jobs

You send work to a ray-vast cluster with Ray Jobs. Every library page uses the steps on this page.

## Status

- `ray job submit`, `list`, `logs --follow` and `stop` work through the `dashboard` tunnel.
- The driver of the job runs on the head. Its tasks run on all workers.

## Run it

Use an example cluster YAML, for example [`gpu-24gb.yaml`](https://github.com/duc-ph/ray-vast/blob/main/src/ray_vast/examples/gpu-24gb.yaml). Set the region of the head (`node_config.region` of the head node type). Read [Getting started](../getting-started.md#4-write-a-cluster-yaml).

Terminal 1: start the cluster and the tunnel. Keep the tunnel open.

```bash
ray-vast up cluster.yaml --profile my-aws-profile
ray-vast dashboard cluster.yaml --profile my-aws-profile
```

Terminal 2: run the `export` line that `dashboard` printed. Then submit the job.

```bash
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=~/.ray-vast/<cluster_name>/token
export RAY_ADDRESS=http://127.0.0.1:8265
ray job submit --working-dir . -- python my_job.py
ray job list
ray job logs <job id>
ray job stop <job id>
```

When you finish:

```bash
ray-vast down cluster.yaml --profile my-aws-profile
```

Every Ray call needs the cluster token. Without the `export` line, `ray job submit` fails with an authentication error.

### Commands: Ray and ray-vast

| Plain Ray | ray-vast |
|---|---|
| `ray up cluster.yaml` | `ray-vast up cluster.yaml` |
| `ray dashboard cluster.yaml` | `ray-vast dashboard cluster.yaml` |
| `ray down cluster.yaml` | `ray-vast down cluster.yaml` |
| `ray attach cluster.yaml` | `ray-vast attach cluster.yaml` (a shell on the head EC2 instance, not in the Ray container) |
| `ray exec`, `ray rsync-up`, `ray rsync-down` | Not available. Use `--working-dir` and `runtime_env`. |
| `ray job submit`, `list`, `logs`, `stop` | The same, through the dashboard tunnel. |

### Send your code with `--working-dir`

Ray uploads the directory to the head. Each worker downloads it before it runs a task of the job.
Keep the directory small. Ray limits the upload size. Put large data in cloud storage.

### Install packages with `runtime_env`

```bash
ray job submit --working-dir . \
  --runtime-env-json '{"pip": ["torch==2.7.1"]}' \
  -- python my_job.py
```

Or put the same settings in a file, and pass `--runtime-env runtime_env.yaml`.
The driver runs on the head, so pip installs the packages on the head too.
Each worker installs the packages when it first runs a task of the job. A new worker installs them again. The first torch install took about 4–5 minutes.
For packages that every job needs, build them into the image. Read [Your own image](../how-to/node-types.md#your-own-image).
Do not install another `ray` version with `pip`.

### Pass credentials to the job

Put credentials in `runtime_env` `env_vars`:

```bash
ray job submit --working-dir . \
  --runtime-env-json '{"env_vars": {"AWS_ACCESS_KEY_ID": "...", "AWS_SECRET_ACCESS_KEY": "..."}}' \
  -- python my_job.py
```

!!! warning
    The credentials go to every worker that runs the job. The vast host can read them.
    Use credentials with the minimum permissions, for example an IAM user that can only read and write one S3 prefix.
    Read [Security model](../security.md).

### Ask for GPUs

Ask for GPUs in your tasks and actors, as usual (`@ray.remote(num_gpus=1)`).
The autoscaler then rents a worker of a node type that has a GPU.
The head has `CPU: 0`. So every task and actor with resources runs on a vast worker.

### Check the cluster

```bash
ray-vast attach cluster.yaml --profile my-aws-profile
sudo docker exec ray-vast-head ray status
```

The dashboard at http://127.0.0.1:8265 shows the nodes, the jobs and the logs.

### How many tasks run at once

The driver runs on the head. It uses no process of a vast worker. So the job itself needs no settings.
The tasks and actors of the job run on the workers. Each worker runs a limited number of them at the same time. Read [Many tasks or actors](many-tasks.md).

## What does not work

| What | Do this instead |
|---|---|
| `ray.init("ray://...")` from your laptop. ray-vast does not open the Ray client port (10001). | Use `ray job submit` or `JobSubmissionClient`. |
| `ray up`, `ray attach`, `ray exec`, `ray rsync-*`. | Use the `ray-vast` commands, `--working-dir` and `runtime_env`. |
| Results on a worker's disk. Ray removes idle workers, and a worker destroys itself when it loses the head. | Write results, checkpoints and logs to cloud storage (for example S3). |
| A local output path (for example `/tmp/out`). The files stay on each worker. | Write shared output to a cloud bucket. |
| A library that opens its own connections between workers on other ports. Only the identity ports of each worker (at most 64) are open. | Keep that work on one worker. |

Ray traffic between workers goes over the public internet. Large objects moved at 3 to 30 MB/s between hosts in our tests. The rate depends on the hosts.

## If it fails

1. **The head's `monitor.log`.** Look for the `is at its process limit` WARNING. It names the worker and shows the worker ports in use.

    ```bash
    ray-vast attach cluster.yaml --profile my-aws-profile
    # in the shell on the head:
    sudo docker exec ray-vast-head grep "process limit" /tmp/ray/session_latest/logs/monitor.log
    ```

2. **The job log.**

    ```bash
    ray job logs <job id>
    ```

3. **The worker logs.** Open the dashboard (http://127.0.0.1:8265) and the **Logs** tab. Choose the worker. Look for `No available ports` in its `worker-*.err` files.

Read [Find logs and troubleshoot](../how-to/troubleshoot.md) for other errors.
