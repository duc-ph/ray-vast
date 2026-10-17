# Ray Jobs

Ray Jobs is how you send work to a ray-vast cluster. Every other library page uses it.
This page also has the rules that are true for all libraries.

## Status

- **One worker ✅:** `ray job submit`, `list`, `logs --follow` and `stop` work through the `dashboard` tunnel.
- **Many workers ✅:** The job's driver runs on the head, and its tasks run on all workers.

## Run it

Use an example cluster YAML, for example [`gpu-24gb.yaml`](https://github.com/duc-ph/ray-vast/blob/main/ray-vast-pypi/src/ray_vast/examples/gpu-24gb.yaml). Set the region of the head (`node_config.region` of the head node type). Read [Getting started](../getting-started.md#3-write-a-cluster-yaml).

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
For packages that every job needs, build them into the image. Read [Use GPU images](../how-to/gpu-images.md).
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

## Settings

The job itself needs no settings. The driver runs on the head and uses no worker port of a vast worker.

The work in the job can need settings. They use **B**, the number of worker processes that your work can use on one worker:

- B = worker ports − 4. The 4 ports stay free for the processes that Ray's libraries start next to your work (the Serve proxy, the Data and Train helpers).
- The rule for every library: each process that your code starts (a task, an actor, a replica, a trial, a Train worker or an env runner) asks for `num_cpus` = CPUS / B. CPUS is the number of Ray CPUs of one worker. Then Ray puts at most B of them on one worker. The library settings come on top of this rule.
- The start line of each worker shows its port count, for example `worker ports 18`. Read it in the vast console (the instance log), or with `vastai logs <instance id>`.
- B depends on the host: 2 on a host with 4 effective cores, 54 from 17 effective cores up (58 worker ports, the maximum). Each library page has the table.
- Rule 1: with Ray's defaults (about one worker process for each CPU), you need the settings only when your node type has more CPUs than B, or when you start many processes with `num_cpus` below 1.
- Rule 2: keep each process under about 60 threads, and keep its child processes few and small.

Each library page gives the two rules and its settings, with example values for B = 10.

## Why

ray-vast gives each worker as many worker ports as its process limit (`pids.max`) allows: 4 to 58. About half of the hosts give 58.
Each Ray actor, task or replica needs a port.
Too many at the same time make the work hang, or fail after 5 worker-start retries, but they do not crash the worker.
The driver of a job runs on the head, so a job needs no setting.
Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts).

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
