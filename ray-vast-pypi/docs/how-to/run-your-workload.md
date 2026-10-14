# Run your Ray workload

You use the normal Ray workflow. Only the cluster commands change.

## Commands: Ray and ray-vast

| Plain Ray | ray-vast |
|---|---|
| `ray up cluster.yaml` | `ray-vast up cluster.yaml` |
| `ray dashboard cluster.yaml` | `ray-vast dashboard cluster.yaml` |
| `ray down cluster.yaml` | `ray-vast down cluster.yaml` |
| `ray attach cluster.yaml` | `ray-vast attach cluster.yaml` (a shell on the EC2 host of the head, not in the Ray container) |
| `ray exec`, `ray rsync-up`, `ray rsync-down` | Not available. Use `--working-dir` and `runtime_env`. |
| `ray job submit`, `ray job list`, `ray job logs`, `ray job stop` | The same, through the dashboard tunnel. |

Other `ray` cluster commands do not work. Read [Limits](../reference/limits.md#some-ray-commands-do-not-work).

## Connect to the cluster

1. Start the tunnel in one terminal, and keep it open:

    ```bash
    ray-vast dashboard cluster.yaml
    ```

2. In a second terminal, run the `export` line that `dashboard` printed:

    ```bash
    export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=/home/you/.ray-vast/<cluster_name>/token
    ```

3. Optional: set the address once, so you do not need `--address` on each command:

    ```bash
    export RAY_ADDRESS=http://127.0.0.1:8265
    ```

Every Ray call needs the cluster token. Without the `export` line, `ray job submit` fails with an authentication error.

Use the Job API: `ray job submit` or `JobSubmissionClient`.
Do not use `ray.init("ray://...")` on the laptop. ray-vast does not open the Ray client port (10001) to your laptop.

## Send your code with `--working-dir`

```bash
ray job submit --working-dir . -- python train.py --epochs 3
```

Ray uploads the directory to the head. Each worker downloads it before it runs a task of the job.
Keep the directory small. Ray limits the upload size. Put large data in cloud storage.

## Install packages with `runtime_env`

Install packages for one job:

```bash
ray job submit --working-dir . \
  --runtime-env-json '{"pip": ["torch", "transformers"]}' \
  -- python train.py
```

Or put the same settings in a file, and pass `--runtime-env runtime_env.yaml`.

Each worker installs the packages when it first runs a task of the job. A new worker installs them again.
For packages that every job needs, build them into the image. This is faster.

Use the image's Python and Ray version. Do not install another `ray` version with `pip`.

## Pass credentials to the job

Put credentials in `runtime_env` `env_vars`:

```bash
ray job submit --working-dir . \
  --runtime-env-json '{"env_vars": {"AWS_ACCESS_KEY_ID": "...", "AWS_SECRET_ACCESS_KEY": "..."}}' \
  -- python train.py
```

!!! warning
    The credentials go to every worker that runs the job. The vast host can read them.
    Use credentials with minimal rights, for example an IAM user that can only read and write one S3 prefix.
    Read [Security model](../security.md).

## Save your results

A worker's disk is lost when the worker ends. Ray removes idle workers, and a worker destroys itself when it loses the head.
So write results, checkpoints and logs to cloud storage (for example S3), not to the local disk of a worker.

## Ask for GPUs

Ask for GPUs in your tasks and actors, as usual:

```python
@ray.remote(num_gpus=1)
def infer(batch):
    ...
```

The autoscaler then rents a worker of a node type that has a GPU.
The head has no CPU and no GPU for tasks (`resources: {CPU: 0}`). So every task with resources runs on a vast worker.

## Ray libraries

Ray Data, Tune, Train and Serve use the same workflow. Follow Ray's own guides: [Ray Data](https://docs.ray.io/en/latest/data/data.html), [Ray Tune](https://docs.ray.io/en/latest/tune/index.html), [Ray Train](https://docs.ray.io/en/latest/train/train.html), [Ray Serve](https://docs.ray.io/en/latest/serve/index.html).
The test results are in [Ray library support](../reference/ray-libraries.md).
The sections below show only what is different on ray-vast.

### Ray Data: limit the CPUs

A vast container has a limit on processes and threads. A Ray Data job that uses all CPUs of a worker can pass this limit. Then the worker dies. Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts).

Limit the CPUs that Ray Data uses. Put this code in your driver script, after `ray.init()`:

```python
from ray.data import DataContext, ExecutionResources

data_context = DataContext.get_current()
data_context.execution_options.resource_limits = ExecutionResources(cpu=12)
data_context.max_hash_shuffle_aggregators = 4  # for hash shuffles, for example groupby
```

The limit is for the full cluster. In a live test on 2026-10-14, a limit of 12 CPUs worked on 2 to 3 workers.
Each hash shuffle aggregator is one more worker process. So also limit `max_hash_shuffle_aggregators`.

A local output path (for example `write_parquet("/tmp/out")`) writes the files to the disk of each worker. Write shared output to a cloud bucket.

### Ray Tune and Ray Train: checkpoints

Ray 2.59 refuses a local `storage_path` on a cluster with more than one node. Set `storage_path` to a cloud bucket:

```python
import ray.tune

run_config = ray.tune.RunConfig(storage_path="s3://my-bucket/experiments")
```

Use `ray.train.RunConfig` for Ray Train in the same way.

The workers and the driver (it runs on the head) need write access to the bucket. Give them credentials in `runtime_env` `env_vars`. Read [Pass credentials to the job](#pass-credentials-to-the-job).
The vast host can read these credentials. Use credentials that can only write to this bucket prefix.

Anonymous S3 access through pyarrow (`s3://anonymous@...`) cannot write checkpoints. S3 refuses its uploads.

### Ray Train: use one worker

Ray Train over 2 or more workers does not work on ray-vast. Read [Limits](../reference/limits.md#no-multi-node-ray-train).

Train on one worker with many GPUs. Make a node type with `GPU: 2` or more. Read [Choose node types and offer queries](node-types.md). Then put all Ray Train workers (`num_workers`) on that one vast worker:

```python
from ray.train import ScalingConfig

scaling_config = ScalingConfig(num_workers=2, use_gpu=True, placement_strategy="STRICT_PACK")
```

### Ray Serve: HTTP access

Serve listens for HTTP on port 8000 of the head. The security groups do not open this port to your machine.
Open an SSH tunnel from your machine to the head:

```bash
ssh -i <key file> -N -L 18000:127.0.0.1:8000 ubuntu@<head ip>
```

Use the same key file as `ray-vast attach`. `ray-vast up` prints the head IP.
Then send requests to `http://127.0.0.1:18000/`. Press Ctrl-C to close the tunnel.

`DeploymentHandle` calls from a job need no tunnel.

## Check the cluster

Open a shell on the head, and run Ray commands in the head container:

```bash
ray-vast attach cluster.yaml
sudo docker exec ray-vast-head ray status
```

The dashboard at http://127.0.0.1:8265 shows the nodes, the jobs and the logs.
