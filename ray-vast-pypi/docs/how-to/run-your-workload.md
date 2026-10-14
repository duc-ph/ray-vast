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

`ray up`, `ray down`, `ray attach`, `ray exec` and `ray rsync-*` do not work with this provider.
They use the provider and SSH to find the head, and the ray-vast provider does not make the head.

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

Use the Job API (`ray job submit` or `JobSubmissionClient`). ray-vast does not open the Ray client port (10001) to your laptop, so `ray.init("ray://...")` from the laptop does not connect.

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

Ray Data, Tune, Train and Serve use the same workflow.
The test results and the steps that each library needs are in [Ray library support](../reference/ray-libraries.md).

## Check the cluster

Open a shell on the head, and run Ray commands in the head container:

```bash
ray-vast attach cluster.yaml
sudo docker exec ray-vast-head ray status
```

The dashboard at http://127.0.0.1:8265 shows the nodes, the jobs and the logs.
