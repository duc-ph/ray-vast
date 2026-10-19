# Getting started

This tutorial starts a small GPU cluster, runs one job on it, and removes it.
It takes about 20 minutes and costs less than $1.

## Prerequisites

You need these on your laptop (the operator machine). The CLI is tested on Linux. macOS is not tested.

- **Python 3.10 or later.** Ray 2.59 needs Python 3.10 or later.
- **`ssh` and `scp`.** ray-vast uses them to reach the head.
- **The AWS CLI, version 2.** Step 2 uses it to save your credentials. Install it from the [AWS CLI page](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
- **An AWS account.**
- **A vast.ai account with credit.**

You do not need an EC2 key pair. `ray-vast up` makes one SSH key for each cluster.

## 1. Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install ray-vast "ray[default]==2.59.*"
```

This installs the `ray-vast` command and the `ray` command.
The pin `ray[default]==2.59.*` keeps Ray on the laptop at the same version as Ray in the cluster image (`rayproject/ray:2.59.0-...`).
Without the pin, `pip` installs the newest Ray. ray-vast is tested with Ray 2.59 only.
`ray-vast up` installs the same ray-vast version on the head, from PyPI.

## 2. Set up AWS { #aws-setup }

ray-vast uses an AWS profile. The user of the profile needs permission to:

- start and stop an EC2 instance (the head), and make its key pair and security groups;
- make an IAM role and an instance profile for the head, and give them to EC2;
- create one IAM policy, `ray-vast-head-boundary`, at the first `up` in the account.

The policy `AdministratorAccess` gives all of this. To give only these permissions, read [A restricted user](reference/iam.md#a-restricted-user).
If you have a profile already, go to item 3.

1. In the AWS console, create an access key for your user: **IAM**, **Users**, your user, **Security credentials**, **Create access key**, **Command Line Interface (CLI)**.
2. Save the key in a profile. Type the access key ID, the secret access key, and the region of your head:

    ```bash
    aws configure --profile my-aws-profile
    ```

3. Check the profile. The output shows your AWS account ID:

    ```bash
    aws sts get-caller-identity --profile my-aws-profile
    ```

The first `up` in your account creates the IAM policy `ray-vast-head-boundary`. It limits what the head can do in AWS.
What `up` makes in AWS is in [AWS permissions](reference/iam.md).

## 3. Save the vast API key

1. In the vast console, open **Account**, then **Keys**. Copy your API key, or create one.
2. Put the key in the file `.env`, in the directory where you run ray-vast:

```bash
VAST_API_KEY=<your key>
```

Never put the key in the cluster YAML.

## 4. Write a cluster YAML

The package has three examples in `ray_vast/examples/`: `gpu-24gb.yaml`, `gpu-80gb.yaml` and `cpu-heavy.yaml`.
Each example lists all fields. A field in a comment keeps its default.
Copy one:

```bash
cp "$(python -c 'import ray_vast, os; print(os.path.dirname(ray_vast.__file__))')/examples/gpu-24gb.yaml" cluster.yaml
```

Edit `cluster.yaml`. Set the region of the head and your budget:

```yaml
--8<-- "src/ray_vast/examples/gpu-24gb.yaml"
```

The fields are in [Configuration](reference/configuration.md).
To choose other GPUs, read [Choose node types and GPU images](how-to/node-types.md).

## 5. Start the cluster

```bash
ray-vast up cluster.yaml --profile my-aws-profile
```

`--profile` is your AWS profile. Without it, `up` uses `AWS_PROFILE` or the default credentials.
`up` saves the profile. `dashboard`, `attach` and `down` use the saved profile, so the commands below do not show it. An explicit `--profile` or `AWS_PROFILE` wins over the saved profile.

The output looks like this:

```text
installing ray-vast==<version> from PyPI
head instance <instance id> starting
head IP <head ip>
cluster gpu-24gb is up. Head IP <head ip>.
  dashboard: ray-vast dashboard cluster.yaml   (then open http://127.0.0.1:8265)
  shell:     ray-vast attach cluster.yaml
```

`up` took 2.5 to 6.5 minutes with the CPU image (`rayproject/ray:2.59.0-py311-cpu`) in our tests.
It takes about 8 to 10 minutes with the 18 GB GPU image, because the head pulls the image.
`up` makes no vast rental. Workers start only when a job needs them.

If `up` stops with an error, run `ray-vast down cluster.yaml` and read [Clean up after a failure](how-to/clean-up.md).

## 6. Open the dashboard

```bash
ray-vast dashboard cluster.yaml
```

The output looks like this:

```text
for `ray job submit` set:
  export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=/home/you/.ray-vast/gpu-24gb/token
dashboard at http://127.0.0.1:8265 (Ctrl-C to close the tunnel); use --address http://127.0.0.1:8265
```

The command copies the cluster token to `~/.ray-vast/<cluster_name>/token` (mode 600).
Then it opens an SSH tunnel to port 8265 of the head.
Keep this terminal open. Open http://127.0.0.1:8265 in your browser to see the Ray dashboard.

## 7. Submit a job

Open a second terminal. Activate the venv, and run the `export` line that `dashboard` printed:

```bash
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=/home/you/.ray-vast/gpu-24gb/token
```

Write a small job, `hello.py`.
This job needs a GPU worker. The autoscaler rents one on vast.ai, so this step costs money.
The cluster YAML must allow at least 1 worker of a GPU type (`max_workers` 1 or more).

```python
import ray

ray.init()

@ray.remote(num_gpus=1)
def where():
    import socket
    return socket.gethostname(), ray.get_gpu_ids()

print(ray.get(where.remote()))
print(ray.cluster_resources())
```

Submit it:

```bash
ray job submit --address http://127.0.0.1:8265 --working-dir . -- python hello.py
```

The task needs a GPU, and the head has none. So the autoscaler rents a worker. This takes from less than 1 minute to several minutes. Read [Worker life cycle and the watchdog](explanation/architecture.md#worker-life-cycle-and-the-watchdog).
The job then prints the host name and the GPU ID.

To see the cluster state, open a shell on the head:

```bash
ray-vast attach cluster.yaml
sudo docker exec ray-vast-head ray status
```

## 8. Watch the cost

- `max_dollars_per_hour` is a hard limit. The provider does not rent above it.
- The vast console shows each instance and its price. The instances have the label `ray-vast:<cluster_name>`.
- The EC2 head costs about $0.08 per hour (`t3.large`). It runs until you run `ray-vast down`.
- After the job, the worker is idle. Ray removes it after `idle_timeout_minutes` (5 in the example).

Read [Control cost](how-to/control-cost.md) for more.

## 9. Remove the cluster

Stop the tunnel with Ctrl-C. The `dashboard` command prints `tunnel closed`. Then run:

```bash
ray-vast down cluster.yaml
```

The output ends with `left: nothing`:

```text
terminating head <instance id>
deleted security group <group id>
...
deleted key pair ray-vast-gpu-24gb
deleted instance profile ray-vast-gpu-24gb
deleted role ray-vast-gpu-24gb
left: nothing
```

`down` takes about 40 seconds. It removes all resources of the cluster. The full list is in [CLI](reference/cli.md#ray-vast-down).
If something is left, `down` lists it and exits with code 1. Run `down` again.

!!! warning
    Always run `ray-vast down` when you finish. The head costs money until you remove it.

## Next steps

- [Submit jobs](libraries/jobs.md): `--working-dir`, `runtime_env` and credentials.
- [Ray libraries](libraries/index.md): what works, and the settings for each Ray library.
- [Choose node types and GPU images](how-to/node-types.md).
- [Security model](security.md): read it before you use real data.
