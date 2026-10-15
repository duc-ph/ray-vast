# Getting started

This tutorial starts a small GPU cluster, runs one job on it, and removes it.
It takes about 20 minutes and costs less than $1.

## Prerequisites

You need these on your laptop (the operator machine):

- **Python 3.10 or later.** Ray 2.59 needs Python 3.10 or later.
- **`ssh` and `scp`.** ray-vast uses them to reach the head.
- **An AWS account and a profile.** The AWS user must be able to make an IAM role, security groups and an EC2 instance. The full list is in [IAM permissions](reference/iam.md).
- **An EC2 key pair** in the region of the head, and its private key file. ray-vast uses it for SSH to the head.
- **A vast.ai account with credit, and an API key.** Get the key in the vast console. Put it in `~/.config/vastai/vast_api_key`, or in the `VAST_API_KEY` environment variable. Never put the key in the cluster YAML.
- **Ray 2.59.** Step 1 installs it on the laptop.

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

## 2. Write a cluster YAML

The package has three examples in `ray_vast/examples/`: `gpu-24gb.yaml`, `gpu-80gb.yaml` and `cpu-heavy.yaml`.
Copy one:

```bash
cp "$(python -c 'import ray_vast, os; print(os.path.dirname(ray_vast.__file__))')/examples/gpu-24gb.yaml" cluster.yaml
```

Edit `cluster.yaml`. Set your region and your budget. Add the `key_name` and `key_file` lines for your EC2 key pair:

```yaml
cluster_name: gpu-24gb
max_workers: 4
idle_timeout_minutes: 5
provider:
  region: us-west-2                       # AWS region of the head
  image: rayproject/ray:2.59.0-py311-gpu  # image for the head and the workers
  max_dollars_per_hour: 3.0               # limit for all workers together
  key_name: my-ec2-key                    # EC2 key pair name
  key_file: ~/my-ec2-key.pem              # its private key file
auth:
  ssh_user: ray
head_node_type: head
available_node_types:
  head:
    resources: {CPU: 0}
    node_config: {}
    max_workers: 0
  gpu24:
    resources: {CPU: 8, GPU: 1}
    min_workers: 0
    max_workers: 4
    node_config:
      disk_gb: 80
      offer_query:
        gpu_ram: {gte: 24000, lte: 32000}   # MB
        dlperf: {gte: 40}
        inet_down: {gte: 500}
```

The fields are in [Configuration](reference/configuration.md).
To choose other GPUs, read [Choose node types and offer queries](how-to/node-types.md).

## 3. Start the cluster

```bash
ray-vast up cluster.yaml --profile my-aws-profile
```

`--profile` is your AWS profile. Give it to every `ray-vast` command, or set `AWS_PROFILE` once.
The commands below do not show it.

The output looks like this:

```text
installing ray-vast==<version> from PyPI
head instance <instance id> starting
head IP <head ip>
cluster gpu-24gb is up. Head IP <head ip>.
  dashboard: ray-vast dashboard cluster.yaml   (then open http://127.0.0.1:8265)
  shell:     ray-vast attach cluster.yaml
```

`up` takes about 2.5 minutes with the CPU image (`rayproject/ray:2.59.0-py311-cpu`).
It takes about 8 to 10 minutes with the 18 GB GPU image, because the head pulls the image.
`up` makes no vast rental. Workers start only when a job needs them.

If `up` stops with an error, run `ray-vast down cluster.yaml` and read [Clean up after a failure](how-to/clean-up.md).

## 4. Open the dashboard

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

## 5. Submit a job

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

The task needs a GPU, and the head has none. So the autoscaler rents a worker. This takes from less than 1 minute to several minutes. Read [Worker life cycle and the watchdog](explanation/worker-lifecycle.md).
The job then prints the host name and the GPU ID.

To see the cluster state, open a shell on the head:

```bash
ray-vast attach cluster.yaml
sudo docker exec ray-vast-head ray status
```

## 6. Watch the cost

- `max_dollars_per_hour` is a hard limit. The provider does not rent above it.
- The vast console shows each instance and its price. The instances have the label `ray-vast:<cluster_name>`.
- The EC2 head costs about $0.08 per hour (`t3.large`). It runs until you run `ray-vast down`.
- After the job, the worker is idle. Ray removes it after `idle_timeout_minutes` (5 in the example).

Read [Control cost](how-to/control-cost.md) for more.

## 7. Remove the cluster

Stop the tunnel with Ctrl-C. The `dashboard` command prints `tunnel closed`. Then run:

```bash
ray-vast down cluster.yaml
```

The output ends with `left: nothing`:

```text
terminating head <instance id>
deleted security group <group id>
...
deleted instance profile ray-vast-gpu-24gb
deleted role ray-vast-gpu-24gb
left: nothing
```

`down` takes about 40 seconds. It removes all resources of the cluster. The full list is in [CLI](reference/cli.md#ray-vast-down).
If something is left, `down` lists it and exits with code 1. Run `down` again.

!!! warning
    Always run `ray-vast down` when you finish. The head costs money until you remove it.

## Next steps

- [Run your Ray workload](how-to/run-your-workload.md): `--working-dir`, `runtime_env` and the Ray libraries.
- [Choose node types and offer queries](how-to/node-types.md).
- [Security model](security.md): read it before you use real data.
