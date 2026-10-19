# Getting started

Start a small GPU cluster, run one job, and remove the cluster. About 20 minutes, less than $1.

## Prerequisites

- Python 3.10 or later
- `ssh` and `scp`
- An AWS account
- A vast.ai account with credit

## 1. Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install ray-vast "ray[default]==2.59.*"
```

Keep the Ray pin: Ray on your laptop must match Ray in the cluster image.

## 2. Set up AWS { #aws-setup }

1. In the AWS console, create an access key for your user: **IAM**, **Users**, your user, **Security credentials**, **Create access key**, **Command Line Interface (CLI)**.
2. Put the key in the file `.env`, in the directory where you run ray-vast:

    ```bash
    AWS_ACCESS_KEY_ID=<your access key ID>
    AWS_SECRET_ACCESS_KEY=<your secret access key>
    ```

The user needs `AdministratorAccess`, or the restricted policy in [AWS permissions](reference/iam.md#a-restricted-user).

## 3. Save the vast API key

1. In the vast console, open **Account**, then **Keys**. Copy your API key, or create one.
2. Add the key to `.env`:

    ```bash
    VAST_API_KEY=<your key>
    ```

## 4. Write a cluster YAML

Copy an example:

```bash
cp "$(python -c 'import ray_vast, os; print(os.path.dirname(ray_vast.__file__))')/examples/gpu-24gb.yaml" cluster.yaml
```

Edit `cluster.yaml`. Set the region of the head and your budget:

```yaml
--8<-- "src/ray_vast/examples/gpu-24gb.yaml"
```

The fields are in [Configuration](reference/configuration.md). Other examples: `gpu-80gb.yaml` and `cpu-heavy.yaml`.

## 5. Start the cluster

```bash
ray-vast up cluster.yaml
```

The output looks like this:

```text
installing ray-vast==<version> from PyPI
head instance <instance id> starting
head IP <head ip>
cluster gpu-24gb is up. Head IP <head ip>.
  dashboard: ray-vast dashboard cluster.yaml   (then open http://127.0.0.1:8265)
  shell:     ray-vast attach cluster.yaml
```

`up` takes about 3 to 10 minutes. It rents no vast worker yet.
If `up` fails, run `ray-vast down cluster.yaml`.

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

Keep this terminal open. The dashboard is at http://127.0.0.1:8265.

## 7. Submit a job

In a second terminal, activate the venv and run the `export` line that `dashboard` printed:

```bash
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=/home/you/.ray-vast/gpu-24gb/token
```

Write `hello.py`:

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

The autoscaler rents a GPU worker on vast.ai. This takes 1 to several minutes. Then the job prints the host name and the GPU ID.

## 8. Remove the cluster

Stop the tunnel with Ctrl-C. Then run:

```bash
ray-vast down cluster.yaml
```

The output ends with `left: nothing`. If something is left, run `down` again.

## Next steps

- [Submit jobs](libraries/jobs.md)
- [Ray libraries](libraries/index.md)
- [Choose node types and GPU images](how-to/node-types.md)
- [Security model](security.md)
