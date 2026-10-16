# Getting started

This tutorial starts a small GPU cluster, runs one job on it, and removes it.
It takes about 20 minutes and costs less than $1.

## Prerequisites

You need these on your laptop (the operator machine):

- **Python 3.10 or later.** Ray 2.59 needs Python 3.10 or later.
- **`ssh` and `scp`.** ray-vast uses them to reach the head.
- **An AWS account and a profile.** You can use your own AWS credentials as they are, for example an admin profile. Step 2 tells what ray-vast does in AWS.
- **A vast.ai account with credit, and an API key.** Get the key in the vast console. Put it in `~/.config/vastai/vast_api_key`, or in the `VAST_API_KEY` environment variable. Never put the key in the cluster YAML.
- **Ray 2.59.** Step 1 installs it on the laptop.

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

### What ray-vast does in AWS

`ray-vast up` makes these resources in the region of the head. `ray-vast down` deletes them.

| Resource | What it is for |
|---|---|
| An EC2 instance, with its disk | The head. It runs the Ray head and the ray-vast provider. |
| 5 security groups | The firewall of the head. They let in your SSH and the IPs of the workers. |
| An EC2 key pair | The SSH key of the head. `up` keeps the private key in `~/.ray-vast/<cluster_name>/ssh_key.pem`. |
| An IAM role and an instance profile | They let the provider on the head add and remove the worker IPs in the security groups. |

All of them have the name `ray-vast-<cluster_name>` and the tag `ray-vast-cluster=<cluster_name>`.
The full list of AWS actions is in [IAM permissions](reference/iam.md).

### Create the boundary policy (one time, for every user)

The head role has a permissions boundary: the IAM policy `ray-vast-head-boundary`.
The boundary is a limit. The head role can never do more than the boundary allows, even if its own policy changes.
The boundary allows only what the head needs: read EC2, and change the rules of the ray-vast security groups.

Create the policy once in your AWS account. `ray-vast up` does not create it. If it is missing, `up` stops before it makes anything, with a link to this section.

This is the policy:

```json
--8<-- "src/ray_vast/iam/ray-vast-head-boundary.json"
```

With the AWS CLI (the file is in the package):

```bash
IAM_DIR="$(python -c 'import ray_vast, os; print(os.path.dirname(ray_vast.__file__))')/iam"
aws iam create-policy --policy-name ray-vast-head-boundary \
    --policy-document "file://$IAM_DIR/ray-vast-head-boundary.json" --profile my-aws-profile
```

Or in the AWS console:

1. Open **IAM**, then **Policies**, then **Create policy**.
2. Select the **JSON** tab. Paste the policy above.
3. Select **Next**. Type the name `ray-vast-head-boundary`. Select **Create policy**.

??? note "Optional: use a restricted IAM user for ray-vast"

    If you use your own admin credentials, skip this section.

    A restricted user can do only what ray-vast needs. The policy `ray-vast-user` gives these permissions:

    - EC2: make, change and delete only resources with the tag `ray-vast-cluster`, and key pairs with names `ray-vast-*`.
    - IAM: make and delete roles and instance profiles with names `ray-vast-*`.
    - IAM: make a role or change its policy only with the boundary `ray-vast-head-boundary`.
    - IAM: pass the roles `ray-vast-*` only to EC2.
    - No `iam:CreatePolicy`.

    So the user cannot make a role with more permissions than the boundary.
    For the same reason, ray-vast cannot create the boundary for a restricted user. A key that can create the boundary policy can also create an admin policy. So an admin creates the boundary once (above), and then the restricted user.

    This is the policy:

    ```json
    --8<-- "src/ray_vast/iam/ray-vast-user.json"
    ```

    Do these steps as an admin.

    With the AWS CLI:

    ```bash
    IAM_DIR="$(python -c 'import ray_vast, os; print(os.path.dirname(ray_vast.__file__))')/iam"
    ACCOUNT=$(aws sts get-caller-identity --query Account --output text --profile my-admin-profile)
    aws iam create-policy --policy-name ray-vast-user \
        --policy-document "file://$IAM_DIR/ray-vast-user.json" --profile my-admin-profile
    aws iam create-user --user-name ray-vast --profile my-admin-profile
    aws iam attach-user-policy --user-name ray-vast \
        --policy-arn "arn:aws:iam::$ACCOUNT:policy/ray-vast-user" --profile my-admin-profile
    aws iam create-access-key --user-name ray-vast --profile my-admin-profile
    ```

    The last command prints the access key ID and the secret access key. Keep the secret safe.

    In the AWS console:

    1. Create the policy `ray-vast-user` as you created the boundary: **IAM**, **Policies**, **Create policy**, **JSON**. Paste the policy above.
    2. Open **IAM**, then **Users**, then **Create user**. Type the name `ray-vast`. Do not give console access.
    3. Select **Attach policies directly**. Select `ray-vast-user`. Create the user.
    4. Open the user, then **Security credentials**, then **Create access key**. Select **Command Line Interface (CLI)**. Copy the access key ID and the secret access key.

    Then add a named profile `ray-vast` to `~/.aws/credentials`:

    ```ini
    [ray-vast]
    aws_access_key_id = <access key id>
    aws_secret_access_key = <secret access key>
    ```

    Use the profile with `export AWS_PROFILE=ray-vast`, or with `--profile ray-vast` on `ray-vast up`.

## 3. Write a cluster YAML

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
To choose other GPUs, read [Choose node types and offer queries](how-to/node-types.md).

## 4. Start the cluster

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

`up` takes about 2.5 minutes with the CPU image (`rayproject/ray:2.59.0-py311-cpu`).
It takes about 8 to 10 minutes with the 18 GB GPU image, because the head pulls the image.
`up` makes no vast rental. Workers start only when a job needs them.

If `up` stops with an error, run `ray-vast down cluster.yaml` and read [Clean up after a failure](how-to/clean-up.md).

## 5. Open the dashboard

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

## 6. Submit a job

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

## 7. Watch the cost

- `max_dollars_per_hour` is a hard limit. The provider does not rent above it.
- The vast console shows each instance and its price. The instances have the label `ray-vast:<cluster_name>`.
- The EC2 head costs about $0.08 per hour (`t3.large`). It runs until you run `ray-vast down`.
- After the job, the worker is idle. Ray removes it after `idle_timeout_minutes` (5 in the example).

Read [Control cost](how-to/control-cost.md) for more.

## 8. Remove the cluster

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

- [Ray Jobs](libraries/jobs.md): `--working-dir`, `runtime_env` and credentials. The other library pages give the settings for each Ray library.
- [Choose node types and offer queries](how-to/node-types.md).
- [Security model](security.md): read it before you use real data.
