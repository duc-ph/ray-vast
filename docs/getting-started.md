# Getting started

This tutorial starts a small GPU cluster, runs one job on it, and removes it.
It takes about 20 minutes and costs less than $1.

## Prerequisites

You need these on your laptop (the operator machine). The CLI is tested on Linux. macOS is not tested.

- **Python 3.10 or later.** Ray 2.59 needs Python 3.10 or later.
- **`ssh` and `scp`.** ray-vast uses them to reach the head.
- **The AWS CLI, version 2.** Step 2 uses it. Install it from the [AWS CLI page](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html). ray-vast itself does not use the AWS CLI.
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

Steps 2.1 to 2.4 need an admin profile. Then you have two choices:

- **Use the admin profile for ray-vast.** This is the simple choice. Do steps 2.1 to 2.4. Then go to step 3.
- **Use a restricted user for ray-vast.** The user can do only what ray-vast needs. Do steps 2.1 to 2.4, and then step 2.5.

Steps 2.1 and 2.2 are one time on your laptop. Step 2.3 is one time in each region. Step 2.4 is one time in each AWS account.
In the commands, replace `my-aws-profile` with the name of your admin profile and `us-west-2` with the region of your head.

### 2.1 Make an AWS profile

ray-vast uses an AWS profile: a named set of credentials on your laptop.
If you have a profile already, go to step 2.2.

1. In the AWS console, open **IAM**, then **Users**. Create a user with the policy `AdministratorAccess`, or use an existing admin user.
2. Open the user, then **Security credentials**, then **Create access key**. Select **Command Line Interface (CLI)**.
3. Run this command. Type the access key ID and the secret access key. For the region, type the region of your head.

```bash
aws configure --profile my-aws-profile
```

### 2.2 Check the profile

```bash
aws sts get-caller-identity --profile my-aws-profile
```

The output shows your AWS account ID. If it shows an error, the profile is not correct.

### 2.3 Check the default VPC (one time in each region)

ray-vast puts the head in the default VPC of the region. New AWS accounts have a default VPC in each region.
Check it:

```bash
aws ec2 describe-vpcs --filters Name=is-default,Values=true \
    --query 'Vpcs[].VpcId' --region us-west-2 --profile my-aws-profile
```

If the output is `[]`, there is no default VPC. Create one:

```bash
aws ec2 create-default-vpc --region us-west-2 --profile my-aws-profile
```

### 2.4 Create the boundary policy (one time in each AWS account)

Run these two commands:

```bash
IAM_DIR="$(python -c 'import ray_vast, os; print(os.path.dirname(ray_vast.__file__))')/iam"
aws iam create-policy --policy-name ray-vast-head-boundary \
    --policy-document "file://$IAM_DIR/ray-vast-head-boundary.json" --profile my-aws-profile
```

The first command finds the policy file in the installed package. The second command creates the IAM policy `ray-vast-head-boundary` in your account.
If the output says `EntityAlreadyExists`, the policy exists already. That is correct.

Why the policy is necessary:

- The head is an EC2 instance. It adds the IPs of new workers to its security groups. For this, `up` gives the head an IAM role.
- The policy `ray-vast-head-boundary` is the permissions boundary of that role. The role can never do more than the boundary allows.
- The boundary allows only two things: read EC2, and change the rules of the ray-vast security groups.
- All clusters in the account use the same policy. So `up` does not create it, and `down` does not delete it.
- If the policy is missing, `up` stops before it makes anything.

This is the policy:

```json
--8<-- "src/ray_vast/iam/ray-vast-head-boundary.json"
```

To create the policy in the AWS console instead:

1. Open **IAM**, then **Policies**, then **Create policy**.
2. Select the **JSON** tab. Paste the policy above.
3. Select **Next**. Type the name `ray-vast-head-boundary`. Select **Create policy**.

### 2.5 Optional: make a restricted user { #use-a-restricted-iam-user }

Do this step only if you do not want to use the admin profile for ray-vast. Do steps 2.1 to 2.4 first.

The policy `ray-vast-user` gives the user these permissions:

- EC2: make, change and delete only resources with the tag `ray-vast-cluster`, and key pairs with names `ray-vast-*`.
- IAM: make and delete roles and instance profiles with names `ray-vast-*`.
- IAM: make a role or change its policy only with the boundary `ray-vast-head-boundary`.
- IAM: pass the roles `ray-vast-*` only to EC2.
- No `iam:CreatePolicy`.

So the user cannot make a role with more permissions than the boundary.
The user cannot create the boundary policy. A user that can create policies can also create an admin policy.

This is the policy:

```json
--8<-- "src/ray_vast/iam/ray-vast-user.json"
```

With the AWS CLI and the admin profile:

```bash
IAM_DIR="$(python -c 'import ray_vast, os; print(os.path.dirname(ray_vast.__file__))')/iam"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text --profile my-aws-profile)
aws iam create-policy --policy-name ray-vast-user \
    --policy-document "file://$IAM_DIR/ray-vast-user.json" --profile my-aws-profile
aws iam create-user --user-name ray-vast --profile my-aws-profile
aws iam attach-user-policy --user-name ray-vast \
    --policy-arn "arn:aws:iam::$ACCOUNT:policy/ray-vast-user" --profile my-aws-profile
aws iam create-access-key --user-name ray-vast --profile my-aws-profile
```

The last command prints the access key ID and the secret access key. Keep the secret safe.

In the AWS console:

1. Create the policy `ray-vast-user` as you created the boundary: **IAM**, **Policies**, **Create policy**, **JSON**. Paste the policy above.
2. Open **IAM**, then **Users**, then **Create user**. Type the name `ray-vast`. Do not give console access.
3. Select **Attach policies directly**. Select `ray-vast-user`. Create the user.
4. Open the user, then **Security credentials**, then **Create access key**. Select **Command Line Interface (CLI)**. Copy the access key ID and the secret access key.

Then make the profile `ray-vast` with this key, and check it:

```bash
aws configure --profile ray-vast
aws sts get-caller-identity --profile ray-vast
```

In step 5, use `--profile ray-vast` instead of the admin profile.

### What `up` makes in AWS

`ray-vast up` makes these resources in the region of the head. `ray-vast down` deletes them.

| Resource | What it is for |
|---|---|
| An EC2 instance, with its disk | The head. It runs the Ray head and the ray-vast provider. |
| 5 security groups | The firewall of the head. They let in SSH from your laptop's IP and the IPs of the workers. |
| An EC2 key pair | The SSH key of the head. `up` keeps the private key in `~/.ray-vast/<cluster_name>/ssh_key.pem`. |
| An IAM role and an instance profile | They let the provider on the head add and remove the worker IPs in the security groups. |

All of them have the name `ray-vast-<cluster_name>` and the tag `ray-vast-cluster=<cluster_name>`.
The full list of AWS actions is in [IAM permissions](reference/iam.md).

## 3. Save the vast API key

1. In the vast console, open **Account**, then **Keys**. Copy your API key, or create one.
2. Save the key in a file that only you can read:

```bash
mkdir -p ~/.config/vastai
(umask 077; cat > ~/.config/vastai/vast_api_key)   # paste the key, press Enter, then Ctrl-D
```

ray-vast can also read the key from the `VAST_API_KEY` environment variable. Never put the key in the cluster YAML.

The `vastai` CLI is optional. The clean-up pages use it to list and destroy instances. To install it, run `pip install vastai`.

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
