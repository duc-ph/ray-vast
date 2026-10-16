# CLI

```text
ray-vast {up,down,attach,dashboard} CLUSTER_YAML [options]
```

Every command takes the path of the cluster YAML. The YAML needs `cluster_name` and `node_config.region` of the head node type.
`ray-vast --help` and `ray-vast <command> --help` show the options.

## Commands

| Command | What it does |
|---|---|
| `up` | Makes the IAM role, the SSH key pair, the 5 security groups, the instance profile and the EC2 head. Builds a derived image on the head. Starts Ray. |
| `down` | Removes everything of the cluster. Prints `left: nothing` when done. |
| `attach` | Opens an SSH shell on the head (user `ubuntu`). |
| `dashboard` | Copies the cluster token to the laptop, prints the `export` line, and opens an SSH tunnel to port 8265 of the head. |

## Options

| Option | Commands | Meaning |
|---|---|---|
| `--profile NAME` | all | AWS profile. Default: `AWS_PROFILE`. Else, for `down`, `attach` and `dashboard`, the profile that `up` saved. Else the default credentials. |
| `--wheel FILE` | `up` | For development. Install this ray-vast wheel on the head. |

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success. |
| 1 | An error. For `down`: something is left. |

`attach` and `dashboard` return the exit code of `ssh`.

## Errors

- A user error prints one line on stderr: `ray-vast <command>: <message>`. The exit code is 1.
- User errors include a missing YAML file, bad YAML syntax and a bad YAML field value. The message names the file or the field.
- An AWS error adds more lines: a hint about missing permissions, and for `up`, the `ray-vast down` command.
- An unexpected error (a bug) prints a Python traceback. The last line of the traceback has the message.

## `ray-vast up`

```bash
ray-vast up cluster.yaml --profile my-aws-profile
```

`up` does these steps:

1. It reads and checks the YAML. It shows the warnings. It stops at once if the cluster already exists: an EC2 head with the cluster tag, or `~/.ray-vast/<cluster_name>.json`.
2. It makes the IAM role `ray-vast-<cluster_name>` with the permissions boundary `ray-vast-head-boundary`. If the boundary policy does not exist, `up` stops here and makes nothing. Read [Getting started](../getting-started.md#aws-setup).
3. It writes `~/.ray-vast/<cluster_name>.json` with the region and the AWS profile.
4. It makes the EC2 key pair `ray-vast-<cluster_name>` and saves the private key in `~/.ray-vast/<cluster_name>/ssh_key.pem` (mode 600).
5. It makes the 5 security groups `ray-vast-<cluster_name>-0` … `-4`, the role policy and the instance profile. Group 0 gets one SSH rule for the public IP of your laptop.
6. It starts the EC2 head: Ubuntu 24.04, the `instance_type` of the head node type, 60 GB gp3 disk, IMDSv2 required, no Elastic IP. User data installs Docker.
7. It builds a derived image on the head: `provider.image` plus ray-vast. Read [Where the head gets ray-vast](#where-the-head-gets-ray-vast). It reads the Ray version of the image, and warns if it is not 2.59.
8. It measures the image size. The size sets the pull limit of the workers.
9. It copies the vast key to the head. It makes the CA, the cluster token and the head certificate on the head. The CA key never leaves the head.
10. It starts the head container `ray-vast-head` and waits until `ray status` answers.

Output:

```text
installing ray-vast==<version> from PyPI
head instance <instance id> starting
head IP <head ip>
cluster <cluster_name> is up. Head IP <head ip>.
  dashboard: ray-vast dashboard cluster.yaml   (then open http://127.0.0.1:8265)
  shell:     ray-vast attach cluster.yaml
```

`up` adds lines that start with `warning:` when they apply. They come in this order:

1. After the `installing` line: the warnings of the YAML checks (`CPU` above 58, `allow_non_datacenter`).
2. After the `head IP` line: an untested Ray version in the image.
3. Just before the `cluster ... is up` line: a CA that expires within 14 days.

Rules:

- `up` reads the vast key from `VAST_API_KEY`, or from `~/.config/vastai/vast_api_key`. It sends the key to the head. The key is never in the YAML.
- `up` saves the AWS profile (`--profile` or `AWS_PROFILE`) in the state file. The other commands use it by default.
- `attach` and `dashboard` use the SSH key that `up` made. So run them on the machine that ran `up`.
- `up` makes no vast rental. Workers start when a job needs them, or for `min_workers`.
- After an AWS error, `up` prints `Run ray-vast down CLUSTER_YAML to remove what was created.`

### Where the head gets ray-vast

`up` installs ray-vast in the derived image. The first line of the `up` output tells the source. `up` uses the first source that applies:

| Source | When | Output line |
|---|---|---|
| A wheel file | You pass `--wheel FILE`. | `installing ray-vast from the wheel FILE` |
| The source tree | `ray-vast` runs from a git clone (an editable install). `up` builds the wheel with `python -m build`. This needs the `build` package (in the `test` extra). | `installing ray-vast from a wheel built from the source tree <path>` |
| PyPI | All other cases: the normal `pip install ray-vast`. | `installing ray-vast==<version> from PyPI` |

- `<version>` is the version of the `ray-vast` package on your laptop. So the head runs the same version as the CLI.
- If the image build fails, `up` stops with `the head image build failed while installing ray-vast==<version> from PyPI; see the build output below`. The end of the build output follows. The head exists then. Run `ray-vast down`.

To test a release on TestPyPI, download its wheel and pass it with `--wheel`:

```bash
pip download --no-deps --index-url https://test.pypi.org/simple/ ray-vast==<version> -d /tmp/rv
ray-vast up cluster.yaml --profile my-aws-profile --wheel /tmp/rv/ray_vast-<version>-py3-none-any.whl
```

## `ray-vast down`

```bash
ray-vast down cluster.yaml
```

`down` does these steps:

1. It terminates the EC2 head first. So the autoscaler cannot rent new workers.
2. It destroys every vast instance with the label `ray-vast:<cluster_name>`. It lists again until none is left.
3. It deletes the 5 security groups, the key pair, the instance profile and the role.
4. It deletes the state files in `~/.ray-vast/`, with the private SSH key.
5. It prints what is left.

Output:

```text
terminating head <instance id>
destroyed 2 vast instance(s)
deleted security group <group id>
...
deleted key pair ray-vast-<cluster_name>
deleted instance profile ray-vast-<cluster_name>
deleted role ray-vast-<cluster_name>
left: nothing
```

Rules:

- `down` needs no SSH key.
- Without `--profile` and `AWS_PROFILE`, `down` uses the profile that `up` saved.
- `down` finds everything by tag, name and label. It works after a failed `up`, when the head is gone, and when the state file is missing.
- If something is left, `down` lists it and exits with code 1. Run `down` again.
- Without a vast key, `down` cleans AWS, warns that the vast instances were not swept, and exits with code 0. Read [Clean up after a failure](../how-to/clean-up.md).

## `ray-vast attach`

```bash
ray-vast attach cluster.yaml
```

Opens an SSH shell on the EC2 host of the head, as the user `ubuntu`. Ray runs in the Docker container `ray-vast-head`. Run Ray commands in it:

```bash
sudo docker exec ray-vast-head ray status
```

## `ray-vast dashboard`

```bash
ray-vast dashboard cluster.yaml
```

1. It copies the cluster token to `~/.ray-vast/<cluster_name>/token` (mode 600).
2. It prints the `export` line for the Ray CLI.
3. It opens an SSH tunnel from port 8265 on your laptop to port 8265 on the head. Ctrl-C closes it. Then the command prints `tunnel closed` and exits with code 0.

Output:

```text
for `ray job submit` set:
  export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=/home/you/.ray-vast/<cluster_name>/token
dashboard at http://127.0.0.1:8265 (Ctrl-C to close the tunnel); use --address http://127.0.0.1:8265
```

## State files

ray-vast keeps these files on your laptop, in `~/.ray-vast/`:

| File | Content |
|---|---|
| `<cluster_name>.json` | Region, AWS profile, head instance ID and IP, security groups, role and instance profile names. `up` writes it. |
| `<cluster_name>/ssh_key.pem` | The private SSH key of the head. `up` writes it. Mode 600. |
| `<cluster_name>.known_hosts` | SSH host keys of the head (`StrictHostKeyChecking=accept-new`). |
| `<cluster_name>/token` | The cluster token. `dashboard` writes it. Mode 600. |

`down` deletes all of them.

## Cluster names

The cluster name must match `[A-Za-z0-9][A-Za-z0-9_-]{0,39}`: 1 to 40 letters, digits, `_` and `-`, and it starts with a letter or digit.

## Environment variables

| Variable | Use |
|---|---|
| `AWS_PROFILE` | The default of `--profile`. It wins over the profile that `up` saved. |
| `VAST_API_KEY` | The vast API key. If it is not set, ray-vast reads `~/.config/vastai/vast_api_key`. |

## Ray commands that do not work

Some `ray` cluster commands do not work with this provider. The list is in [Limits](limits.md#some-ray-commands-do-not-work).
