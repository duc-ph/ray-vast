# ray-vast

ray-vast is a Ray autoscaler node provider.
It runs the Ray head on an AWS EC2 instance.
It rents the Ray workers from vast.ai, and removes them when the work is done.
The workers talk to the head over the public internet, across NAT, with TLS and a token.
You do not need a VPN or an overlay network.

Status: version 0.1.0 is not released yet. It is not on PyPI.

## How it works

When Ray needs a node, the provider rents up to `candidate_factor` vast instances (candidates) for it. Each candidate downloads a small program from the head, checks the host, and proves its identity with a one-time join token. The first candidates that enroll get a certificate and the cluster token, and join Ray. The provider destroys the others. Each worker watches its link to the head and destroys its own instance when the head is gone for 5 minutes. Ray removes idle workers after `idle_timeout_minutes`.

The full flow and the design are in [docs/design.md](../docs/design.md) (section 2, "Life of a worker"). The security model is in [docs/security.md](docs/security.md).

## Requirements

- Python 3.9 or later on your laptop. The `ray-vast` command runs there.
- An AWS account. The AWS user that runs `ray-vast up` and `ray-vast down` needs permission to:
  - make and delete an IAM role, a role policy and an instance profile;
  - make and delete security groups, and add rules to them;
  - start and terminate EC2 instances, and describe images and instances.

  `ray-vast` gives the head a role. The head can only change the rules of the cluster's 5 security groups and describe EC2.
- An EC2 key pair and its private key file (for SSH to the head).
- A vast.ai account with credit, and an API key. Put it in `VAST_API_KEY` or in `~/.config/vastai/vast_api_key`. The key is never written in the cluster YAML.
- `ssh` and `scp` on your laptop.
- Docker image rules. The image runs on the head and on the workers. It must have:
  - `sh`, and a Python that can `import ray`. Nothing else is needed: no sshd, no apt, no `sudo`, no curl;
  - no `ENTRYPOINT`, or an `ENTRYPOINT` that ends with `exec "$@"` (as in `rayproject/ray:*-gpu`). The provider sets the container command, and the ENTRYPOINT runs it;
  - Ray 2.59 or later. Ray 2.59 is the tested version. `ray-vast up` warns if the image has a different Ray version.

## Install

```
pip install ray-vast
```

Note: ray-vast is not published yet, so this command does not work now.
Until then, install from the source tree:

```
pip install ./ray-vast-pypi
```

`ray-vast up` builds a wheel from the source tree and installs it in a derived image on the head.
Use `ray-vast up --wheel FILE` if you do not run from a source tree.

## Quick start

Copy an example from `src/ray_vast/examples/` (`gpu-24gb.yaml`, `gpu-80gb.yaml`, `cpu-heavy.yaml`). Edit the region, the image and `max_dollars_per_hour`. Add your EC2 key pair:

```yaml
provider:
  key_name: my-ec2-key          # EC2 key pair name
  key_file: ~/my-ec2-key.pem    # private key file
```

Then run:

```
ray-vast up example.yaml          # make the head and start the cluster
ray-vast dashboard example.yaml   # terminal 1: copy the token, open the SSH tunnel
```

`dashboard` copies the cluster token to `~/.ray-vast/<cluster_name>/token` (mode 600). It prints this line:

```
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=/home/you/.ray-vast/<cluster_name>/token
```

Then keep the tunnel open and use a second terminal. Run the export line, and submit the job:

```
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=...      # the line that dashboard printed
ray job submit --address http://127.0.0.1:8265 -- python -c "import ray; print(ray.cluster_resources())"
```

Open http://127.0.0.1:8265 for the dashboard. Stop the tunnel with Ctrl-C. When you finish:

```
ray-vast down example.yaml        # destroy all workers and the head
```

You can also submit a job on the head. Run `ray-vast attach example.yaml`, then `sudo docker exec ray-vast-head ray job submit --address http://127.0.0.1:8265 -- ...`.

## CLI reference

```
ray-vast {up,down,attach,dashboard} CLUSTER_YAML [options]
```

| Command | What it does |
|---|---|
| `up` | Makes the IAM role, the 5 security groups, the instance profile and the EC2 head. Builds a derived image on the head. Starts Ray. |
| `down` | Removes everything of the cluster. Prints `left: nothing` when done. Exit code 1 if something is left. |
| `attach` | Opens an SSH shell on the head. |
| `dashboard` | Copies the token to the laptop, prints the `export` line, and opens an SSH tunnel to port 8265 of the head. |

| Option | Commands | Meaning |
|---|---|---|
| `--profile NAME` | all | AWS profile. Default: `AWS_PROFILE`, or the default credentials. |
| `--key-file FILE` | `up`, `attach`, `dashboard` | Private SSH key of the head. Or set `provider.key_file`. |
| `--key-name NAME` | `up` | EC2 key pair name. Or set `provider.key_name`. |
| `--wheel FILE` | `up` | The ray-vast wheel to install on the head. Default: build it from the source tree. Use it if you do not run from a source tree. |

Rules:

- The option wins over the YAML field.
- `down` needs no SSH key.
- The cluster name must match `[A-Za-z0-9][A-Za-z0-9_-]{0,39}`. The YAML needs `cluster_name` and `provider.region`.
- `up` stops at once if the cluster already exists. It checks for an EC2 head with the cluster tag and for `~/.ray-vast/<cluster>.json`. Run `ray-vast down` first.
- `up` reads the vast key from `VAST_API_KEY`, or from `~/.config/vastai/vast_api_key`. It sends the key to the head.
- `up` measures the image size and writes `image_size_gb` into the head config. A value in your YAML wins.
- `down` ends the head first. So the autoscaler cannot rent new workers. Then it destroys the vast instances. Then it deletes the 5 security groups, the instance profile and the role.
- `down` without a vast key cleans AWS, prints a warning that the vast instances were not swept, and exits with code 0. Set `VAST_API_KEY` and run `down` again. Or destroy the instances with the label `ray-vast:<cluster_name>` by hand (`vastai show instances`, `vastai destroy instance ID`).
- `down` finds everything by tag and name. It works when `up` failed half way, when the head is gone, and when the local state file is missing.

State files on your laptop, in `~/.ray-vast/`:

| File | Content |
|---|---|
| `<cluster>.json` | Region, head instance ID and IP, security groups, role and instance profile names. |
| `<cluster>.known_hosts` | SSH host keys of the head (`StrictHostKeyChecking=accept-new`). |
| `<cluster>/token` | The cluster token. Written by `dashboard`. Mode 600. |

`down` deletes all three.

`up` takes several minutes: the head installs Docker, builds the derived image and starts Ray.
Workers start only when a job needs them. A new worker takes about 1 to 2 minutes to join (the image pull time depends on the host).

## Config reference

The cluster file is a Ray cluster YAML. ray-vast adds fields to it.

### Top level

| Field | Meaning |
|---|---|
| `cluster_name` | Required. 1 to 40 characters: letters, digits, `_` and `-`. It must start with a letter or digit. Vast instances get the label `ray-vast:<cluster_name>`. |
| `max_workers` | Ray field. The most workers in the cluster. |
| `idle_timeout_minutes` | Ray field. An idle worker is removed after this time. |
| `head_node_type` | Required. The name of the head entry in `available_node_types`. |
| `auth.ssh_user` | Ray needs this field. ray-vast does not use it. Default `ray`. |

### `provider`

| Field | Default | Meaning |
|---|---|---|
| `region` | required | AWS region of the head. |
| `image` | required | Docker image for the head and the workers (see "Requirements"). |
| `max_dollars_per_hour` | required | Limit for the sum of `dph_total` over all workers and candidates. The provider does not rent above it. A missing value is an error, not "no limit". |
| `key_name` | none | EC2 key pair name. `up` needs it, here or in `--key-name`. |
| `key_file` | none | Private key file for SSH. `up`, `attach` and `dashboard` need it, here or in `--key-file`. |
| `candidate_factor` | 2 | The provider rents up to this many candidates for each slot. Extra candidates are destroyed when the slots are full. |
| `image_size_gb` | measured by `up` | Size of the image in GB. `up` measures it and sets it. A value in your YAML wins. The pull limit for a candidate is `2 x image_size_gb x 8000 / inet_down` seconds, between 5 and 15 minutes. If the value is missing, the limit is 15 minutes. |
| `cert_max_age_days` | 365 | Life of the CA and of every node certificate. |
| `allow_non_datacenter` | false | If true, the datacenter filter is removed and `up` shows a warning. See [docs/security.md](docs/security.md). |
| `order_by` | `dph_total` | The sort field for matching offers. The cheapest offers come first. |
| `head_instance_type` | `t3.large` | EC2 instance type of the head. |
| `port_a` | 7443 | Head server port for download and enrollment (TLS). |
| `port_b` | 7444 | Head server port for heartbeat and reports (mTLS). |

ray-vast sets `type`, `module` and `disable_node_updaters` itself. Do not set them.

### `available_node_types`

The head type needs `resources: {CPU: 0}`. It is an error if you omit `resources` or use another CPU value. Example:

```yaml
head:
  resources: {CPU: 0}
  node_config: {}
  max_workers: 0
```

Each worker type has these fields:

| Field | Default | Meaning |
|---|---|---|
| `resources.CPU` | required | A minimum. See below. A whole number. |
| `resources.GPU` | required | Exact. The offer must have exactly this many GPUs. A whole number (0 is allowed). |
| `min_workers`, `max_workers` | Ray defaults | Ray fields. |
| `node_config.disk_gb` | 50 | Disk size of the instance, in GB. |
| `node_config.offer_query` | required | Which vast offers the type can rent. See below. |

**CPU is a minimum, GPU is exact.** The provider adds `cpu_cores_effective >= CPU` and `num_gpus == GPU` to the query. A running worker reports its real core count to Ray. Ray plans a new worker with the declared `CPU` value. So declare a `CPU` value that is close to the real hosts.

**Cold-start overshoot.** Before the first worker reports its real cores, Ray plans with the declared `CPU`. It can start extra workers. In a test, Ray started 2 workers for 4 tasks. The extra worker stays idle until `idle_timeout_minutes` removes it. This costs a few minutes of one extra worker. A `CPU` value close to the real hosts reduces it.

**`offer_query`.** It is a YAML mapping in the vast API JSON form. Each key is a vast offer field. Each value is a mapping from an operator to a value. The operators are `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in` and `notin`. `in` and `notin` need a list. The other operators need a single value.

```yaml
offer_query:
  gpu_ram: {gte: 24000, lte: 48000}   # MB
  dlperf: {gte: 40}
  inet_down: {gte: 500}
  geolocation: {in: [US]}
```

- `gpu_ram`, `cpu_ram` and `gpu_total_ram` are in **MB**. A value below 1000 is an error ("the unit is MB").
- There is no parser for the vast CLI query syntax.
- ray-vast adds these filters: `num_gpus = GPU`, `cpu_cores_effective >= CPU`, `direct_port_count >= 64`, `verified = true`, `rentable = true`, and `datacenter = true` (unless `allow_non_datacenter` is true).
- You can add a stricter filter on `cpu_cores_effective` or `direct_port_count`, for example one node type for each CPU size. A looser filter is an error. A different `num_gpus` is an error.

**Choose a GPU by VRAM and speed.** `GPU: 1` alone is not enough: a GTX 1080 and a B200 are very different. Set a VRAM range and a `dlperf` minimum.
- VRAM tiers in the vast market: 8, 12, 16, 24, 32, 48, 80, 96 and 140+ GB. Speed changes up to 8 times inside one tier.
- Do not use `total_flops`. It is FP32 without tensor cores.
- Use `static_ip: {eq: true}` for long jobs. When the IP of a worker changes, ray-vast destroys the worker.
- Use `geolocation` and `inet_down` for location and network speed.
- Only about 7% of offers are on datacenter hosts. A narrow query can match no offer.

`ray-vast up` stops with an error if the declared resources and the query disagree. It warns if `CPU` is above 58 (see "Limits").

## Cost controls

- `max_dollars_per_hour`: a hard limit on the `dph_total` of all workers and candidates together. The provider does not rent above it.
- `candidate_factor`: the provider rents extra candidates, and this costs a little. It destroys a candidate that it does not need as soon as the slots are full. Use 1 to rent no extra candidates. Then one bad host costs a retry.
- `idle_timeout_minutes`: the time before Ray removes an idle worker.
- `max_workers` (cluster and each node type): limits for the cluster size.
- Watchdog: a worker destroys itself if it loses the head for 5 minutes. A stopped container still costs money on vast, so the watchdog destroys and never only exits.
- The EC2 head costs about $0.08/hour for `t3.large`. It runs until `ray-vast down`.

Always run `ray-vast down` when you finish.

## Limits

- **About 299 worker IPs.** Each worker IP needs a security group rule. One cluster uses 5 security groups (the AWS default limit for one network interface). The default AWS quota is 60 rules for each group, and the SSH rule uses one. Raise the quota in AWS Service Quotas for a bigger cluster. When all 5 groups are full, the provider destroys the candidate and the error says to raise the quota. Two workers on the same IP share one rule.
- **58 worker ports for each node.** Each worker has 64 ports. Ray uses 6 fixed ports, and the other 58 are for worker processes. Each Ray worker process (a task, an actor or a driver) needs one port.
  - When no port is free, tasks wait.
  - A driver fails at once with `No available ports`.
  - An actor fails after about 6 minutes. The error is misleading: `ActorUnschedulableError ... Failed to startup worker after retrying 5 times ... Python environment setup`. The real cause (`No available ports`) is only in the worker log.
  - Actors also use ports. An idle worker process keeps its port for about 60 seconds.
  - Ray has no setting for the total number of worker processes. ray-vast sets `RAY_num_workers_soft_limit` to `min(cores, 58)`, so that idle processes release their ports sooner.
  - Do not set `CPU` above 58.
- **Certificates do not renew.** To renew, run `ray-vast down` and `ray-vast up`. `up` and the head log warn 14 days before expiry.
- **The head has no Elastic IP.** A reboot keeps the IP. After an EC2 stop and start, run `ray-vast down` and `ray-vast up`. The old workers destroy themselves.
- **No self-healing.** If Ray's monitor (the autoscaler and the provider) dies, the head stops. The workers destroy themselves within 5 minutes. Run `ray-vast down` and `ray-vast up`. The head keeps all state in memory, so a new head starts from zero.
- **Tested only with Ray 2.59.**
- **Some Ray commands do not work.** `ray up`, `ray down`, `ray attach`, `ray exec` and `ray rsync-*` do not work with this provider. Use the `ray-vast` commands. `ray job submit`, `ray status` on the head, and the Python API work as usual.

## Troubleshooting

**Logs.**

- Head, all output: `ray-vast attach example.yaml`, then `sudo docker logs ray-vast-head`.
- Provider and head server log: `sudo docker exec ray-vast-head cat /tmp/ray/session_latest/logs/monitor.log`.
- Worker logs: the Ray dashboard and `ray logs`.
- Vast keeps no container log after a destroy. So a worker sends the reason for each destroy to the head before it destroys itself. The reason is in the head log. The full list of reasons is in [design.md](../docs/design.md) section 6.2.
- The head logs every enrollment decision: instance ID, machine ID, IP and the reason. It never logs secrets.

**A worker never arrives.**

- Check that the offer query matches offers. A narrow query, or the datacenter filter, can match none.
- The pull limit can end a candidate. Set `image_size_gb` for a big image.
- Look in the head log for enrollment refusals. A host can fail the host checks (egress IP, capabilities, own network, `/dev/shm`, identity ports). The provider adds the host to a blocklist for this cluster and rents another one.

**Jobs that wait.** Check `max_dollars_per_hour`. The provider does not rent above it.

**`ray-vast up` fails half way.** Run `ray-vast down example.yaml`. It finds everything by tag and name. It works when the head is gone, and when the local state file is missing. It is safe to run again.

**Clean-up check.** `down` prints `left: nothing` when it is done. If something remains, it lists it and exits with code 1. Then run `down` again. Nothing should be left after `down`: no vast instance with the label `ray-vast:<cluster_name>`, no EC2 head, no security group `ray-vast-<cluster_name>-0` … `-4`, no IAM role or instance profile with the name `ray-vast-<cluster_name>`.

## Tests

Run the tests on a Linux box with Docker. Install with `pip install -e '.[test]'`.

Fast run, for the TDD loop (about 30 s). It skips tests that start processes, containers, TLS servers or wheel builds, or that wait on real timers:

```
pytest
```

Full run, once before you finish. It adds the slow tests (bootstrap subprocesses, wheel build, Docker containers) and still skips the tests that spend money or need a Ray cluster:

```
pytest -o addopts="" -m "not live and not local_e2e"
```

A slow test has `@pytest.mark.slow`. A test that runs longer than 300 s fails (`pytest-timeout`). Every test that starts a process starts it in a new session and kills the whole process group in a finalizer. A session check fails the run if a test leaves a process or a container behind. The bootstrap tests set `RAYVAST_DIR` to a temp directory, so runs can overlap.

## More documents

- [docs/security.md](docs/security.md): the security model and the known gaps. Read it before you use the cluster with real data.
- [../docs/design.md](../docs/design.md): the design, with the worker and head protocol.
- [CHANGELOG.md](CHANGELOG.md)
