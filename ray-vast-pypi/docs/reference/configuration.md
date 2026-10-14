# Configuration

The cluster file is a Ray cluster YAML. ray-vast adds fields to it.
`ray-vast up` checks the file before it makes anything. It stops at the first error.

Examples are in the package, in `ray_vast/examples/`: `gpu-24gb.yaml`, `gpu-80gb.yaml` and `cpu-heavy.yaml`.

## Full example

```yaml
cluster_name: gpu-24gb
max_workers: 4
idle_timeout_minutes: 5
provider:
  region: us-west-2
  image: rayproject/ray:2.59.0-py311-gpu
  max_dollars_per_hour: 3.0
  key_name: my-ec2-key
  key_file: ~/my-ec2-key.pem
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

## Top level

| Field | Type | Default | Meaning |
|---|---|---|---|
| `cluster_name` | string | required | 1 to 40 characters: letters, digits, `_` and `-`. It starts with a letter or digit. The AWS resources get the name `ray-vast-<cluster_name>`. The vast instances get the label `ray-vast:<cluster_name>`. |
| `max_workers` | integer | Ray default | Ray field. The most workers in the cluster. |
| `idle_timeout_minutes` | number | Ray default | Ray field. Ray removes a worker after it is idle for this time. |
| `head_node_type` | string | required | The name of the head entry in `available_node_types`. |
| `available_node_types` | mapping | required | The head type and the worker node types. |
| `auth.ssh_user` | string | `ray` | Ray needs this field. ray-vast does not use it. |

ray-vast does not use the Ray fields `file_mounts`, `setup_commands`, `initialization_commands` and the other command lists. It sets them to empty values if they are missing.

## `provider`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `region` | string | required | AWS region of the head. |
| `image` | string | required | Docker image for the head and the workers. Read [Use GPU images](../how-to/gpu-images.md). |
| `max_dollars_per_hour` | number above 0 | required | Limit for the sum of `dph_total` over all workers and candidates. The provider does not rent above it. A missing value is an error, not "no limit". |
| `key_name` | string | none | EC2 key pair name. `up` needs it, here or in `--key-name`. |
| `key_file` | path | none | Private key file for SSH to the head. `up`, `attach` and `dashboard` need it, here or in `--key-file`. ray-vast expands `~`. |
| `candidate_factor` | integer | `2` | The provider rents up to this many candidates for each slot. It destroys the extra candidates when the slots are full. |
| `image_size_gb` | number | measured by `up` | Size of the image in GB. It sets the pull limit of a candidate: `2 × image_size_gb × 8000 / inet_down` seconds, between 5 and 15 minutes. Without a value, the limit is 15 minutes. If your YAML has a value, ray-vast uses it and not the measured value. |
| `cert_max_age_days` | integer | `365` | Life of the CA and of every node certificate. |
| `allow_non_datacenter` | boolean | `false` | If true, ray-vast does not add the `datacenter = true` filter, and `up` shows a warning. Read [Security model](../security.md). |
| `order_by` | string | `dph_total` | The vast offer field to sort by, in ascending order. The default puts the cheapest offers first. |
| `head_instance_type` | string | `t3.large` | EC2 instance type of the head. |
| `port_a` | integer | `7443` | Head server port for the download and the enrollment (TLS). |
| `port_b` | integer | `7444` | Head server port for the heartbeat and the reports (mTLS). |

ray-vast sets `type`, `module` and `disable_node_updaters` itself. Do not set them.
`up` removes `key_name` and `key_file` from the YAML that it writes to the head.

The vast API key is not a config field. ray-vast reads it from `VAST_API_KEY`, or from `~/.config/vastai/vast_api_key`.

## `available_node_types`: the head

The head type must declare `resources: {CPU: 0}`. It is an error if you omit `resources` or use another CPU value.

```yaml
head:
  resources: {CPU: 0}
  node_config: {}
  max_workers: 0
```

So Ray schedules no task with resources on the head.

## `available_node_types`: worker types

| Field | Type | Default | Meaning |
|---|---|---|---|
| `resources.CPU` | whole number ≥ 0 | required | A minimum. The offer must have at least this many effective cores. A worker reports its real core count to Ray. |
| `resources.GPU` | whole number ≥ 0 | required | Exact. The offer must have exactly this many GPUs. |
| `min_workers` | integer | Ray default | Ray field. The fewest workers of this type. |
| `max_workers` | integer | Ray default | Ray field. The most workers of this type. |
| `node_config.offer_query` | mapping | required | Which vast offers the type can rent. |
| `node_config.disk_gb` | number | `50` | Disk size of the instance, in GB. |

`up` warns if `CPU` is above 58. Read [Limits](limits.md#58-worker-ports-for-each-worker).

## `offer_query`

A YAML mapping in the vast API JSON form. Each key is a vast offer field. Each value is a mapping from an operator to a value.

| Operator | Meaning | Value |
|---|---|---|
| `eq`, `neq` | equal, not equal | one value (not a list or a mapping) |
| `gt`, `gte` | greater than, greater than or equal | one value |
| `lt`, `lte` | less than, less than or equal | one value |
| `in`, `notin` | in the list, not in the list | a list |

Checks:

- An unknown operator is an error.
- `gpu_ram`, `cpu_ram` and `gpu_total_ram` are in MB. Their values must be numbers of 1000 or more ("the unit is MB").
- A range that can never match, for example `{gte: 10, lte: 5}`, is an error.

ray-vast adds these filters:

| Field | Filter | Your own filter on this field |
|---|---|---|
| `num_gpus` | `eq: <GPU>` | Must be the same (`eq` or `in` with the one value `GPU`). |
| `cpu_cores_effective` | `gte: <CPU>` | Allowed when it is at least as strict. |
| `direct_port_count` | `gte: 64` | Allowed when it is at least as strict. |
| `verified` | `eq: true` | Must be the same. |
| `rentable` | `eq: true` | Must be the same. |
| `datacenter` | `eq: true`, unless `allow_non_datacenter` | Must be the same. |

The search also sends `type: on-demand` and `limit: 100`.

How to write a good query: [Choose node types and offer queries](../how-to/node-types.md).
