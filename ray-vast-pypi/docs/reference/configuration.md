# Configuration

The cluster file is a Ray cluster YAML. ray-vast adds fields to it.
`ray-vast up` checks the file before it makes anything. It stops at the first error.

Examples are in the package, in `ray_vast/examples/`: `gpu-24gb.yaml`, `gpu-80gb.yaml` and `cpu-heavy.yaml`.

## Full example

Each example lists all fields. A field in a comment keeps its default, or does not apply.

```yaml
--8<-- "src/ray_vast/examples/gpu-24gb.yaml"
```

## Top level

| Field | Type | Default | Meaning |
|---|---|---|---|
| `cluster_name` | string | required | 1 to 40 characters: letters, digits, `_` and `-`. It starts with a letter or digit. The AWS resources get the name `ray-vast-<cluster_name>`. The vast instances get the label `ray-vast:<cluster_name>`. |
| `max_workers` | integer | the sum of `max_workers` of the worker types | Ray field. The most workers in the cluster. A worker type without `max_workers` counts as 0. |
| `idle_timeout_minutes` | number | Ray default | Ray field. Ray removes a worker after it is idle for this time. |
| `head_node_type` | string | required | The name of the head entry in `available_node_types`. |
| `available_node_types` | mapping | required | The head type and the worker node types. |
| `auth.ssh_user` | string | `ray` | Ray needs this field. ray-vast does not use it. |

ray-vast does not use the Ray fields `file_mounts`, `setup_commands`, `initialization_commands` and the other command lists. It sets them to empty values if they are missing.

## `provider`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `image` | string | required | Docker image for the head and the workers. Read [Use GPU images](../how-to/gpu-images.md). |
| `max_dollars_per_hour` | number above 0 | required | Limit for the sum of `dph_total` over all workers and candidates. The provider does not rent above it. A missing value is an error, not "no limit". |
| `candidate_factor` | integer | `2` | The provider rents up to this many candidates for each slot. It destroys the extra candidates when the slots are full. |

ray-vast sets `type`, `module` and `disable_node_updaters` itself. Do not set them.

The vast API key is not a config field. ray-vast reads it from `VAST_API_KEY`, or from `~/.config/vastai/vast_api_key`.

### Fields of older versions

A YAML with one of these `provider` fields gets an error. The error tells what to do.

| Old field | Now |
|---|---|
| `region` | `node_config.region` of the head node type. |
| `head_instance_type` | `node_config.instance_type` of the head node type. |
| `key_name`, `key_file` | Removed. `up` makes an EC2 key pair for each cluster. |
| `port_a`, `port_b` | Removed. The head server always uses ports 7443 and 7444. |
| `cert_max_age_days` | Removed. The CA and the certificates always live 365 days. |
| `image_size_gb` | Removed. `up` measures the image. |
| `order_by` | Removed. The provider always rents the cheapest offers first (`dph_total`). Remove the field before you run `up`. `down`, `attach` and `dashboard` accept it. |
| `allow_non_datacenter` | Removed. ray-vast adds no `datacenter` filter. Set `datacenter: {eq: true}` and `verified: {eq: true}` in the `offer_query` of each node type (highly recommended). Remove the field before you run `up`. `down`, `attach` and `dashboard` accept it. |

## `available_node_types`: the head

```yaml
head:
  resources: {CPU: 0}
  node_config:
    region: us-west-2
    # instance_type: t3.large
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `resources` | mapping | required | Must be `{CPU: 0}`. So Ray schedules no task with resources on the head. It is an error if you omit `resources` or use another CPU value. |
| `node_config.region` | string | required | AWS region of the EC2 head. |
| `node_config.instance_type` | string | `t3.large` | EC2 instance type of the head. It must be an x86_64 type. |

## `available_node_types`: worker types

| Field | Type | Default | Meaning |
|---|---|---|---|
| `resources.CPU` | whole number ≥ 0 | required | The number of Ray CPUs of each worker. The offer must have at least this many effective cores. The head refuses a worker with fewer cores than 90 % of `CPU`. |
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
| `cpu_cores_effective` | `gte: <the larger of CPU and 4>` | Allowed when it is at least as strict. |
| `direct_port_count` | `gte: 10` (the 6 fixed identity ports and at least 4 worker ports) | Allowed when it is at least as strict. |
| `rentable` | `eq: true` | Must be the same. |

ray-vast does not add `verified` or `datacenter`. We highly recommend `verified: {eq: true}` and `datacenter: {eq: true}` in every query. Read [Keep the verified and datacenter filters](../how-to/node-types.md#keep-the-verified-and-datacenter-filters).

The search also sends `type: on-demand` and `limit: 100`.

How to write a good query: [Choose node types and offer queries](../how-to/node-types.md).
