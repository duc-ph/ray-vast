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
| `image` | string | required | Docker image for the head and the workers. Read [Use GPU images](../how-to/node-types.md#use-gpu-images). |
| `max_dollars_per_hour` | number above 0 | required | Limit for the sum of `dph_total` over all workers and candidates. The provider does not rent above it. A missing value is an error, not "no limit". |
| `candidate_factor` | integer | `2` | The provider rents up to this many candidates for each slot. It destroys the extra candidates when the slots are full. |

ray-vast sets `type`, `module` and `disable_node_updaters` itself. Do not set them.

The vast API key is not a config field. ray-vast reads it from `VAST_API_KEY`, or from `~/.config/vastai/vast_api_key`.

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

`up` warns if `CPU` is above 54. A worker runs at most 54 processes at once. Read [Many tasks or actors](../libraries/many-tasks.md).

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

ray-vast does not check the field names. vast rejects an unknown field with HTTP 400 ("is not a valid search key"). Then the autoscaler log shows this error each time Ray tries to add a worker.

### Offer fields

The vast search API accepts these fields. The units are the units of the API. They are not always the units of the `vastai` CLI.

GPU:

| Field | Type | Meaning |
|---|---|---|
| `num_gpus` | integer | The number of GPUs. ray-vast sets it from `resources.GPU`. |
| `gpu_name` | string | The GPU model as vast writes it, with spaces. For example `RTX 4090`, `H100 SXM`, `A100 PCIE`. |
| `gpu_arch` | string | `nvidia` or `amd`. |
| `gpu_ram` | number | The memory of one GPU, in **MB**. |
| `gpu_total_ram` | number | The memory of all GPUs of the offer, in **MB**. |
| `gpu_mem_bw` | number | GPU memory bandwidth, in GB/s. |
| `compute_cap` | integer | CUDA compute capability × 100. For example `890` is 8.9. |
| `dlperf` | number | vast's deep learning speed score. Read [Choose a GPU by memory and speed](../how-to/node-types.md#choose-a-gpu-by-memory-and-speed). |
| `total_flops` | number | FP32 TFLOPS of all GPUs, without tensor cores. |
| `bw_nvlink` | number | NVLink bandwidth. `0` means no NVLink. |
| `gpu_max_power` | number | GPU power limit, in W. |
| `gpu_max_temp` | number | GPU temperature limit, in °C. |
| `gpu_display_active` | boolean | `true` if a display is attached to the GPU. |
| `gpu_frac` | number | The GPUs of the offer divided by the GPUs of the machine. `1` means the full machine. |
| `pci_gen` | number | PCIe generation. |
| `gpu_lanes` | integer | PCIe lanes of the GPU. |
| `pcie_bw` | number | Bandwidth from the CPU to the GPU, in GB/s. |
| `cuda_max_good` | number | The highest CUDA version that the driver supports. For example `12.8`. |
| `driver_version` | integer | The driver version as one number: major × 1000000 + minor × 1000 + patch. For example `570133007` is 570.133.07. |

CPU and memory:

| Field | Type | Meaning |
|---|---|---|
| `cpu_cores_effective` | number | The virtual CPUs that the instance gets. ray-vast adds a floor. |
| `cpu_cores` | integer | The virtual CPUs of the machine. |
| `cpu_ghz` | number | CPU clock speed, in GHz. |
| `cpu_ram` | number | System memory of the instance, in **MB**. |
| `cpu_arch` | string | `amd64` or `arm64`. |
| `cpu_name` | string | The CPU model. |

Disk:

| Field | Type | Meaning |
|---|---|---|
| `disk_space` | number | Free disk space, in GB. |
| `disk_bw` | number | Disk read bandwidth, in MB/s. |

Network:

| Field | Type | Meaning |
|---|---|---|
| `inet_down` | number | Download speed, in Mb/s. It sets the pull limit of a candidate. Read [Image size and the pull limit](../how-to/node-types.md#image-size-and-the-pull-limit). |
| `inet_up` | number | Upload speed, in Mb/s. A fast network moves Ray objects between hosts sooner. |
| `inet_down_cost` | number | Download traffic price, in $/GB. |
| `inet_up_cost` | number | Upload traffic price, in $/GB. |
| `direct_port_count` | integer | Open ports on the router of the host. ray-vast adds a floor. |
| `static_ip` | boolean | `true` if the IP address is stable. When the IP address of a worker changes, ray-vast destroys the worker. Use it for long jobs. |

Host:

| Field | Type | Meaning |
|---|---|---|
| `verified` | boolean | `true` if vast verified the machine. |
| `datacenter` | boolean | `true` if the host is a datacenter. |
| `reliability` | number | vast's reliability score of the machine, from 0 to 1. `reliability2` is the same value. |
| `duration` | number | The longest rental time that the host allows, in **seconds**. |
| `geolocation` | string | Two-letter country code. Use `eq`, `neq`, `in` or `notin`. For example `{in: [US, CA]}`. |
| `os_version` | string | Ubuntu version of the host, for example `22.04`. `ubuntu_version` is the same field. |
| `vms_enabled` | boolean | `true` if the machine is a VM instance (vast's meaning). |
| `mobo_name` | string | The motherboard model. |
| `machine_id` | integer | The machine. Use `notin` to skip a machine. |
| `host_id` | integer | The host account. Use `notin` to skip a host. |
| `cluster_id` | integer | vast's cluster of the machine, or `null`. |
| `rentable` | boolean | ray-vast sets it to `true`. |

Price:

| Field | Type | Meaning |
|---|---|---|
| `dph_total` | number | Price of the instance, in $/hour. The provider rents the cheapest offers first. |
| `dlperf_per_dphtotal` | number | `dlperf` for each $/hour. |
| `flops_per_dphtotal` | number | `total_flops` for each $/hour. |
| `storage_cost` | number | Disk price, in $/GB/month. |

The fields `external`, `rented` and `has_avx` do not change the result. Do not use them.

ray-vast adds these fields to each offer query:

| Field | Added value | Your own value for this field |
|---|---|---|
| `num_gpus` | `eq: <GPU>` | Must be the same (`eq` or `in` with the one value `GPU`). |
| `cpu_cores_effective` | `gte: <the larger of CPU and 5>` | Allowed when it is at least as strict. |
| `direct_port_count` | `gte: <10 + the smaller of CPU and 54>` (the 6 fixed identity ports, 4 ports for the library helpers, and one port for each CPU) | Allowed when it is at least as strict. |
| `rentable` | `eq: true` | Must be the same. |

ray-vast does not add `verified` or `datacenter`. We highly recommend `verified: {eq: true}` and `datacenter: {eq: true}` in every query. Read [Keep verified and datacenter in the offer query](../how-to/node-types.md#keep-verified-and-datacenter-in-the-offer-query).

The search also sends `type: on-demand` and `limit: 100`.

How to write a good query: [Choose node types and GPU images](../how-to/node-types.md).
