# Choose node types and offer queries

A node type is a named class of workers. It has fixed declared Ray resources and one offer query.
The offer query tells the provider which vast offers it may rent for the node type.

## Write a node type

```yaml
available_node_types:
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

- `resources.GPU` is exact. The offer must have exactly this many GPUs.
- `resources.CPU` is a minimum. The offer must have at least this many effective cores.
- `node_config.offer_query` is a YAML mapping in the vast API JSON form.
- `node_config.disk_gb` is the disk size of the instance. The default is 50 GB. Make it large enough for the image and your data.

## CPU is a minimum, GPU is exact

The provider adds `cpu_cores_effective >= CPU` and `num_gpus == GPU` to the query.
A running worker reports its real core count to Ray. Ray uses the real count for that worker.
But Ray plans a new worker with the declared `CPU` value. So declare a `CPU` value that is close to the real hosts.

**Cold-start overshoot.** Before the first worker reports its real cores, Ray plans with the declared `CPU`.
So Ray can start more workers than necessary. In a test, Ray started 2 workers for 4 tasks.
The extra worker stays idle until `idle_timeout_minutes` removes it. This costs a few minutes of one worker.

Do not set `CPU` above 58. Each worker has 58 ports for Ray worker processes. Read [Limits](../reference/limits.md).

## Write the offer query

Each key is a vast offer field. Each value is a mapping from an operator to a value.

| Operator | Meaning | Value |
|---|---|---|
| `eq`, `neq` | equal, not equal | one value |
| `gt`, `gte` | greater than, greater than or equal | one value |
| `lt`, `lte` | less than, less than or equal | one value |
| `in`, `notin` | in the list, not in the list | a list |

Example:

```yaml
offer_query:
  gpu_ram: {gte: 24000, lte: 48000}   # MB
  dlperf: {gte: 40}
  geolocation: {in: [US, CA]}
  static_ip: {eq: true}
```

Rules:

- `gpu_ram`, `cpu_ram` and `gpu_total_ram` are in **MB**. A value below 1000 is an error ("the unit is MB"). For 24 GB, write `24000`.
- A range that can never match (for example `{gte: 48000, lte: 24000}`) is an error.
- ray-vast has no parser for the `vastai search offers` query syntax. Write the JSON form.

## Filters that ray-vast adds

The provider adds these filters to every query:

| Field | Filter |
|---|---|
| `num_gpus` | `eq: <GPU>` |
| `cpu_cores_effective` | `gte: <CPU>` |
| `direct_port_count` | `gte: 64` |
| `verified` | `eq: true` |
| `rentable` | `eq: true` |
| `datacenter` | `eq: true` (not added when `allow_non_datacenter` is true) |

You can add a stricter filter on `cpu_cores_effective` or `direct_port_count`. ray-vast merges it.
A looser filter is an error. A `num_gpus` filter other than `GPU` is an error.

Example: one node type for each CPU size.

```yaml
  gpu-small:
    resources: {CPU: 8, GPU: 1}
    node_config:
      offer_query:
        gpu_ram: {gte: 24000}
        cpu_cores_effective: {gte: 8, lt: 16}
  gpu-large:
    resources: {CPU: 16, GPU: 1}
    node_config:
      offer_query:
        gpu_ram: {gte: 24000}
        cpu_cores_effective: {gte: 16, lt: 32}
```

## Choose a GPU by memory and speed

`GPU: 1` alone is not enough. A GTX 1080 and an H100 are very different.
Set a GPU memory range and a `dlperf` minimum.

- GPU memory tiers in the vast market: 8, 12, 16, 24, 32, 48, 80, 96 and 140+ GB. Speed changes up to 8 times inside one tier.
- `dlperf` is vast's deep learning speed score. Use it for speed.
- Do not use `total_flops`. It is FP32 speed without tensor cores.
- Only about 7% of offers are on datacenter hosts. A narrow query can match no offer.

## Optional filters

| Filter | Use |
|---|---|
| `inet_down: {gte: 500}`, `inet_up: {gte: 500}` | Network speed in Mb/s. A fast download pulls the image sooner. A fast network moves Ray objects between hosts sooner. |
| `static_ip: {eq: true}` | For long jobs. When the IP of a worker changes, ray-vast destroys the worker. |
| `geolocation: {in: [US]}` | Location of the host. |
| `reliability2: {gte: 0.98}` | vast's reliability score of the host. |
| `dph_total: {lte: 0.5}` | Price limit for one instance, in $/hour. |

ray-vast does not add the network speed filters. Add them when your jobs need them.

## How the provider picks an offer

- The provider searches with your query plus the added filters. It asks vast for up to 100 offers.
- It sorts the offers by `order_by` (default `dph_total`), in ascending order. So the cheapest offers come first.
- It skips the hosts on the blocklist. A host that failed a check goes on the blocklist for this cluster.
- It skips an offer that would take the total above `max_dollars_per_hour`.

## Check your node types

`ray-vast up` checks the YAML before it makes anything.
It stops with an error if the declared resources and the query disagree.
It warns if `CPU` is above 58.

If no worker arrives, the query can match no offer. Read [Find logs and troubleshoot](troubleshoot.md).
