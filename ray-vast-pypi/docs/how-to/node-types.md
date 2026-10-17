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
        verified: {eq: true}                # highly recommended: keep these two lines
        datacenter: {eq: true}
        gpu_ram: {gte: 24000, lte: 32000}   # MB
        dlperf: {gte: 40}
        inet_down: {gte: 500}
```

- `resources.GPU` is exact. The offer must have exactly this many GPUs.
- `resources.CPU` is the number of Ray CPUs of each worker. The offer must have at least this many effective cores.
- `node_config.offer_query` is a YAML mapping in the vast API JSON form.
- `node_config.disk_gb` is the disk size of the instance. The default is 50 GB. Make it large enough for the image and your data.

## Keep the verified and datacenter filters

ray-vast does not add `verified` or `datacenter` to the query. You set them. We highly recommend `verified: {eq: true}` and `datacenter: {eq: true}` in every node type. The example YAMLs have them.

The vast host survey of 2026-10-17 measured these rates:

| Hosts | Not started in 12 min | All identity ports reachable from outside |
|---|---|---|
| Verified | 2 % | 99 % |
| Unverified | 9 % | 77 % |
| Deverified | 50 % | 87 % |
| Datacenter | 10 % | 100 % |
| Non-datacenter | 21 % | 89 % |

A host that does not start, or that fails the port check, costs a retry. Then the worker arrives later. These filters also make a dishonest host less likely. Read [Security model](../security.md).

## CPU and GPU

The provider adds `cpu_cores_effective >= max(CPU, 4)` and `num_gpus == GPU` to the query.

- The core floor is 4 or more. Reason: vast sets the container process limit (`pids.max`) to 256 × the CPU quota in whole cores. Below about 3 effective cores, the limit is below 640. Then the worker gets the minimum of 4 worker ports with no room for their threads. Read [Limits](../reference/limits.md#58-worker-ports-for-each-worker).

- Ray gets the declared `CPU` on each worker. A host with more cores gives no more Ray CPUs. To use more cores, declare a larger `CPU`.
- Ray plans new workers with the same `CPU` value. So Ray starts the number of workers that the work needs.
- vast gives a container a CPU quota of 0.96 × the offer's effective cores. For example, an offer with 8 effective cores gives 7.68 cores. Ray still gets 8 CPUs.
- The head refuses a worker that has fewer cores than 90 % of `CPU`. The cores are the smaller of the CPU affinity and the CPU quota of the container. The provider blocks the host and rents another one. Read [A worker is refused for its CPU or GPU count](troubleshoot.md#a-worker-is-refused-for-its-cpu-or-gpu-count).
- `GPU` is exact. The head refuses a worker that has another number of GPUs.

Do not set `CPU` above 58. Read [Limits](../reference/limits.md#58-worker-ports-for-each-worker).

## Write the offer query

Each key is a vast offer field. Each value is a mapping from an operator to a value.
The operators are in [Configuration](../reference/configuration.md#offer_query).

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

## Make one node type for each CPU size

The provider adds filters to every query, for example `cpu_cores_effective >= CPU`. The list is in [Configuration](../reference/configuration.md#offer_query).
You can add a stricter filter on `cpu_cores_effective`. ray-vast merges it with the added filter. A looser filter is an error.

Use this to make one node type for each CPU size:

```yaml
  gpu-small:
    resources: {CPU: 8, GPU: 1}
    node_config:
      offer_query:
        verified: {eq: true}
        datacenter: {eq: true}
        gpu_ram: {gte: 24000}
        cpu_cores_effective: {gte: 8, lt: 16}
  gpu-large:
    resources: {CPU: 16, GPU: 1}
    node_config:
      offer_query:
        verified: {eq: true}
        datacenter: {eq: true}
        gpu_ram: {gte: 24000}
        cpu_cores_effective: {gte: 16, lt: 32}
```

## Choose a GPU by memory and speed

`GPU: 1` alone is not enough. A GTX 1080 and an H100 are very different.
Set a GPU memory range and a `dlperf` minimum.

- GPU memory tiers in the vast market: 8, 12, 16, 24, 32, 48, 80, 96 and 140+ GB. The speed can be very different inside one tier.
- `dlperf` is vast's deep learning speed score. Use it for speed.
- Do not use `total_flops`. It is FP32 speed without tensor cores.
- The `verified` and `datacenter` filters remove many offers. A narrow query can match no offer.

## Optional filters

| Filter | Use |
|---|---|
| `inet_down: {gte: 500}`, `inet_up: {gte: 500}` | Network speed in Mb/s. A fast download pulls the image sooner. A fast network moves Ray objects between hosts sooner. |
| `static_ip: {eq: true}` | For long jobs. When the IP of a worker changes, ray-vast destroys the worker. |
| `geolocation: {in: [US]}` | Location of the host. |
| `reliability2: {gte: 0.98}` | vast's reliability score of the host. |
| `dph_total: {lte: 0.5}` | Price limit for one instance, in $/hour. |

ray-vast does not add the network speed filters. Add them when your jobs need them.

The provider sorts the offers by `dph_total`, the cheapest first. Read [How the provider picks an offer](../explanation/worker-lifecycle.md#how-the-provider-picks-an-offer).

## Check your node types

`ray-vast up` checks the YAML before it makes anything.
It stops with an error if the declared resources and the query disagree.
It warns if `CPU` is above 58.

If no worker arrives, the query can match no offer. Read [Find logs and troubleshoot](troubleshoot.md).
