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
So Ray can start more workers than necessary. In a local spike (one machine, Ray 2.59), Ray started 2 workers for 4 tasks.
The extra worker stays idle until `idle_timeout_minutes` removes it. This costs a few minutes of one worker.

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

- GPU memory tiers in the vast market: 8, 12, 16, 24, 32, 48, 80, 96 and 140+ GB. The speed can be very different inside one tier.
- `dlperf` is vast's deep learning speed score. Use it for speed.
- Do not use `total_flops`. It is FP32 speed without tensor cores.
- The datacenter filter removes many offers. A narrow query can match no offer.

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
