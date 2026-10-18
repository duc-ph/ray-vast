# Choose node types and GPU images

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

## Keep verified and datacenter in the offer query

ray-vast does not add `verified` or `datacenter` to the query. You set them. We highly recommend `verified: {eq: true}` and `datacenter: {eq: true}` in every node type. The example YAMLs have them.

We rented about 430 vast machines and measured these rates:

| Hosts | Not started in 12 min | All identity ports reachable from outside |
|---|---|---|
| Verified | 2 % | 99 % |
| Unverified | 9 % | 77 % |
| Deverified | 50 % | 87 % |
| Datacenter | 10 % | 100 % |
| Non-datacenter | 21 % | 89 % |

A host that does not start, or that fails the port check, costs a retry. Then the worker arrives later. These two fields also make a dishonest host less likely. Read [Security model](../security.md).

## CPU and GPU

The provider adds `cpu_cores_effective >= max(CPU, 5)` and `num_gpus == GPU` to the query.

- Ray gets the declared `CPU` on each worker. A host with more cores gives no more Ray CPUs. To use more cores, declare a larger `CPU`.
- Ray plans new workers with the same `CPU` value. So Ray starts the number of workers that the work needs.
- vast gives a container a CPU quota of 0.96 × the offer's effective cores. For example, an offer with 8 effective cores gives 7.68 cores. Ray still gets 8 CPUs.
- The head refuses a worker that has fewer cores than 90 % of `CPU`. The cores are the smaller of the CPU affinity and the CPU quota of the container. The provider blocks the host and rents another one. Read [A worker is refused for its CPU or GPU count](troubleshoot.md#a-worker-is-refused-for-its-cpu-or-gpu-count).
- `GPU` is exact. The head refuses a worker that has another number of GPUs.

### Why the provider adds these filters

Each worker runs a limited number of Ray processes at the same time. Read [Many tasks or actors](../libraries/many-tasks.md).
The filters make sure that every worker runs at least `CPU` processes at once, for `CPU` up to 54:

- The core floor: vast sets the process limit of the container from its CPU quota. More cores give more processes. A host with 5 effective cores runs 6 processes at once. A host with 4 runs only 2, so the floor is 5.
- The port floor `direct_port_count >= 10 + min(CPU, 54)`: each process needs one port.

A worker never runs more than 54 processes at once. If you set `CPU` above 54, `ray-vast up` warns. Then use `num_cpus` above 1 for your tasks and actors. Read [Many tasks or actors](../libraries/many-tasks.md).

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

The provider adds fields to every offer query, for example `cpu_cores_effective >= max(CPU, 5)`. The list is in [Configuration](../reference/configuration.md#offer_query).
You can set a stricter `cpu_cores_effective` in your offer query. ray-vast merges it with the added value. A looser value is an error.

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
- `verified` and `datacenter` in the offer query remove many offers. A narrow offer query can match no offer.

## Optional fields

| Field | Use |
|---|---|
| `inet_down: {gte: 500}`, `inet_up: {gte: 500}` | Network speed in Mb/s. A fast download pulls the image sooner. A fast network moves Ray objects between hosts sooner. |
| `static_ip: {eq: true}` | For long jobs. When the IP of a worker changes, ray-vast destroys the worker. |
| `geolocation: {in: [US]}` | Location of the host. |
| `reliability2: {gte: 0.98}` | vast's reliability score of the host. |
| `dph_total: {lte: 0.5}` | Price limit for one instance, in $/hour. |

ray-vast does not add the network speed fields. Add them to the offer query when your jobs need them.

The provider sorts the offers by `dph_total`, the cheapest first. Read [How the provider picks an offer](../explanation/architecture.md#how-the-provider-picks-an-offer).

## Check your node types

`ray-vast up` checks the YAML before it makes anything.
It stops with an error if the declared resources and the query disagree.
It warns if `CPU` is above 54.

If no worker arrives, the query can match no offer. Read [Find logs and troubleshoot](troubleshoot.md).

## Use GPU images

One Docker image runs on the head and on every worker. You set it in `provider.image`.
`ray-vast up` builds a derived image on the head: your image plus the ray-vast package.
So the Python and Ray versions are the same on all nodes.

### Image rules

The image must have:

- `sh`;
- a Python that can `import ray`. The worker looks for `python3` and `python` on `$PATH`, then `/home/ray/anaconda3/bin/python`, `/opt/conda/bin/python`, `/usr/local/bin/python3` and `/usr/bin/python3`;
- `pip`, because `up` installs ray-vast into the derived image with `pip`;
- Ray 2.59. `ray-vast up` warns if the image has another Ray version. Ray 2.59 is the only tested version;
- no `ENTRYPOINT`, or an `ENTRYPOINT` that ends with `exec "$@"`.

The image does not need sshd, apt, `sudo` or curl.

### The ENTRYPOINT

ray-vast keeps the image's `ENTRYPOINT`. Docker runs the ENTRYPOINT with the container command as its arguments.
ray-vast sets the container command on the head and on the workers.

So the ENTRYPOINT must start the command at its end, with `exec "$@"`:

```bash
#!/bin/bash
# prepare the GPU environment here
exec "$@"
```

`rayproject/ray:*-gpu` images have the ENTRYPOINT `/opt/nvidia/nvidia_entrypoint.sh`. It prepares CUDA and ends with `exec "$@"`. These images work.

An ENTRYPOINT can write text to stdout before `exec "$@"`. This is fine. `up` reads only one marked line from the output of its image check.

If the ENTRYPOINT does not run the command, the image check of `up` prints no `RAYVAST_IMAGE_INFO` line, and `up` stops with an error. Read [`up` fails](troubleshoot.md#up-fails).

### Recommended images

| Image | Use |
|---|---|
| `rayproject/ray:2.59.0-py311-gpu` | GPU workers. About 18 GB. |
| `rayproject/ray:2.59.0-py311-cpu` | CPU-only tests. Smaller and faster to pull. |

### Your own image

Build your image `FROM rayproject/ray:2.59.0-py311-gpu`, and add your packages:

```dockerfile
FROM rayproject/ray:2.59.0-py311-gpu
RUN pip install --no-cache-dir torch transformers
```

Push it to a public registry, for example a public Docker Hub repository. ray-vast does not send registry credentials, so the head and vast cannot pull a private image.
Set `provider.image` to its name.

### Image size and the pull limit

A big image takes long to pull. The provider gives each candidate a pull limit. The limit comes from the image size and the download speed of the offer: `2 × size in GB × 8000 / inet_down` seconds, between 5 and 15 minutes.
`up` measures the image size on the head. You cannot set it.

`up` also takes longer with a big image, because the head pulls it. In our tests, `up` took 8 to 10 minutes with the 18 GB GPU image, and 2.5 to 6.5 minutes with the CPU image.

To pull faster, add `inet_down: {gte: 500}` to the offer query. Read [Optional fields](#optional-fields).
