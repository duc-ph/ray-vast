# Use GPU images

One Docker image runs on the head and on every worker. You set it in `provider.image`.
`ray-vast up` builds a derived image on the head: your image plus the ray-vast package.
So the Python and Ray versions are the same on all nodes.

## Image rules

The image must have:

- `sh`;
- a Python that can `import ray`. The worker looks for `python3` and `python` on `$PATH`, then `/home/ray/anaconda3/bin/python`, `/opt/conda/bin/python`, `/usr/local/bin/python3` and `/usr/bin/python3`;
- `pip`, because `up` installs ray-vast into the derived image with `pip`;
- Ray 2.59. `ray-vast up` warns if the image has another Ray version. Ray 2.59 is the only tested version;
- no `ENTRYPOINT`, or an `ENTRYPOINT` that ends with `exec "$@"`.

The image does not need sshd, apt, `sudo` or curl.

## The ENTRYPOINT

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

If the ENTRYPOINT does not run the command, the candidate never enrolls. The provider destroys it at a time limit, or when vast shows it as exited.

## Recommended images

| Image | Use |
|---|---|
| `rayproject/ray:2.59.0-py311-gpu` | GPU workers. About 18 GB. |
| `rayproject/ray:2.59.0-py311-cpu` | CPU-only tests. Smaller and faster to pull. |

## Your own image

Build your image `FROM rayproject/ray:2.59.0-py311-gpu`, and add your packages:

```dockerfile
FROM rayproject/ray:2.59.0-py311-gpu
RUN pip install --no-cache-dir torch transformers
```

Push it to a public registry, for example a public Docker Hub repository. ray-vast does not send registry credentials, so the head and vast cannot pull a private image.
Set `provider.image` to its name.

## Image size and the pull limit

A big image takes long to pull. The provider gives each candidate a pull limit. The limit comes from `image_size_gb` and the download speed of the offer. The formula is in [Configuration](../reference/configuration.md#provider).
`up` measures the image size and sets `image_size_gb`.

`up` also takes longer with a big image, because the head pulls it. `up` takes about 8 to 10 minutes with the 18 GB GPU image, and about 2.5 minutes with the CPU image.

To pull faster, add `inet_down: {gte: 500}` to the offer query. Read [Choose node types and offer queries](node-types.md).
