# ray-vast

(work in progress)

Run a Ray cluster with the head on AWS EC2 and GPU workers on vast.ai.

- The Ray head runs on one EC2 instance in your AWS account.
- The Ray autoscaler rents GPU workers from vast.ai when your jobs need them, and removes them when they are idle.
- The workers talk to the head over the public internet, across NAT, with TLS and a token.
- You do not need a VPN or an overlay network.

**Documentation: https://duc-ph.github.io/ray-vast/**

## Why

- vast.ai has cheap GPUs. But a vast instance can stop at any time, and its IP can change.
- ray-vast keeps the head on EC2, so the cluster stays up. Only workers come and go.
- You keep the normal Ray workflow. Only the cluster commands change.
- A worker that loses the head destroys its own instance. You do not pay for lost workers.

## Install

```
pip install ray-vast "ray[default]==2.59.*"
```

The Ray pin keeps Ray on your laptop at the same version as Ray in the cluster image. ray-vast is tested with Ray 2.59.

The CLI is tested on Linux. macOS is not tested.
You need Python 3.10 or later, an AWS profile, the IAM policy `ray-vast-head-boundary` in your AWS account (a one-time step), and a vast.ai API key.
Details: [Getting started](https://duc-ph.github.io/ray-vast/getting-started/).

## Quick start

Copy an example cluster YAML (`gpu-24gb.yaml`, `gpu-80gb.yaml` or `cpu-heavy.yaml` in `ray_vast/examples/`).
Set the region of the head (`node_config.region` of the head node type) and the budget (`max_dollars_per_hour`). `up` makes an SSH key for the cluster.

```
ray-vast up cluster.yaml --profile my-aws-profile
ray-vast dashboard cluster.yaml   # keep this terminal open; uses the profile that up saved
```

In a second terminal, run the `export` line that `dashboard` printed, and submit a job:

```
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=...     # the line that dashboard printed
ray job submit --address http://127.0.0.1:8265 --working-dir . -- python train.py
```

When you finish:

```
ray-vast down cluster.yaml
```

| Plain Ray | ray-vast |
|---|---|
| `ray up` | `ray-vast up` |
| `ray dashboard` | `ray-vast dashboard` |
| `ray down` | `ray-vast down` |

Some `ray` cluster commands do not work. Read [limits](https://duc-ph.github.io/ray-vast/reference/limits/#some-ray-commands-do-not-work). Use `--working-dir` and `runtime_env` to send code to the cluster.

## Ray libraries

Each library page shows how to run your work. It also gives the settings that keep a worker in its process limit.

| Library | One worker | Many workers | Notes |
|---|---|---|---|
| [Ray Core](https://duc-ph.github.io/ray-vast/libraries/core/) | [✅](https://duc-ph.github.io/ray-vast/libraries/core/#status) | [✅](https://duc-ph.github.io/ray-vast/libraries/core/#status) |  |
| [Ray Jobs](https://duc-ph.github.io/ray-vast/libraries/jobs/) | [✅](https://duc-ph.github.io/ray-vast/libraries/jobs/#status) | [✅](https://duc-ph.github.io/ray-vast/libraries/jobs/#status) |  |
| [Ray Data](https://duc-ph.github.io/ray-vast/libraries/data/) | [🟨](https://duc-ph.github.io/ray-vast/libraries/data/#status) | [🟨](https://duc-ph.github.io/ray-vast/libraries/data/#status) | Write the output to S3. A local output path stays on each worker. |
| [Ray Train](https://duc-ph.github.io/ray-vast/libraries/train/) | [🟨](https://duc-ph.github.io/ray-vast/libraries/train/#status) | [❌](https://duc-ph.github.io/ray-vast/libraries/train/#status) | Use S3 storage for checkpoints. DDP across workers fails behind the NAT of vast: use one worker with many GPUs. |
| [Ray Tune](https://duc-ph.github.io/ray-vast/libraries/tune/) | [🟨](https://duc-ph.github.io/ray-vast/libraries/tune/#status) | [🟨](https://duc-ph.github.io/ray-vast/libraries/tune/#status) | Use S3 storage for checkpoints. |
| [Ray Serve](https://duc-ph.github.io/ray-vast/libraries/serve/) | [✅](https://duc-ph.github.io/ray-vast/libraries/serve/#status) | [✅](https://duc-ph.github.io/ray-vast/libraries/serve/#status) |  |
| [RLlib](https://duc-ph.github.io/ray-vast/libraries/rllib/) | [✅](https://duc-ph.github.io/ray-vast/libraries/rllib/#status) | [🟨](https://duc-ph.github.io/ray-vast/libraries/rllib/#status) | Put all learners on one worker. Learners across workers fail behind the NAT of vast. |

✅ works (the process settings on its page do not lower the mark). 🟨 works with an extra change, or a part fails. ❌ the main use fails.

## Documentation

- [Getting started](https://duc-ph.github.io/ray-vast/getting-started/): your first cluster, step by step.
- Ray libraries: [Core](https://duc-ph.github.io/ray-vast/libraries/core/), [Jobs](https://duc-ph.github.io/ray-vast/libraries/jobs/), [Data](https://duc-ph.github.io/ray-vast/libraries/data/), [Train](https://duc-ph.github.io/ray-vast/libraries/train/), [Tune](https://duc-ph.github.io/ray-vast/libraries/tune/), [Serve](https://duc-ph.github.io/ray-vast/libraries/serve/), [RLlib](https://duc-ph.github.io/ray-vast/libraries/rllib/).
- How-to guides: [choose node types](https://duc-ph.github.io/ray-vast/how-to/node-types/), [use GPU images](https://duc-ph.github.io/ray-vast/how-to/gpu-images/), [control cost](https://duc-ph.github.io/ray-vast/how-to/control-cost/), [troubleshoot](https://duc-ph.github.io/ray-vast/how-to/troubleshoot/), [clean up](https://duc-ph.github.io/ray-vast/how-to/clean-up/).
- Reference: [CLI](https://duc-ph.github.io/ray-vast/reference/cli/), [configuration](https://duc-ph.github.io/ray-vast/reference/configuration/), [limits](https://duc-ph.github.io/ray-vast/reference/limits/), [IAM permissions](https://duc-ph.github.io/ray-vast/reference/iam/), [changelog](https://duc-ph.github.io/ray-vast/reference/changelog/).
- Explanation: [architecture](https://duc-ph.github.io/ray-vast/explanation/architecture/), [enrollment](https://duc-ph.github.io/ray-vast/explanation/enrollment/), [worker life cycle](https://duc-ph.github.io/ray-vast/explanation/worker-lifecycle/), [design decisions](https://duc-ph.github.io/ray-vast/explanation/design-decisions/).
- [Security model](https://duc-ph.github.io/ray-vast/security/). Read it before you use the cluster with real data. A vast host has root access to the worker container.

## Status

Version 0.2.0. Tested with Ray 2.59.

## Contributing

Read [CONTRIBUTING.md](https://github.com/duc-ph/ray-vast/blob/main/ray-vast-pypi/CONTRIBUTING.md) for the tests and the docs build.

## License

MIT. See [LICENSE](https://github.com/duc-ph/ray-vast/blob/main/ray-vast-pypi/LICENSE).
