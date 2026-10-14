# ray-vast

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
pip install ray-vast
```

You need Python 3.10 or later, an AWS profile, an EC2 key pair, and a vast.ai API key.
Details: [Getting started](https://duc-ph.github.io/ray-vast/getting-started/).

## Quick start

Copy an example cluster YAML (`gpu-24gb.yaml`, `gpu-80gb.yaml` or `cpu-heavy.yaml` in `ray_vast/examples/`).
Set the region, the budget (`max_dollars_per_hour`) and your EC2 key pair (`key_name`, `key_file`).

```
ray-vast up cluster.yaml --profile my-aws-profile
ray-vast dashboard cluster.yaml --profile my-aws-profile   # keep this terminal open
```

In a second terminal, run the `export` line that `dashboard` printed, and submit a job:

```
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=...     # the line that dashboard printed
ray job submit --address http://127.0.0.1:8265 --working-dir . -- python train.py
```

When you finish:

```
ray-vast down cluster.yaml --profile my-aws-profile
```

| Plain Ray | ray-vast |
|---|---|
| `ray up` | `ray-vast up` |
| `ray dashboard` | `ray-vast dashboard` |
| `ray down` | `ray-vast down` |

`ray attach`, `ray exec` and `ray rsync-*` do not work. Use `--working-dir` and `runtime_env`.

## Documentation

- [Getting started](https://duc-ph.github.io/ray-vast/getting-started/): your first cluster, step by step.
- How-to guides: [run your Ray workload](https://duc-ph.github.io/ray-vast/how-to/run-your-workload/), [choose node types](https://duc-ph.github.io/ray-vast/how-to/node-types/), [use GPU images](https://duc-ph.github.io/ray-vast/how-to/gpu-images/), [control cost](https://duc-ph.github.io/ray-vast/how-to/control-cost/), [troubleshoot](https://duc-ph.github.io/ray-vast/how-to/troubleshoot/), [clean up](https://duc-ph.github.io/ray-vast/how-to/clean-up/).
- Reference: [CLI](https://duc-ph.github.io/ray-vast/reference/cli/), [configuration](https://duc-ph.github.io/ray-vast/reference/configuration/), [Ray library support](https://duc-ph.github.io/ray-vast/reference/ray-libraries/), [limits](https://duc-ph.github.io/ray-vast/reference/limits/), [IAM permissions](https://duc-ph.github.io/ray-vast/reference/iam/), [changelog](https://duc-ph.github.io/ray-vast/reference/changelog/).
- Explanation: [architecture](https://duc-ph.github.io/ray-vast/explanation/architecture/), [enrollment](https://duc-ph.github.io/ray-vast/explanation/enrollment/), [worker life cycle](https://duc-ph.github.io/ray-vast/explanation/worker-lifecycle/), [design decisions](https://duc-ph.github.io/ray-vast/explanation/design-decisions/).
- [Security model](https://duc-ph.github.io/ray-vast/security/). Read it before you use the cluster with real data. A vast host has root access to the worker container.

## Status

Version 0.1.0rc1, a release candidate of 0.1.0. Tested with Ray 2.59.

## Contributing

Read [CONTRIBUTING.md](https://github.com/duc-ph/ray-vast/blob/main/ray-vast-pypi/CONTRIBUTING.md) for the tests and the docs build.

## License

MIT. See [LICENSE](https://github.com/duc-ph/ray-vast/blob/main/ray-vast-pypi/LICENSE).
