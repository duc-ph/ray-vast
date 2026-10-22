# ray-vast

Run a Ray cluster with the head on AWS EC2 and the workers on vast.ai.
You keep the normal Ray workflow. Only the cluster commands change: `ray-vast up`, `ray-vast dashboard` and `ray-vast down`.

**Documentation: https://duc-ph.github.io/ray-vast/**

## Why

Usually, all nodes of a Ray cluster come from one cloud provider, such as AWS, GCP or Azure. The nodes are in one private network. In this network, they communicate simply and securely.

vast.ai is a market with cheaper GPUs. But vast instances have many constraints:

- An instance is not a VM. It is a container on a host machine. Many renters (co-tenants) can share one host.
- The instances communicate over the internet, not over a private network.
- An instance can stop at any time. Its IP address can change.
- The number of ports and the number of processes and threads have hard limits.

## What ray-vast does

- It implements a Ray autoscaler that works with these constraints.
- It solves the communication between the nodes over the internet.
- No critical endpoint is accessible without credentials. All traffic is encrypted. Co-tenants on the same host cannot read it.
- It evaluates the Ray libraries on vast: Core, Data, Train, Tune, Serve and RLlib. The results are in the table below.

The vast host has root access to the worker container. Read the [security model](https://duc-ph.github.io/ray-vast/security/) before you use private data.

## Ray libraries

| Library | One worker | Many workers | What to know |
|---|---|---|---|
| [Ray Core](https://duc-ph.github.io/ray-vast/libraries/core/) | ✅ | ✅ | |
| [Ray Data](https://duc-ph.github.io/ray-vast/libraries/data/) | 🟨 | 🟨 | Write the output to a cloud bucket. A local output path stays on each worker. |
| [Ray Train](https://duc-ph.github.io/ray-vast/libraries/train/) | 🟨 | ❌ | Write checkpoints to a cloud bucket. Training across workers fails behind the NAT of vast. Use one worker with many GPUs. |
| [Ray Tune](https://duc-ph.github.io/ray-vast/libraries/tune/) | 🟨 | 🟨 | Write checkpoints to a cloud bucket. |
| [Ray Serve](https://duc-ph.github.io/ray-vast/libraries/serve/) | ✅ | ✅ | |
| [RLlib](https://duc-ph.github.io/ray-vast/libraries/rllib/) | ✅ | 🟨 | Put all learners on one worker. Learners across workers fail behind the NAT of vast. |

✅ works. 🟨 works with an extra change. ❌ the main use fails.

## Install

```
pip install ray-vast "ray[default]==2.59.*"
```

Keep the Ray pin: Ray on your laptop must match Ray in the cluster image.
Then follow [Getting started](https://duc-ph.github.io/ray-vast/getting-started/).

## Documentation

- [Getting started](https://duc-ph.github.io/ray-vast/getting-started/): your first cluster, step by step.
- Run your work: [submit jobs](https://duc-ph.github.io/ray-vast/libraries/jobs/), [Ray libraries](https://duc-ph.github.io/ray-vast/libraries/), [many tasks or actors](https://duc-ph.github.io/ray-vast/libraries/many-tasks/).
- How-to guides: [choose node types and GPU images](https://duc-ph.github.io/ray-vast/how-to/node-types/), [control cost](https://duc-ph.github.io/ray-vast/how-to/control-cost/), [troubleshoot](https://duc-ph.github.io/ray-vast/how-to/troubleshoot/), [clean up](https://duc-ph.github.io/ray-vast/how-to/clean-up/).
- Reference: [CLI](https://duc-ph.github.io/ray-vast/reference/cli/), [configuration](https://duc-ph.github.io/ray-vast/reference/configuration/), [limits](https://duc-ph.github.io/ray-vast/reference/limits/), [AWS permissions](https://duc-ph.github.io/ray-vast/reference/iam/), [changelog](https://duc-ph.github.io/ray-vast/reference/changelog/).
- How it works: [architecture](https://duc-ph.github.io/ray-vast/explanation/architecture/), [security model](https://duc-ph.github.io/ray-vast/security/). Read the security model before you use the cluster with real data. A vast host has root access to the worker container.

## Status

Tested with Ray 2.59.

## Contributing

Read [CONTRIBUTING.md](https://github.com/duc-ph/ray-vast/blob/main/CONTRIBUTING.md) for the tests and the docs build.

## License

MIT. See [LICENSE](https://github.com/duc-ph/ray-vast/blob/main/LICENSE).
