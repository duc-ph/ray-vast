# ray-vast

(work in progress)

ray-vast runs a Ray cluster with the head on AWS EC2 and the workers on vast.ai.

- The Ray head runs on one EC2 instance in your AWS account.
- The Ray autoscaler rents GPU workers from vast.ai when your jobs need them.
- The autoscaler removes the workers when they are idle.
- The workers talk to the head over the public internet, across NAT, with TLS and a token.
- You do not need a VPN or an overlay network.

You keep the normal Ray workflow. Only the cluster commands change: `ray-vast up`, `ray-vast dashboard` and `ray-vast down`.

## Who is it for

ray-vast is for you if:

- you use Ray, and you want cheap GPUs from the vast.ai market;
- you want a stable head that vast cannot stop;
- you accept the security model of rented hosts (read [Security model](security.md) first).

ray-vast is not for you if your data must not go to third-party hosts. A vast host has root access to the worker container.

## How it works

```mermaid
flowchart LR
  laptop["Your laptop<br/>ray-vast CLI<br/>ray job submit"]
  subgraph aws["AWS EC2"]
    head["Head<br/>Ray head + autoscaler<br/>ray-vast provider<br/>head server"]
  end
  subgraph vast["vast.ai"]
    w1["Worker 1<br/>watchdog + Ray"]
    w2["Worker 2<br/>watchdog + Ray"]
  end
  laptop -- "SSH tunnel (dashboard, jobs)" --> head
  head -- "rent and destroy (vast API)" --> vast
  w1 -- "enroll, heartbeat (TLS)" --> head
  w2 -- "enroll, heartbeat (TLS)" --> head
  w1 <-- "Ray traffic (TLS + token)" --> w2
```

1. `ray-vast up` makes the EC2 head and starts Ray on it.
2. Your job asks for resources. The autoscaler asks the ray-vast provider for a worker.
3. The provider rents vast instances. Each instance is a candidate. It downloads the bootstrap (a small program) from the head. The bootstrap checks the host and enrolls the candidate.
4. The enrolled candidate is now a worker. It joins Ray and runs your tasks.
5. Ray removes an idle worker after `idle_timeout_minutes`. A worker that loses the head destroys itself.

Read [Architecture](explanation/architecture.md) for the full picture.

## Quick start

You need an AWS profile, the boundary policy (a one-time AWS step), and a vast API key in `~/.config/vastai/vast_api_key`.
The full steps are in [Getting started](getting-started.md).

```bash
pip install ray-vast "ray[default]==2.59.*"
ray-vast up cluster.yaml --profile my-aws-profile
ray-vast dashboard cluster.yaml --profile my-aws-profile   # terminal 1: keep it open
```

In a second terminal:

```bash
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=~/.ray-vast/<cluster_name>/token   # the line that dashboard printed
ray job submit --address http://127.0.0.1:8265 --working-dir . -- python train.py
```

When you finish:

```bash
ray-vast down cluster.yaml --profile my-aws-profile
```

## Ray libraries

Each library page shows how to run your work. It also gives the settings that keep a worker in its process limit.

| Library | One worker | Many workers | Notes |
|---|---|---|---|
| [Ray Core](libraries/core.md) | ✅ | ✅ |  |
| [Ray Jobs](libraries/jobs.md) | ✅ | ✅ |  |
| [Ray Data](libraries/data.md) | 🟨 | 🟨 | `groupby` and other shuffles also need `override_num_blocks`. A local output path stays on each worker. |
| [Ray Train](libraries/train.md) | 🟨 | ❌ | Checkpoints need S3. DDP over 2 or more workers fails behind the NAT of vast. |
| [Ray Tune](libraries/tune.md) | 🟨 | 🟨 | Checkpoints need S3. |
| [Ray Serve](libraries/serve.md) | ✅ | ✅ |  |
| [RLlib](libraries/rllib.md) | ✅ | 🟨 | Env runners on many workers work. Learners on 2 or more workers fail behind the NAT of vast. |

✅ works (with the settings on its page). 🟨 works with an extra change, or a part fails. ❌ the main use fails.

## Links

- [Getting started](getting-started.md): your first cluster, step by step.
- [Ray libraries](libraries/jobs.md): run jobs, and the settings for each Ray library.
- [How-to guides](how-to/node-types.md): choose GPUs, control cost, troubleshoot.
- [Reference](reference/cli.md): the CLI, the configuration, the limits.
- [Explanation](explanation/architecture.md): the architecture, enrollment, the watchdog, security.
- Source code: [github.com/duc-ph/ray-vast](https://github.com/duc-ph/ray-vast).
