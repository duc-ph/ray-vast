# Find logs and troubleshoot

## Where the logs are

| Log | How to read it |
|---|---|
| Head container, all output | `ray-vast attach cluster.yaml`, then `sudo docker logs ray-vast-head` |
| Provider and head server | `sudo docker exec ray-vast-head cat /tmp/ray/session_latest/logs/monitor.log` |
| Ray logs of the workers | The Ray dashboard (http://127.0.0.1:8265, through `ray-vast dashboard`) |
| Worker bootstrap (the start line, failed heartbeats) | The vast console (the instance's log), or `vastai logs <instance id>`. Only while the instance exists. |
| Job output | `ray job logs <job id>`, or the dashboard |

The provider and the head server write to `monitor.log`. It has these lines:

- each rental: `create 1 x gpu24: rented 2 candidates`;
- each enrollment decision, with the instance ID, the machine ID, the IP and the reason. Examples: `enrolled instance <id> (machine <id>, ip <ip>) into <slot>` and `enroll refused for instance <id> from <ip>: <reason>`;
- each end of an instance: `ending instance <id> (machine <id>, ip <ip>): <reason>`;
- each reason that a worker sends before it destroys itself: `worker <id> asks to be destroyed: <reason>`;
- a WARNING when a worker is at its process limit: `worker <slot> (instance <id>, ip <ip>) is at its process limit: ...`. Read [A worker is at its process limit](#a-worker-is-at-its-process-limit).

The log never holds a secret.

vast keeps no container log after a destroy. So a worker sends the reason for each destroy to the head first. The reason is in `monitor.log`.

The head container logs in the local time zone of the head.
`monitor.log` has two Ray warnings every 5 seconds: `Dropping the empty legacy field head_node` and `worker_nodes`. They are not errors.

## A worker never arrives

1. **Check the demand.** Run `sudo docker exec ray-vast-head ray status` on the head. Look at "Pending" and "Recent failures".
2. **Check the offers.** A narrow query can match no offer. The datacenter filter removes many offers. In `monitor.log`, look for `no rentable offer matches the node type query`. Make the query wider.
3. **Check the budget.** Look for `every matching offer would pass max_dollars_per_hour`. Raise the budget or choose cheaper GPUs.
4. **Check the enrollment.** Look for `enroll refused` and `ending instance`. A host can fail a host check (egress IP, capabilities, own network namespace, `/dev/shm`, the 64 identity ports). The provider puts the host on the blocklist for this cluster and rents another one.
5. **Check the pull time.** A big image on a slow host can pass the pull limit. Add `inet_down: {gte: 500}` to the query. Read [Use GPU images](gpu-images.md).

### `LaunchFailed` in `ray status`

`ray status` shows `<node type>: LaunchFailed` under "Recent failures". It does not show the reason. The reason is in `monitor.log`:

| Text in `monitor.log` | Meaning | What to do |
|---|---|---|
| `no rentable offer matches the node type query` | No offer matches. | Make the offer query wider. |
| `every matching offer would pass max_dollars_per_hour` | The budget stops the rental. | Raise `max_dollars_per_hour`, or wait for workers to end. |
| `slots in a row ended with no enrolled worker` | The rent-loop breaker. 3 slots of this node type ended without a worker. The provider stops this node type for 10 minutes. | Read the `enroll refused` and `ending instance` lines before it. Fix the cause (often the image). |

## Jobs wait

- The budget can stop new workers. Check `max_dollars_per_hour`.
- `max_workers` can stop new workers.
- A worker has a fixed number of ports for Ray worker processes. Tasks wait when no port is free. Read [Limits](../reference/limits.md#58-worker-ports-for-each-worker) and [A worker is at its process limit](#a-worker-is-at-its-process-limit).

## A worker is at its process limit

A vast container has a limit on processes and threads (the process limit, `pids.max`). Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts).
ray-vast keeps the worker below the limit with fewer worker ports. So too much work queues or hangs. It does not crash the worker.

### The warning in `monitor.log`

```text
WARNING ... worker slot-3-a1b2c3 (instance 27145501, ip 203.0.113.7) is at its process limit: 12 refused forks/threads (+12), pids 1279 of 1280, worker ports in use 14 of 14. Ray work on this worker can queue, hang or fail. See https://duc-ph.github.io/ray-vast/how-to/troubleshoot/#a-worker-is-at-its-process-limit
```

| Part | Meaning |
|---|---|
| `worker slot-3-a1b2c3` | The slot of the worker (its `RAY_CLOUD_INSTANCE_ID`). `instance` is the vast instance ID. |
| `12 refused forks/threads (+12)` | The container refused this many process or thread starts since it started. `+12`: the rise since the last warning. A value above 0 means that something failed to start a thread: a Ray process or your code. |
| `pids 1279 of 1280` | Processes and threads now, and the limit. |
| `worker ports in use 14 of 14` | Ray worker processes that hold a port, and the number of ports. When all are in use, new tasks and actors wait. |

The head writes the line for a worker in these cases:

- The refused count goes up. At most once a minute.
- All worker ports are in use, or the pids are above 90 % of the limit. At most once in 5 minutes.

### The start line of the worker

The worker log has one line with the values that ray-vast chose:

```text
bootstrap: limits: pids.max 1280 (cgroup v1), cpus 5, worker ports 14, soft limit 5, grpc threads 4, startup concurrency 8
```

`pids.max none` means that the container has no limit. Then the worker gets all 58 worker ports.

### What to do

- **All ports in use, no refused forks:** the work queues. This is safe. To finish faster, use fewer worker processes at the same time, or rent bigger hosts with a higher limit.
- **Refused forks or high pids:** your tasks start many threads, or too many processes run. Use the settings below.
- **A job hangs with all ports in use:** the job needs more processes at the same time than the worker has ports. Use the settings below.

Each library page gives the settings that use fewer worker processes:

| Library | Settings |
|---|---|
| Ray Core | [Ray Core](../libraries/core.md#settings) |
| Ray Data | [Ray Data](../libraries/data.md#settings) |
| Ray Train | [Ray Train](../libraries/train.md#settings) |
| Ray Tune | [Ray Tune](../libraries/tune.md#settings) |
| Ray Serve | [Ray Serve](../libraries/serve.md#settings) |
| RLlib | [RLlib](../libraries/rllib.md#settings) |

Your own threads count too. Use fewer threads in each task, for example `torch.set_num_threads(4)` and a small `DataLoader` `num_workers`.

Or rent hosts with fewer cores, so that one worker gets less work. Add an upper bound to `cpu_cores_effective` in the node type's `offer_query`, for example `cpu_cores_effective: {gte: 4, lte: 24}` for a node type with `CPU: 4`. Keep `gte` at least `CPU`. Read [Choose node types and offer queries](node-types.md).

## An actor hangs or fails with a misleading error

An actor hangs, or fails with:

```text
Failed to startup worker after retrying 5 times
```

The real cause can be that the worker has no free port (`No available ports`). The cause is only in the worker's Ray log. Read [Limits](../reference/limits.md#58-worker-ports-for-each-worker).

## A multi-GPU job hangs at the first NCCL call

A job with 2 or more GPUs on one worker can hang at its first NCCL call (for example the first all-gather in PyTorch DDP).
In one test, this happened on one host only. The same code passed on another 2-GPU host.
The cause is probably the GPU-to-GPU link of that host.

What to do:

1. Set `NCCL_P2P_DISABLE=1` in the job's `runtime_env` `env_vars`, and run the job again. NCCL then sends the data through host memory. This is slower, but it does not use the GPU-to-GPU link.
2. Or run the job on another host. End the worker, and the autoscaler rents a new one.

## `ray job submit` fails with an authentication error

Run the `export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=...` line that `ray-vast dashboard` printed. Do it in the terminal where you run `ray job submit`.

## `ray-vast` cannot connect to the head

- `up` adds an SSH rule for the public IP of your laptop when it starts. If your IP changes, SSH to the head times out. Add a rule for your new IP to the security group `ray-vast-<cluster_name>-0` (TCP 22, your IP /32).
- `attach` and `dashboard` use the SSH key that `up` made, `~/.ray-vast/<cluster_name>/ssh_key.pem`. Run them on the machine that ran `up`.

## `up` fails

| Error | Cause | What to do |
|---|---|---|
| ``cluster <name> already exists; use `ray-vast down` first`` | An EC2 head with the cluster tag, or `~/.ray-vast/<cluster_name>.json`, exists. | Run `ray-vast down cluster.yaml`. |
| `the head image build failed while installing ray-vast==<version> from PyPI` | The `docker build` on the head failed. Possible causes: the version is not on PyPI, PyPI is not reachable, or the base image pull failed. | Read the build output below the message. Run `ray-vast down cluster.yaml`. Read [CLI](../reference/cli.md). |
| `no vast key: set VAST_API_KEY or put it in ~/.config/vastai/vast_api_key` | No vast key. | Set the key. |
| `AWS error ...` and `The AWS user lacks a permission` | The AWS user does not have a permission that ray-vast needs. | Read [IAM permissions](../reference/iam.md). |
| `the image check printed no single RAYVAST_IMAGE_INFO ... line` | The image has no Python with `ray` and `ray_vast`, or its ENTRYPOINT does not run the command. | Read [Use GPU images](gpu-images.md). |

After a failed `up`, run `ray-vast down cluster.yaml`. Read [Clean up after a failure](clean-up.md).

## The head stopped

Run `ray-vast down` and `ray-vast up`.
Read [When the head dies](../explanation/worker-lifecycle.md#when-the-head-dies) for the cause and for what happens to the workers.
