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
2. **Check the offers.** A narrow offer query can match no offer. `verified` and `datacenter` in the offer query remove many offers. In `monitor.log`, look for `no rentable offer matches the node type query`. Make the query wider.
3. **Check the budget.** Look for `every matching offer would pass max_dollars_per_hour`. Raise the budget or choose cheaper GPUs.
4. **Check the enrollment.** Look for `enroll refused` and `ending instance`. A host can fail a host check (egress IP, capabilities, own network namespace, `/dev/shm`, the identity ports, the CPU or GPU count). The provider puts the host on the blocklist for this cluster and rents another one.
5. **Check the pull time.** A big image on a slow host can pass the pull limit. Add `inet_down: {gte: 500}` to the query. Read [Image size and the pull limit](node-types.md#image-size-and-the-pull-limit).

### `LaunchFailed` in `ray status`

`ray status` shows `<node type>: LaunchFailed` under "Recent failures". It does not show the reason. The reason is in `monitor.log`:

| Text in `monitor.log` | Meaning | What to do |
|---|---|---|
| `no rentable offer matches the node type query` | No offer matches. | Make the offer query wider. |
| `every matching offer would pass max_dollars_per_hour` | The budget stops the rental. | Raise `max_dollars_per_hour`, or wait for workers to end. |
| `slots in a row ended with no enrolled worker` | The rent-loop breaker. 3 slots of this node type ended without a worker. The provider stops this node type for 10 minutes. | Read the `enroll refused` and `ending instance` lines before it. Fix the cause (often the image). |

### A worker is refused for its CPU or GPU count

`monitor.log` has one of these lines:

```text
ending instance <id> (machine <id>, ip <ip>): 'CPU: the container has <cores> cores, the node type declares <CPU> (at least 90% needed)'
ending instance <id> (machine <id>, ip <ip>): 'GPU count: the container has <n>, the node type declares <GPU>'
```

The host gave the container fewer cores, or another number of GPUs, than its offer.
The cores are the smaller of the CPU affinity and the CPU quota of the container. vast usually gives 0.96 × the offer's effective cores.
The provider destroys the instance, blocks the host for 30 days, and rents another one. You do not have to do anything.

If many hosts are refused for their CPU, vast gives less CPU than before. Then lower `CPU`, or set a stricter `cpu_cores_effective` in the offer query, for example `cpu_cores_effective: {gte: <CPU + 2>}`. Read [CPU and GPU](node-types.md#cpu-and-gpu).

## Jobs wait

- The budget can stop new workers. Check `max_dollars_per_hour`.
- `max_workers` can stop new workers.
- A worker runs a limited number of Ray processes at the same time. Tasks wait when the worker is full. Read [Many tasks or actors](../libraries/many-tasks.md) and [A worker is at its process limit](#a-worker-is-at-its-process-limit).

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
bootstrap: limits: pids.max 1280 (cgroup v1), cpus 5, processes at once 10, worker ports 14, soft limit 5, grpc threads 4, startup concurrency 8
```

`processes at once` is *N*: the number of your tasks and actors that the worker runs at the same time. Read [Many tasks or actors](../libraries/many-tasks.md).
`pids.max none` means that the container has no limit. Then the worker gets all its worker ports: the identity ports minus 6, at most 58.

### What to do

- **All ports in use, no refused forks:** the work queues. This is safe. To finish faster, use fewer worker processes at the same time, or rent bigger hosts with a higher limit.
- **Refused forks or high pids:** your tasks start many threads, or too many processes run. Use the settings below.
- **A job hangs with all ports in use:** the job needs more processes at the same time than the worker has ports. Use the settings below.

[Many tasks or actors](../libraries/many-tasks.md) tells you when your work needs settings. The library pages give the settings that use fewer worker processes:

| Library | Settings |
|---|---|
| Ray Core | [Ray Core](../libraries/core.md#settings) |
| Ray Data | [Ray Data](../libraries/data.md#settings) |
| Ray Train | [Ray Train](../libraries/train.md#settings) |
| Ray Tune | [Ray Tune](../libraries/tune.md#settings) |
| Ray Serve | [Ray Serve](../libraries/serve.md#settings) |
| RLlib | [RLlib](../libraries/rllib.md#settings) |

Your own threads count too, also when worker ports are free. Use fewer threads in each task, for example a small `DataLoader` `num_workers`. Look for code that sets a high thread count, for example `torch.set_num_threads()`. Read [Threads](../libraries/many-tasks.md#threads).

Or rent hosts with fewer cores, so that one worker gets less work. Add an upper bound to `cpu_cores_effective` in the node type's `offer_query`, for example `cpu_cores_effective: {gte: 5, lte: 24}` for a node type with `CPU: 4`. Keep `gte` at least the larger of `CPU` and 5. Read [Choose node types and GPU images](node-types.md).

## An actor hangs or fails with a misleading error

An actor hangs, or fails with:

```text
Failed to startup worker after retrying 5 times
```

The real cause can be that the worker has no free port (`No available ports`). The cause is only in the worker's Ray log. Read [At most 54 processes at once on a worker](../reference/limits.md#at-most-54-processes-at-once-on-a-worker).

## A multi-GPU job hangs at the first NCCL call

A job with 2 or more GPUs on one worker can hang at its first NCCL call (for example the first all-gather in PyTorch DDP).
In our tests, this happened on one host only. The same code passed on another 2-GPU host.
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
| `region <region> has no default VPC` | ray-vast puts the head in the default VPC of the region. Somebody deleted it. | Run the command in the message: `aws ec2 create-default-vpc --region <region>`. Then run `up` again. |
| `the IAM policy ray-vast-head-boundary does not exist ... these credentials cannot create it` | A restricted user runs the first `up` in the account. | An admin creates the policy once. Read [The boundary policy](../reference/iam.md#the-boundary-policy). |
| `no AWS credentials. Put AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env` | No AWS access key in `.env`, and no other AWS credentials. Or you ran `ray-vast` in another directory. | Put the keys in `.env`. Read [Set up AWS](../getting-started.md#aws-setup). Run `ray-vast` in the directory of `.env`. |
| `no vast key: put VAST_API_KEY in .env in the current directory, or set VAST_API_KEY` | No vast key. | Put `VAST_API_KEY=<your key>` in `.env`. Read [Save the vast API key](../getting-started.md#3-save-the-vast-api-key). |
| `AWS error ...` and `The AWS user lacks a permission` | The AWS user does not have a permission that ray-vast needs. | Read [AWS permissions](../reference/iam.md). |
| `the image check printed no single RAYVAST_IMAGE_INFO ... line` | The image has no Python with `ray` and `ray_vast`, or its ENTRYPOINT does not run the command. | Read [Use GPU images](node-types.md#use-gpu-images). |

After a failed `up`, run `ray-vast down cluster.yaml`. Read [Clean up after a failure](clean-up.md).

## The head stopped

Run `ray-vast down` and `ray-vast up`.
Read [When the head dies](../explanation/architecture.md#when-the-head-dies) for the cause and for what happens to the workers.
