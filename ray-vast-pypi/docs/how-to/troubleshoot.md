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
- a WARNING when a worker is at its process limit: `node <slot> (instance <id>, ip <ip>) is at its process limit: ...`. Read [A node is at its process limit](#a-node-is-at-its-process-limit).

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
- A worker has a fixed number of ports for Ray worker processes. Tasks wait when no port is free. Read [Limits](../reference/limits.md#58-worker-ports-for-each-worker) and [A node is at its process limit](#a-node-is-at-its-process-limit).

## A node is at its process limit

A vast container has a limit on processes and threads (the pids limit). Read [Limits](../reference/limits.md#the-process-limit-of-vast-hosts).
ray-vast keeps the worker below the limit with fewer worker ports. So too much work queues or hangs. It does not crash the node.

### The warning in `monitor.log`

```text
WARNING ... node slot-3-a1b2c3 (instance 27145501, ip 203.0.113.7) is at its process limit: 12 refused forks/threads (+12), pids 1279 of 1280, worker ports in use 25 of 25. Ray work on this node can queue, hang or fail. See https://duc-ph.github.io/ray-vast/how-to/troubleshoot/#a-node-is-at-its-process-limit
```

| Part | Meaning |
|---|---|
| `node slot-3-a1b2c3` | The Ray node (its `RAY_CLOUD_INSTANCE_ID`). `instance` is the vast instance ID. |
| `12 refused forks/threads (+12)` | The container refused this many process or thread starts since it started (`+`: since the last heartbeat with a lower count). A value above 0 means that something failed to start a thread: a Ray process or your code. |
| `pids 1279 of 1280` | Processes and threads now, and the limit. |
| `worker ports in use 25 of 25` | Ray worker processes that hold a port, and the number of ports. When all are in use, new tasks and actors wait. |

The head writes the line when the refused count goes up. It also writes it when all worker ports are in use, or the pids are above 90 % of the limit, at most once in 5 minutes for each node.

### The start line of the worker

The worker log has one line with the values that ray-vast chose:

```text
bootstrap: limits: pids.max 1280 (cgroup v1), cores 48, ray cpus 48, worker ports 25, soft limit 25, grpc threads 4, startup concurrency 8
```

`pids.max none` means that the container has no limit. Then the worker gets all 58 worker ports.

### What to do

- **All ports in use, no refused forks:** the work queues. This is safe. To finish faster, use fewer worker processes at the same time, or rent bigger hosts with a higher limit.
- **Refused forks or high pids:** your tasks start many threads, or too many processes run. Use the settings below.
- **A job hangs with all ports in use:** the job needs more processes at the same time than the node has ports. Use the settings below.

Settings that use fewer worker processes:

| Library | Setting |
|---|---|
| Ray Data | For `groupby` and other hash shuffles, use both: `DataContext.get_current().max_hash_shuffle_aggregators = 4` and `DataContext.get_current().execution_options.resource_limits = ExecutionResources(cpu=N)`, with N below the worker ports minus the aggregators. Read [Ray Data: limit the CPUs](run-your-workload.md#ray-data-limit-the-cpus). For a map, `map_batches(..., concurrency=N)`. |
| Ray Core | Give each actor `num_cpus=1`. An actor with the default `num_cpus` uses 0 CPUs when it runs, so many actors fit on one node. A task with a small `num_cpus` (for example 0.05) has the same effect. |
| Ray Tune | `tune.TuneConfig(max_concurrent_trials=N)`. |
| Ray Serve | A smaller `num_replicas`. |
| Your threads | Fewer threads in each task, for example `torch.set_num_threads(4)` and a small `DataLoader` `num_workers`. |

Or rent hosts with fewer cores, so that one node gets less work. Add an upper bound to `cpu_cores_effective` in the node type's `offer_query`, for example `cpu_cores_effective: {gte: 4, lte: 24}` for a node type with `CPU: 4`. Keep `gte` at least `CPU`. Read [Choose node types and offer queries](node-types.md).

## An actor fails with a misleading error

An actor fails after about 6 minutes with:

```text
ActorUnschedulableError ... Failed to startup worker after retrying 5 times ... Python environment setup
```

The real cause can be that the worker has no free port (`No available ports`). The cause is only in the worker's Ray log. Read [Limits](../reference/limits.md#58-worker-ports-for-each-worker).

## `ray job submit` fails with an authentication error

Run the `export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=...` line that `ray-vast dashboard` printed. Do it in the terminal where you run `ray job submit`.

## `ray-vast` cannot connect to the head

- `up` adds an SSH rule for the public IP of your laptop when it starts. If your IP changes, SSH to the head times out. Add a rule for your new IP to the security group `ray-vast-<cluster_name>-0` (TCP 22, your IP /32).
- `attach` and `dashboard` need `--key-file` or `provider.key_file`.

## `up` fails

| Error | Cause | What to do |
|---|---|---|
| ``cluster <name> already exists; use `ray-vast down` first`` | An EC2 head with the cluster tag, or `~/.ray-vast/<cluster_name>.json`, exists. | Run `ray-vast down cluster.yaml`. |
| `the head image build failed while installing ray-vast==<version> from <index>` | The `docker build` on the head failed. Possible causes: the version is not on the index, the index is not reachable, or the base image pull failed. | Read the build output below the message. Run `ray-vast down cluster.yaml`. Read [CLI](../reference/cli.md). |
| `no vast key: set VAST_API_KEY or put it in ~/.config/vastai/vast_api_key` | No vast key. | Set the key. |
| `AWS error ...` and `The AWS user lacks a permission` | The AWS user does not have a permission that ray-vast needs. | Read [IAM permissions](../reference/iam.md). |
| `the image check printed no single RAYVAST_IMAGE_INFO ... line` | The image has no Python with `ray` and `ray_vast`, or its ENTRYPOINT does not run the command. | Read [Use GPU images](gpu-images.md). |

After a failed `up`, run `ray-vast down cluster.yaml`. Read [Clean up after a failure](clean-up.md).

## The head stopped

Run `ray-vast down` and `ray-vast up`.
Read [When the head dies](../explanation/worker-lifecycle.md#when-the-head-dies) for the cause and for what happens to the workers.
