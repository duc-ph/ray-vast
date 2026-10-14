# Find logs and troubleshoot

## Where the logs are

| Log | How to read it |
|---|---|
| Head container, all output | `ray-vast attach cluster.yaml`, then `sudo docker logs ray-vast-head` |
| Provider and head server | `sudo docker exec ray-vast-head cat /tmp/ray/session_latest/logs/monitor.log` |
| Ray logs of the workers | The Ray dashboard (http://127.0.0.1:8265, through `ray-vast dashboard`) |
| Job output | `ray job logs <job id>`, or the dashboard |

The provider and the head server write to `monitor.log`. It has these lines:

- each rental: `create 1 x gpu24: rented 2 candidates`;
- each enrollment decision, with the instance ID, the machine ID, the IP and the reason. For example `enrolled instance <id> (machine <id>, ip <ip>) into <slot>`, or `enroll refused for instance <id> from <ip>: <reason>`;
- each end of an instance: `ending instance <id> (machine <id>, ip <ip>): <reason>`;
- each reason that a worker sends before it destroys itself: `worker <id> asks to be destroyed: <reason>`.

The log never holds a secret.

vast keeps no container log after a destroy. So a worker sends the reason for each destroy to the head first. The reason is in `monitor.log`.

The head container logs in the local time zone of the head.
`monitor.log` has two Ray warnings every 5 seconds: `Dropping the empty legacy field head_node` and `worker_nodes`. They are not errors.

## A worker never arrives

1. **Check the demand.** Run `sudo docker exec ray-vast-head ray status` on the head. Look at "Pending" and "Recent failures".
2. **Check the offers.** A narrow query can match no offer. The datacenter filter removes about 93% of offers. In `monitor.log`, look for `no rentable offer matches the node type query`. Make the query wider.
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
- A worker has 58 ports for Ray worker processes. Tasks wait when no port is free. Read [Limits](../reference/limits.md).

## An actor fails with a misleading error

An actor fails after about 6 minutes with:

```text
ActorUnschedulableError ... Failed to startup worker after retrying 5 times ... Python environment setup
```

The real cause can be that the worker has no free port (`No available ports`). The cause is only in the worker's Ray log. Read [Limits](../reference/limits.md).

## `ray job submit` fails with an authentication error

Run the `export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=...` line that `ray-vast dashboard` printed. Do it in the terminal where you run `ray job submit`.

## `ray-vast` cannot connect to the head

- `up` adds an SSH rule for the public IP of your laptop when it starts. If your IP changes, SSH to the head times out. Add a rule for your new IP to the security group `ray-vast-<cluster_name>-0` (TCP 22, your IP /32).
- `attach` and `dashboard` need `--key-file` or `provider.key_file`.

## `up` fails

| Error | Cause | What to do |
|---|---|---|
| `cluster <name> already exists; use ray-vast down first` | An EC2 head with the cluster tag, or `~/.ray-vast/<cluster_name>.json`, exists. | Run `ray-vast down cluster.yaml`. |
| `not running from a source tree: pass --wheel` | `up` needs a ray-vast wheel for the head. | Pass `--wheel FILE`. Read [CLI](../reference/cli.md). |
| `no vast key: set VAST_API_KEY or put it in ~/.config/vastai/vast_api_key` | No vast key. | Set the key. |
| `AWS error ...` and `The AWS user lacks a permission` | The AWS user cannot make an IAM role, a security group or an instance. | Read [IAM permissions](../reference/iam.md). |
| `the image check printed no single RAYVAST_IMAGE_INFO ... line` | The image has no Python with `ray` and `ray_vast`, or its ENTRYPOINT does not run the command. | Read [Use GPU images](gpu-images.md). |

After a failed `up`, run `ray-vast down cluster.yaml`. Read [Clean up after a failure](clean-up.md).

## The head stopped

If Ray's monitor (the autoscaler and the provider) dies, the head container stops. ray-vast does not restart it.
Every worker then destroys itself within 5 minutes.
Run `ray-vast down` and `ray-vast up`. The new head starts from zero.
