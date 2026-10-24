# Limits

## About 299 worker IPs

Each worker IP needs a rule in a security group of the head.
One cluster uses 5 security groups. This is the AWS default limit for one network interface.
The default AWS quota is 60 rules for each group, and the SSH rule uses one. So the limit is about 299 worker IPs.

- Two workers on the same IP share one rule.
- Raise the quota "Inbound or outbound rules per security group" in AWS Service Quotas for a bigger cluster.
- When all 5 groups are full, the provider destroys the candidate. The error says to raise the quota.
- No test had this many workers.

## One worker on an IP without hairpin NAT

Two workers on one public IP reach each other through hairpin NAT. Some hosts do not have it.
On such an IP, the cluster has one worker only. The provider rents other hosts for the other workers.
Read [Workers on one public IP](../explanation/architecture.md#workers-on-one-public-ip).

## At most 54 processes at once on a worker

Each Ray worker process (a task or an actor) needs one port of the worker. The driver of a job runs on the head and uses no worker port.

- Each worker has up to 64 identity ports: the smaller of 64 and the offer's `direct_port_count`.
- Ray uses 6 fixed ports. The other ports (at most 58) are the worker ports.
- 4 worker ports stay free for the helper processes of the Ray libraries (the Serve proxy, the Data and Train helpers).
- So a worker runs at most 54 of your processes at the same time. We call this number *N*. The start line of the worker shows it (`processes at once`).
- A worker with a process limit gets fewer worker ports. Read [The process limit of vast hosts](#the-process-limit-of-vast-hosts).
- The provider adds `direct_port_count >= 10 + min(CPU, 54)` to each offer query. So a worker has enough ports for one process for each CPU.

When no port is free:

- Tasks wait.
- An actor that gets no port hangs, or fails with `Failed to startup worker after retrying 5 times`. This error does not name the port limit. The real cause (`No available ports`) is only in the worker log.

Other facts:

- Ray has no setting for the total number of worker processes. Idle worker processes keep their ports up to `RAY_num_workers_soft_limit`. ray-vast sets this limit to the smaller of the node type's `CPU` and half the worker ports. So a job that needs a new kind of worker process (for example, another runtime env) finds a free port.
- `up` warns if `CPU` is above 54. Read [Many tasks or actors](../libraries/many-tasks.md).
- An outbound connection can hold a worker port. The worker ports are in the kernel's ephemeral port range, because vast maps each host port to the same container port. Ray then uses another free worker port. When no worker port is free, a worker process does not start (`No available ports` in the worker log). The heartbeat counts these ports as in use. ray-vast cannot prevent this: a reserved port range needs `NET_ADMIN`, and vast hosts do not give it.

## The process limit of vast hosts

A vast container has a limit on processes and threads together (the process limit, `pids.max`).
vast sets the limit from the CPU share of the container: 256 for each whole core of its CPU quota. The CPU quota is 0.96 × the offer's `cpu_cores_effective`.
So a host with 6 effective cores (a quota of 5.76 cores) has 1280, and a host with 40 effective cores has 9728. We measured this rule on 319 of 322 machines.
At the limit, Ray cannot start a thread. The raylet can stop, and then the worker destroys itself.

ray-vast keeps each worker below its limit:

- Each Ray process gets small gRPC thread pools (`RAY_num_grpc_internal_threads=4`). Then a Ray task or actor process has 25 to 29 threads, not about 50. A Serve replica with a FastAPI ingress has 50.
- The raylet starts at most 8 worker processes at the same time (`RAY_worker_maximum_startup_concurrency=8`).
- The worker gives Ray fewer worker ports: `(pids.max - 400) // 60`, at least 4, and at most the identity ports minus 6 (at most 58). The value 60 is for the heaviest common worker process (a Serve replica) plus 10 of your threads.
- The process limit does not change the Ray CPUs. Ray gets the `CPU` of the node type. Read [CPU and GPU](../how-to/node-types.md#cpu-and-gpu).

This table gives *N* for each host size, for a host with 64 or more direct ports:

| `cpu_cores_effective` of the host | CPU quota (cores) | `pids.max` | Worker ports | *N* (processes at once) |
|---|---|---|---|---|
| 5 | 4.8 | 1024 | 10 | 6 |
| 6 | 5.76 | 1280 | 14 | 10 |
| 7 | 6.72 | 1536 | 18 | 14 |
| 8 | 7.68 | 1792 | 23 | 19 |
| 12 | 11.52 | 2816 | 40 | 36 |
| 16 | 15.36 | 3840 | 57 | 53 |
| 17 or more | 16.32 or more | 4096 or more | 58 | 54 |

The provider adds `cpu_cores_effective >= max(CPU, 5)` to each offer query. So a node type with `CPU` up to 54 gets an *N* of at least `CPU`.
A host with fewer direct ports gives fewer worker ports: at most the direct ports minus 6.

The symptom of too much work is a queue, not a crash. The head log warns when a worker is at its limit. Read [A worker is at its process limit](../how-to/troubleshoot.md#a-worker-is-at-its-process-limit) and [Many tasks or actors](../libraries/many-tasks.md).

## No multi-node Ray Train

Ray Train with PyTorch DDP over 2 or more workers does not work. This is true for NCCL and for Gloo.

- The rendezvous (`TCPStore`) of rank 0 uses a random port.
- Gloo and NCCL open more connections on random ports. They also send the container's private address to the other ranks.
- vast maps only the identity ports of each worker (at most 64). Ray uses all of them.
- Ray Train 2.59 has no setting for the rendezvous port.

The job fails after about 3 minutes with `DistNetworkError` or `DistStoreError ... 1/2 clients joined`.
This is a limit of running behind the NAT of vast.

Use one worker with many GPUs. Make a node type with `GPU: 2` or more, and train on that worker. Ray Train on one worker with 2 GPUs and NCCL works.

## Checkpoints need cloud storage

Ray Tune and Ray Train write checkpoints to `storage_path`.
Ray 2.59 refuses a local `storage_path` on a cluster with more than one node. The error is `A local path on the head node is not accessible by worker nodes`.
The default `storage_path` is local, so the default fails too.

Set `storage_path` to a cloud bucket, and give the workers credentials for it. Read [Ray Tune](../libraries/tune.md) and [Ray Train](../libraries/train.md).

## Certificates do not renew

The CA and every node certificate live 365 days.
To renew, run `ray-vast down` and `ray-vast up`. The head log warns 14 days before the CA expires.

## The head has no Elastic IP

A reboot of the head keeps its IP, but the cluster does not survive it. The head container has no restart policy, and the provider keeps its state in memory only.
After a reboot, or after an EC2 stop and start (the IP changes), run `ray-vast down` and `ray-vast up`. The old workers destroy themselves.

## No self-healing

ray-vast does not restart a stopped head. Read [When the head dies](../explanation/architecture.md#when-the-head-dies).

## Tested only with Ray 2.59

`up` warns if the image has another Ray version.

## Some Ray commands do not work

`ray up`, `ray down`, `ray attach`, `ray exec` and `ray rsync-*` do not work with this provider. These commands use the provider and SSH to find the head. The ray-vast provider does not make the head. Use the `ray-vast` commands.
`ray job submit` (through the tunnel), `ray status` on the head, and the Ray Python API work as usual.
The Ray client port (10001) is not open to your laptop.

## One region, one head

The head runs in one AWS region. The workers can be anywhere that the offer query allows.
Ray traffic between the head and the workers goes over the public internet.

## Public images only

ray-vast sends no registry credentials. The head and vast must be able to pull `provider.image` without a login.

## Linux x86_64 workers

The networking shim is a Linux x86_64 library. `up` starts the head from an Ubuntu 24.04 amd64 image, so the head `instance_type` must be an x86_64 type (not a Graviton type such as `t4g`).
