# Limits

## About 299 worker IPs

Each worker IP needs a rule in a security group of the head.
One cluster uses 5 security groups. This is the AWS default limit for one network interface.
The default AWS quota is 60 rules for each group, and the SSH rule uses one. So the limit is about 299 worker IPs.

- Two workers on the same IP share one rule.
- Raise the quota "Inbound or outbound rules per security group" in AWS Service Quotas for a bigger cluster.
- When all 5 groups are full, the provider destroys the candidate. The error says to raise the quota.
- Unit tests check the group order and the full-group case. No live test had this many workers.

## 58 worker ports for each worker

Each worker has up to 64 identity ports: the smaller of 64 and the offer's `direct_port_count` (at least 10). Ray uses 6 fixed ports. The others (at most 58) are for Ray worker processes. A host with 20 direct ports gives at most 14 worker ports.
Each Ray worker process (a task or an actor) needs one port. A driver runs on the head and uses no worker port.
A worker with a process limit gets fewer worker ports. Read [The process limit of vast hosts](#the-process-limit-of-vast-hosts).

- When no port is free, tasks wait.
- An actor that gets no port hangs, or fails with `Failed to startup worker after retrying 5 times`. This error does not name the port limit. The real cause (`No available ports`) is only in the worker log.
- Actors use ports too.
- Ray has no setting for the total number of worker processes. Idle worker processes keep their ports up to `RAY_num_workers_soft_limit`. ray-vast sets this limit to the smaller of the node type's `CPU` and half the worker ports. So a job that needs a new kind of worker process (for example, another runtime env) finds a free port.
- Do not set `CPU` above 58.

## The process limit of vast hosts

A vast container has a limit on processes and threads together (the process limit, `pids.max`).
vast sets the limit from the CPU share of the container: 256 for each whole core of its CPU quota. The CPU quota is 0.96 × the offer's `cpu_cores_effective`. So a host with 6 effective cores (a quota of 5.76 cores) has 1280, and a host with 40 effective cores has 9728. A host with many cores and the limit 1280 does not occur. We measured this rule on 319 of 322 hosts (2026-10-17).
At the limit, Ray cannot start a thread. The raylet can stop, and then the worker destroys itself.

ray-vast keeps each worker below its limit:

- Each Ray process gets small gRPC thread pools (`RAY_num_grpc_internal_threads=4`). Then a Ray task or actor process has 25 to 29 threads, not about 50, and a Serve replica with a FastAPI ingress has 50. We measured these numbers with `RAY_num_grpc_internal_threads=4` on a simulated 48-core host (2026-10-16).
- The raylet starts at most 8 worker processes at the same time (`RAY_worker_maximum_startup_concurrency=8`).
- The worker gives Ray fewer worker ports: `(pids.max - 400) // 60`, at least 4, and at most the identity ports minus 6 (at most 58). With 1280, that is 14 ports. With 3328, it is 48. The value 60 is for the heaviest common worker process (a Serve replica) plus 10 of your threads.
- The library pages use B = worker ports − 4: the number of worker processes that your work can use on one worker. The 4 ports stay free for the processes that Ray's libraries start next to your work (the Serve proxy, the Data and Train helpers).
- The process limit does not change the Ray CPUs. Ray gets the `CPU` of the node type. Read [CPU and GPU](../how-to/node-types.md#cpu-and-gpu).

The worker ports and B for each host size (a host with 64 or more direct ports):

| `cpu_cores_effective` of the host | CPU quota (cores) | `pids.max` | Worker ports | B |
|---|---|---|---|---|
| 4 | 3.84 | 768 | 6 | 2 |
| 5 | 4.8 | 1024 | 10 | 6 |
| 6 | 5.76 | 1280 | 14 | 10 |
| 7 | 6.72 | 1536 | 18 | 14 |
| 8 | 7.68 | 1792 | 23 | 19 |
| 12 | 11.52 | 2816 | 40 | 36 |
| 16 | 15.36 | 3840 | 57 | 53 |
| 17 or more | 16.32 or more | 4096 or more | 58 | 54 |

A host with fewer direct ports gives at most the direct ports minus 6 worker ports.

Each Ray worker process needs a port, so at most that many tasks and actors run on the worker at the same time.
With Ray's defaults, Ray starts about one worker process for each CPU. So the defaults fit when the node type's `CPU` is at most B. The library pages call this rule 1. They give their process settings for the other cases.
The symptom of too much work is a queue, not a crash:

- Tasks wait for a free port. A job takes longer.
- An actor that waits for a port hangs, or fails with an error that does not name the port limit. Read [58 worker ports for each worker](#58-worker-ports-for-each-worker).
- A job that needs more processes at the same time than the ports allow can hang. For example, a Ray Data `groupby` needs its map tasks and its aggregator actors at the same time.

Your own threads count too. Each worker process has room for about 10 of your threads. A task that starts many threads (for example, PyTorch with many intra-op threads, or a `DataLoader` with many workers) can pass the limit. Free worker ports do not help: in a local test (2026-10-17), 6 tasks with 200 threads each reached the limit of 1536 with 13 Ray worker processes for 18 worker ports.
Thread pools often size themselves by the CPUs that the container sees. A vast container often sees all the CPUs of the host (for example 384), not its CPU quota (for example 46 cores). The threads of each library's processes on such a host, and the fixes: _measured in LB15 (not run yet)_.
The library pages call this rule 2: keep each process under about 60 threads, and keep its child processes few and small.

The head log warns when a worker is at its limit. Read [A worker is at its process limit](../how-to/troubleshoot.md#a-worker-is-at-its-process-limit). That section also lists the Ray settings that use fewer processes.

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

ray-vast does not restart a stopped head. Read [When the head dies](../explanation/worker-lifecycle.md#when-the-head-dies).

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
