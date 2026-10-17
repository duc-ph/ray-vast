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
- The library pages use B = worker ports − 4. Read [B](#b).
- The process limit does not change the Ray CPUs. Ray gets the `CPU` of the node type. Read [CPU and GPU](../how-to/node-types.md#cpu-and-gpu).

Each Ray worker process needs a port, so at most that many tasks and actors run on the worker at the same time.
Read [The process rules](#the-process-rules) to find out if your work needs the process settings.
The symptom of too much work is a queue, not a crash:

- Tasks wait for a free port. A job takes longer.
- An actor that waits for a port hangs, or fails with an error that does not name the port limit. Read [58 worker ports for each worker](#58-worker-ports-for-each-worker).
- A job that needs more processes at the same time than the ports allow can hang. For example, a Ray Data `groupby` needs its map tasks and its aggregator actors at the same time.

Your own threads count too. Read [Rule 2](#rule-2-threads-and-child-processes).

The head log warns when a worker is at its limit. Read [A worker is at its process limit](../how-to/troubleshoot.md#a-worker-is-at-its-process-limit). That section also lists the Ray settings that use fewer processes.

## The process rules

This section has the two rules that tell you if you need the process settings of the library pages.

### B

B is the number of worker processes that your work can use on one worker. B = worker ports − 4. The 4 ports stay free for the processes that Ray's libraries start next to your work (the Serve proxy, the Data and Train helpers).

CPUS is the number of Ray CPUs of one worker. It is the `CPU` of the node type in your cluster YAML.

B depends on the host. The table is for a host with 64 or more direct ports:

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

The provider adds `cpu_cores_effective >= max(CPU, 4)` to the offer query of each node type. So on a host with 64 or more direct ports, a node type with `CPU` from 5 to 54 gets a B of at least `CPU`.
A node type with `CPU` 3 or 4 can get a host with 4 effective cores. That host has B = 2.
A node type with `CPU` above 54 always has more CPUs than B.
A host with fewer direct ports gives fewer worker ports: at most the direct ports minus 6. For example, 20 direct ports give at most 14 worker ports. Then B = 10.

Read the real value in the start line of the worker. The start line is in the vast console (the instance log), or in the output of `vastai logs <instance id>`:

```text
bootstrap: limits: pids.max 1536 (cgroup v2), cpus 6, worker ports 18, soft limit 6, grpc threads 4, startup concurrency 8
```

Here B = 18 − 4 = 14. The value `cpus 6` is less than B. So this worker needs no settings for the defaults.

### Rule 1: processes

With Ray's defaults, Ray starts about one worker process for each CPU.
You need the process settings only if your node type has more CPUs than B, or if you start many tasks or actors with `num_cpus` below 1.
An actor with the default `num_cpus` needs 1 CPU to start, but it holds 0 CPUs while it runs. So many actors also count.
Each library page lists the settings of that library that also start many processes.

### Rule 2: threads and child processes

The process limit counts threads and child processes too. ray-vast sizes the worker ports for about 60 threads in each worker process.
Keep each process under about 60 threads, and keep its child processes few and small.

- Your own thread pools count, for example `concurrent.futures.ThreadPoolExecutor`. Give them a fixed `max_workers`. The default of Python is the number of CPUs that the container sees, plus 4.
- Each worker process has room for about 10 of your threads. A task that starts many threads can pass the limit. Examples are PyTorch with many intra-op threads, or a `DataLoader` with many workers.
- Thread pools often size themselves by the CPUs that the container sees. A vast container often sees all the CPUs of the host (for example 384), not its CPU quota (for example 46 cores).
- Ray sets `OMP_NUM_THREADS` for each task and actor. PyTorch and the CPU pool of Arrow use it. In a task with 1 CPU, `torch.get_num_threads()` and `pyarrow.cpu_count()` were 1, although the container saw 128 CPUs. The I/O pool of Arrow had 8 threads.

We measured the threads of each process on such a host (scenario LB15, 2026-10-17). The worker had 128 visible CPUs, a CPU quota of 15.36 cores, `pids.max` 3840, 57 worker ports and `CPU: 16`. Each workload ran with Ray's defaults, then with the fixes. The fixes were `OMP_NUM_THREADS=2` in `env_vars`, and `pyarrow.set_cpu_count(2)`, `pyarrow.set_io_thread_count(2)`, `torch.set_num_threads(2)` and `torch.set_num_interop_threads(2)` in each worker process.

| Process | Threads with Ray's defaults | Threads with the fixes |
|---|---|---|
| Ray Data shuffle aggregator (`read_parquet`, `map_batches`, `groupby`) | 37 | 38 |
| Ray Train worker (`TorchTrainer`, 4 workers, `DataLoader` with 2 workers) | 35 | 37 |
| `DataLoader` worker process | 1 | Not seen (the sample period is 0.5 s) |
| RLlib env runner (PPO, CartPole, 4 env runners) | 27 | 30 |
| A task that uses PyTorch and pyarrow | 27 | 28 |
| An idle Ray worker process | 27 to 29 | 30 to 32 |
| The raylet and the dashboard agent | 40 to 42 | 41 to 42 |

- With Ray's defaults, no Ray worker process had more than 37 threads. That is less than the 60 threads of rule 2.
- The fixes did not lower the counts. They added 1 to 3 threads to each process, because 2 threads is more than Ray's 1.
- The container refused no fork or thread. The highest `pids.current` was 1281 of 3840 (the Ray Data run).
- So you need no thread fix for these libraries with Ray's defaults. Use a fix only when your code sets a higher thread count.

Each library page lists the threads and child processes of that library.

### Tests of the rules

We tested the two rules on a local worker with the real ray-vast head and worker bootstrap (2026-10-17). The worker had 6 Ray CPUs, `pids.max` 1536 and 18 worker ports.

Rule 1 (scenario F21): the default code of each library completed. The head wrote no process-limit warning, and the container refused no thread. The default code of each library was:

- Ray Core: 6 actors with the default options. Each actor waits for a task with the default `num_cpus`.
- Ray Data: `range`, `map_batches` with the default `num_cpus`, and `groupby`.
- Ray Train: a `TorchTrainer` with 6 workers of 1 CPU each.
- Ray Tune: 22 trials with the default of 1 CPU each. 6 trials ran at the same time.
- Ray Serve: 6 replicas with the default `num_cpus` of 1.
- RLlib: PPO with 6 env runners of 1 CPU each.

Rule 2 (scenario F22): 6 tasks (one for each CPU) started 200 threads each. The worker had only 13 Ray worker processes for its 18 worker ports. Free worker ports did not help. The container reached its limit of 1536 in two runs. In the two runs, 5 and 6 of the 6 tasks got `RuntimeError: can't start new thread`. The container refused 47 and 48 thread starts. The worker stayed alive in this test. But at the limit the raylet can stop, and then the worker ends.

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
