# Limits

## About 299 worker IPs

Each worker IP needs a rule in a security group of the head.
One cluster uses 5 security groups. This is the AWS default limit for one network interface.
The default AWS quota is 60 rules for each group, and the SSH rule uses one. So the limit is about 299 worker IPs.

- Two workers on the same IP share one rule.
- Raise the quota "Inbound or outbound rules per security group" in AWS Service Quotas for a bigger cluster.
- When all 5 groups are full, the provider destroys the candidate. The error says to raise the quota.

## 58 worker ports for each node

Each worker has 64 ports. Ray uses 6 fixed ports. The other 58 are for Ray worker processes.
Each Ray worker process (a task, an actor or a driver) needs one port.

- When no port is free, tasks wait.
- A driver fails at once with `No available ports`.
- An actor fails after about 6 minutes. The error is misleading: `ActorUnschedulableError ... Failed to startup worker after retrying 5 times ... Python environment setup`. The real cause (`No available ports`) is only in the worker log.
- Actors use ports too. An idle worker process keeps its port for about 60 seconds.
- Ray has no setting for the total number of worker processes. ray-vast sets `RAY_num_workers_soft_limit` to `min(cores, 58)`, so that idle processes release their ports sooner.
- Do not set `CPU` above 58.

## The process limit of vast hosts

A vast container has a limit on processes and threads together (the pids limit).
On many 1-GPU hosts, the limit is about 1280. One 2-GPU host had 3328.

Each Ray worker process has about 50 threads. The raylet has about 70. An idle node has about 300 threads.
ray-vast starts Ray with the real core count of the host as `CPU`. A job that uses all CPUs of a 24-core or 48-core host starts 24 to 48 worker processes. That is more threads than the limit allows.

When the node reaches the limit:

- Tasks fail with `RuntimeError: can't start new thread` or `pthread_create failed: Resource temporarily unavailable`.
- The raylet aborts, and the node dies.
- The worker destroys itself. The autoscaler rents a replacement.

So keep the number of busy worker processes on one node low (about 20 on a host with a limit of 1280).
For Ray Data, limit its CPUs. Read [Run your Ray workload](../how-to/run-your-workload.md#ray-data-limit-the-cpus).

A fix in ray-vast is under review.

## No multi-node Ray Train

Ray Train with PyTorch DDP over 2 or more nodes does not work. This is true for NCCL and for Gloo.

- The rendezvous (`TCPStore`) of rank 0 uses a random port.
- Gloo and NCCL open more connections on random ports. They also send the container's private address to the other ranks.
- vast maps only the 64 identity ports of each worker. Ray uses all of them.
- Ray Train 2.59 has no setting for the rendezvous port.

The job fails after about 3 minutes with `DistNetworkError` or `DistStoreError ... 1/2 clients joined`.
This is a limit of running behind the NAT of vast.

Use one node with many GPUs. Make a node type with `GPU: 2` or more, and train on that node. Ray Train on one node with 2 GPUs and NCCL works.

## Checkpoints need cloud storage

Ray Tune and Ray Train write checkpoints to `storage_path`.
Ray 2.59 refuses a local `storage_path` on a cluster with more than one node. The error is `A local path on the head node is not accessible by worker nodes`.
The default `storage_path` is local, so the default fails too.

Set `storage_path` to a cloud bucket, and give the workers credentials for it. Read [Run your Ray workload](../how-to/run-your-workload.md#ray-tune-and-ray-train-checkpoints).

## Certificates do not renew

The CA and every node certificate live `cert_max_age_days` (default 365).
To renew, run `ray-vast down` and `ray-vast up`. `up` and the head log warn 14 days before the CA expires.

## The head has no Elastic IP

A reboot of the head keeps its IP.
After an EC2 stop and start, the IP changes. Run `ray-vast down` and `ray-vast up`. The old workers destroy themselves.

## No self-healing

If Ray's monitor (the autoscaler and the provider) dies, the head container stops. ray-vast does not restart it.
The workers destroy themselves within 5 minutes.
Run `ray-vast down` and `ray-vast up`. The head keeps all state in memory, so a new head starts from zero.

## Tested only with Ray 2.59

`up` warns if the image has another Ray version.

## Some Ray commands do not work

`ray up`, `ray down`, `ray attach`, `ray exec` and `ray rsync-*` do not work with this provider. Use the `ray-vast` commands.
`ray job submit` (through the tunnel), `ray status` on the head, and the Ray Python API work as usual.
The Ray client port (10001) is not open to your laptop.

## One region, one head

The head runs in one AWS region. The workers can be anywhere that the offer query allows.
Ray traffic between the head and the workers goes over the public internet.

## Public images only

ray-vast sends no registry credentials. The head and vast must be able to pull `provider.image` without a login.

## Linux x86_64 workers

The networking shim is a Linux x86_64 library. `up` starts the head from an Ubuntu 24.04 amd64 image, so `head_instance_type` must be an x86_64 type (not a Graviton type such as `t4g`).
