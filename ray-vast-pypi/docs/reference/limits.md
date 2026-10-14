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
