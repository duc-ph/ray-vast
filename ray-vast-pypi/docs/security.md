# ray-vast security model

This document tells what ray-vast protects, from whom, and what it does not protect. Read the known gaps before you run a cluster with private data or valuable credentials. The mechanisms (enrollment, PKI, security groups, watchdog) are specified in [design.md](../../docs/design.md); this file links to them.

## Summary

- A cluster is one trust group. Every node holds the same token and the same CA. One compromised node means a compromised cluster.
- Traffic between nodes is encrypted (TLS), and every Ray call needs the token.
- The vast host that runs a worker has root access to the worker container. Treat the host as trusted with the data that you send to it.
- vast has no firewall. All worker ports are open to the internet. The head is behind AWS security groups.

## Assets

| Asset | Where it is | Who can read it |
|---|---|---|
| CA key | Head only, mode 600 | Anyone with access to the head |
| Cluster token | Head and every worker | Any code on a node, including your own tasks |
| Node key and cert | The node, mode 600 | Any code on that node |
| vast account API key | Head, mode 600 | Anyone with access to the head |
| Join token (one for each candidate) | Provider memory; the instance env until enrollment | See "The join token in the env" |
| `CONTAINER_API_KEY` (vast key of one instance) | Worker: the bootstrap's memory and the instance env | See "`CONTAINER_API_KEY` is readable by tasks" |
| Job credentials (`runtime_env` `env_vars`) | Every worker that runs the job | Any code on those workers |

## Threats and defenses

| Threat | Defense | Spec |
|---|---|---|
| Someone reads the rental request or the container log on vast | The command and the log hold no secrets. The env holds only public values and the one-time join token. `instance_api_key` in the create answer is never logged. | design 5.1, 5.2 |
| A fake head, or a man in the middle, sends a changed bootstrap or CA | The bootloader checks the bootstrap's SHA-256. The bootstrap pins the CA by its SHA-256, refuses a PEM with more than one cert, and builds the PEM again from the hashed DER. All later calls verify the head (chain and IP SAN). | design 5.3, 5.4 |
| A stranger enrolls as one of our candidates | The head accepts `/enroll` only for a known candidate, from its public IP, with its join token, once. | design 5.2, 5.7 |
| A stranger kills our candidates with junk `/enroll` calls | A refusal before a valid join token destroys nothing. | design 5.7 |
| A worker sends heartbeats or reports for another instance | Node certs are bound to the instance (IP SAN and URI SAN `rayvast:instance/<id>`); port B checks both. | design 5.7 |
| A host or co-tenant sniffs or spoofs traffic on the Docker bridge | TLS and the token. The host check refuses a container with NET_RAW or NET_ADMIN in `CapBnd`. Co-tenants share the bridge (172.17.0.0/16) and can reach every Ray port, so this is checked on each host. | design 5.4 |
| A host with a wrong egress IP, closed ports or a shared network namespace | The host checks and the port check refuse it, destroy it, and blocklist the host. | design 4, 5.4, 5.7 |
| Anyone runs commands through the job agent | `RAY_AUTH_MODE=token` on all nodes. Both agent HTTP ports listen on 127.0.0.1 only. After start, each worker checks that both ports answer 401 without the token; a failure destroys the worker. | design 5.8, 8.2 |
| An attack from the internet on the head | The head's security groups allow SSH from your IP only, and the Ray and head server ports from worker IPs only. The dashboard binds to 127.0.0.1; `ray-vast dashboard` uses an SSH tunnel. | design 7.1, 8.3 |
| A stolen head credential is used on AWS | The head's IAM role can only change the rules of the cluster's 5 security groups and describe EC2. | design 7.1 |
| A worker that the head no longer controls keeps running and costs money | The watchdog destroys its own instance after 5 min with no heartbeat, on a 403, and on signals; it never only exits. | design 6 |
| A dishonest host | `datacenter = true` and `verified = true` are added to every offer query. `allow_non_datacenter: true` removes the datacenter filter and `up` warns. This makes a bad host less likely; it does not stop one (see below). | design 9 |

## What a malicious vast host can do

A host has root access to the container. A malicious host can:

- read the join token, the cluster token, the node key and cert, `CONTAINER_API_KEY`, and any credential that your jobs send to the worker;
- read and change your data and your results on that worker;
- join the cluster as a valid node and run code on other nodes, as any node can.

ray-vast cannot stop this. Do not send secrets to a cluster that you cannot afford to lose. Use credentials with minimal rights for your jobs.

## Known gaps (documented risks)

### The join token in the env

vast shows the env in the instance metadata. So the join token can be read by your vast account key, by any `CONTAINER_API_KEY` of your account (that is, your own workers, which already hold the cluster token), and by the candidate's host (root there anyway). Co-tenants and the internet cannot read it. The token is valid for one instance, from one IP, for one enrollment. The token stays readable in `/proc/1/environ` of the worker. This is harmless, because the head accepts the token only once.

### Full vast account key on the head

The head holds your full vast account API key. It is not scoped: vast adds fixed rights to every key (read the account, list all instances, change the account SSH keys), so a scope gives little protection. A thief who reaches the head can spend your credit, destroy or reach every instance of the account, and read the account. The thief also controls the cluster. If you suspect a leak, rotate the key in the vast console.

### A task can read the head credentials

A worker can run code on the head: `CPU: 0` on the head does not stop a task with `num_cpus=0`. Such a task can read the head credentials, including the vast key and the CA key. Only run code that you trust on this cluster.

### `CONTAINER_API_KEY` is readable by tasks

Ray runs as the same user as the bootstrap (PID 1). So a Ray task can read `CONTAINER_API_KEY` from `/proc/1/environ`. The key can destroy its own instance and nothing else, which is less than what a task already has (the token and the node cert). A task can also send `TERM`, `INT` or `HUP` to PID 1, which then destroys the instance; `SIGKILL` from inside the container is dropped by the kernel. The bootstrap removes the key from the Ray environment, so it does not leak into logs by accident.

### One compromised node means a compromised cluster

All nodes share the token and the CA. Ray has no permissions for each node. A node with the token can call any Ray API on any node.

### The metrics port has no authentication

The metrics port (Prometheus export) answers HTTP 200 without a token and without TLS (about 380 `ray_` series). It is read-only.

### Worker ports are open to the internet

All 64 worker ports are open on the host's public IP. TLS and the token protect the services. A DDoS or a connection flood can stop single workers, but not the cluster.

### Job credentials go to every worker

`runtime_env` `env_vars` are on every worker that runs the job. Use a separate read-only IAM user for one prefix, not your main credentials.

### vast account SSH keys in the container

vast puts the SSH keys of your account into `/root/.ssh/authorized_keys` of every instance. ray-vast does not use SSH on workers. An image with no sshd is not affected.

### No automatic certificate renewal

Certs live `cert_max_age_days` (default 365). Plan a `down` and an `up` before the CA expires; `up` and the head log warn 14 days before (design 7.2).

## Reporting a problem

TODO: add a contact or an issue link before release. The contact is not decided yet.
