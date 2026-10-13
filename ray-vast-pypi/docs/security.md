# ray-vast security model

This document describes what ray-vast protects, how it protects it, and what it does not protect.
Read the known gaps before you run a cluster with private data or valuable credentials.

## Summary

- A cluster is one trust group. Every node holds the same token and the same CA.
- One compromised node means a compromised cluster.
- Traffic between nodes is encrypted (TLS), and every Ray call needs a token.
- The vast host that runs a worker has root access to the worker container. Treat the host as trusted with the data that you send to it.
- Vast has no firewall. All worker ports are open to the internet.

## Assets

| Asset | Where it is | Who can read it |
|---|---|---|
| CA key | Head only, mode 600 | Anyone with access to the head |
| Cluster token | Head and every worker | Any code on a node, including your own tasks |
| Node key and cert | The node, mode 600 | Any code on that node |
| vast account API key | Head, mode 600 | Anyone with access to the head |
| `CONTAINER_API_KEY` (per-instance vast key) | Worker: the bootstrap process, and the instance environment | See "CONTAINER_API_KEY" below |
| Job credentials (`runtime_env` `env_vars`) | Every worker that runs the job | Any code on those workers |

## What ray-vast does

### New CA and token for each cluster

`ray-vast up` makes a new CA and a new token on the head. The CA key stays on the head. TLS and `RAY_AUTH_MODE=token` are on for all nodes. Without the token, the job agent runs any shell command for anyone. So after Ray starts, each worker checks that both agent ports on 127.0.0.1 answer 401 without a token. If the check fails, the head destroys the worker.

### Certificate life

The CA and every node cert live for `cert_max_age_days` (default 365). There is no automatic renewal. To renew, run `ray-vast down` and `ray-vast up`. `ray-vast up` and the head log warn 14 days before expiry. When a cert expires, all TLS handshakes with that node fail.

### No secrets in the rental request

Vast shows the environment variables and the command of an instance in its metadata, and the container log is at a public URL. So the create request holds only public values: the head address, the SHA-256 of the bootstrap, the SHA-256 of the CA cert, and the cluster name. The create response holds `instance_api_key`. ray-vast never logs it.

### Enrollment checks

A rented instance (a candidate) gets its secrets only after these steps:

1. The bootloader downloads the bootstrap and checks its SHA-256. A changed byte stops it.
2. The bootstrap downloads the CA cert without verification, and compares its SHA-256 with the value in the environment. It then uses that CA for all later calls, with full verification (chain and IP SAN).
3. The bootstrap runs the host checks: the egress IP equals `PUBLIC_IPADDR`, no NET_RAW and no NET_ADMIN, its own network namespace, an empty `/dev/shm`.
4. The bootstrap sends its instance ID and `CONTAINER_API_KEY` to the head. The head calls vast with that key. Vast answers 200 only for that instance.
5. The head checks that the instance is one of its candidates, that it has not enrolled before, and that the request comes from the instance's public IP.
6. The head connects to all 64 identity ports from outside.
7. If a slot is free, the head sends the cert, key, token and shim inside the TLS connection. If no slot is free, it destroys the candidate.

A refusal because of an unknown instance, a second enrollment, a wrong source IP or a bad key does not destroy the instance. Reason: a stranger, for example a co-tenant behind the same NAT IP, must not be able to kill your candidates with guessed IDs. The provider ends such candidates with its own time limits. A vast outage gives HTTP 503, and the worker tries again.

After a container restart, the bootstrap runs again. The head refuses a second enrollment of the same instance and destroys it.

### Instance-bound node certificates

Each node cert has the IP SAN of the worker's public IP and 127.0.0.1, and a URI SAN `rayvast:instance/<id>`. The head checks both on its mTLS port. So a worker cannot send heartbeats or reports for another instance, even on the same IP. A former holder of a re-rented IP also cannot.

### Head server ports

The head server has two ports. Port A (TLS, no client cert) serves the CA cert, the bootstrap and the enrollment. Port B (mTLS) serves the heartbeat and the reports. The security group lets only worker IPs reach them. Port A holds no secrets in its downloads.

### Security group rules

The head security group allows SSH from your IP when you run `up`. For each worker public IP, the provider adds a rule (/32, all TCP) when the instance is `running`, before enrollment. It removes the rule when no worker or candidate uses the IP. The rules have the description `ray-vast:<cluster>`. A new provider removes all rules with that description at start.

### Head credentials

The IAM role of the head can only change the rules of its own security group, and describe EC2 instances and security groups. The head needs no rule for its own public IP.

### Offer filters

ray-vast adds `datacenter=true` and `verified=true` to every offer query. `allow_non_datacenter: true` removes the datacenter filter and shows a warning.

### The watchdog

The bootstrap is PID 1 on each worker. If the heartbeat fails for 5 minutes, or the head refuses it, the worker destroys its own instance. This limits the cost and the exposure of a worker that the head no longer controls. See the README for the full list of triggers.

## What a malicious vast host can do

A host has root access to the container. A malicious host can:

- read the token, the node key and cert, `CONTAINER_API_KEY`, and any credential that your jobs send to the worker;
- read and change your data and your results on that worker;
- join the cluster as a valid node and run code on other nodes, as any node can (see below).

ray-vast cannot stop this. The datacenter and verified filters make a malicious host less likely, but they do not remove the risk. Do not send secrets to a cluster that you cannot afford to lose. Use credentials with minimal rights for your jobs.

## Co-tenants

A co-tenant is another party's instance on the same host. Co-tenants share the Docker bridge (172.17.0.0/16). They can reach every Ray port of your worker directly. TLS and the token protect the traffic and the calls. The host check for no NET_RAW and no NET_ADMIN makes sure that a co-tenant cannot sniff or spoof the traffic. This depends on the host, so ray-vast checks it on each host.

## Known gaps

### Full vast account key on the head

The head uses your vast account API key. It is not scoped. Vast adds fixed rights to every key: read the account, list all instances, and change the account SSH keys. So a scope gives little protection. A thief who reaches the head can spend your credit, destroy or reach every instance of the account, and read the account. The thief also controls the cluster. If you suspect a leak, rotate the key in the vast console. Use a separate vast account for ray-vast, with a small credit balance, if you want to limit the damage.

### A task can read the head credentials

A worker can run code on the head. `CPU: 0` on the head does not stop a task with `num_cpus=0`. Such a task can read the head credentials, including the vast key and the CA key. Only run code that you trust on this cluster.

### `CONTAINER_API_KEY` is readable by tasks

Ray runs as the same user as the bootstrap (PID 1). So a Ray task can read `CONTAINER_API_KEY` from `/proc/1/environ`. The key can destroy its own instance and nothing else, which is less than what a task already has (the token and the node cert). A task can also send a signal to PID 1. The signal causes a destroy of the instance, not an exit. `SIGKILL` from inside the container is dropped by the kernel. The bootstrap removes the key from the environment of the Ray process, so it does not leak into logs by accident. It is not hidden from a task that reads `/proc/1/environ`.

### One compromised node means a compromised cluster

All nodes share the token and the CA. Ray has no permissions for each node. A node with the token can call any Ray API on any node.

### The metrics port has no authentication

The metrics port (the Prometheus export) answers with HTTP 200 without a token and without TLS. It shows about 380 `ray_` series. It is read-only. It shows metrics such as task counts and resource use.

### Worker ports are open to the internet

Vast has no firewall. All 64 worker ports are open on the public IP of the host. TLS and the token protect the services. A DDoS or a connection flood can stop single workers. It cannot stop the cluster, because the head is behind the AWS security group.

### Job credentials go to every worker

You give job credentials with `runtime_env` `env_vars`. They are on every worker that runs the job. Use a separate read-only IAM user for one prefix, not your main credentials.

### Vast account SSH keys in the container

Vast puts the SSH keys of your account into `/root/.ssh/authorized_keys` of every instance. ray-vast does not use SSH on workers. An image with no sshd is not affected.

### No automatic certificate renewal

See "Certificate life". Plan a `down` and an `up` before the cert expires.

### Dashboard and the head

The dashboard binds to 127.0.0.1 on the head. `ray-vast dashboard` uses an SSH tunnel. The only open ports on the head are SSH (your IP) and the Ray and head server ports (worker IPs). Keep the head key file safe: it gives root access to the head.

## Reporting a problem

TODO: add a contact or an issue link before release.
