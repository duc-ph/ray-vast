# Security model

This page tells what ray-vast protects, from whom, and what it does not protect.
Read the known gaps before you run a cluster with private data or valuable credentials.

[design.md](https://github.com/duc-ph/ray-vast/blob/main/docs/design.md) on GitHub specifies the mechanisms: enrollment, PKI, security groups and the watchdog. Each defense below gives its section of design.md.
For a short explanation, read [Enrollment and the join token](explanation/enrollment.md) and [Worker life cycle and the watchdog](explanation/worker-lifecycle.md).

## Summary

- A cluster is one trust group. Every node holds the same token and the same CA. If an attacker controls one node, the attacker controls the cluster.
- TLS encrypts the traffic between nodes. Every Ray call needs the token.
- The vast host that runs a worker has root access to the worker container. The host can read all data that you send to the worker.
- vast has no firewall. All worker ports are open to the internet. AWS security groups protect the head.

## Assets

| Asset | Where it is | Who can read it |
|---|---|---|
| CA key | Head only, mode 600 | Anyone with access to the head |
| Cluster token | Head and every worker | Any code on a node, including your own tasks |
| Node key and cert | The node, mode 600 | Any code on that node |
| vast account API key | Head, mode 600 | Anyone with access to the head |
| SSH key of the head (one EC2 key pair for each cluster) | Your laptop: `~/.ray-vast/<cluster_name>/ssh_key.pem`, mode 600. `down` deletes it and the key pair. | Your laptop user |
| Join token (one for each candidate) | Provider memory; the instance env until enrollment | Read "The join token in the env" |
| `CONTAINER_API_KEY` (vast key of one instance) | Worker: the bootstrap's memory and the instance env | Read "`CONTAINER_API_KEY` is readable by tasks" |
| Job credentials (`runtime_env` `env_vars`) | Every worker that runs the job | Any code on those workers |

## Threats and defenses

**Someone reads the rental request or the container log on vast** (design 5.1, 5.2)

- The container command and the log hold no secrets.
- The env holds only public values and the one-time join token.
- ray-vast never logs `instance_api_key` from the create answer.

**A fake head, or a man in the middle, sends a changed bootstrap or CA** (design 5.3, 5.4)

- The bootloader checks the SHA-256 of the bootstrap.
- The bootstrap pins the CA by its SHA-256.
- The bootstrap refuses a PEM with more than one certificate. It builds the PEM again from the hashed DER.
- All later calls verify the head: the chain and the IP SAN.

**A stranger enrolls as one of our candidates** (design 5.2, 5.7)

- The head accepts `/enroll` only for a known candidate, from its public IP, with its join token, and only once.

**A stranger kills our candidates with junk `/enroll` calls** (design 5.7)

- A refusal before a valid join token destroys nothing.

**A worker sends heartbeats or reports for another instance** (design 5.7)

- ray-vast binds each node certificate to its instance: an IP SAN and the URI SAN `rayvast:instance/<id>`.
- Port B checks both.

**A host or a co-tenant sniffs or spoofs traffic on the Docker bridge** (design 5.4)

- TLS and the token protect the traffic.
- Co-tenants share the bridge (172.17.0.0/16). They can reach every Ray port.
- So the host check runs on each host. It refuses a container with NET_RAW or NET_ADMIN in `CapBnd`.

**A host with a wrong egress IP, closed ports or a shared network namespace** (design 4, 5.4, 5.7)

- The host checks and the port check refuse the candidate.
- The provider destroys the candidate and puts the host on the blocklist.

**Anyone runs commands through the job agent** (design 5.8, 8.2)

- All nodes use `RAY_AUTH_MODE=token`.
- Both agent HTTP ports listen on 127.0.0.1 only.
- After start, each worker checks that both ports answer 401 without the token. If the check fails, the worker destroys itself.

**An attack from the internet on the head** (design 7.1, 8.3)

- The security groups of the head allow SSH from your IP only.
- They allow the Ray ports and the head server ports from worker IPs only.
- The dashboard binds to 127.0.0.1. `ray-vast dashboard` uses an SSH tunnel.

**Someone uses a stolen head credential on AWS** (design 7.1)

- The IAM role of the head can only change the rules of the 5 security groups of the cluster, and describe EC2.
- The role has the permissions boundary `ray-vast-head-boundary`. The role can never do more than the boundary allows: describe EC2, and change the rules of security groups with the tag `ray-vast-cluster`. This stays true if someone changes the role policy.
- A restricted user with the policy `ray-vast-user` can make a head role only with this boundary. Read [IAM permissions](reference/iam.md).

**A worker that the head no longer controls keeps running and costs money** (design 6)

- The watchdog destroys its own instance after 5 minutes with no heartbeat, on a 403 answer, and on signals.
- The watchdog never only exits.

**A dishonest host** (design 9)

- ray-vast adds `datacenter = true` and `verified = true` to every offer query.
- `allow_non_datacenter: true` removes the datacenter filter. Then `up` shows a warning.
- These filters make a bad host less likely. They do not stop a bad host. Read the next section.

## What a malicious vast host can do

A host has root access to the container. A malicious host can:

- read the join token, the cluster token, the node key and cert, `CONTAINER_API_KEY`, and any credential that your jobs send to the worker;
- read and change your data and your results on that worker;
- join the cluster as a valid node and run code on other nodes, as any node can.

ray-vast cannot stop this. Do not send secrets to a cluster that you cannot afford to lose. Use credentials with minimal rights for your jobs.

## Known gaps (documented risks)

### The join token in the env

vast shows the env in the instance metadata. So these parties can read the join token:

- your vast account key;
- any `CONTAINER_API_KEY` of your account, that is, your own workers (they already hold the cluster token);
- the host of the candidate (it has root access there anyway).

Co-tenants and the internet cannot read the token.
The token is valid for one instance, from one IP, for one enrollment.
The token stays readable in `/proc/1/environ` of the worker. This is harmless, because the head accepts the token only once.

### Full vast account key on the head

The head holds your full vast account API key. The key has no scope.
A scope would give little protection: vast adds fixed rights to every key. These rights let the key read the account, list all instances, and change the SSH keys of the account.

A thief who reaches the head can:

- spend your credit;
- destroy or reach every instance of the account;
- read the account;
- control the cluster.

If you think that the key leaked, replace it in the vast console.

### A task can read the head credentials

A task can run on the head. `CPU: 0` on the head does not stop a task with `num_cpus=0`.
Such a task can read the head credentials, including the vast key and the CA key. Run only code that you trust on this cluster.

### `CONTAINER_API_KEY` is readable by tasks

Ray runs as the same user as the bootstrap (PID 1). So a Ray task can read `CONTAINER_API_KEY` from `/proc/1/environ`.

- The key can destroy its own instance, and nothing else. A task already has more: the token and the node cert.
- A task can also send `TERM`, `INT` or `HUP` to PID 1. Then the bootstrap destroys the instance.
- The kernel drops `SIGKILL` to PID 1 from inside the container.
- The bootstrap removes the key from the Ray environment. So the key does not go into logs by accident.

### One compromised node means a compromised cluster

All nodes share the token and the CA. Ray has no permissions for each node. A node with the token can call any Ray API on any node.

### The metrics port has no authentication

The metrics port (Prometheus export) answers HTTP 200 without a token and without TLS. It shows about 380 `ray_` series. It is read-only.

### Worker ports are open to the internet

All 64 worker ports are open on the public IP of the host. TLS and the token protect the services.
A DDoS or a connection flood can stop single workers. It cannot stop the cluster.

### Job credentials go to every worker

`runtime_env` `env_vars` are on every worker that runs the job. Do not use your main credentials. Use a separate IAM user with rights for one S3 prefix only.

### vast account SSH keys in the container

vast puts the SSH keys of your account into `/root/.ssh/authorized_keys` of every instance. ray-vast does not use SSH on workers. This does not affect an image with no sshd.

### No automatic certificate renewal

Certificates are valid for 365 days. Plan a `down` and an `up` before the CA expires.
`up` and the head log warn 14 days before the CA expires (design.md 7.2). Read [Limits](reference/limits.md#certificates-do-not-renew).

## Reporting a problem

Report a problem in the [GitHub issues](https://github.com/duc-ph/ray-vast/issues).
Do not put a key, a token or another secret in an issue.
<!-- TODO: a private contact for vulnerabilities is not decided yet. -->
