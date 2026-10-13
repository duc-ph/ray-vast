# Ray on vast.ai

Status: design agreed on 2026-10-12, with worker-first enrollment from 2026-10-13. Tests C0–C4 ran on 2026-10-12/13. Items marked **(to test)** are not proven yet.

This is an overview. The technical details are in [design.md](design.md), the test data is in [test-results.md](test-results.md), and the open tests are in [test-plan.md](test-plan.md).

## Why

vast.ai rents GPUs at a low price, but it is not a cloud. We want a normal Ray cluster on vast GPUs: autoscaling, `ray job submit`, tasks, actors and the object store. A user runs `ray-vast up cluster.yaml` and `ray-vast down`. We will publish `ray-vast` as a PyPI package.

## The network problem

On AWS, GCP and Azure, instances are virtual machines on a private network. Nodes connect to each other with private IPs, and the traffic stays inside the cloud.

On vast, each instance is a container on a host machine. A host can run containers for many customers (co-tenants). Each container has a private IP and a list of ports. A NAT gateway maps each (public IP, public port) to a (private IP, private port), and sends incoming traffic to the correct container.

What we measured (2026-10-06/07):

- **No overlay network.** A WireGuard or Tailscale overlay needs `/dev/net/tun` and `NET_ADMIN` in the container. 0 of 146 random Docker hosts gave them. So Ray nodes must talk over the internet, with the host's public IP.
- **Identity ports work.** vast can map a public port to the same private port (P→P). This worked on 202 of 202 hosts. An instance can have at most 64 ports.
- **Hairpin NAT is not reliable.** A Ray node advertises its public IP. Then the components inside one node send data to (public IP, port), and the NAT must send it back to the same container. This is hairpin NAT. It worked on about 72% of hosts.
- **Two Ray components bind to the advertised IP.** The container does not own the public IP, so the bind fails with `Errno 99`, and the raylet stops. This also occurs on EC2, where the public IP is not on the interface either.

## The fix: one LD_PRELOAD shim

A small C library that we load with `LD_PRELOAD` into all Ray processes (C++ and Python), on the head and on the workers:

- A bind to the public IP changes to a bind to the private IP.
- A connect to the node's own public IP changes to a connect to the private IP. Traffic inside a node then never goes through the NAT, so **hairpin NAT is not necessary**, and we can use all hosts.
- The two agent HTTP ports listen on 127.0.0.1 only.

Tested (C0–C2): a worker on a host without hairpin NAT ran the full workload (tasks, actors, `runtime_env`, 50 MB objects, job submission).

## Security

**What Ray gives us:** mutual TLS for gRPC, and token auth for the HTTP endpoints. Tested: plaintext, a missing cert and a cert from an unknown CA are refused; a request without the token gets 401.

**What we add:**

- A new CA and a new token for each cluster.
- No agent HTTP port on a worker is open to the internet, so nobody can send a job directly to a worker. Jobs go through the head.
- The token never goes over the network (tested: found only on `lo`, never on the network interface).
- No secrets at rental time. A new instance **enrolls itself** with the head and gets its secrets inside a TLS connection (see "Enrollment"). We first planned to push the secrets over SSH, but this needs sshd, apt and `sudo` in the image, and many user images do not have them.
- Datacenter and verified hosts only, by default, because a host has root access to our container.
- Small credentials on the head, because a worker can run code on the head.

**What vast exposes (found in C2–C4):**

- vast gives each instance its own API key. It can destroy its own instance, and it can list all instances of the account with their env and start scripts.
- The instance metadata shows the env and the start script in clear text, and the container log is at a public URL. So nothing secret goes into them.
- vast adds fixed rights to every API key that we make, for example "change the account SSH keys".
- vast puts the account's SSH keys into every instance. ray-vast does not use SSH.

**Remaining risks:** a malicious host can take control of the cluster (we trust datacenter hosts). One compromised node compromises the whole cluster. The metrics port has no auth (read-only). All worker ports are open to the internet, so a DDoS can stop single workers, but not the head.

## Design overview

### Head on AWS, workers on vast

The head is on EC2, behind the AWS firewall, because a cluster without its head is difficult to recover, and vast instances can stop or change IP at any time. The head runs the same Docker image as the workers, so the Ray and Python versions match. The head has `CPU: 0` and runs no work.

### Node types

A node type has fixed Ray resources (for example `CPU: 8, GPU: 1`) and an offer query in vast's search syntax (for example a VRAM range and a `dlperf` minimum). The provider rents only offers that guarantee those resources.

### Limits

- About 299 worker IPs in one cluster (one security group rule for each IP).
- About 58 worker processes on one node (vast gives 64 ports, and each task, actor or driver needs one). When no port is free, tasks wait, actors fail after about 6 min, and drivers fail at once (tested).

### Provisioning: a hidden race

Some vast instances never start, and some hosts fail our checks. So the provider rents about twice as many **candidates** as the autoscaler asks for, and keeps the first ones that pass the checks and enroll. It destroys the others at once. Hosts that fail go on a **blocklist**.

### Enrollment **(to test)**

The image needs only `sh` and a Python with Ray.

1. The container command is a small **bootloader**. It downloads the **bootstrap** from the head, checks its SHA-256, and starts it as PID 1.
2. The bootstrap checks the host and sends the head its instance ID and its vast instance key.
3. The head checks the key with the vast API and checks the host from outside.
4. The head sends the cert, the key, the token and the shim. The bootstrap starts Ray and becomes the **watchdog**.

### Failure handling

- **IP change:** the worker fails its health check, and the provider replaces it. **(to test)**
- **Head loss:** the watchdog calls a heartbeat on the head every 30 s. After 5 min without a heartbeat, it destroys its own instance. Tested with a test watchdog: destroy 5 min 11 s after the last heartbeat; a pause of 2 min 18 s gave no destroy. **(to test with the real bootstrap)**
- **Destroy, never stop:** when a container stops, vast starts it again and keeps the full cost (tested). So the watchdog and the provider always destroy.
- **`ray-vast down`** destroys all instances of the cluster and removes everything on AWS.

### Commands

`ray-vast up`, `ray-vast down`, `ray-vast attach` (SSH to the head) and `ray-vast dashboard` (tunnel for `ray job submit`). `ray status` and the Python API work as usual.

## Open items

- The items marked **(to test)**. See [test-plan.md](test-plan.md).
- Ray 2.61 makes token auth the default for all clusters. We run the tests again for each new Ray version.
- Future work ([future-work.md](future-work.md)): short-lived S3 credentials for workers, autoscaler v2 with label selectors, a separate Linux user for the autoscaler on the head, more CLI commands, and a total budget.
