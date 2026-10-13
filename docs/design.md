# ray-vast design (agreed 2026-10-12, worker-first enrollment 2026-10-13, plan decisions 2026-10-12)

Terms are defined in [CONTEXT.md](../CONTEXT.md). Open tests are in [test-plan.md](test-plan.md). Deferred items are in [future-work.md](future-work.md). Test results are in [test-results.md](test-results.md). The implementation plan and the reasons for the decisions D1–D9 are in [ray-vast-pypi/docs/implementation-plan.md](../ray-vast-pypi/docs/implementation-plan.md).

Note: the first design pushed secrets to workers over SSH. We discarded it, because it needs sshd, apt and `sudo` in the image, and many user images do not have them. Workers now enroll themselves with the head.

## Topology

- The head is on AWS EC2, in a region that the user chooses. The workers are vast instances.
- The head runs the same Docker image as the workers, with `--network host`. Then Python and Ray versions match exactly.
- Reason for AWS: if the head stops, the cluster is difficult to recover. vast instances can stop at any time, and on many hosts the IP can change while the instance runs. The AWS security group protects the parts that only the head has (GCS, the dashboard), so a DDoS can stop only single workers, not the cluster.
- The head has `resources: {CPU: 0}` set explicitly. Without explicit resources, the autoscaler asks the provider for the head node, and our provider lists only workers. So `ray-vast up` stops with an error if the head type has no resources. The autoscaler is v2 (`RAY_enable_autoscaler_v2=1`, Ray's default since 2.50). v2 wraps our `NodeProvider` in its `NodeProviderAdapter`. v2 matches a Ray node to an instance by `RAY_CLOUD_INSTANCE_ID`, not by IP, so two workers can share one public IP. `disable_node_updaters` is always True: the provider never uses the Ray node updater.
- The head and the workers run the same Ray version (the image sets it). `ray-vast up` warns if the version is not in the tested list (now: 2.59).
- Nodes talk over the public internet with identity ports. There is no overlay network (vast Docker hosts give no TUN and no NET_ADMIN).
- Measured on 2026-10-06/07 (random Docker hosts): TUN and NET_ADMIN on 0 of 146 hosts; identity ports on 202 of 202; hairpin NAT on about 72%; about 8% had a port that we could not reach, and about 8% had an egress IP that is not `PUBLIC_IPADDR` (the host checks catch both).

## What the image must have

- `sh`, and a Python that can `import ray`. Nothing else: no sshd, no apt, no `sudo`, no curl.
- No ENTRYPOINT (the provider sets the container command).

## Networking shim

- One `LD_PRELOAD` shim runs in all Ray processes on the head and on the workers:
  - `bind(public_ip)` changes to `bind(private_ip)`.
  - `connect(own public_ip)` changes to `connect(private_ip)`. So we do not need hairpin NAT.
  - The dashboard-agent job port and the runtime-env agent port are bound to 127.0.0.1 only (with `SO_REUSEPORT`, because the job port is bound twice in one process).
- The head serves the shim to workers during enrollment.
- Why a shim: without it, the runtime-env agent and the dashboard agent bind to the advertised public IP, which the container (or the EC2 instance) does not own. The bind fails with `Errno 99`, and the raylet stops. Ray's commit history shows that this bug was reported, fixed and then reverted, for a reason that is not clear. Our first fix was a Python `.pth` file that patched `socket.socket.bind`. It covered only Python and needed the correct Python in each image, so the C shim replaced it.
- Details, build and version history: [shim/README.md](../shim/README.md).

## Enrollment (worker-first)

The provider writes the container command (the `args` of the create request). It contains no secrets: vast shows the env and the command in the metadata, and the container log is at a public URL.

1. **Env:** the head address, the SHA-256 of the bootstrap, the SHA-256 fingerprint of the cluster CA (both public), and the cluster name.
2. **Bootloader:** a short `sh` and Python script in the command (about 500 bytes). It finds the Python that can `import ray`. It downloads the bootstrap program from the head, checks its SHA-256, and `exec`s it. So the bootstrap becomes PID 1. The bootstrap then downloads the CA cert, checks its fingerprint, and uses it to verify the head for all later calls.
3. **Bootstrap, local checks:** egress IP equals `PUBLIC_IPADDR`, no NET_RAW and no NET_ADMIN, its own network namespace, empty `/dev/shm`.
4. **Bootstrap, enrollment request:** it opens a listener on every identity port, then sends the head its instance ID, `CONTAINER_API_KEY`, the local check results and the port map.
5. **Head:**
   - It calls `GET /instances/<id>` with that key. vast answers 200 only for that instance.
   - It checks that the instance is one of its candidates, that it has not enrolled before, and that the request comes from the instance's public IP.
   - It connects to every identity port (the outside check).
6. **Head, answer:** if a slot is free, the head makes a key and a cert (SAN = the public IP and 127.0.0.1). It sends them with the token, the shim and the `ray start` parameters, inside the TLS connection. If no slot is free, it destroys the candidate.
7. **Bootstrap, start:** it closes its listeners and writes the files with mode 600. It keeps `CONTAINER_API_KEY` in its own memory for the destroy, removes it from the environment of Ray, and starts Ray as the image's user, with `RAY_CLOUD_INSTANCE_ID=<slot id>` and the host's real core count. If the head refuses the enrollment, the bootstrap destroys its instance at once.
8. **Bootstrap, token check:** without a token, both agent ports on 127.0.0.1 must answer 401. The bootstrap reports the result, and the head destroys the worker if the check fails.

## Security

- Each cluster gets a new CA and a new token. The CA key stays on the head. TLS and `RAY_AUTH_MODE=token` are on for all nodes.
- Cert life: `cert_max_age` (default 365 days) for the CA and for every node cert. There is no automatic renewal: to renew, make a new cluster (`down` + `up`). `ray-vast up` and the head log warn 14 days before expiry. Reason: when a cert expires, all TLS handshakes with that node fail, and Ray probably loads cert files only at process start. So short certs make outages more likely, but they do not add much security.
- Secrets never go into the create request, the env, the command or its output. The create response contains `instance_api_key` and must never be logged.
- The token check after start (enrollment step 8) is necessary: without `RAY_AUTH_MODE=token`, the job agent runs any shell command for anyone.
- Co-tenants on the same host share the Docker bridge (172.17.0.0/16) and can reach every Ray port directly. TLS protects the traffic, and the host check for no NET_RAW and no NET_ADMIN makes sure that a co-tenant cannot sniff or spoof. This depends on the host, so we check it on each host.
- A host has root access to our container, so a malicious host can steal the token and the cert and control the cluster. `datacenter=true` and `verified=true` are added to every offer query. `allow_non_datacenter: true` turns off the datacenter filter and shows a warning.
- The head's credentials are kept to the minimum:
  - The IAM role can only change its own security group and describe EC2.
  - The head uses the user's vast account key, not scoped. vast adds fixed rights to every key (read the account, list all instances, change the account SSH keys), so a scope gives little protection, and a thief who reaches the head controls the cluster anyway. Documented risk: a thief on the head gets the full account key (spend the credit, destroy or reach every instance of the account). If you suspect a leak, rotate the key in the vast console.
  - A worker can run code on the head (`CPU: 0` does not stop a task with `num_cpus=0`), and such a task can read the head's credentials; this is a documented risk.
- vast puts the account's SSH keys into `/root/.ssh/authorized_keys` of every instance. ray-vast does not use SSH.
- Security group rules are kept for each public IP, not for each worker. Two workers can share one public IP: two instances on one host, or two hosts of one vast operator behind one NAT IP (seen in C2–C4: 85.10.218.46).
  - The provider adds a rule (/32, all TCP) for an IP when the instance is `running`, before enrollment.
  - It removes the rule only when no worker or candidate uses that IP any more.
  - The rules have the description `ray-vast:<cluster>`, so the provider can rebuild the mapping after a restart.
  - Limit: about 299 worker IPs without a quota increase. An ENI can have 5 security groups with 60 rules each (300), and the head uses 1 rule for SSH. The head needs no rule for its own public IP, because the shim keeps that traffic inside the instance (C1: 0 packets).
- The enrollment and heartbeat port on the head is reachable only from worker IPs (security group).
- The user gives job credentials with `runtime_env` `env_vars`. They are on every worker. The docs say to use a separate read-only IAM user for one prefix.
- Known gaps (documented):
  - The metrics port has no auth and no TLS (C2: 200 without a token, about 380 `ray_` series). It is read-only.
  - One compromised node means the whole cluster is compromised: all nodes share the token and the CA, and Ray has no permissions for each node.
  - vast has no firewall, so all worker ports are open to the internet. A DDoS can stop single workers.

## Node types

- A node type has declared resources (`CPU`, `GPU`) and an offer query in the vast API JSON form, as a YAML mapping, for example `{gpu_ram: {gte: 24000, lte: 48000}, dlperf: {gte: 40}, num_gpus: {eq: 2}, geolocation: {in: [US]}, inet_down: {gte: 500}}`. `gpu_ram` and `cpu_ram` are in MB; a value below 1000 is an error. There is no parser for the vast CLI syntax.
- `CPU` is a minimum. v2 plans a new node with the declared `CPU`, and a running node with the real core count that Ray reports. `GPU` is exact.
- The provider adds these filters: `num_gpus = GPU`, `cpu_cores_effective >= CPU`, `direct_port_count >= 64`, plus the datacenter and verified filters.
- `ray-vast up` stops with an error if the declared resources contradict the query (for example `GPU: 2` with `num_gpus=1`).
- Ports: 64 identity ports. Ray uses 6 fixed ports, and the other 58 are worker ports. Each worker process (task, actor or driver) needs one port. When no port is free: tasks wait, actors fail after about 6 min with a misleading error, and drivers fail at once. Idle workers keep their ports for about 60 s.
  - Ray has no setting that limits the total number of worker processes.
  - `ray-vast up` warns if `CPU` is more than the number of worker ports. The docs explain the actor error, and that actors also use ports.
  - The provider starts Ray with `--worker-port-list` set to the identity ports, and with `RAY_num_workers_soft_limit` below the port count, so that idle workers release their ports earlier.
- `order_by` sorts the matching offers (default `dph`).
- `GPU: 1` alone is not sufficient: a GTX 1080 and a B200 are very different. So a node type sets both a VRAM range and a `dlperf` minimum.
  - The vast market (1825 offers on 2026-10-12) groups into VRAM tiers: 8, 12, 16, 24, 32, 48, 80, 96 and 140+ GB. Speed changes up to 8× within one tier.
  - Do not use vast's `total_flops`: it is FP32 without tensor cores (an A100 shows 15.6, an RTX 5090 shows 108).
  - Only 7% of offers (124 of 1825) are on datacenter hosts. Most are 16–32 GB consumer GPUs.
- The user chooses the node type names. Example YAMLs give hints: VRAM tiers, `dlperf` floors, `static_ip=true` for long jobs, location and network speed.

## Provisioning (hidden race)

- `create_node(n)` returns n slots at once. The provider rents min(k·n, matching offers that are not on the blocklist) candidates. k is `candidate_factor`, default 2.
- Time limits for each candidate:
  - Pull: `2 × image_size / inet_down`, between 5 and 15 min.
  - Enrollment: 1 min after `running`.
  - An `exited` or error status fails the candidate at once.
- The first candidates that enroll fill the slots. The provider destroys the other candidates at once. A slot that gets no candidate is removed, and the autoscaler asks again.
- Blocklist by `machine_id`, in the provider's memory, one for each cluster (lost at `down` or a head restart). It expires after 30 days for security failures and after 1 day for network or timeout failures.
- `max_dollars_per_hour` is a limit on the `dph_total` of workers plus candidates. The provider does not rent above the limit.

## Failure handling

- When a worker's IP changes, its health check fails. The provider destroys it and removes its security group rule (if no other worker uses the IP). The autoscaler then rents a new worker.
- **Watchdog:** the bootstrap stays as PID 1 and is the watchdog.
  - It calls the head's heartbeat endpoint every 30 s with its cert. The provider serves this endpoint, so the heartbeat fails if the head is down or if only the provider is down.
  - If no heartbeat succeeds for 5 min, it destroys its instance. Before enrollment, it destroys its instance if enrollment does not succeed within 10 min.
  - Destroy = `DELETE /instances/<id>` with `CONTAINER_API_KEY`, retried if the API does not answer. The watchdog always destroys and never only exits: when PID 1 exits, vast starts the container again and runs the command again (C4: 7 restarts in 2 min), so the instance keeps its cost.
  - Before each destroy, it sends the reason to the head and waits up to 10 s for the answer. Reason: after the destroy, vast keeps no container log (C4).
  - On `TERM`, `INT`, `HUP` or an unexpected error, it destroys its instance.
  - As PID 1, it collects its finished child processes.
  - The kernel drops `SIGKILL` to PID 1 from inside the container. Ray runs as the same user as PID 1, so a Ray task can read `CONTAINER_API_KEY` from `/proc/1/environ` and signal PID 1. The signal then causes a destroy. This is documented: a task already has the token and the node cert, which give more control than the key.
- The provider destroys every worker or candidate with the status `exited`. Reason: vast starts a stopped container again and keeps the full cost (C4: `dph_total` did not change).
- After a container restart, the bootloader and the bootstrap run again. The head refuses a second enrollment of the same instance and destroys it.
- Outside check: the provider regularly destroys every instance with the cluster label that is not one of its slots or candidates.
- The provider keeps all its state in memory. A provider restart happens only with a head restart, and then the GCS is new and the old workers cannot rejoin. So, when the provider starts, it destroys every instance with the cluster label and removes every `ray-vast:<cluster>` security group rule (start-up sweep). The enrollment and heartbeat endpoints accept only instances in memory.
- The head server (bootstrap download, enrollment, heartbeat, reports) is a thread in the provider, in the Ray monitor process. If the monitor stops, the heartbeats stop, and the workers destroy themselves.
- `ray-vast down` destroys everything by label and retries the list call (rate limit). Then it removes the security group rules, terminates the EC2 head, and deletes the security group and the IAM role.
- The head has no Elastic IP. A reboot keeps its IP. After a stop and a start, run `down` and `up`.

## CLI (first version)

`ray-vast up`, `ray-vast down`, `ray-vast attach` (SSH to the EC2 head), `ray-vast dashboard` (SSH tunnel to port 8265, for `ray job submit`). Worker logs are available through Ray (dashboard and `ray logs`).

- `ray up`, `ray down`, `ray attach`, `ray exec` and `ray rsync-*` use the provider and SSH to find the head. Our provider does not create the head, so these commands do not work.
- `ray-vast up` creates the EC2 head and starts Ray with `--autoscaling-config`. It runs `bootstrap_config()` itself, because only `ray up` calls it.
- `ray job submit` (through the tunnel), `ray status` on the head, and the Python API work as usual.
