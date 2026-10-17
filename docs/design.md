# ray-vast design

This file is the spec of the system: what each part does, and how the parts work together. Terms are in [CONTEXT.md](../CONTEXT.md). The security model is in [security.md](../ray-vast-pypi/docs/security.md). The reasons for the decisions are in [decisions.md](decisions.md). The user guide (config, CLI, limits) is the docs site, with its source in [ray-vast-pypi/docs/](../ray-vast-pypi/docs/index.md). The parts of this spec that are not in the code yet are in [implementation-plan.md](implementation-plan.md).

Contents:
1. Overview
2. Life of a worker
3. Ray autoscaler v2 contract
4. Provisioning
5. Enrollment protocol
6. The watchdog and the end of a worker
7. The head
8. Network
9. Node types
10. CLI

## 1. Overview

ray-vast runs a Ray cluster with the head on AWS EC2 and the workers on vast.ai containers. The nodes talk over the public internet, across NAT, with TLS and a token. There is no VPN and no overlay network.

```
 your laptop                AWS EC2                          vast.ai
 -----------                -------                          -------
 ray-vast up  ------------> head (Docker, --network host)
                            - Ray head, GCS, dashboard
                            - autoscaler v2 + ray-vast provider
                            - head server (ports 7443, 7444)
                            - CA key, token, vast key
                                  ^   |
                                  |   | rent (vast API)
          enroll + heartbeat      |   v
          (TLS, then mTLS)        |   worker 1 (container)
                                  +-- bootstrap = PID 1 = watchdog
                                  |   Ray worker
                                  +-- worker 2 ...
```

- **Head on EC2, in a region that the user chooses.** Reason: vast instances can stop at any time, and on many hosts the IP can change while the instance runs. If the head stops, the cluster is lost. The AWS security groups protect the parts that only the head has (GCS, dashboard), so a DDoS can stop single workers, but not the cluster.
- **One image for all nodes.** The head runs the workers' Docker image plus the ray-vast package (a derived image that `ray-vast up` builds on the head). So the Python and Ray versions are the same on all nodes.
- **One process on the head does the work.** The provider runs in the Ray monitor process. The head server (enrollment, heartbeat, reports) is a thread in the provider. The provider keeps all its state in memory.
- **Workers enroll themselves.** The head never connects to a worker over SSH. A worker downloads its program from the head, proves its identity with a one-time join token, and gets its secrets. Note: the first design pushed secrets over SSH. We discarded it, because it needs sshd, apt and `sudo` in the image, and many user images do not have them.
- **Measured on 2026-10-06/07 (random Docker hosts):** TUN and NET_ADMIN on 0 of 146 hosts, so an overlay network is not possible; identity ports on 202 of 202; hairpin NAT on about 72%; about 8% had a port that we could not reach, and about 8% had an egress IP that is not `PUBLIC_IPADDR` (the host checks catch both).

## 2. Life of a worker

This is the full flow, from a Ray demand to a running worker and back. Each step names the part that acts. The details are in the sections after this one.

1. **Demand (Ray).** A task, an actor or a placement group needs resources that no node has. Autoscaler v2, in the Ray monitor on the head, chooses a node type and calls `create_node_with_resources_and_labels(node_config, tags, count=n)` from a launcher thread.
2. **Slots (provider).** The provider makes n slots (`slot-<n>-<hex6>`). The n slots of one call are one request.
3. **Rent (provider, in the same call).** The provider searches the vast offers with the node type's query plus the added filters (section 9). It skips the hosts on the blocklist and the offers that would pass `max_dollars_per_hour`. It rents up to k × n candidates (k = `candidate_factor`, default 2) with `PUT /asks/<offer>/`. Each create request has the image, the label `ray-vast:<cluster>`, the identity ports (the smaller of 64 and the offer's `direct_port_count`), the bootloader as the container command, and the env with a new join token for this candidate (5.1). The call then returns the slot IDs, and v2 shows the slots as allocated nodes.
   - If it rents nothing, the provider removes the slots and raises `NodeLaunchException` ("no offers" or "budget"). v2 tries again later.
4. **Pull (vast).** vast pulls the image: `loading`, then `running`. The provider's reconcile thread lists the instances every 10 s. When it first sees the candidate `running` with a public IP, it records the IP and adds a security group rule for it (8.3). A candidate that is not `running` within the pull limit, or that shows `exited` or an error, is ended (section 4).
5. **Bootloader (worker).** The container command is the bootloader. It finds a Python that can `import ray`, downloads the bootstrap from the head (port A), checks its SHA-256, and `exec`s it. The bootstrap is now PID 1.
6. **Bootstrap, before enrollment (worker).** It downloads the CA cert and checks it against the pinned fingerprint. It runs the local host checks. It opens a listener on each of its identity ports. It sends `POST /enroll` with its instance ID, the join token, the check results and its port map.
7. **Enrollment (head server).** The head checks the request: a known candidate, from its own IP, with its own join token, with good host checks. It then connects to all its identity ports from outside (the port check). If a slot of the node type is free, it makes a node key and cert, marks the instance enrolled, sets the slot to `setting-up`, and answers with the cert, the key, the CA, the cluster token, the shim and the `ray start` parameters. If no slot is free, it destroys the candidate.
8. **The other candidates (provider).** When every slot of a request has a worker, the next reconcile pass destroys the other candidates of that request. A candidate that does not enroll within 1 min after `running` is ended. A slot that has no pending candidate left is removed, and v2 asks again. After 3 failed slots in a row of one node type, the rent-loop breaker stops that type for 10 min.
9. **Ray start (worker).** The bootstrap closes its listeners, writes the files, removes the secrets from Ray's environment, and starts `ray start --block` with `RAY_CLOUD_INSTANCE_ID=<slot id>` and the node type's `CPU` (from the `/enroll` answer). The raylet joins the GCS on the head (TLS, token, shim). v2 matches the Ray node to the slot by `RAY_CLOUD_INSTANCE_ID`, marks it running, and schedules the pending work on it.
10. **Token check (worker).** The bootstrap checks that both agent ports on 127.0.0.1 answer 401 without the token, and reports the result. A failed check makes the head destroy the worker.
11. **Running (worker and head).** The bootstrap sends a heartbeat every 30 s on port B (mTLS). The first heartbeat sets the slot to `up-to-date`.
12. **End.** v2 ends an idle worker after `idle_timeout_minutes`: it drains the node and calls `terminate_nodes`, and the provider destroys the instance and removes the IP rule if no other instance uses the IP. All the other ways a worker ends are in section 6.

## 3. Ray autoscaler v2 contract

Facts from the Ray source and the spikes on Ray 2.59.0 ([spikes.md](spikes.md)). The provider is built on them.

| # | Fact | Effect on ray-vast |
|---|---|---|
| F1 | `provider: {type: external, module: "ray_vast.provider.VastNodeProvider"}` loads our class (`_private/providers.py`). | The head image has ray-vast installed. |
| F2 | The autoscaler is v2 (`RAY_enable_autoscaler_v2=1`, the default of `ray up` since 2.50). v2 wraps a normal `NodeProvider` in `NodeProviderAdapter` and calls the same methods. | One `NodeProvider` class. The head starts Ray with `RAY_enable_autoscaler_v2=1`. |
| F3 | v2 matches a Ray node to an instance by `RAY_CLOUD_INSTANCE_ID`, not by IP. | Workers start Ray with `RAY_CLOUD_INSTANCE_ID=<slot id>` and `RAY_NODE_TYPE_NAME=<node type>`. Two workers can share one public IP (S1 check 1). |
| F4 | v2 plans a new node with the declared resources, and a running node with the resources that Ray reports. | Ray gets the declared `CPU`: the `/enroll` answer has it, and the worker starts Ray with `--num-cpus=<CPU>`. So v2 plans and sees the same CPU count. The offer filter is `cpu_cores_effective >= CPU`. vast gives the container a CPU quota of 0.96 × `cpu_cores_effective` (S6), so the cores can be a little below `CPU`. The head refuses a container with fewer cores than 0.9 × `CPU` (5.7). `GPU` is exact. The container's process limit does not change the Ray CPUs. It limits the worker ports (5.5). |
| F5 | v2 needs the head in `non_terminated_nodes`, with the tags `ray-node-type: head` and `ray-user-node-type: head`. Without it, v2 marks the head TERMINATED and never scales, with no error. A node without a correct `ray-node-type` tag is skipped as "unmanaged". | The provider lists the node `head` (the head's `RAY_CLOUD_INSTANCE_ID`). |
| F6 | Each 5 s loop calls `non_terminated_nodes` once, and `node_tags` and `is_running` for each node. `create_node_with_resources_and_labels` runs in the `NodeLauncherPool` threads; every exception becomes a `LaunchNodeError`. `terminate_nodes` runs in one adapter thread. v2 never calls `set_node_tags`, `is_terminated`, `internal_ip` or `external_ip`. | The read methods answer from memory, with no vast call. One `RLock` guards the state; no network call is made while it is held. |
| F7 | v2 makes the provider once per monitor process and reads the config again each loop. | One provider object, so the head server thread is started once. |
| F8 | v2 times out an instance that stays "allocated, not Ray running" after `RAY_AUTOSCALER_RECONCILE_ALLOCATE_STATUS_TIMEOUT_S` (default 3600 s). | Keep the default. Our limits (pull ≤ 15 min, enrollment 1 min) end a failed slot first. |
| F9 | `disable_launch_config_check` is True by default in v2. With False, v2 marks every worker as outdated. | Keep the default. |
| F10 | `bootstrap_config()` is called only by `ray up`. The monitor calls only `validate_config()`. | `ray-vast up` runs `bootstrap_config()` and writes the final YAML to the head. `disable_node_updaters` is always True. |
| F11 | Ray never restarts a dead monitor. With `ray start --block`, Ray stops the whole head when a child dies. A worker raylet exits 60 s after it loses the GCS; `ray start --block` then exits with code 1. | No self-healing (7.3). The bootstrap relies on the exit of `ray start --block`. |
| F12 | The head container needs `docker run --init`. Without it, Ray processes stay as zombies after `ray stop`. | `up` uses `--init`. |

## 4. Provisioning

- **Requests.** One create call makes one request: its slots and its candidates. The candidates are spread over the slots of the request. The head server fills any free slot of the candidate's node type. When every slot of a request has a worker, the provider destroys the other unenrolled candidates of that request.
- **Time limits for each candidate:**
  - Pull: `2 × image_size_gb × 8000 / inet_down` seconds, between 5 and 15 min (15 min when `image_size_gb` is not set). `up` measures the image size; the user cannot set it.
  - Enrollment: 1 min after the provider first sees `running`.
  - Missing from vast's list: ended 60 s after the last list that showed it (60 s after the rental if no list showed it). One list without it does not end it: a list can be partial, or a user can edit the label in the console.
  - An `exited` or error status ends the candidate at once.
- **Slots.** A slot with a worker stays until the worker ends. A free slot is removed when no pending candidate of its node type is left. v2 then asks again.
- **Budget.** `max_dollars_per_hour` (required) is a limit on the sum of `dph_total` over workers, candidates and rentals in progress. The provider does not rent above it.
- **Blocklist.** By `machine_id`, in memory, one for each cluster (lost at `down` or a head restart). Kinds and expiry:
  - `security` (30 days): a failed capability, network namespace or `/dev/shm` check, a GPU count other than the declared one, or fewer CPU cores than 0.9 × the declared `CPU`.
  - `network` (1 day): a failed egress IP check or port check; an `exited` or error status before enrollment.
  - `timeout` (1 day): a pull or enrollment timeout.
  - The end of an enrolled worker (IP change, exit, no heartbeat) does not blocklist the host.
- **Rent-loop breaker.** For each node type, the provider counts the slots that end with no enrolled worker. A successful enrollment sets the count to 0. After 3 failed slots in a row, the provider refuses new launches of that node type for 10 min (`NodeLaunchException("repeated enrollment failures")`, logged at ERROR in `monitor.log`; `ray status` shows `<node type>: LaunchFailed` under "Recent failures", without the reason text), then tries again. Reason: in C5a, a fault that affected every host made the provider rent 10 instances in 5 min.
- **Retries.** A create is retried only on HTTP 429: a lost answer may already have rented the offer. The orphan sweep (6.2) destroys a rental that the provider did not record. List and destroy calls also retry on 5xx and connection errors (5 tries, backoff 1-2-4-8 s with jitter, 30 s limit for each call). A destroy of an instance that is already gone (404) is a success.
- **Start-up sweep.** A new provider destroys every instance with the cluster label and removes every `ray-vast:<cluster>` security group rule. Reason: a provider starts only with a new head, and a new head has a new GCS, so the old workers cannot rejoin.

## 5. Enrollment protocol

This is the contract between the worker side (`worker/bootloader.sh`, `worker/bootstrap.py`) and the head server (`head_server.py`). A change here needs both sides to change.

### 5.1 Container environment

vast shows the env and the command of an instance in its metadata, and the container log is at a public URL. So the command and the log hold no secrets. The env holds one secret with a small exposure, the join token.

The provider sets these in the create request (`env`):

| Variable | Value |
|---|---|
| `RAYVAST_HEAD_IP` | the head public IP |
| `RAYVAST_PORT_A` | head port A (TLS), default 7443 |
| `RAYVAST_PORT_B` | head port B (mTLS), default 7444 |
| `RAYVAST_BOOTSTRAP_SHA256` | SHA-256 (hex) of the bootstrap bytes that `/bootstrap` serves |
| `RAYVAST_CA_SHA256` | SHA-256 (hex, lowercase) of the CA cert DER bytes |
| `RAYVAST_CLUSTER` | the cluster name |
| `RAYVAST_JOIN_TOKEN` | the one-time join token of this instance: 32 random bytes, lowercase hex (secret) |

vast sets: `CONTAINER_ID` (the instance ID), `CONTAINER_API_KEY` (secret, one for each instance), `PUBLIC_IPADDR`, and `VAST_TCP_PORT_70000` up to at most `VAST_TCP_PORT_70063` (the host port P for each identity port that the create request asked for; vast maps P→P).

For tests only, the bootstrap reads these when they are set: `RAYVAST_VAST_API` (vast API base URL, default `https://console.vast.ai/api/v0`), `RAYVAST_TIME_SCALE` (a float; all bootstrap timers are divided by it), `RAYVAST_DIR` (the directory for the files, default `/tmp/rayvast`), `RAYVAST_CGROUP_DIR` (the cgroup directory for the CPU quota files and the pids files, default `/sys/fs/cgroup`), `RAYVAST_SKIP_HOST_CHECKS` (`1` = report the checks but do not fail on them; for local Docker tests, where the egress IP is not `PUBLIC_IPADDR`).

### 5.2 Join token

The join token proves to the head that an `/enroll` request comes from the instance that the provider rented. It is the bootstrap-token pattern of `kubeadm join --token`.

- The provider makes a new token for each candidate at rent time (`secrets.token_hex(32)`), keeps it in memory with the candidate, and puts it in the create request env.
- The token is valid for one instance ID, only from that instance's public IP, and only for one enrollment. It becomes invalid when the instance enrolls or when the provider ends the candidate.
- The head compares tokens in constant time (`hmac.compare_digest`).
- Nobody prints or logs the token. The bootstrap keeps it in memory, removes `RAYVAST_JOIN_TOKEN` from its own environment and from Ray's environment, and sends it only inside `/enroll` (TLS with the pinned CA).
- Who can read it: see [security.md](../ray-vast-pypi/docs/security.md).
- `CONTAINER_API_KEY` never leaves the worker. The bootstrap uses it only for its own `DELETE`. Reason for the join token: on real vast, the head's `GET /instances/<id>/` with a worker's `CONTAINER_API_KEY` gives HTTP 401 (C5a, C5d), so the head cannot use that key to check the instance.

### 5.3 Bootloader

A short `sh` and Python script in the container command (`args`, about 1.5 KB). It holds no secrets.

The image's `ENTRYPOINT` stays. Docker runs it with the container command as its arguments. An ENTRYPOINT that ends with `exec "$@"` (for example `/opt/nvidia/nvidia_entrypoint.sh` in `rayproject/ray:*-gpu`, which prepares CUDA) then runs the bootloader, and the bootstrap is still PID 1. Text that the ENTRYPOINT writes goes to the container log.

1. Find a Python that can `import ray`: `$PATH`, `/home/ray/anaconda3/bin`, `/opt/conda/bin`, `/usr/local/bin`.
2. Download `/bootstrap` from port A (TLS, no cert check).
3. Compare its SHA-256 with `RAYVAST_BOOTSTRAP_SHA256`. If it is the same, `exec` it.
4. It never exits. When a download fails, it tries again. After a hash mismatch, or after 10 min of failures, it destroys its own instance with `CONTAINER_API_KEY`. If the image has no Python with Ray, it waits, and the provider's limits end the instance.

### 5.4 Bootstrap, before enrollment

1. **Pin the CA.** Download `/ca.pem` from port A without verification. Refuse a PEM with more than one cert. Compare the SHA-256 of the DER with `RAYVAST_CA_SHA256`. Make the PEM again from the DER that was hashed, and use only that copy. All later calls verify the head with it (chain and IP SAN). Reason for the rebuild: a PEM with two certs could pass the hash while OpenSSL trusts both.
2. **Local host checks:**
   - `egress_ip_ok`: the egress IP equals `PUBLIC_IPADDR`. It asks `https://api.ipify.org`, then `https://ifconfig.me/ip` if the first does not answer 200.
   - `no_net_raw_admin`: bits 12 (NET_ADMIN) and 13 (NET_RAW) are clear in `CapBnd` of `/proc/self/status`. It reads `CapBnd` (the container's bounding set), not `CapEff`: a non-root PID 1 always has an empty `CapEff`.
   - `own_netns`: its own network namespace.
   - `shm_empty`: `/dev/shm` is empty.
3. **Listeners.** Open a listener on each identity port that has a `VAST_TCP_PORT_700xx` variable (host ports P), for the head's port check.
4. **Enroll.** `POST /enroll` (5.6). The body has the size of the CPU affinity (`cpu_count`) and the CPU quota in cores (`cpu_quota`): `cpu.max` on cgroup v2; `cpu/cpu.cfs_quota_us` / `cpu/cpu.cfs_period_us` on cgroup v1, where a quota of -1 or a period of 0 is no limit. No limit or no file gives null. A 503, a connection error or a timeout: try again after 10 s. A 403: report and destroy at once. No enrollment within 10 min of the start: destroy. The worker-side limit for one `/enroll` call is 120 s.

### 5.5 Port map (worker)

| Identity port | Use | Ray flag |
|---|---|---|
| 70000 | node manager | `--node-manager-port` |
| 70001 | object manager | `--object-manager-port` |
| 70002 | metrics export | `--metrics-export-port` |
| 70003 | agent gRPC | `--dashboard-agent-grpc-port` |
| 70004 | agent HTTP (job port, 127.0.0.1 through the shim) | `--dashboard-agent-listen-port` |
| 70005 | runtime-env agent (127.0.0.1 through the shim) | `--runtime-env-agent-port` |
| 70006 up to 70063 | worker processes: the mapped identity ports minus 6, at most 58 | `--worker-port-list` |

The bootstrap uses the host ports P (`VAST_TCP_PORT_700xx`), not the identity numbers.

**Worker ports and the process limit.** Many vast containers have a cgroup pids limit (the process limit). Every process and every thread counts. Measured: 1280 on two cgroup v1 hosts, 3328 on a cgroup v2 host. vast sets `pids.max` = 256 × the CPU quota in whole cores (rounded down; the probes of 2026-10-17: 1280 at 5.76 cores, 9728 at 38.4, 15616 at 61.44). So many CPUs and `pids.max` 1280 do not occur together. `RLIMIT_NPROC` is unlimited. At the limit, a thread start fails (`EAGAIN`). Then the raylet aborts, and the worker dies. The bootstrap reads the limit from `/sys/fs/cgroup/pids.max` (cgroup v2, when `/sys/fs/cgroup/cgroup.controllers` exists), else from `/sys/fs/cgroup/pids/pids.max` (cgroup v1). `max` or no file means no limit. Three settings keep the worker below the limit:

1. `RAY_num_grpc_internal_threads=4`, always. Ray's gRPC sizes its thread pools by `sysconf(_SC_NPROCESSORS_CONF)` (the host cores) unless this variable is set. Every process of `ray start` inherits it. Measured on a simulated 48-core host: a worker process has 25 threads, not 50; the raylet 59, not 85.
2. `RAY_worker_maximum_startup_concurrency=8`, always. The raylet starts at most 8 worker processes at the same time (without it: `min(cores, Ray CPUs)`, 48 on a 48-core host). The variable wins over the `--maximum_startup_concurrency` flag of `ray start` (measured: at most 2 starting processes with the value 2, 39 without it). A starting process has threads before it can find a free port, so this caps the burst.
3. Worker ports = the mapped identity ports − 6 (at most 58) with no limit, else `min(identity ports − 6, max(4, (pids.max − 400) // 60))`. `worker_ports_and_soft_limit` in the bootstrap is the one place of this formula. The offer filter `direct_port_count >= 10` keeps at least 4 worker ports.
    - 400 = the threads of the fixed Ray processes and the bootstrap (about 160: raylet, dashboard agent, runtime-env agent, log monitor, `ray start`), plus 8 starting worker processes (8 × 25), plus 40 of headroom. A process that finds no free port also starts, and then exits. The startup concurrency caps these processes too.
    - 60 = the threads of the heaviest common worker process, plus 10 for the user's own threads. Measured with gRPC threads 4 on a simulated 48-core host: a Serve replica with a FastAPI ingress has 50 (its Python `grpcio` adds an event-engine pool of 16). A Ray Train worker process has 35, plus 3 for each `DataLoader` worker process. The Serve proxy has 35. A Data actor, an RLlib env runner, a task and an idle worker process have 25 to 29.
    - With 1280, the worker gets 14 ports. With 3328, it gets 48 ports.
    - The floor of 4 keeps a worker with a very small limit useful. A Data map task and its aggregator need 2 ports. Below a limit of about 640, such a worker can pass the limit. Then the head warns (5.7a).

A Ray worker process needs a free worker port. A process that finds no free port exits, and its task or actor waits for a port. So the port count caps the worker processes. Too much work queues or hangs on the worker. It does not crash the worker. `RAY_num_workers_soft_limit` = min(Ray CPUs, worker ports // 2). The raylet keeps idle worker processes while the number of worker processes is at or below this limit. It ends idle worker processes above the limit after they are idle for `idle_worker_killing_time_threshold_ms` (default 1 s). It never ends an idle worker process to free a port for a new kind of worker process (for example, another runtime env). Measured with 25 ports: with the soft limit 25, 25 idle worker processes held all ports, and a task with another runtime env waited forever. With the soft limit 12 or 6, it ran. So the soft limit is at most half the ports. The bootstrap decides all these values. The head does not send them.

### 5.6 Head server endpoints

All bodies and answers are JSON. Port A: TLS, no client cert. Port B: TLS, a client cert from the cluster CA is required. Reason for two ports: one TLS context cannot ask for client certs on some paths only (S5). The TLS handshake runs in the request thread, with a time limit, so one slow client cannot stop the server. Only worker IPs can reach the ports (8.3).

| Port | Method, path | Request | Answer |
|---|---|---|---|
| A | `GET /ca.pem` | — | the CA cert PEM (`application/x-pem-file`) |
| A | `GET /bootstrap` | — | the bootstrap bytes (`text/x-python`) |
| A | `POST /enroll` | `{"instance_id": int, "join_token": str, "checks": {"egress_ip_ok": bool, "no_net_raw_admin": bool, "own_netns": bool, "shm_empty": bool, "details": str}, "ports": {"70000": P, …} (10 to 64 identity ports, from 70000 up), "cpu_count": int, "cpu_quota": float|null, "gpu_count": int}` | 200 `{"slot_id", "node_type", "cert_pem", "key_pem", "token", "shim_b64", "gcs_address": "<head_ip>:6379", "num_cpus": int}`; 403 `{"reason"}` (refused); 503 `{"reason"}` (the head has not seen the instance `running` yet; try again) |
| A | `POST /report` | `{"instance_id": int, "kind": "destroy", "reason": str}` (before enrollment) | 200 `{}` if it can be parsed. The head only logs it. |
| B | `POST /heartbeat` | `{"instance_id": int, "pids_current": int\|null, "pids_max": int\|null, "pids_refused": int\|null, "ports_in_use": int\|null, "ports_total": int}` | 200 `{}`; 403 `{"reason": "unknown"}` when the head does not know this worker |
| B | `POST /report` | `{"instance_id": int, "kind": "token_check", "ok": bool, "reason": str}` or `{"instance_id": int, "kind": "destroy", "reason": str}` | 200 `{}` |

### 5.7 Head rules for `/enroll`

- **Order of the checks:** the body (400 if it is not valid); a known candidate that is not enrolled yet; the source IP equals the candidate's public IP; the join token equals the candidate's token; the host checks; the GPU count and the CPU cores; the port check; a free slot. The head makes no vast call during `/enroll`.
- **GPUs:** the worker's `gpu_count` is the number of `/dev/nvidia<N>` device nodes in its container. `/proc/driver/nvidia/gpus` is not used: it shows every GPU of the host (C5). The head accepts only a `gpu_count` equal to the node type's `GPU` (F4). So the worker starts Ray with its own `gpu_count`, and the answer has no GPU field.
- **CPUs:** `cpu_count` is the size of the worker's CPU affinity (`os.sched_getaffinity`). `cpu_quota` is its cgroup CPU quota in cores (a float), or null when it has no quota (5.4). The cores are the smaller of the two (`cpu_count` when there is no quota). The head refuses cores below 0.9 × the node type's `CPU`. Reason: vast gives a quota of 0.96 × `cpu_cores_effective` (S6, 303 of 322 machines), and the offer filter is `cpu_cores_effective >= max(CPU, 4)`. The 0.9 also catches a future change of vast's factor. The answer's `num_cpus` is the node type's `CPU`, and the worker starts Ray with it (F4).
- **Body limits:** `ports` has the keys 70000 up to 70000 + n − 1, with n from 10 to 64 (the provider rents min(64, `direct_port_count`) identity ports, and the filter is `direct_port_count >= 10`), and n different values in 1–65535; `cpu_count` is 1–4096; `cpu_quota` is null or a number above 0 and at most 4096; `gpu_count` is 0–64. Other values give 400.
- **Refusals without a destroy:** an unknown instance, an instance that is already enrolled, a wrong source IP, a missing or wrong join token. Reason: a stranger must not be able to kill our candidates. The provider's time limits end such candidates.
- **Refusals with a destroy:** after a valid token, a failed host check, a `gpu_count` other than the node type's `GPU` (in both directions; blocklist `security`), fewer cores than 0.9 × the node type's `CPU` (blocklist `security`), a failed port check or no free slot destroys the candidate. A host fault also puts the host on the blocklist (section 4).
- **Port check:** the head opens a TCP connection to every identity port P on the public IP, in parallel, 5 s limit for each. Every port must accept.
- **Node cert:** the head makes it before it marks the instance enrolled, so a failure leaves no half-enrolled slot. The cert has the IP SANs of the worker's public IP and 127.0.0.1, and the URI SAN `rayvast:instance/<instance_id>`. On port B the head checks both against the body's `instance_id` and the enrolled IP. A cert for another instance gets 403 `unknown`.
- **Lost answer:** if an `/enroll` answer is lost, the retry gets 403 "already enrolled", and the worker destroys itself. This is rare and safe.
- **Log:** the head logs each decision with the instance ID, machine ID, IP and reason, never a secret.

### 5.7a Heartbeat status

The heartbeat carries the state of the process limit (5.5). Only `instance_id` is required: the head accepts a heartbeat without the other fields, and treats a value that is not an integer as missing.

| Field | Source on the worker |
|---|---|
| `pids_current` | `pids.current` (null without a limit) |
| `pids_max` | `pids.max` (null without a limit) |
| `pids_refused` | the number in the `max N` line of `pids.events`: forks and thread starts that the limit refused (null without a limit) |
| `ports_in_use` | the worker ports with a listening socket in `/proc/net/tcp` and `/proc/net/tcp6`. A registered worker process listens on its port. Two small file reads; no process scan. |
| `ports_total` | the number of worker ports (5.5) |

The head logs one WARNING line in `monitor.log` for a worker in these cases:

- `pids_refused` is higher than in the last warning. At most once a minute.
- All worker ports are in use, or `pids_current` is above 90 % of `pids_max`. At most once in 5 minutes.

`+<new>` is the rise of `pids_refused` since the last warning. The line:

```text
worker <slot id> (instance <id>, ip <ip>) is at its process limit: <N> refused forks/threads (+<new>), pids <current> of <max>, worker ports in use <in use> of <total>. Ray work on this worker can queue, hang or fail. See https://duc-ph.github.io/ray-vast/how-to/troubleshoot/#a-worker-is-at-its-process-limit
```

### 5.8 Bootstrap, after enrollment

1. Close the listeners. Write the files in `/tmp/rayvast` (mode 700): `ca.pem`, `node.pem`, `node.key` (600), `token` (600), `librayvast_shim.so`.
2. Keep `CONTAINER_API_KEY` and the join token in memory only, and remove both from the environment of the Ray child.
3. Start `ray start --block` as a child, as the image's user, with: `--address` (the GCS), `--node-ip-address` (the public IP), the 6 fixed ports and `--worker-port-list` (5.5), `--num-cpus` = the answer's `num_cpus` (the node type's `CPU`, F4), `--num-gpus` = its own `gpu_count` (5.7), `RAY_CLOUD_INSTANCE_ID=<slot id>`, `RAY_NODE_TYPE_NAME`, `RAY_num_workers_soft_limit` = min(`num_cpus`, worker ports // 2) (Ray's own default soft limit is the node's CPU count too), `RAY_num_grpc_internal_threads=4`, `RAY_worker_maximum_startup_concurrency=8` (5.5), the TLS and token env, and the shim env.
   It logs one start line: `limits: pids.max <N or none> (cgroup v1|v2), cpus <n>, worker ports <n>, soft limit <n>, grpc threads 4, startup concurrency 8`.
4. Token check: without a token, both agent ports on 127.0.0.1 must answer 401. Send the result in `POST /report` (port B). Reason: without `RAY_AUTH_MODE=token`, the job agent runs any shell command for anyone.
5. Become the watchdog (section 6).

## 6. The watchdog and the end of a worker

### 6.1 The watchdog

The bootstrap stays as PID 1 and is the watchdog.

- It sends `POST /heartbeat` on port B every 30 s, with the status of the process limit (5.7a). The interval (30 s) and the timeout (300 s) are constants of the bootstrap. The head does not send them. The provider serves this endpoint, so the heartbeat fails when the head is down and also when only the provider is down.
- It collects its finished child processes.
- **Destroy, never exit.** When PID 1 exits, vast starts the container again and runs the command again, and the instance keeps its full cost (C4: 7 restarts in 2 min). So the watchdog always destroys its instance: `DELETE {RAYVAST_VAST_API}/instances/<CONTAINER_ID>/` with `Authorization: Bearer <CONTAINER_API_KEY>`, retried every 5 s until 200 or 404. Vast calls do not follow redirects.
- **Report first.** Before each destroy, it sends the reason to the head (port B if enrolled, else port A) and waits up to 10 s. Reason: after a destroy, vast keeps no container log (C4).
- Every HTTP call of the worker has a limit on the total time, not only on each read, so a slow server cannot hang PID 1. The call runs in a thread. After the limit, that thread can still read a slow answer, and each thread counts against `pids.max`. So a URL has at most one call thread: while it runs, a new call to the URL fails at once (for example, the next heartbeat).
- If `CONTAINER_ID` or `CONTAINER_API_KEY` is not set, the bootstrap cannot destroy its instance. It logs the names of the missing variables and waits. It does not exit, because vast would start the container again in a loop. The provider's time limits end the instance.

### 6.2 All the ways a worker ends

| Trigger | Who acts | What happens |
|---|---|---|
| Idle for `idle_timeout_minutes` | v2 | v2 drains the node and calls `terminate_nodes`; the provider destroys the instance. |
| No heartbeat success for 300 s | bootstrap | Report, then destroy. |
| A 403 on `/heartbeat` or `/enroll` | bootstrap | Report, then destroy. |
| No enrollment within 10 min | bootstrap | Report, then destroy. |
| `ray start --block` exits (raylet death; GCS lost for 60 s) | bootstrap | Report, then destroy. |
| `TERM`, `INT`, `HUP`, or an unexpected error in the bootstrap | bootstrap | Report, then destroy. |
| A container restart | bootstrap | The bootloader and the bootstrap run again; `/enroll` gets 403 "already enrolled"; destroy. |
| A failed token check | head | The head destroys the worker. |
| No heartbeat for 5 min (head side) | provider | Destroy. |
| The public IP in vast changes | provider | Destroy (the Ray node cannot keep its address). |
| vast status `exited` or an error | provider | Destroy. Reason: vast starts a stopped container again and keeps the cost (C4). |
| Gone from vast's list for 60 s after the last list that showed it | provider | Destroy (through `_end`); the slot goes. Reason: if the instance still runs, a forgotten instance costs money. A destroy of an instance that is gone is harmless (404 is a success). |
| An instance with the cluster label that the provider does not know | provider (orphan sweep, each reconcile pass) | Destroy. |
| `ray-vast down` | CLI | Terminate the head, then destroy every instance with the label. |

Every end by the provider or the head server goes through one function, the provider's `_end`. It removes the candidate from the state, removes the slot that the instance fills, blocks the host if the reason has a block kind, destroys the instance, lowers the IP rule's ref count, and removes the rule at 0 (8.3). A failed destroy is logged; the orphan sweep tries again. A free slot stays; the reconcile pass removes it when no pending candidate of its node type is left (section 4). A worker that leaves vast's list goes through the same function.

### 6.3 Signals to PID 1

The kernel drops `SIGKILL` to PID 1 from inside the container. Ray runs as the same user as PID 1, so a Ray task can send `TERM`, `INT` or `HUP` to PID 1, which then destroys the instance. It can also read `CONTAINER_API_KEY` from `/proc/1/environ`. Both are documented risks in [security.md](../ray-vast-pypi/docs/security.md).

## 7. The head

### 7.1 `ray-vast up`

1. Read and check the cluster YAML, and run `bootstrap_config()` (F10). Show the warnings. Stop if the cluster already exists (an EC2 head with the cluster tag, or `~/.ray-vast/<cluster>.json`).
2. Make the IAM role with the permissions boundary `arn:aws:iam::<account>:policy/ray-vast-head-boundary`. The user creates that policy once (`ray_vast/iam/ray-vast-head-boundary.json`); `up` never creates it and never checks for it. If it is missing, `CreateRole` fails with `NoSuchEntity`, and `up` stops with a pointer to the setup docs. The role is the first resource, so nothing is left.
3. Write `~/.ray-vast/<cluster>.json` with the region and the AWS profile. `down`, `attach` and `dashboard` use that profile when `--profile` and `AWS_PROFILE` are not given.
4. Make the EC2 key pair `ray-vast-<cluster>` (ed25519, tagged). Save the private key in `~/.ray-vast/<cluster>/ssh_key.pem` (mode 600, directory 700). All SSH to the head uses it.
5. Make the 5 security groups `ray-vast-<cluster>-0` … `-4` (tagged). Group 0 gets one SSH rule for the operator's IP. Make the role policy and the instance profile. The role can only add and remove rules in the cluster's 5 security groups, and describe EC2.
6. Start the EC2 head: Ubuntu 24.04, the head node type's `node_config.instance_type` (default `t3.large`) in its `node_config.region`, 60 GiB gp3, IMDSv2 required, no Elastic IP, Docker installed by user data. All 5 groups go on its network interface.
7. Build the derived image `FROM <image>` plus ray-vast. The first source that applies wins:
    - `--wheel FILE`. `up` copies the wheel to the head.
    - The source tree, when the CLI runs from a git clone. `up` builds the wheel on the laptop. This is for development.
    - PyPI, in all other cases. The image runs `pip install ray-vast==<the CLI's version>`.

    The first output line names the source. A failed build from PyPI stops `up`. The message names the version, and the end of the build output follows. To test a release on TestPyPI, download its wheel (`pip download --no-deps --index-url https://test.pypi.org/simple/ ray-vast==<version>`) and pass it with `--wheel`. Read the image's Ray version (warn if it is not in the tested list, now 2.59) and the shim path: `docker run` with the image's ENTRYPOINT prints one line `RAYVAST_IMAGE_INFO <ray version> <shim path>`, and `up` reads only that line, because an ENTRYPOINT can write other text to stdout first (C8). No such line stops `up` with an error. Read the image's size (`image_size_gb`).
8. Copy the vast key to the head (mode 600). Make the CA, the cluster token and the head cert on the head (`pki.py`). Write the final YAML. Its `provider` gets the internal keys `head_ip`, `secrets_dir`, `security_group_ids`, `region` and `image_size_gb`; they are not fields of the user's YAML.
9. Start the head container: `docker run --init --network host` with no restart policy, `--shm-size` = 0.33 × the head's RAM (Ray's object store rule; Docker's default is 64 MB), main process `ray start --head --block --num-cpus=0 --autoscaling-config …`, with `RAY_enable_autoscaler_v2=1`, `RAY_CLOUD_INSTANCE_ID=head`, the TLS, token and shim env, and the fixed ports below.
10. Wait until `ray status` works.

`down` deletes the key pair (found by name and tag, like the security groups) and the private key file, also after a failed `up`.

Head ports (`--network host`): GCS 6379; dashboard 8265 (127.0.0.1); node manager 6380; object manager 6381; metrics 6382; agent gRPC 6383; agent HTTP (job port) 6384 and runtime-env agent 6385 (127.0.0.1 through the shim); workers 10002–10100; head server 7443 (A) and 7444 (B). Ray also opens 10001 (Ray client, private IP and 127.0.0.1) and random ports for the monitor and the dashboard.

The provider and the head server log to `/tmp/ray/session_latest/logs/monitor.log` in the head container.

### 7.2 Cluster PKI

- Each cluster gets a new CA and a new token. The CA key stays on the head. TLS and `RAY_AUTH_MODE=token` are on for all nodes.
- RSA-2048 keys. Leaf certs have serverAuth and clientAuth. `not_before` is 5 min in the past (worker clock skew). The CA has `pathlen:0`.
- Cert life: 365 days (a constant) for the CA and for every node cert. There is no automatic renewal: to renew, make a new cluster (`down` + `up`). The head log warns 14 days before the CA expires (a leaf cert never lives longer than the CA). Reason: when a cert expires, all TLS handshakes with that node fail, and Ray loads cert files only at process start. Short certs make outages more likely, and they do not add much security.
- Files on the head: `ca.pem` and `head.pem` mode 644; keys and the token mode 600; the directory mode 700.
- The CA fingerprint is the SHA-256 of the CA cert's DER bytes, lowercase hex (S5).

### 7.3 No self-healing

The head container runs `ray start --head --block` with no Docker restart policy. Ray never restarts a dead monitor (F11). With `--block`, a dead monitor stops the head. Then the heartbeats stop, and every worker destroys itself within 5 min. The EC2 head stays until the user runs `ray-vast down` (and `up` again). This is the same result as with standard `ray up` clusters. A restart of only the autoscaler is in [future-work.md](future-work.md).

The head has no Elastic IP. A reboot keeps its IP. After an EC2 stop and start, run `down` and `up`.

## 8. Network

### 8.1 Identity ports

Each worker asks vast for the identity ports from 70000 up (`-p 700xx:700xx`): the smaller of 64 and the offer's `direct_port_count`. A host with 10–63 direct ports is 137 of 705 verified machines (2026-10-17), so a fixed 64 left out about 20 % of them. vast gives a host port P for each, maps P→P, and puts P in `VAST_TCP_PORT_700xx`. Ray advertises the public IP and the ports P, so other nodes reach the worker directly. The port map is in 5.5.

### 8.2 Networking shim

One `LD_PRELOAD` shim (`librayvast_shim.so`) runs in all Ray processes on the head and on the workers:

- `bind(public_ip)` becomes `bind(private_ip)`.
- `connect(own public_ip)` becomes `connect(private_ip)`. So hairpin NAT is not necessary, and the head needs no security group rule for its own IP.
- The agent job port and the runtime-env agent port are bound to 127.0.0.1 only. So the token never goes over the network.

Reason: without it, the runtime-env agent and the dashboard agent bind to the advertised public IP, which the container (or the EC2 instance) does not own. The bind fails with `Errno 99`, and the raylet stops. The head serves the shim to the workers in the `/enroll` answer. Rules, build and version history: [shim.md](shim.md).

### 8.3 Security group rules

- Rules are kept for each public IP, not for each worker. Two workers can share one public IP: two instances on one host, or two hosts of one vast operator behind one NAT IP (seen in C2–C4: 85.10.218.46).
- The provider adds a rule (/32, all TCP) for an IP when it first sees a candidate `running`, before enrollment. Under the security group lock, it checks the IP's ref count again first: if the candidate ended in the meantime, it adds no rule. The rule goes into the first of the 5 groups that has room (when AWS refuses a rule because a group is full, it tries the next group). When all 5 groups are full, the candidate is ended, and the error says to raise the quota.
- It removes the rule only when no worker or candidate uses the IP any more (a ref count).
- The rules have the description `ray-vast:<cluster>`, so the start-up sweep and `down` can find them.
- Capacity: 5 groups (the AWS default limit for one network interface), 60 rules each by default: about 299 worker IPs (group 0 also holds the SSH rule). The user can raise the per-group quota in AWS Service Quotas.

## 9. Node types

This section is about how the provider uses a node type. How to write one is in the user docs: [node types](../ray-vast-pypi/docs/how-to/node-types.md) and [configuration](../ray-vast-pypi/docs/reference/configuration.md).

- A node type has declared resources (`CPU`, `GPU`) and an `offer_query` in the vast API JSON form (a YAML mapping, MB units). There is no parser for the vast CLI syntax.
- The provider adds these filters: `num_gpus = GPU`, `cpu_cores_effective >= max(CPU, 4)`, `direct_port_count >= 10`, `rentable = true`. A user filter on an added field must be at least as strict, and is merged.
    - The core floor of 4: below about 3 effective cores, `pids.max` is below 640 (S6), and the 4 worker ports have no thread room (B = 0).
    - The port floor of 10: the 6 fixed identity ports and the minimum of 4 worker ports (5.5).
- `verified = true` and `datacenter = true` are recommended, not added. The example YAMLs have them as normal `offer_query` lines. Reason (S6): verified hosts did not start in 12 min in 2 % of the rentals, deverified hosts in 50 %; all identity ports were reachable from outside on 100 % of the datacenter hosts and 77 % of the unverified hosts.
- The search sends `type: on-demand`, `limit: 100`, and sorts by `dph_total` ascending (the cheapest first). The user cannot change the order.
- `up` stops with an error if the declared resources contradict the query, or if the head type does not declare `resources: {CPU: 0}`.

## 10. CLI

`ray-vast up`, `down`, `attach` (SSH to the head), `dashboard` (copies the token to the laptop and opens an SSH tunnel to port 8265). The options and the state files are in the user docs ([CLI](../ray-vast-pypi/docs/reference/cli.md)).

- `up`: 7.1.
- `down`: terminate the EC2 head first (so its autoscaler cannot rent new workers); then destroy every vast instance with the cluster label (list again until 0, retried on rate limits); then delete the 5 security groups (with their rules), the role and the instance profile. It finds everything by tag, name and label, so it works after a failed `up`, when the head is gone, and when the local state file is missing. It needs no SSH key. Without a vast key, it cleans AWS and warns that the vast instances were not swept. It prints `left: nothing`, or lists what is left and exits with code 1.
- `ray up`, `ray down`, `ray attach`, `ray exec` and `ray rsync-*` do not work: they use the provider and SSH to find the head, and our provider does not make the head. `ray job submit` (through the tunnel), `ray status` on the head, and the Python API work as usual.
