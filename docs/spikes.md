# Spike results

## S1 — autoscaler v2 with an external provider (2026-10-13): PASS

Ray 2.59.0, autoscaler v2, `rayproject/ray:2.59.0-py311-cpu` on EC2 `ray-vast-dev` (`t3.xlarge`). A test provider started extra raylets on the same box, so all nodes shared one IP. vast cost $0. Wall time 38 min. Code, config and logs: [`../spikes/s1/`](../spikes/s1/).

| # | Check | Result | Key numbers | Evidence |
|---|---|---|---|---|
| 1 | Matching by `RAY_CLOUD_INSTANCE_ID` with a shared IP | Pass | 2 slots, both raylets on one IP; ALLOCATED → RAY_RUNNING 18 s after submit; no assert | `logs/run1/check1.log`, `monitor/` |
| 2 | Scale-down removes the correct node | Pass | drain after 61.7 s idle, `terminate_nodes([slot])` 5 s later; the correct raylet stopped | `logs/run1/raylet-slot-*.log` |
| 3 | Real cores on a running node | Pass | `CPU: 2` type started with 4 CPUs ran 4 × 1-CPU tasks at once; v2 plans a new node with 2 CPUs | `logs/run1/check3*.log`; `v2/scheduler.py:454` (running node: reported resources), `:571` (new node: declared) |
| 4 | Slow start (15 min) | Pass | ALLOCATED → RAY_RUNNING after 905 s, no timeout. Timeout `RAY_AUTOSCALER_RECONCILE_ALLOCATE_STATUS_TIMEOUT_S`, default 3600 s (`v2/instance_manager/config.py:84-113`, used in `reconciler.py:1036-1050`). With 60 s: terminate and a new slot every 65 s, with no limit. | `logs/run2_timeout60/` |
| 5 | Tags | Info | Create writes `ray-node-name`, `ray-node-type`, `ray-node-status`, `ray-launch-config`, `ray-launch-request`, `ray-user-node-type`. 0 `set_node_tags` calls. `node_tags` reads only `ray-node-type`, `ray-user-node-type`, `ray-launch-request` (`node_provider.py:357-373`). | `logs/run1/provider.log` |
| 6 | One provider object | Pass | `__init__` ran 1 time in 28 min; a config edit was used without a new object (`v2/autoscaler.py:118`, config read again each loop `:211`) | `logs/run1/provider.log` |

### Findings that change the plan

- **A. The provider must list the head.** If `non_terminated_nodes` does not return the head's cloud ID (`RAY_CLOUD_INSTANCE_ID=head`), v2 marks the head TERMINATED after 5 s (`reconciler.py:561`). Then v2 does not scale at all, and it shows no error (`:1143`, `:1264`). The provider lists `head` with the tags `{ray-node-type: head, ray-user-node-type: head}`. A node without a correct `ray-node-type` tag is "unmanaged", and v2 skips it without an error (`node_provider.py:358`).
- **B. A cold start can start too many nodes.** v2 plans a new node with the declared minimum CPU, so before the first node reports its real cores, v2 can start extra nodes. In check 3b, v2 started 2 nodes for 4 tasks; the second node stayed idle until the idle timeout removed it.
- **C. Call rate.** Each 5 s loop calls `non_terminated_nodes` once, and `node_tags` and `is_running` for every node. These must answer from memory, without vast calls. `is_terminated`, `internal_ip` and `external_ip` were never called.
- **D.** Each create call had `count=1`, one launch per 5 s loop.
- **E.** `disable_launch_config_check` is True by default in v2 (`config.py:406`). Keep the default: with False, v2 marks every worker as outdated (`scheduler.py:2031`).
- **F.** The head container needs `docker run --init`. Without it, Ray processes stay as zombies after `ray stop`.
- **G.** Timeouts are read from the env when the monitor starts, so they go on the head's `ray start`. `REQUEST_STATUS` (600 s) and `MAX_NUM_RETRY_REQUEST_TO_ALLOCATE` (3) do not affect us, because the provider returns slots at once.

## S2 — Ray facts on 2.59.0 (2026-10-13): done

| Question | Answer | Evidence |
|---|---|---|
| Does Ray restart a dead monitor (SIGKILL or SIGTERM)? | No. GCS and raylet keep running; no autoscaling, no error. | `_private/node.py:1540` (`start_monitor` called once, `:1655`); 180 s watch |
| Worker raylet after GCS loss | Exits 60 s after the head stops ("Failed to connect to GCS within 60 seconds"); `ray start --block` exits with code 1 about 0.3 s later. | `gcs_rpc_server_reconnect_timeout_s`, default 60 (`src/ray/common/ray_config_def.h:549`, tag ray-2.59.0) |
| Worker raylet killed (SIGKILL) | `ray start --block` exits with code 1 after about 0.9 s. | `scripts/scripts.py:1284-1317` |
| F1, F6 | Confirmed: external provider loaded by `load_function_or_class`; v2 calls `create_node_with_resources_and_labels` from the `ray::NodeLauncherPool` thread pool and turns every exception into a `LaunchNodeError`. | `_private/providers.py:179-181`; `v2/instance_manager/node_provider.py:486` |
| F7 | v2 calls `non_terminated_nodes`, `is_running`, `node_tags` (reconciler thread), `terminate_nodes` (single adapter thread), `post_process`, `safe_to_scale`. It never calls `set_node_tags`, `is_terminated`, `internal_ip`, `external_ip`. | `v2/instance_manager/node_provider.py:513-530` |

### Findings that change the plan

- **A dead monitor stays dead.** With D2 the heartbeats then stop and the workers destroy themselves, but the head stays without autoscaling. With `--block`, Ray stops the whole head when a child process dies unexpectedly (`scripts.py:1284-1317`). First decision: Docker restarts the head (`--restart unless-stopped`). Final decision: no self-healing; the head stays stopped until `down` and `up` (design.md 7.3, decisions.md).
- **The worker bootstrap can rely on `ray start --block` exiting** (about 60 s after GCS loss, about 1 s after a raylet death). Then the bootstrap destroys the instance.

## S5 — CA pinning with stdlib Python (2026-10-13): PASS

`python -I`, `urllib.request` only, in `rayproject/ray:2.59.0-py311-cpu` (Python 3.11.16, OpenSSL 3.6) and `python:3.9-slim` (3.9.25, OpenSSL 3.5). Same results on both. Code: [`../spikes/s5/`](../spikes/s5/) (client 23 lines).

| Check | Result (3.11 and 3.9) |
|---|---|
| Happy path: unverified `/ca.pem` → fingerprint → verified `/enroll` → mTLS `/heartbeat` | Pass |
| Wrong fingerprint | Stops before any pinned call |
| Server cert from another CA | `CERTIFICATE_VERIFY_FAILED` |
| Rogue server with its own `ca.pem` | Fingerprint mismatch |
| Head cert without the IP in the SAN | `IP address mismatch` |
| `/heartbeat` without a client cert | `TLSV13_ALERT_CERTIFICATE_REQUIRED` |
| `/heartbeat` with a client cert from another CA | `TLSV1_ALERT_UNKNOWN_CA` |

Limit: all tests used the IP `127.0.0.1` (the same IP-SAN code path), not the box's public IP.

**Fingerprint convention:** SHA-256 of the DER form of the CA cert, lowercase hex. Worker: `hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem)).hexdigest()`. Head: `pki.ca_fingerprint()` (same value as `openssl x509 -outform DER | sha256sum`).

**Stdlib calls:** unverified context (`check_hostname = False`, `CERT_NONE`) only for `/ca.pem`; then `ssl.create_default_context(cafile=ca.pem)` (checks the IP SAN by default); mTLS adds `load_cert_chain(crt, key)`; `urllib.request.urlopen(req, context=ctx, timeout=...)`.

### Findings that change the plan

- **Two head ports.** One `SSLContext` has one `verify_mode`, so the head server uses one port without client certs (`/ca.pem`, `/bootstrap`, `/enroll`, `/report` before enrollment) and one port that requires a client cert (`/heartbeat`, `/report` after enrollment).
- With TLS 1.3, a refused client cert shows on the first read as `ssl.SSLError` (not `URLError`). The bootstrap catches both around each call.
- The head server must do the TLS handshake in the request thread, with a timeout. A handshake in the accept loop lets one slow client stop the server.

## S6 — vast host survey (2026-10-17): done

Goal: find the rule for the container process limit (`pids.max`) and measure the host checks on many distinct machines. Each rental ran a stdlib probe (`python:3.11-slim`, 10 GB disk, the 64 identity ports or the offer's `direct_port_count` if lower). The probe read the limits and the network facts, served one JSON on every identity port, and then destroyed its own instance with `CONTAINER_API_KEY`. The operator (the dev box) read the JSON and checked every identity port from outside (5 s, as the head's port check). When no port answered, the operator read the JSON from the vast log. Stratified by offer `cpu_cores_effective` (7 classes), over GPU types, datacenter and non-datacenter, verified, unverified and deverified, and countries. Offer cap $1.50/h, $3.00/h for 64 or more effective cores. Label `ray-vast-test-2026-10-17`; the final label sweep found 0 instances. Code: [`../tests/manual/survey/`](../tests/manual/survey/) (`probe.py`, `survey.py`, `analyze.py`). Data: [`../archive/results/2026-10-17/survey/`](../archive/results/2026-10-17/survey/) (`rentals.jsonl` one row for each rental, `machines.csv`, `analysis.txt`, `events.log`, `credit.jsonl`, the offer lists).

### Counts

| Item | Number |
|---|---|
| Distinct machines tried (small image) | 431, on 278 hosts |
| Probed | 322 machines, 219 hosts |
| Never started (12 min limit) | 78 |
| Create failed (`no_such_ask`), retried with another machine | 22 rentals, 20 machines |
| Stopped by the two operator restarts, not tried again | 11 |
| Ray image check (`rayproject/ray:2.59.0-py311-cpu`, same offer) | 11 rentals, 9 probed |

Probed machines by `cpu_cores_effective`: <4 or unknown 33, 4–8 52, 8–16 56, 16–32 53, 32–64 48, 64–128 56, 128+ 24.

Cost: the credit fell by $8.56 (13.40 → 14.83 with one auto top-up of $9.99). The estimate from `dph_total` × lifetime is $13.98; it counts the time before `running`, which vast does not bill. Estimate by class ($): <4 1.77, 4–8 0.67, 8–16 0.74, 16–32 1.55, 32–64 2.36, 64–128 4.83, 128+ 2.07. API calls had 4 HTTP 429 answers; the backoff handled them.

### The process limit rule

- `pids.max` = 256 × floor(CPU quota) on 319 of 322 machines. The CPU quota is cgroup v2 `cpu.max`, or cgroup v1 `cpu.cfs_quota_us` / `cpu.cfs_period_us`.
- The 3 other machines have a quota below 1 core (0.24, 0.64, 0.96). floor(quota) is 0, and `pids.max` is `max` (no limit).
- Every container has a CPU quota (322 of 322). Every container has a memory limit.
- The limit does not depend on the image. On 9 machines, the Ray image gave the same cgroup version, `pids.max`, quota, memory limit, affinity, CapBnd and seccomp mode as the small image. The Ray image runs as uid 1000.
- The quota is 0.96 × the offer's `cpu_cores_effective` on 303 of 322 machines. Exceptions (19):
    - The offer's `cpu_cores_effective` is unknown (0 or null) on 5 machines. The instance listing then gives the value.
    - On 2 machines with 384 offer cores (RTX PRO 6000), the container has a cpuset of 48 CPUs and a quota of 46.08 (0.96 × 48).
    - m97081 (offer 64 cores) has a quota of 7.68 (0.96 × 8).
    - 11 machines with a GPU fraction below 1 (for example 0.125 or 0.5 of a 256- or 512-core host) have a quota of 0.72 to 1.44 × `cpu_cores_effective`.
- So `pids.max` = 256 × floor(0.96 × `cpu_cores_effective`) on 302 of 317 machines with a known value. The worker port count from that prediction is correct on 311 of 317 (98 %).
- The machine with `pids.max` 1280 is the machine with 6 effective cores (5.76 quota). The rule explains it. Only 22 of 322 probed machines (7 %) have 1280.

Worker ports (the bootstrap formula `max(4, min(58, (pids.max − 400) // 60))`) and B = ports − 4, for integer `cpu_cores_effective`:

| `cpu_cores_effective` | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 10 | 12 | 14 | 16 | 17+ |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `pids.max` | 256 | 512 | 768 | 1024 | 1280 | 1536 | 1792 | 2304 | 2816 | 3328 | 3840 | ≥ 4096 |
| Worker ports | 4 | 4 | 6 | 10 | 14 | 18 | 23 | 31 | 40 | 48 | 57 | 58 |
| B | 0 | 0 | 2 | 6 | 10 | 14 | 19 | 27 | 36 | 44 | 53 | 54 |
| Ray CPUs (floor of quota) | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 9 | 11 | 13 | 15 | floor(0.96 × n) |

Measured distribution of worker ports: 58 ports on 164 machines (51 %), 40–57 on 51, 14–36 on 59, 4–10 on 48. 23 machines have `pids.max` below 640, so 4 ports have no thread room (4 × 60 + 400 = 640).

### Host attributes

Rates over probed machines. Datacenter is `hosting_type` 1.

| Check | All (322) | Datacenter (47) | Non-datacenter (275) | Verified (162) | Unverified (95) | Deverified (65) |
|---|---|---|---|---|---|---|
| cgroup v1 | 17 % | 43 % | 13 % | 22 % | 8 % | 17 % |
| Hairpin NAT works | 65 % | 45 % | 69 % | 70 % | 58 % | 66 % |
| All identity ports reachable from outside | 91 % | 100 % | 89 % | 99 % | 77 % | 87 % |
| Egress IP = `PUBLIC_IPADDR` | 93 % | 98 % | 93 % | 95 % | 96 % | 86 % |
| Not started in 12 min (of the rentals) | 20 % | 10 % | 21 % | 2 % | 9 % | 50 % |

The same on all 322 machines:

- CapBnd is `a80405fb`: no NET_ADMIN, NET_RAW, SYS_ADMIN or SYS_PTRACE. The probe ran as root, with seccomp mode 2, in its own network namespace (172.17.x.x), and with no user namespace.
- `/dev/shm` is empty. Its size is 6 % to 50 % of the host RAM, not the Docker default of 64 MiB (2 machines).
- `/.dockerenv` exists. The number of `/dev/nvidia<N>` devices is the same as `num_gpus`. The probe found no global IPv6 address.
- `RLIMIT_NPROC` is unlimited. The `nofile` soft limit is 1024 on 92 machines and 1048576 on 230. The eth0 MTU is 1500.
- The identity port env (`VAST_TCP_PORT_*`) holds every requested port (318 of 318).
- The self-destroy with `CONTAINER_API_KEY` worked on 290 of 290 machines. On the 32 machines whose JSON came from the log, the operator destroyed the instance.

Other findings:

- No identity port answered from outside on 27 machines. A part of the ports answered on 3 (29 of 64, 51 of 62, 9 of 64). The head's port check refuses such hosts.
- Hairpin failures: a timeout on 70, a refused connection on 36, other errors on 4.
- The egress IP was not `PUBLIC_IPADDR` on 21 machines. On 4 of them, the two echo services did not agree.
- Kernels: 6.8 (166), 5.15 (101), 7.0 (38), 5.4 (7), others (10).
- Timing: create → `running` median 42 s, p90 62 s (the list period is 20 s). Create → probe line median 30 s, p90 45 s (host clock).
- Never-started causes (78): a GPU CDI device error 32 (`failed to inject CDI devices: unresolvable CDI devices .../gpu=N`, a host Docker/NVIDIA fault), another OCI create error 16, no status message 9, a Docker port setup error 5, a pull that did not end 4, a pull error 2, other 10. These come before the container starts. The ray-vast shim and bootstrap are not involved.
- The vast container log keeps only the first 500 characters of a line. The probe prints its JSON in parts of 400 characters for this reason.

### Findings that contradict the docs or the design

1. **B = 10 is not "most workers".** The library pages say that most workers have 14 ports (B = 10). Only 7 % of the hosts have 1280. Half of the hosts have 58 ports (B = 54). B depends on `cpu_cores_effective`. Fixed 2026-10-17: each library page has the table of B for each host size and the two rules (decisions.md).
2. **Ray gets fewer CPUs than `cpu_cores_effective`.** The bootstrap gives Ray floor(quota) = floor(0.96 × `cpu_cores_effective`) CPUs. That is lower than the offer value on 311 of 322 machines. The provider filter is `cpu_cores_effective >= CPU`. So an offer with exactly `CPU` effective cores gives a worker with `CPU − 1` (`CPU − 2` from 25 cores). F4 says that `CPU` is a minimum. `limits.md` says "Ray still gets all the cores as `CPU`".
3. design.md (section 1, measured 2026-10-06/07): hairpin about 72 % is now 65 % (45 % on datacenter hosts). Unreachable ports about 8 % is now 9 % (1 % on verified hosts). Egress mismatch about 8 % is now 7 %.

### Proposals (not done: product code and user docs are not changed)

- **Provider filter for CPU.** Require floor(0.96 × `cpu_cores_effective`) ≥ `CPU`, that is `cpu_cores_effective >= CPU / 0.96`. Then the worker has at least the declared CPUs on 95 % of hosts. Alternative: the head accepts the measured CPUs and does not count the slot as a match failure.
- **Predict the ports before the rent.** ports = `max(4, min(58, (256 × floor(0.96 × cpu_cores_effective) − 400) // 60))`. The provider can log the predicted ports for each candidate. A node type option (for example `min_worker_ports`) can become an offer filter on `cpu_cores_effective` (17 or more effective cores give 58 ports). The bootstrap must still read `pids.max`: the prediction is wrong on 2 % of hosts.
- **Refuse very small hosts.** Below about 3.2 effective cores, `pids.max` is under 640, and the 4 minimum ports have no thread room. A floor of `cpu_cores_effective >= 4` in the added filters stops these rentals.
- **B advice in the docs.** Replace "Hosts with `pids.max` 1280 give 14 worker ports, so B = 10" with the table above, keyed by `cpu_cores_effective`. Tell the user to choose `cpu_cores_effective >= 17` in `offer_query` for B = 54. Keep "read the start line".
- **The limits page.** Write the rule as "256 × each whole core of the quota; the quota is 0.96 × `cpu_cores_effective`; below 1 core there is no limit". Change "Ray still gets all the cores" to "Ray gets floor(quota) CPUs".
- **Keep `verified = true` as the default.** Deverified hosts did not start in 50 % of the rentals; verified hosts in 2 %. Verified hosts also had all ports reachable in 99 %.
- **design.md section 1:** update the measured rates with this survey.
