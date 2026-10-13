# Worker ↔ head protocol (contract for steps 7, 8 and 9)

This file is the contract between the worker side (`worker/bootloader.sh`, `worker/bootstrap.py`, step 7) and the head server (`head_server.py`, step 8). Both sides follow it exactly. A change here needs both sides to change.

## Container environment

The provider sets these in the vast create request (`env`). All are public, except `RAYVAST_JOIN_TOKEN` (see "Join token").

| Variable | Value |
|---|---|
| `RAYVAST_HEAD_IP` | head public IP |
| `RAYVAST_PORT_A` | head port A (plain TLS), default 7443 |
| `RAYVAST_PORT_B` | head port B (mTLS), default 7444 |
| `RAYVAST_BOOTSTRAP_SHA256` | SHA-256 (hex) of the bootstrap bytes served at `/bootstrap` |
| `RAYVAST_CA_SHA256` | SHA-256 (hex, lowercase) of the CA cert DER bytes (S5) |
| `RAYVAST_CLUSTER` | cluster name |
| `RAYVAST_JOIN_TOKEN` | one-time join token for this instance: 32 random bytes, lowercase hex (64 characters) |

vast sets: `CONTAINER_ID` (instance ID), `CONTAINER_API_KEY` (secret, per instance), `PUBLIC_IPADDR`, `VAST_TCP_PORT_70000` … `VAST_TCP_PORT_70063` (host port P for each identity port; P→P).

For tests only, the bootstrap reads these overrides when they are set: `RAYVAST_VAST_API` (vast API base URL, default `https://console.vast.ai/api/v0`), `RAYVAST_TIME_SCALE` (float; all bootstrap timers are divided by it), `RAYVAST_DIR` (directory for the node key, cert, CA, token and shim; default `/tmp/rayvast`; lets parallel test runs use separate directories), `RAYVAST_SKIP_HOST_CHECKS` (`1` = report the checks but do not fail on them; used in local Docker tests where egress IP ≠ `PUBLIC_IPADDR`).

## Join token

The join token proves to the head that an `/enroll` request comes from the instance that the provider rented. It follows the bootstrap-token pattern of `kubeadm join --token` and the kubelet bootstrap token.

- The provider makes a new token for each candidate at rent time (`secrets.token_hex(32)`), keeps it in memory with the candidate, and puts it in the create request env.
- The token is valid for one instance ID, only from that instance's public IP, and only for one successful enrollment. It becomes invalid when the instance enrolls, or when the provider ends the candidate.
- Who can read it: vast shows the env in the instance metadata, so our account's API key, any `CONTAINER_API_KEY` of our account (that is, our own workers, which already hold the cluster token) and the candidate's host (root in that container) can read it. Co-tenants and the internet cannot. This is a documented risk.
- Nobody prints or logs the token. The bootstrap keeps it in memory, removes `RAYVAST_JOIN_TOKEN` from its own environment and from the Ray child's environment, and sends it only inside `/enroll` (TLS with the pinned CA).
- The head compares tokens in constant time (`hmac.compare_digest`).

`CONTAINER_API_KEY` never leaves the worker. The bootstrap uses it only for its own `DELETE`.

## Port map (worker)

| Identity port | Use | Ray flag |
|---|---|---|
| 70000 | node manager | `--node-manager-port` |
| 70001 | object manager | `--object-manager-port` |
| 70002 | metrics export | `--metrics-export-port` |
| 70003 | agent gRPC | `--dashboard-agent-grpc-port` |
| 70004 | agent HTTP (job port, 127.0.0.1 via shim) | `--dashboard-agent-listen-port` |
| 70005 | runtime-env agent (127.0.0.1 via shim) | `--runtime-env-agent-port` |
| 70006–70063 | 58 worker processes | `--worker-port-list` |

The bootstrap uses the host ports P (`VAST_TCP_PORT_700xx`), not the identity numbers.

## Head server endpoints

All bodies are JSON. All answers are JSON. Port A: TLS, no client cert. Port B: TLS, client cert from the cluster CA required.

| Port | Method, path | Request | Answer |
|---|---|---|---|
| A | `GET /ca.pem` | — | the CA cert PEM (`application/x-pem-file`) |
| A | `GET /bootstrap` | — | the bootstrap bytes (`text/x-python`) |
| A | `POST /enroll` | `{"instance_id": int, "join_token": str, "checks": {"egress_ip_ok": bool, "no_net_raw_admin": bool, "own_netns": bool, "shm_empty": bool, "details": str}, "ports": {"70000": P, …, "70063": P}, "cpu_count": int, "gpu_count": int}` | 200 `{"slot_id", "node_type", "cert_pem", "key_pem", "ca_pem", "token", "shim_b64", "gcs_address": "<head_ip>:6379", "num_gpus": int, "heartbeat_interval_s": 30, "heartbeat_timeout_s": 300, "num_workers_soft_limit": int}`; 403 `{"reason": str}` (refused); 503 `{"reason": str}` (the head has not yet seen the instance running — try again) |
| A | `POST /report` | `{"instance_id": int, "kind": "destroy", "reason": str}` (before enrollment; log only) | 200 `{}` (always, if parsable) |
| B | `POST /heartbeat` | `{"instance_id": int}` | 200 `{}`; 403 `{"reason": "unknown"}` when the head does not know this worker |
| B | `POST /report` | `{"instance_id": int, "kind": "token_check" \| "destroy", "ok": bool, "reason": str}` | 200 `{}` |

Rules:
- The head checks the request in this order: the body (400 if malformed); a known candidate that is not enrolled yet; the source IP equals the candidate's public IP; the join token equals the candidate's token; the host checks; the outside check; a free slot. The head makes no vast call during `/enroll`.
- 403 on `/enroll` or `/heartbeat` means "refused": the bootstrap reports (best effort) and destroys its instance at once.
- 503 on `/enroll`, or a connection error or timeout: the bootstrap tries `/enroll` again after 10 s, until the enrollment deadline. (If an earlier answer was lost, the retry gets 403 "already enrolled", and the worker destroys itself. This is rare and safe.)
- The head refuses WITHOUT a destroy when the instance is unknown, already enrolled, from a wrong source IP, or has a missing or wrong join token (a stranger must not be able to kill a candidate). Refusals after a valid token (failed host checks, failed outside check, no free slot) destroy the candidate and blocklist the host when the reason is a host fault.
- `ports` must hold 64 distinct values in 1–65535. `cpu_count` must be 1–4096 and `gpu_count` 0–64; other values give 400.
- Each node cert has the IP SAN of the worker's public IP and 127.0.0.1, and a URI SAN `rayvast:instance/<instance_id>`. On port B the head checks both against the body's `instance_id` and its enrolled public IP. A cert for another instance is refused with 403 `unknown`.
- The head makes the node cert before it marks the instance enrolled, so a failure leaves no half-enrolled slot.
- The outside check: the head opens a TCP connection to every identity port P on the public IP (parallel, 5 s timeout each). The bootstrap listens on all 64 ports until it is enrolled. Every port must accept. The head answers `/enroll` only after this check. Timeout of the whole `/enroll` call on the worker side: 120 s.
- The `no_net_raw_admin` check reads `CapBnd` (the bounding set) in `/proc/self/status`, not `CapEff`: a non-root PID 1 has an empty `CapEff` whatever the container has. The check is false when bit 12 (NET_ADMIN) or bit 13 (NET_RAW) is set.
- The `egress_ip_ok` check asks `https://api.ipify.org`, then `https://ifconfig.me/ip` when the first one does not answer 200.
- `num_cpus` is decided by the worker: the real core count = min(`os.sched_getaffinity` size, cgroup `cpu.max` quota / period if set).

## Bootstrap timers (defaults)

- Enrollment deadline: 10 min after start → destroy.
- Heartbeat: every 30 s. No success for 300 s → destroy. A 403 → destroy at once.
- Before each destroy: report the reason (port B if enrolled, else port A), wait up to 10 s.
- Destroy: `DELETE {RAYVAST_VAST_API}/instances/<CONTAINER_ID>/` with `Authorization: Bearer <CONTAINER_API_KEY>`, retried (every 5 s, forever, until 200/404). Never exit without a destroy.

## Files on the worker

Directory `/tmp/rayvast` (mode 700): `ca.pem`, `node.pem`, `node.key` (600), `token` (600), `librayvast_shim.so`. The bootstrap writes them after enrollment.
