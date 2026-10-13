# Worker ↔ head protocol (contract for steps 7, 8 and 9)

This file is the contract between the worker side (`worker/bootloader.sh`, `worker/bootstrap.py`, step 7) and the head server (`head_server.py`, step 8). Both sides follow it exactly. A change here needs both sides to change.

## Container environment

The provider sets these in the vast create request (`env`). All are public (no secrets).

| Variable | Value |
|---|---|
| `RAYVAST_HEAD_IP` | head public IP |
| `RAYVAST_PORT_A` | head port A (plain TLS), default 7443 |
| `RAYVAST_PORT_B` | head port B (mTLS), default 7444 |
| `RAYVAST_BOOTSTRAP_SHA256` | SHA-256 (hex) of the bootstrap bytes served at `/bootstrap` |
| `RAYVAST_CA_SHA256` | SHA-256 (hex, lowercase) of the CA cert DER bytes (S5) |
| `RAYVAST_CLUSTER` | cluster name |

vast sets: `CONTAINER_ID` (instance ID), `CONTAINER_API_KEY` (secret, per instance), `PUBLIC_IPADDR`, `VAST_TCP_PORT_70000` … `VAST_TCP_PORT_70063` (host port P for each identity port; P→P).

For tests only, the bootstrap reads these overrides when they are set: `RAYVAST_VAST_API` (vast API base URL, default `https://console.vast.ai/api/v0`), `RAYVAST_TIME_SCALE` (float; all bootstrap timers are divided by it), `RAYVAST_SKIP_HOST_CHECKS` (`1` = report the checks but do not fail on them; used in local Docker tests where egress IP ≠ `PUBLIC_IPADDR`).

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
| A | `POST /enroll` | `{"instance_id": int, "container_api_key": str, "checks": {"egress_ip_ok": bool, "no_net_raw_admin": bool, "own_netns": bool, "shm_empty": bool, "details": str}, "ports": {"70000": P, …, "70063": P}, "cpu_count": int, "gpu_count": int}` | 200 `{"slot_id", "node_type", "cert_pem", "key_pem", "ca_pem", "token", "shim_b64", "gcs_address": "<head_ip>:6379", "num_gpus": int, "heartbeat_interval_s": 30, "heartbeat_timeout_s": 300, "num_workers_soft_limit": int}`; 403 `{"reason": str}` |
| A | `POST /report` | `{"instance_id": int, "container_api_key": str, "kind": "destroy", "reason": str}` (before enrollment) | 200 `{}` (always, if parsable) |
| B | `POST /heartbeat` | `{"instance_id": int}` | 200 `{}`; 403 `{"reason": "unknown"}` when the head does not know this worker |
| B | `POST /report` | `{"instance_id": int, "kind": "token_check" \| "destroy", "ok": bool, "reason": str}` | 200 `{}` |

Rules:
- The head checks `container_api_key` with `GET /instances/<id>/` on vast (200 = valid for that ID). The head never stores or logs it after the check.
- On port B, the head takes the worker identity from the request body and checks that the client cert's IP SAN equals that worker's enrolled public IP.
- 403 on `/enroll` or `/heartbeat` means "refused": the bootstrap reports (best effort) and destroys its instance at once.
- The head answers `/enroll` only after the outside check (connect to every identity port P on the public IP). Timeout of the whole `/enroll` call on the worker side: 120 s.
- `num_cpus` is decided by the worker: the real core count = min(`os.sched_getaffinity` size, cgroup `cpu.max` quota / period if set).

## Bootstrap timers (defaults)

- Enrollment deadline: 10 min after start → destroy.
- Heartbeat: every 30 s. No success for 300 s → destroy. A 403 → destroy at once.
- Before each destroy: report the reason (port B if enrolled, else port A), wait up to 10 s.
- Destroy: `DELETE {RAYVAST_VAST_API}/instances/<CONTAINER_ID>/` with `Authorization: Bearer <CONTAINER_API_KEY>`, retried (every 5 s, forever, until 200/404). Never exit without a destroy.

## Files on the worker

Directory `/tmp/rayvast` (mode 700): `ca.pem`, `node.pem`, `node.key` (600), `token` (600), `librayvast_shim.so`. The bootstrap writes them after enrollment.
