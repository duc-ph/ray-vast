# Changelog

## 0.1.0 (unreleased)

First version.

- Ray autoscaler v2 node provider (`VastNodeProvider`) for vast.ai workers. The head runs on AWS EC2.
- Commands: `ray-vast up`, `down`, `attach` and `dashboard`.
- `down` finds everything by tag and label, so it works after a failed `up` and when the head is gone. It reports what is left.
- Cluster YAML with node types. Each type has declared resources (`CPU` minimum, `GPU` exact) and an `offer_query` in the vast API JSON form (MB units). ray-vast adds the filters `num_gpus`, `cpu_cores_effective`, `direct_port_count`, `verified`, `rentable` and `datacenter`.
- Example YAMLs: `gpu-24gb`, `gpu-80gb`, `cpu-heavy`.
- Candidates: the provider rents `candidate_factor` times as many instances as slots. The first candidates that enroll fill the slots. The provider destroys the others.
- `max_dollars_per_hour` budget limit. It is required.
- Worker-first enrollment: the container command holds no secrets. The worker checks the host, proves its identity with the vast per-instance key, and then gets its certificate, key and token.
- Host checks: egress IP, no NET_RAW or NET_ADMIN, own network namespace, empty `/dev/shm`, and the 64 identity ports (checked from the head).
- TLS and token authentication on all Ray nodes. A new CA and token for each cluster. Node certificates are bound to the instance (IP and URI SAN). The worker pins the CA by its SHA-256.
- Two head server ports: TLS for download and enrollment, and mTLS for heartbeat and reports.
- `LD_PRELOAD` networking shim: workers run behind NAT with no hairpin NAT and no overlay network.
- Watchdog: the bootstrap is PID 1. It sends a heartbeat every 30 seconds. It destroys its own instance after 5 minutes without a heartbeat, after 10 minutes without enrollment, on TERM, INT or HUP, and when Ray ends. It sends the reason to the head first.
- Blocklist of bad hosts, in memory for each cluster.
- Security group rules for each worker IP, with reference counts for shared IPs.
- Start-up sweep: a new provider destroys every instance with the cluster label and removes the cluster's security group rules. The outside check destroys instances that the provider does not know.
- The head container runs with `--restart unless-stopped`, so Docker restarts the head if Ray's monitor dies.
- Warnings: untested Ray version, `CPU` above the 58 worker ports, `allow_non_datacenter`, and certificate expiry in 14 days.
- Documents: README, security model (`docs/security.md`), worker protocol (`docs/protocol.md`).
- Tested with Ray 2.59.
- Not published to PyPI yet.
