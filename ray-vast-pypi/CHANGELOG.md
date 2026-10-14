# Changelog

## 0.1.0 (unreleased)

First version.

- Ray autoscaler v2 node provider (`VastNodeProvider`) for vast.ai workers. The head runs on AWS EC2 (`head_instance_type`, default `t3.large`).
- Commands: `ray-vast up`, `down`, `attach` and `dashboard`. Options: `--profile`, `--key-file`, `--key-name`, `--wheel`, `--index-url`.
- `up` makes an IAM role, 5 security groups, an instance profile and the head. It builds a derived image with ray-vast. By default, `pip` installs the CLI's own version (`ray-vast==<version>`) from PyPI. `--index-url URL` installs it from another index, for example TestPyPI. `--wheel FILE` installs a wheel file, and a source tree builds its own wheel (both for development). It stops if the cluster already exists. It measures the image size and sets `image_size_gb`.
- `down` ends the head first, then destroys the vast instances, then deletes the AWS resources. It finds everything by tag and label, so it works after a failed `up` and when the head is gone. It reports what is left (exit code 1 if anything). Without a vast key it cleans AWS and warns that vast instances were not swept.
- `dashboard` copies the token to `~/.ray-vast/<cluster>/token`, prints the `RAY_AUTH_MODE` and `RAY_AUTH_TOKEN_PATH` export line, and opens the SSH tunnel, so `ray job submit` works from the laptop.
- State files in `~/.ray-vast/`: `<cluster>.json`, `<cluster>.known_hosts`, `<cluster>/token`.
- Cluster names match `[A-Za-z0-9][A-Za-z0-9_-]{0,39}`.
- Cluster YAML with node types. Each type has declared resources (`CPU` minimum, `GPU` exact), `node_config.disk_gb` (default 50) and an `offer_query` in the vast API JSON form (MB units). ray-vast adds the filters `num_gpus`, `cpu_cores_effective`, `direct_port_count`, `verified`, `rentable` and `datacenter`. A stricter user filter on `cpu_cores_effective` or `direct_port_count` is allowed.
- Provider fields with defaults: `candidate_factor` 2, `cert_max_age_days` 365, `allow_non_datacenter` false, `order_by` `dph_total`, `head_instance_type` `t3.large`, `port_a` 7443, `port_b` 7444. Optional: `image_size_gb`, `key_name`, `key_file`.
- Example YAMLs: `gpu-24gb`, `gpu-80gb`, `cpu-heavy`.
- Images with an `ENTRYPOINT` that ends with `exec "$@"` work, for example `rayproject/ray:*-gpu`. `up` keeps the ENTRYPOINT and reads the image check from one marked line of its output.
- Candidates: the provider rents `candidate_factor` times as many instances as slots. The first candidates that enroll fill the slots. The provider destroys the others. The pull time limit comes from `image_size_gb` and `inet_down` (5 to 15 minutes).
- `max_dollars_per_hour` budget limit. It is required. The vast key is not a config field: it comes from `VAST_API_KEY` or `~/.config/vastai/vast_api_key`.
- Worker-first enrollment: the container command holds no secrets. The worker checks the host, proves its identity with a one-time join token, and then gets its certificate, key and the cluster token.
- Host checks: egress IP, no NET_RAW or NET_ADMIN (read from `CapBnd`), own network namespace, empty `/dev/shm`, and the 64 identity ports (checked from the head).
- TLS and token authentication on all Ray nodes. A new CA and token for each cluster. Node certificates are bound to the instance (IP SAN and URI SAN `rayvast:instance/<id>`); the head checks both on the mTLS port. The worker pins the CA by its SHA-256, rebuilds the CA PEM from the pinned DER, and refuses a PEM with more than one certificate.
- Two head server ports (`port_a`, `port_b`): TLS for download and enrollment, and mTLS for heartbeat and reports.
- Refusals before a valid join token (unknown instance, second enrollment, wrong source IP, wrong token) give 403 and do not destroy the instance. The head makes no vast call during enrollment.
- `LD_PRELOAD` networking shim: workers run behind NAT with no hairpin NAT and no overlay network.
- Watchdog: the bootstrap is PID 1. It sends a heartbeat every 30 seconds. It destroys its own instance after 5 minutes without a heartbeat, after 10 minutes without enrollment, on TERM, INT or HUP, and when Ray ends. It sends the reason to the head first.
- The bootloader (about 1.5 KB) never exits. It retries, and it destroys its own instance after a hash mismatch or after 10 minutes of failures.
- Wall-clock limit on every HTTP call of the worker. Vast calls do not follow redirects.
- Blocklist of bad hosts, in memory for each cluster.
- Security group rules for each worker IP, with reference counts for shared IPs, in up to 5 groups (about 299 worker IPs).
- `down` finds only the 5 group names `ray-vast-<cluster>-0` … `-4`. A cluster that an older development build made has other group names. Clean up such a cluster by hand.
- Start-up sweep: a new provider destroys every instance with the cluster label and removes the cluster's security group rules. The orphan sweep destroys instances that the provider does not know.
- Rent-loop breaker: after 3 failed slots in a row of one node type, the provider stops that type for 10 minutes.
- The head container runs `ray start --head --block` with no restart policy. If Ray's monitor dies, the head stops, and the workers destroy themselves within 5 minutes.
- Warnings: untested Ray version, `CPU` above the 58 worker ports, `allow_non_datacenter`, and certificate expiry in 14 days.
- Documented risks: the head holds the full vast account key; Ray tasks can read `CONTAINER_API_KEY` from `/proc/1/environ`. See `docs/security.md`.
- Tests: a fast default run (`pytest`, about 30 s) and a full run (`pytest -o addopts="" -m "not live and not local_e2e"`).
- Documents: the user docs site (`docs/`, MkDocs Material, published to https://duc-ph.github.io/ray-vast/), README, security model (`docs/security.md`), `CONTRIBUTING.md`.
- Tested with Ray 2.59.
- Not published to PyPI yet.
