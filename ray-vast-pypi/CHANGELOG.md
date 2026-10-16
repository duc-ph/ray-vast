# Changelog

## Unreleased

- The process limit of vast containers (`pids.max`) no longer kills a worker under load. Too much work now waits for a port. It does not crash the worker. Read [The process limit of vast hosts](https://duc-ph.github.io/ray-vast/reference/limits/#the-process-limit-of-vast-hosts).
    - Each Ray process gets small gRPC thread pools (`RAY_num_grpc_internal_threads=4`).
    - The raylet starts at most 8 worker processes at the same time (`RAY_worker_maximum_startup_concurrency=8`).
    - A worker with a process limit gets fewer worker ports. The count comes from `pids.max`.
    - Ray still gets all the cores as `CPU`.
- The worker sets `RAY_num_workers_soft_limit` to at most half its worker ports. Then idle worker processes cannot hold all ports, and a job that needs a new kind of worker process (for example, another runtime env) finds a free port. The `/enroll` answer no longer has `num_workers_soft_limit`.
- The worker logs one start line. It shows the process limit, the cgroup version, the CPUs, the worker ports, the soft limit, the gRPC threads and the startup concurrency.
- The heartbeat carries `pids_current`, `pids_max`, `pids_refused`, `ports_in_use` and `ports_total`.
- The head logs a WARNING in `monitor.log` when a worker is at its process limit. Read [A worker is at its process limit](https://duc-ph.github.io/ray-vast/how-to/troubleshoot/#a-worker-is-at-its-process-limit).
    - When the refused forks of the worker go up: at most once a minute.
    - When all its worker ports are in use, or its pids are above 90 % of the limit: at most once in 5 minutes.
- A Ray Data CPU limit is no longer necessary to keep a worker alive. A hash shuffle (`groupby`) on a worker with few ports can still hang. To prevent this, set the CPU limit and `max_hash_shuffle_aggregators`.

- Breaking: AWS setup and cluster YAML changes.
    - `up` makes an EC2 key pair for each cluster (`ray-vast-<cluster_name>`) and saves the private key in `~/.ray-vast/<cluster_name>/ssh_key.pem` (mode 600). `down` deletes both, also after a failed `up`. The options `--key-name` and `--key-file` and the fields `provider.key_name` and `provider.key_file` are removed.
    - The head settings move to the head node type: `node_config.region` (required) and `node_config.instance_type` (default `t3.large`). They replace `provider.region` and `provider.head_instance_type`.
    - The provider fields `port_a`, `port_b` (now always 7443 and 7444), `cert_max_age_days` (now always 365) and `image_size_gb` (always measured by `up`) are removed.
    - A YAML with a removed or moved `provider` field gets an error that tells what to do.
    - The top-level `max_workers` is optional. The default is the sum of `max_workers` of the worker types.
    - The head role gets the permissions boundary `ray-vast-head-boundary`. Create this IAM policy once in your account (getting started, "Set up AWS"). Without it, `up` stops before it makes anything. The package has the policy files `ray_vast/iam/ray-vast-head-boundary.json` and `ray_vast/iam/ray-vast-user.json` (a least-privilege policy for a restricted IAM user).
    - `up` saves the AWS profile. `down`, `attach` and `dashboard` use it when you give no `--profile` and no `AWS_PROFILE`.
    - The provider field `order_by` is removed. The provider always rents the cheapest offers first (`dph_total`).
    - The `up` option `--index-url` is removed. To test a release on TestPyPI, download the wheel with `pip download --no-deps --index-url https://test.pypi.org/simple/ ray-vast==<version>`, then run `ray-vast up --wheel <file>`.
- The example YAMLs list all fields, each with a short comment.
- The example offer queries match more vast offers: `gpu-24gb.yaml` takes GPUs of 24 GB to 32 GB (also the RTX 5090), `cpu-heavy.yaml` takes any GPU of 8 GB or more, and `gpu-80gb.yaml` has no location filter. On 2026-10-16, the old queries matched 1 offer each.
- The Ray Tune page shows how a trial that starts Ray tasks holds their CPUs (`tune.PlacementGroupFactory`). Without it, the tasks do not start.
- Fixes:
    - `down`, `attach` and `dashboard` accept a 0.1.0 YAML. They take the region from the state file of the cluster, else from `provider.region` of the 0.1.0 YAML. Only `up` rejects the old `provider` fields. Tear down a 0.1.0 cluster with the AWS credentials that made it: a restricted `ray-vast-user` cannot delete a 0.1.0 head role (it has no permissions boundary).
    - An unknown AWS profile or missing AWS credentials give a one-line error, not a traceback.
    - One vast list without a worker no longer ends it. The provider ends an instance 60 s after the last list that showed it.
    - The worker reads the CPU quota on cgroup v1 hosts too (`cpu.cfs_quota_us`).
    - A worker that ended just before its security group rule was made no longer leaves the rule until `down`.
    - A slow answer from the head can hold at most one thread for each URL on the worker. Before, each heartbeat could leave one more thread.
    - A container without `CONTAINER_ID` or `CONTAINER_API_KEY` waits for the provider to end it. Before, PID 1 crashed and vast restarted the container in a loop.
- Protocol: the `/enroll` answer no longer has `ca_pem`, `num_gpus`, `heartbeat_interval_s` and `heartbeat_timeout_s`. The worker uses its pinned CA, its own GPU count and fixed heartbeat times. The destroy report no longer has `ok`. A worker and a head of different versions do not work together.

## 0.1.0 (2026-10-15)

Changes since 0.1.0rc1:

- `dashboard`: Ctrl-C closes the tunnel, prints `tunnel closed` and exits with code 0, with no traceback.
- The commands write each output line at once, also when stdout is a pipe or a file.
- The watchdog retries a heartbeat when a thread cannot start (`can't start new thread`). Only the heartbeat time limit ends the worker.
- The head log keeps the last line (the error) of a long destroy reason.
- A missing YAML file, bad YAML syntax or a bad YAML field value prints one line `ray-vast <command>: <message>` and exits with code 1, with no traceback.

## 0.1.0rc1 (2026-10-14, TestPyPI)

First version. A release candidate on TestPyPI.

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
- Published to TestPyPI only.
