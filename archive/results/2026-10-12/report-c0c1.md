# C0 + C1 report (2026-10-12)

Ran by the `rayvast-c0c1` agent. The lead saved this file from the agent's messages. Raw logs: `remote/`, `progress.md`, `state.json`, `c1_reuseport_check.txt`, `shim_builds/` (v1, v2, v3).

C0 and C1 pass with shim v3. Two shim bugs were found and fixed during the run (v1 → v2 → v3).

## Test items

| Item | Result | Evidence | Numbers |
|---|---|---|---|
| T1 (local) | pass (v3) | The shim is mapped in every Ray process (`/proc/<pid>/maps`). 0 Errno 99. Job `raysubmit_76G8Pmt9eEMRqT9f` succeeded. | 225 rewrite lines. Tasks 0.5 s, actor with 10 calls 0.6 s, pip task (`emoji 2.12.1`) 4.6 s. |
| T5 (head) | pass | `127.0.0.1:6384/api/jobs/`: no token 401, wrong token 403, right token 404 (auth passed; not an agent route). Private IP:6384 is refused. | 20 of 20 requests with no token gave 401. |
| T8 baseline | pass | `rayproject/ray@sha256:77366add7fc5146be6a354bfbf9c700cf90bcf71c650c6ca392eb8d13f1345c3` | Pull 36.4 s. 951,297,021 B compressed (9 layers), 3.54 GB unpacked. |
| T9 | pass (v3) | `ray status` shows 1 node. 0 packets on `ens5` to or from HEAD_IP. Job `raysubmit_knZnUqxGS3fb6Usp` succeeded. | 329 connects to HEAD_IP were rewritten: 327 to 172.31.31.46 and 2 to 127.0.0.1. From the laptop, ports 6379, 8265 and 6384 are filtered. |

## Differences from the plan

- **C0.2 step 1:** without the shim, the `ray start` CLI itself connects to the GCS at the advertised IP. With the unroutable TEST-NET IP, it hung for 327 s and then failed with `Failed to start GCS`. Variant 1b used a temporary iptables DNAT (removed after the test) and reproduced the expected failure: `Errno 99` in the runtime-env agent, then the raylet exited.
- **C0.2 step 2:** 9 `ray-dashboard-*` processes show no `LD_PRELOAD` in `/proc/<pid>/environ`, because setproctitle overwrites that memory. `/proc/<pid>/maps` shows that the shim is loaded. Check `maps`, not `environ`.
- **C1.2:** `ray status` shows no CPU row, not "0 CPU".
- **C1.4:** the image has no curl. The agent used curl on the host (the same network namespace) and Python urllib.
- The AMI came from `describe-images`, because the IAM user had no `ssm:GetParameter` permission.
- `LD_PRELOAD` was added to `head.env`.
- The agent added these checks: `ray job submit` (C0.2, C1.5), tcpdump on `ens5`, `nc` from the laptop, and a scan of the results for the token and private keys (0 hits).

## Shim changes

- **v1 → v2:** the shim sets `SO_REUSEPORT` on each bind to a port in `LOCALHOST_PORTS`. The dashboard agent binds port 6384 twice (to the node IP, then to 127.0.0.1). On v1 the second bind failed with Errno 98, the agent logged "HTTP service will be disabled", and `ray job submit` failed with 500. Both sockets are in the same process (pid 14429, `ray::DashboardA`) and on the same aiohttp runner (`http_server_agent.py` makes two `TCPSite` objects on one runner). So sharing connections between them is safe.
- **v2 → v3:** the shim copies `RAYVAST_SHIM_LOG` once at start. v2 kept the `getenv` pointer, and setproctitle overwrote it, so renamed processes lost their log lines. The rewrites still worked.
- Scope: `AF_INET`; `AF_INET6` with `::ffff:` addresses (gRPC uses dual-stack sockets); `::` for rule 1 only. The shim passes the exact struct size to the real call, keeps errno, and writes each log line with one `O_APPEND` write.

## Head ports not in the plan

- 10001: Ray client server, on the private IP and 127.0.0.1.
- 44217 (autoscaler monitor) and 44227 (dashboard): random ports, on 0.0.0.0.

## Recommendations

- Keep `SO_REUSEPORT` for rule 1.
- The shim must handle `::ffff:` addresses and copy the environment strings that it keeps.
- Add `ray job submit` to the checks, because the v1 bug passed every other check.
- Use Python, not curl, for the checks inside the container.
- The image is 0.95 GB compressed, not 2–3 GB. Use this for the pull limit in C2.
- `ray-vast up` needs `ssm:GetParameter`, or the `describe-images` fallback.
- There were 0 packets to HEAD_IP, so the head's security group needs no rule for its own public IP. The limit is then 299 workers, not 298.

## Resources still up

- EC2 `i-0c343fdb37b555119` (t3.large), 35.85.154.224 / 172.31.31.46, created 22:05:27Z.
- EBS `vol-03061b5a39ddf335d`, 40 GiB gp3.
- Security group `sg-04966d2ef911b874d`, rule `sgr-0de963f434d70300d` (TCP 22 from 172.59.160.24/32).
- Container `rayvast-head`: Ray head with CPU 0, TLS, token and shim v3.
- Cost so far: about $0.04 for EC2, $0 on vast.

## Durations

| Step | Time |
|---|---|
| EC2 launch to status OK | 127 s |
| C0.2.1 hang | 327 s |
| Image pull (2 vCPU busy, ~0.85 GB RAM) | 36.4 s |
| apt install | 24 s |
| Shim build | 27 s |
| C0.2 driver | 8 s |
| C1.5 driver + job submit (user CPU avg 29%, max 50%) | 17 s |
| Idle head | container 3.8% CPU and 640 MiB; host 1.3 of 7.6 GB |
