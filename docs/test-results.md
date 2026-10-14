# Test results

This file has the important data from the test cycles that ran (C0–C4 on 2026-10-12/13, C5a/C5d on 2026-10-13): the setup, the result of each test item, the facts that we found about vast and Ray, and the lessons for the next cycles. The open tests are in [test-plan.md](test-plan.md). The shim is described in [shim.md](shim.md).

The raw data is in `archive/results/`:

- `archive/results/2026-10-12/test-plan-c0-c4.md`: the full plan that we ran (setup, steps, expected output). It has the old SSH design.
- `archive/results/2026-10-12/report-c0c1.md`: the C0/C1 report.
- `archive/results/2026-10-12/progress.md`: the step log of C0–C3 and the C2/C3 report.
- `archive/results/2026-10-13/progress.md`: the step log and the report of C4.
- `resources.csv` (one file for each day), `remote/`, `c2/`, `c3/`, `c4/`: the raw logs, offers, rent requests and poll data.

Note: C2–C4 used SSH to push secrets and to run the tests. The design does not use SSH now (worker-first enrollment). The network, TLS, token, port and vast facts are still correct.

## Test setup

### Machines

- **Head:** a new EC2 instance for each test day, not the dev box. `us-west-2`, `t3.large` (2 vCPU, 8 GiB), latest Ubuntu 24.04 amd64 (from `describe-images`, owner 099720109477, because the IAM user has no `ssm:GetParameter`), 40 GiB gp3, IMDSv2 required, key `~/phduc-personal.pem`, no Elastic IP. Security group `rayvast-test-sg`: TCP 22 from the operator IP only, then one `TCP 0-65535 from <worker IP>/32` rule for each worker IP (description `rayvast-test`). About $0.08/hour.
- **Workers:** the cheapest vast offers for this query:
  `datacenter=true verified=true rentable=true direct_port_count>=64 num_gpus=1 dph<=0.4 inet_down>=500 inet_up>=250`
  It matched 9–13 offers on 2026-10-12/13. The user approves the offers (id, GPU, `dph_total`, `inet_down`, `inet_up`, location, `static_ip`) before a rental.

### Software

- Image `rayproject/ray:2.59.0-py311-cpu` on all nodes. Digest `sha256:77366add7fc5146be6a354bfbf9c700cf90bcf71c650c6ca392eb8d13f1345c3`. 951,297,021 B compressed (9 layers), 3.54 GB unpacked. Ray 2.59.0, Python 3.11.16 (anaconda in `/home/ray/anaconda3`). Ubuntu 22.04.5.
- The image has **no** `curl`, `ss`, `/usr/bin/python3`, or sshd. It runs as user `ray` (uid 1000), with `ray ALL=NOPASSWD: ALL` in `/etc/sudoers`.
- Ray 2.61 makes token auth the default. Run the tests again for each new Ray version.

### Ports

Head (EC2, `--network host`): GCS 6379, dashboard 8265 (127.0.0.1), node manager 6380, object manager 6381, metrics 6382, agent gRPC 6383, agent HTTP/job port 6384 (127.0.0.1 with the shim), runtime-env agent 6385, workers 10002–10100. Ports that Ray also opens: 10001 (Ray client, private IP and 127.0.0.1), and random ports for the autoscaler monitor and the dashboard (on 0.0.0.0).

Worker: request the identity ports 70000–70063 (`-p 700xx:700xx`). vast gives a host port P and maps P→P. The container gets P in `$VAST_TCP_PORT_700xx`.

| Use | Request | Ray flag |
|---|---|---|
| Node manager | 70000 | `--node-manager-port` |
| Object manager | 70001 | `--object-manager-port` |
| Metrics export | 70002 | `--metrics-export-port` |
| Agent gRPC | 70003 | `--dashboard-agent-grpc-port` |
| Agent HTTP (job port) | 70004 | `--dashboard-agent-listen-port` |
| Runtime-env agent | 70005 | `--runtime-env-agent-port` |
| sshd (tests only) | 70040 | — |
| Worker processes (57) | 70006–70039, 70041–70063 | `--worker-port-list` |

### Secrets and Ray environment

- For each test day: a new CA (`openssl req -x509 -newkey rsa:2048 -days 2 -nodes`) and a token (`openssl rand -hex 32`), mode 600 in `/opt/rayvast-test/secrets/`.
- For each node: a key and a cert from the CA, SAN = `IP:<public IP>, IP:127.0.0.1`.
- On every node: `RAY_USE_TLS=1 RAY_TLS_SERVER_CERT RAY_TLS_SERVER_KEY RAY_TLS_CA_CERT RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH`, plus `LD_PRELOAD=/opt/rayvast/librayvast_shim.so` and the `RAYVAST_*` values.

### Safety rules

- Label every vast instance `rayvast-test-<date>`. Record the vast credit before each cycle. Stop if the day's spend goes above $8.
- Teardown ends with a label sweep (list, destroy each labelled instance, list again until 0; retry on HTTP 429), then removes the worker rules, terminates the EC2 head, deletes the group, and checks that nothing is left.
- Never copy `secrets/` or a pcap from the loopback interface (it has the token) to the laptop. Scan the results for the token, `PRIVATE KEY`, the vast key and `instance_api_key` values before you keep them.

### Test scripts

All scripts are in `tests/manual/`:

| Cycle | Scripts |
|---|---|
| C0 | `c0_1_shim_unit.sh`, `c0_2_ray_fake_ip.sh`, `ray_driver.py` |
| C1 | `c1_head.sh` |
| C2 | `c2_rent.py`, `c2_hostcheck.sh`, `c2_hostcheck_worker.py`, `c2_push.sh`, `worker_start.sh`, `battery.py`, `battery_worker_check.py`, `c2_negative.sh`, `c2_tls_probe.py`, `c2_hop.sh`, `c2_ports.py` |
| C3 | `c3_key_probe.py`, `watchdog.sh` |
| C4 | `c4_rent.py`, `c4_poll.py`, `c4_kill_task.py`, `watchdog.py`, `heartbeat.sh` |

## Cycles

| Cycle | What | Machines | vast cost |
|---|---|---|---|
| C0 | Shim unit tests and a single Ray node with a fake public IP (`203.0.113.10`) | EC2 only | $0 |
| C1 | Ray head with its real public IP, TLS, token and shim | EC2 only | $0 |
| C2 | Head + 2 workers (W-hp with hairpin NAT, W-nohp without): shim, TLS, token, localhost ports, plain HTTP hop, port exhaustion | EC2 + 6 rentals | $0.28 |
| C3 | Scoped vast key, `CONTAINER_API_KEY`, worker loss, watchdog (TCP check to GCS) | EC2 + W-hp + 2 rentals | $0.07 |
| C4 | Watchdog as PID 1 (with `sudo`), heartbeat watchdog | new EC2 + 3 rentals | $0.11 |

Total: vast $0.46 (credit 12.0547 → 11.6000) for 11 rentals. EC2 about $0.21 (1.9 h + 34 min).

## Results for each test item

| # | Item | Result | Key numbers |
|---|---|---|---|
| T1 | Shim rewrites bind/connect in all Ray processes. Hairpin NAT is not necessary. | Pass (shim v3) | The battery passed on W-nohp (hairpin fails there). On W-nohp, the raylet's connects to its own public IP were rewritten to 172.17.0.3 for 19 ports. 0 Errno 99. |
| T2 | Job port and runtime-env port on 127.0.0.1 only | Pass, both ports | From the head, `W:P(70005)` is refused. The battery passed. Nodes stayed ALIVE. |
| T3 | Cluster with TLS and token | Pass | Actor RTT median/p95: W-hp (TX) 66/68 ms, W-nohp (CZ) 171/171 ms. 50 MB objects: 3–28 MB/s (table below). |
| T4 | Plain HTTP hop: the token does not go over the network | Pass | Token found in 4 packets, all on `lo`. 0 matches on `ens5` (268 MB capture). Workers: the raylet→runtime-env connection always has a private or loopback peer. |
| T5 | Agent port without a token gives 401 | Pass | No token 401, wrong token 403, right token 404 (auth passed, not an agent route). 20/20 requests with no token gave 401. |
| T6 | Secret push over SSH | Pass (design dropped) | 0 secrets in the vast instance metadata. |
| T7 | Worker loss: time until the head marks it DEAD | Pass | 41.4 s after the DELETE (includes vast's destroy time). Reason: `health check failed due to missing too many heartbeats`. The full IP-change flow is still open. |
| T8 | Image pull time | Pass | create→running 54–77 s on all hosts (inet_down 877–7453 Mbps); 22–23 s when the image is cached on the host. EC2 pull 36–47 s. The time is mostly vast setup, not transfer. |
| T9 | Shim on the EC2 head | Pass (shim v3) | 329 connects to HEAD_IP rewritten (327 to the private IP, 2 to 127.0.0.1). 0 packets on `ens5` to HEAD_IP. |
| T10 | Watchdog and a scoped vast key | Pass, with changes | `CONTAINER_API_KEY` can destroy its own instance (HTTP 200) and nothing else. Self-destroy 2:36–2:52 after the head went down (limit 180 s, check every 15 s). |
| T11 | Port exhaustion | Different from the plan | Tasks wait and then run. Actors fail after about 6 min with a misleading error. Drivers fail at once. The raylet stays ALIVE. |
| T12 | Cert reload without a restart | Deferred | Certs are valid for 365 days. |
| T13 | Watchdog cannot be stopped by a task | Partly fails (sudo PID 1 design, now dropped) | Ray user: `kill -9 <watchdog>` EPERM, `sudo -n` refused. But `kill -9 1` gives no error (the kernel drops it). sudo relays TERM/INT. Open again with the bootstrap as root PID 1. |
| T14 | Heartbeat watchdog | Pass (test watchdog) | Normal max age 40 s. Pause of 2:18: max age 134 s, no destroy. Stop: destroy at t0 + 5:11. Repeat with the real bootstrap. |

### C2 battery numbers (C2.2 ; C2.5)

| | W-hp (TX, 143.55.45.86) | W-nohp (CZ, 185.62.108.226) |
|---|---|---|
| Actor RTT median/p95 ms | 65.7/66.1 ; 66.3/68.1 | 170.8/171.3 ; 170.8/171.1 |
| pip `emoji` task (s) | 9.5 ; 9.8 | 10.8 ; 10.3 |
| 50 MB head→W MB/s | 27.8 ; 21.4 | 5.8 ; 6.3 |
| 50 MB W→head MB/s | 3.2 ; 4.4 | 12.4 ; 12.4 |
| W-hp→W-nohp / back MB/s | 13.1 / 13.5 ; 14.3 / 16.5 | |

`SPREAD` with 200 tasks gave 192/8 (W-hp/W-nohp) in both runs. The cause is not known.

### Hosts that we rented

| Instance | Role | machine / host | GPU | Location | inet_down | $/h | running | Hairpin |
|---|---|---|---|---|---|---|---|---|
| 55632730 | W-hp | 3969 / 33081 | P100 | Texas | 924 | 0.103 | 64 s | pass |
| 55632732 | candidate | 148075 / 62819 | 5060 Ti | Germany | 881 | 0.178 | 65 s | pass |
| 55632733 | candidate | 29217 / 1647 | A5000 | Romania | 882 | 0.233 | 64 s | pass |
| 55633384 | candidate | 28701 / 152538 | 5060 Ti | Texas | 7453 | 0.229 | 65 s | pass |
| 55633386 | W-nohp | 32816 / 3497 | RTX 3060 | Czechia | 877 | 0.272 | 77 s | fail |
| 55638058 | W-loss | 142607 / 62819 | 5060 Ti | Germany | 6620 | 0.178 | 54 s | pass |
| 55640303 | key test | 3969 / 33081 | P100 | Texas | 924 | 0.10 | 22 s (cached) | — |
| 55643068 | W-kill | 3969 / 33081 | P100 | Texas | 924 | 0.103 | 23 s (cached) | pass |
| 55643070 | candidate | — / 70142 | 5060 Ti | Maryland | 1377 | 0.161 | 64 s | — (failed the sshd limit) |
| 55643474 | W-hb | 142609 / 62819 | 5060 Ti | Germany | 5434 | 0.178 | 64 s | pass |

All hosts passed the host checks: 64/64 identity ports, 63/63 ports reachable from the head, egress IP = `PUBLIC_IPADDR`, no NET_ADMIN or NET_RAW (root CapEff = 0xa80405fb, ray CapEff = 0), own netns (172.17.0.x), empty `/dev/shm`. Hairpin worked on 5 of 6 distinct datacenter hosts.

## Shim history

The final shim is v3. It passed C0–C4. The versions and the bugs that each one fixed are in [shim.md](shim.md).

## Facts that we found

### Ray

- Without the shim, `ray start --node-ip-address=<IP not on an interface>` fails: the runtime-env agent gets `Errno 99`, and the raylet exits. With an unroutable IP, the CLI itself hangs for about 5 min on the GCS connect first.
- To check that the shim is loaded, read `/proc/<pid>/maps`, not `/proc/<pid>/environ` (setproctitle overwrites it).
- With `--num-cpus=0`, `ray status` shows no CPU row.
- The head sends 0 packets to its own public IP, so the head's security group needs no rule for its own IP.
- A lost worker is DEAD after about 41 s.
- Port exhaustion (`--worker-port-list` with 4 ports, `RAY_num_workers_soft_limit=4`):
  - Tasks queue until a port is free. About 13 worker processes die with `No available ports` (log noise only).
  - Actors over the limit fail after 360 s with `ActorUnschedulableError ... Failed to startup worker after retrying 5 times ... Python environment setup`. The real cause (`No available ports`) is only in the worker log.
  - A driver fails in 2 s with `No available ports`. Drivers use the same port pool.
  - Idle task workers keep their ports for about 60 s.
- The metrics port answers 200 without auth (about 380 `ray_` series). This is a known gap.
- `openssl s_client` cannot show a client-cert failure on TLS 1.3. Use `c2_tls_probe.py` (it sends the h2 preface after the handshake): no cert or a rogue cert → the server closes; a valid cert → a SETTINGS frame.

### vast API

- Create: `PUT /asks/<offer>/`. `env` can be a dict (`{"-p 70000:70000": "1", ..., "NAME": "value"}`). `runtype: args` with `args: ["bash", "-c", script]` replaces the image command.
- **The create response contains `instance_api_key`.** Treat it as a secret and never log it.
- vast puts that key in the container as `CONTAINER_API_KEY`. Other env values: `JUPYTER_TOKEN`, `OPEN_BUTTON_TOKEN`, `PUBLIC_KEY`/`SSH_PUBLIC_KEY`.
- The instance metadata shows `extra_env`, `image_args` and `jupyter_token` in clear text. `request_logs` gives an unauthenticated S3 URL with the container stdout. So no secret may go into args or stdout, and a secret in the env is visible to the account and the host (the rule is in design.md 5.1).
- `CONTAINER_API_KEY`: own GET and own DELETE give 200 (the instance is gone in less than 2 s). Other instances: 401. `users/current`, `auth/apikeys`, `/ssh/`: 401. But `GET /instances/` lists **all** instances of the account. Offer search is public (no auth needed).
- Scoped keys: the format in the vast docs is refused. This format works with `POST /api/v0/auth/apikeys/`:
  `{"api":{"instance_read":{"api.instance":{"GET":{"constraints":{"id":{"eq":ID}}}}},"instance_write":{"api.instance":{"DELETE":{"constraints":{"id":{"eq":ID}}}}}}}`
  Delete with `DELETE /auth/apikeys/<id>/`. vast adds a fixed `*` group to every key: `users/current` GET (email, credit), `instances` GET (all), `auth/apikeys` GET (no values), `ssh/keys` all methods (can add an SSH key to the account), `team` POST. A scoped key is thus wider than `CONTAINER_API_KEY`.
- The `ports` field of the instance is keyed by host port P (identity mapping). The ports are not contiguous. The sshd port P(70040) is not shown; we found it with a banner scan.
- One public IP can belong to several `machine_id`s: 148075, 142607 and 142609 (host 62819) all used 85.10.218.46.
- vast replaces `/root/.ssh/authorized_keys` after the start script (owner uid 129, mode 644, the account's SSH keys).
- **When PID 1 exits, vast starts the container again** (`intended_status` stays `running`, 7 restarts in 2 min), and `dph_total` does not change. A stopped container is not a stopped bill. Destroy, never only exit.
- **After a destroy, vast keeps no container log.** `request_logs` returns a 58 B file `No such container`. Send the destroy reason to the head before the destroy.

### PID 1 and signals (C4, sudo as PID 1)

- `exec sudo ...` as PID 1: sudo is setuid, so the real uid of PID 1 is ray. The ray user can thus signal PID 1. sudo catches HUP, INT, QUIT, USR1, USR2, PIPE, ALRM, TERM, CHLD, CONT and TSTP, and relays them to its child.
- The kernel drops `SIGKILL` to the namespace init from inside the container: `kill -9 1` gives rc 0, but PID 1 stays.
- sudo as PID 1 does not collect orphans: 47 zombie sshd processes after 13 min.
- With a root PID 1, the ray user cannot read `/proc/1/environ`, so `CONTAINER_API_KEY` is hidden from Ray tasks. With a ray PID 1 (C2/C3), any Ray task can read it.
- The C4 design (sudo as PID 1) is dropped. The new design: the bootstrap is PID 1 as root, Ray runs as non-root, and the bootstrap collects child processes.

## Lessons for the next cycles

- Run `ray job submit` in every check. The v1 shim bug passed every other check.
- Use Python, not curl, inside the container. Use absolute anaconda paths over SSH (`/home/ray/anaconda3/bin/{python,ray}`).
- Start the heartbeat (or extend the grace time) before the setup. A slow host can take about 10 min for the setup.
- apt can take about 80 s on some hosts. Bake the tools into the image, or allow 3 min.
- The heartbeat period is 30 s plus the SSH time, so the max age is about 40 s.
- A watchdog fires between `limit − 15 s` and `limit + 15 s` after the head goes down, because the count starts at the last good check.
- Hairpin works on most datacenter hosts (5/6). Finding a host without hairpin can need more rentals.
- The Claude Code permission classifier blocked the security group changes from the test agent. The lead ran them.
- Get the container logs before the destroy.
- Not tested: `kill -TERM 1` and `kill -INT 1` from the ray user (no approval for a third rental). Test it in T13.

## Live cycle C5a and C5d (2026-10-13)

Setup: `ray-vast up` from a clean copy of commit 207c74b (EC2 `t3.large` head, us-west-2), image `rayproject/ray:2.59.0-py311-cpu`, one worker type `{CPU: 4, GPU: 1}`, `min_workers: 1`, `max_dollars_per_hour: 1.0`. Logs: `archive/results/2026-10-13/c5a/`.

| Item | Result |
|---|---|
| `up` on real AWS | Pass |
| Provider rents candidates on real vast | Pass (2 per slot with `candidate_factor: 2`) |
| Enrollment | **Fail.** The head refused every candidate: "container key is not valid". C5d (with more logging) showed that vast answers the head's `GET /instances/<id>/` with HTTP 401 when the head sends the worker's `CONTAINER_API_KEY`. On 2026-10-12 (C3) the same call worked from inside the container. |
| Rent loop | Each refused candidate was destroyed, and the provider rented new ones: 10 rentals in 5 min. Only `max_dollars_per_hour` limits this. |
| vast instance fields | `id`, `label`, `actual_status` (`loading` → `running`), `cur_state`, `intended_status`, `public_ipaddr`, `machine_id`, `ports` (`"P/tcp": [{"HostIp": "0.0.0.0", "HostPort": "P"}, …]`, 64 entries), `dph_total`, `gpu_name`, `extra_env`, `onstart`, `image_args`. No `instance_api_key` in the list. |
| Monitor log | `/tmp/ray/session_latest/logs/monitor.log` (the provider and the head server log there too). |
| `down` | Pass: "left: nothing"; independent checks found 0 vast instances and no EC2, SG or IAM objects. An `up` that was stopped in the middle was also cleaned up by `down`. |
| Cost | C5a $0.017, C5d $0.017 (vast). |

Changes from these findings:
- Enrollment no longer uses `CONTAINER_API_KEY` on the head. The provider gives each candidate a one-time join token in the env, and `/enroll` carries it (design.md 5.2; decisions.md).
- The rent-loop breaker: after 3 failed slots in a row of one node type, the provider stops that type for 10 min (design.md 4).

## Step 10b (2026-10-14)

The bootloader container test failed once. Cause: the session leak check removed every container with the name prefix `st7b-`. It also removed the containers of a parallel test run. Fix: each run uses its own name prefix `<step>-<pid>-`. The leak check removes only the containers of its own run.

## Shim tests and mutation run (step 10c, 2026-10-14)

The test `ray-vast-pypi/tests/integration/test_shim.py` (14 tests, `slow`, Linux only) loads the shim with `LD_PRELOAD` into small Python processes on the dev box. `RAYVAST_PUBLIC_IP` is `203.0.113.10` (TEST-NET-3, not owned). `RAYVAST_PRIVATE_IP` is `127.0.0.2` (on the loopback interface without setup). The tests use the committed `.so`, or the `.so` in `RAYVAST_TEST_SHIM_SO`. They replace the hand test C0.1 (`tests/manual/c0_1_shim_unit.sh`), except the setproctitle check in the Ray image: the test clears the `RAYVAST_SHIM_LOG` value in the environment memory with `ctypes` instead.

The script `ray-vast-pypi/scripts/shim_mutants.py` makes 25 mutants of `rayvast_shim.c`. It builds the unchanged source and each mutant in `quay.io/pypa/manylinux2014_x86_64`, and runs the test against each `.so`. The unchanged source builds to the committed `.so` (sha256 `79055dea…17fd`). The build is reproducible only when the source file has the name `rayvast_shim.c`, because the file name goes into the `.so`. The run takes about 2.5 min.

Result: 23 killed, 2 survived.

| Mutant | Result | Tests that fail |
|---|---|---|
| `rule1-removed` | killed | bind any on a localhost port; second bind; `::` bind |
| `rule1-target-private` | killed | rule 1 and rule 3 tests |
| `rule3-removed` | killed | connect on a localhost port (IPv4 and `::ffff:`) |
| `rules2-4-removed` | killed | rule 2 and rule 4 tests, errno, log |
| `rule2-removed` | killed | bind to the public IP, `::ffff:` bind, log |
| `rule4-removed` | killed | connect to the public IP, `::ffff:` connect, errno, log |
| `port-compare-flipped` | killed | pass-through, rule 2 and rule 4 tests |
| `public-compare-flipped` | killed | pass-through, rules 2–4 |
| `same-target-compare-flipped` | killed | 11 tests |
| `port-range-flipped` | killed | rule 1 and rule 3 tests |
| `port-list-first-only` | killed | rule 1 and rule 3 tests (the test port is the second in the list) |
| `inet-length-check-flipped` | killed | all IPv4 rewrite tests |
| `inet6-family-check-wrong` | killed | `::` bind, `::ffff:` tests |
| `inet6-v4mapped-check-removed` | killed | plain IPv6 (`::1`) passes through |
| `inet6-any-not-detected` | killed | `::` bind |
| `inet6-any-no-ffff-prefix` | killed | `::` bind |
| `inet6-length-wrong` | killed | `::` bind, `::ffff:` tests |
| `reuseport-removed` | killed | second bind in the same process (the v2 bug) |
| `connect-uses-old-address` | killed | all connect tests |
| `errno-save-removed-in-log` | killed | errno after a good bind when the log open fails |
| `errno-save-removed-in-bind` | survived | see below |
| `errno-save-removed-in-init` | survived | see below |
| `env-copy-removed` | killed | log after the environment memory is cleared (the v3 bug) |
| `log-bind-removed` | killed | log tests |
| `log-connect-removed` | killed | one log line for each rewrite |

Why the 2 survivors are acceptable (both are equivalent mutants; no test can see them):
- `errno-save-removed-in-bind`: the save is around `setsockopt(SO_REUSEPORT)`. `setsockopt` fails only on a bad or non-socket fd. Then the `bind` that follows also fails and sets errno again.
- `errno-save-removed-in-init`: `init()` runs in the library constructor, before `main`. No caller can read errno at that time.

Run it again: `cd ray-vast-pypi && .venv/bin/python scripts/shim_mutants.py` (Docker is necessary).

## Local end-to-end (step 11, 2026-10-14)

The dev box runs the head and each worker as a local Docker container. A fake vast server rents the containers. The cost is $0. Command: `pytest -o addopts="" -m local_e2e tests/local_e2e`.

| Run | Result | Time |
|---|---|---|
| 1 | 8 passed | 716 s |
| 2 | 8 passed | 715 s |
| 3 | 8 passed | 716 s |

The 8 scenarios:
1. Scale up 3 workers with k=2: 6 rentals, 3 workers enroll, the provider destroys the 3 unused candidates. The battery runs. Idle scale-down goes to 0.
2. Two workers with the same public IP both join. Scale-down removes the idle one (F3).
3. A host with more cores than declared: Ray uses the real count (F4).
4. A candidate that never enrolls: the provider destroys it after the 1-min enrollment limit and rents again. The second candidate joins.
5. The head goes silent: each worker tries to report, then destroys itself.
6. A container restart: the head refuses the second enrollment, and the worker destroys itself.
7. The monitor stops: the head stops (no restart), and every worker destroys itself.
8. Three failed enrollments in a row: the breaker stops the node type for the pause (scaled to 60 s), then the node type launches again.

Proof: each new or changed scenario failed against a temporary mutant (for example, the Docker restart policy put back, the enrollment limit set to 6000 s, the breaker turned off, a fake vast that keeps the container on DELETE). Each scenario checks through the fake vast, before the teardown, that no instance is left that the provider must destroy.

Root causes of the flaky results before step 10b:
- **A container left after the k=2 scenario.** The old session leak check matched every `st7b-` container, so it saw the containers of a parallel run. Also, the harness stopped the fake vast before the head, and the fake did not wait for its request threads, so a rental in progress could leave a container. Fix: per-run container prefixes, the head is removed first, the fake waits for its threads. The provider does not leak.
- **`tasks_on_nodes == 1`, expected 2.** The test was wrong. Ray's SPREAD strategy is only a best effort: Ray reuses a worker lease for the next queued tasks, so 40 short tasks can all run on one node. Fix: one task in each bundle of a STRICT_SPREAD placement group.

Facts:
- Ray 2.59 shows a launch refusal in `ray status` as `<node type>: LaunchFailed` under "Recent failures". It does not show the exception text. The breaker reason is only in `monitor.log`.
- The 4-vCPU dev box runs a head and 3 Ray workers without trouble.
