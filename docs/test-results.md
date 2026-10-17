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

Re-run after the one-ending-path refactor (9078c6e): 3 runs, 8 passed each, 723 s, 720 s, 719 s.

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

## Live cycle C5 (2026-10-14)

Setup: `ray-vast up` from commit e270070 on the dev box (the operator machine), EC2 `t3.large` head, us-west-2, image `rayproject/ray:2.59.0-py311-cpu`, one worker type `w` `{CPU: 4, GPU: 1}`, `min_workers: 1`, `candidate_factor` 2, `max_dollars_per_hour: 1.0`. Data: `archive/results/2026-10-14/` (`progress.md`, `c5/`, `spend.md`, `resources.csv`).

| # | Result | Key numbers | Notes and links |
|---|---|---|---|
| T15 | Pass | `up` 2 min 28 s (exit 0). Create → `running` ≤ 24 s → enrolled 26.7 s after the create → Ray node running 7.9 s later. `bootloader.sh` 1456 B, accepted in `args`. | The P100 host (machine 3969) had the image in its cache from C2–C4. A host without the image takes longer. `c5/head/monitor.log`, `c5/vast_timeline.jsonl` |
| Join | Pass | 2 rentals, 1 enrollment, 0 refusals. The provider ended the extra candidate 8.6 s after the enrollment (limit 60 s). | The extra candidate was still `loading`. It never called `/enroll`. Log line: `ending instance 55866629 … its request has no free slot left`. |
| Fields | Pass | `actual_status` values: `null`, `loading`, `running`. | All 10 names exist. `public_ipaddr` equals the worker's public IP (143.55.45.86). `ports` has 64 entries: `"P/tcp": [{"HostIp": "0.0.0.0", "HostPort": "P"}, {"HostIp": "::", "HostPort": "P"}]`. No `unknown vast actual_status` line. `c5/instance_fields.json` |
| CapBnd | Pass | `00000000a80405fb` (bit 12 and bit 13 clear) | Host checks all `True` (`c5/worker.log`). |
| Task (C5.3) | Pass | Job `SUCCEEDED` in 14 s | uid 1000; Ray 2.59.0; `CONTAINER_API_KEY` and `RAYVAST_JOIN_TOKEN` not in `os.environ`; PID 1 is the bootstrap; `/tmp/rayvast` 700, all files 600. Ray uses the real core count: 24 CPU (4 declared). `c5/task_probe.out` |
| GPU count | **Fail (product bug)** | Ray shows GPU 4.0 on a 1-GPU rental | See below. `c5/gpu_probe.out` |
| Teardown | Pass | `down` 40 s, `left: nothing`, exit 0; sweep `0 left`; `aws left: nothing` | |
| Cost | | vast $0.0074 (credit 11.5658 → 11.5584); EC2 about $0.01 | `spend.md` |

**Product bug: the worker counts the GPUs of the host.** `bootstrap.py` counts the entries in `/proc/driver/nvidia/gpus/`. This directory is not in the container namespace. It shows all the GPUs of the host. On instance 55866627 (1 rented GPU), the directory had 4 entries. The container had only `/dev/nvidia3`, and `nvidia-smi -L` showed 1 GPU. The head sent `num_gpus` 4, and Ray started with `--num-gpus=4`. A test task with `num_gpus=4` ran on this worker with `CUDA_VISIBLE_DEVICES=0,1,2,3`. Effect: Ray can put up to 4 GPU tasks on a worker that has 1 GPU. A fix must count only the GPUs that the container can use (for example, the `/dev/nvidia<N>` device files, or `nvidia-smi -L`). The provider can also limit the count to the `num_gpus` of the offer.

Other findings:
- `vast_tool.py logs` failed with HTTP 400: vast limits `tail` to 20000. The script now sends `"20000"`.
- The extra candidate ended before `vast_tool.py env-secret` ran, so its join token was not in the secret scan. vast keeps no data after a destroy, and no kept file had its token in the head log format. For C6 and C8, read the join tokens while the poll shows `loading`.
- The scan finds the word `Bearer`, a space and the byte `z` in `tests/manual/__pycache__/vast_tool.cpython-312.pyc` (a constant in the bytecode, not a secret). `resources.sh` makes this file. Delete `__pycache__` before the scan.
- `up` took 2.5 min, not 8–12 min as in the plan.
- The head container has a 64 MB `/dev/shm`, so the Ray object store on the head uses `/tmp/ray` (a Ray warning in `c5/head/docker_logs.txt`).
- `monitor.log` has two Ray warnings every 5 s: `Dropping the empty legacy field head_node` and `worker_nodes`. They are not errors.
- The head container logs in local time (UTC−7), not UTC.
- The provider got one HTTP 429 from vast and retried. The operator's poller used the same key every 5 s.

## Live cycle C6 (2026-10-14)

Setup: `ray-vast up` from commit f830338 on the dev box (the operator machine), EC2 `t3.large` head, us-west-2, image `rayproject/ray:2.59.0-py311-cpu`, one worker type `w` `{CPU: 4, GPU: 1}`, `min_workers: 3`, `candidate_factor` 1, `max_dollars_per_hour: 1.5`. Data: `archive/results/2026-10-14/` (`progress.md`, `c6/`, `spend.md`, `resources.csv`).

Workers: A = 55870649 (RTX A5000, 5.2.174.20), B = 55870661 (RTX A5000, 5.2.174.20), C = 55870811 (P100, 143.55.45.86). The first plan put B on 55870811. After C6.1, a replacement got the same IP as 55870811. A rule by source IP would then block two workers. So B and C changed places, and C6.3 ran after C6.2.

| # | Result | Key numbers | Notes and links |
|---|---|---|---|
| T13 | Pass | Five signals: no effect after 120 s. `SIGTERM`: report 0.5 s after the signal, gone from vast ≤ 3.3 s after the signal (limit 20 s). Ray node DEAD 31.6 s after the signal. | `SIGKILL`, `SIGQUIT`, `SIGUSR1`, `SIGUSR2` and `SIGCONT` were sent. PID 1 stayed the same bootstrap process. 50 orphans went to PID 1; zombies of PID 1: 0. Head log: `worker 55870649 asks to be destroyed: 'received SIGTERM'`. vast never showed `exited`. v2 rented a replacement. `c6/signals.out`, `c6/sigterm.out`, `c6/logloop_A/08.log` |
| T14 | Pass | Provider destroy at `t_last` + 308.9 s (limit 270–330 s). Watchdog destroy decision at about `t_last` + 311 s. | `t_last` comes from a SYN capture on the head host, because the head does not log heartbeats. The provider acted first (`no heartbeat for 5 min`). The worker log shows the watchdog decision `no successful heartbeat for 300 s` about 2 s later. B's Ray node was ALIVE in every check until after the destroy. `c6/b_syn_capture.txt`, `c6/b_watch.txt`, `c6/logloop_B/16.log` |
| Restart | Pass | Reboot → refusal 8 s; refusal → gone from vast ≤ 0.7 s | Head log: `enroll refused for instance 55870811 … already enrolled`, no destroy by the head for it. The worker's report reached port A (`unverified report … 'head refused enrollment: already enrolled'`). vast showed `rebooting` for one poll. The provider did not end C first. `c6/t_r.txt`, `c6/head/monitor.log` |
| GPU count | Pass | GPU 1.0 on each of the 6 Ray workers | Host m3969 counted 4 GPUs in C5. Now it shows 1. No GPU refusal. `c6/node_resources_start.txt`, `c6/ray_nodes_end.json` |
| Head shm | Pass | `/dev/shm` 2.6 GB (ShmSize 2704651264 B = 0.33 × 8003824 kB) | C5 had 64 MB. `c6/head_shm.txt` |
| Teardown | Pass | `down` 41 s, `left: nothing`, exit 0; sweep `0 left`; `aws left: nothing` | Secret scan with the cluster token and all 7 join tokens: no output. |
| Cost | | vast $0.1670 (credit 11.5584 → 11.3915); EC2 about $0.03 | 7 rentals, peak $0.583/h. `spend.md` |

Other findings:
- vast stopped one new candidate (55870639) by itself. `intended_status` became `stopped` before `running`. The worker enrolled, and then vast showed `exited`. The provider ended it (`instance status 'exited'`) and v2 rented a replacement. This is correct behavior. No worker report came, because the container was stopped from outside.
- vast has the status `rebooting`. It is in neither provider status set. The provider would log it once as unknown and treat it as pending. That is correct for a reboot.
- vast returned `no_such_ask` (HTTP 400) for two offers between the search and the rent. The provider took the next offer.
- Two workers on different machines (m29217, m29278) had the same public IP. Both joined (F3).
- The plan expects `t_last` in the head log. The head does not log heartbeats. A header-only SYN capture of port 7444 on the head host gives `t_last`.
- The log of a container after a reboot is hard to get: C destroyed itself 9 s after the reboot.
- `vast_tool.py` has a new command `reboot <id>` (`PUT /instances/reboot/<id>/`, from the vast CLI source). It refuses instances without a test label.
- Do not use `pkill -f` with a pattern in the same shell command: it matches the shell itself.

## Live cycle C8 (2026-10-14)

Setup: `ray-vast up` from commit 93b6e9b on the dev box (the operator machine), EC2 `t3.large` head, us-west-2, image `rayproject/ray:2.59.0-py311-gpu`, node types `gpu-a` and `gpu-b` `{CPU: 4, GPU: 1}`, `max_workers` 2 each, `candidate_factor` 2, `max_dollars_per_hour: 3.0`, `idle_timeout_minutes: 2`. Data: `archive/results/2026-10-14/` (`progress.md`, `c8/`, `spend.md`, `resources.csv`).

Node types. The committed ranges gave `gpu-a` only 5 offers. The rule is 6 offers on 4 machines. With `inet_down` and `inet_up` ≥ 500, vast had only 29 one-GPU offers. We changed the ranges in `tests/manual/c8/cluster.yaml`:

| Type | `gpu_ram` (MB) | Offers | Machines | Cheapest 4 ($/h) |
|---|---|---|---|---|
| `gpu-a` | ≥ 8000, < 24570 | 6 | 6 | P100 0.097, A5000 0.233, A5000 0.233, RTX 3060 0.272 |
| `gpu-b` | ≥ 24570, < 70000 | 12 | 12 | RTX 3090 0.295 × 3, RTX 3090 0.298 |

The first 8 candidates cost $2.02/h, below the $3.00 limit. The upper limit of `gpu-b` excludes the 80–180 GB cards ($1.3–9.4/h).

| # | Result | Key numbers | Notes and links |
|---|---|---|---|
| L6 | **Not run (product bug in `up`)** | submit → all workers running: not measured; peak $/h: 0; scale-down time: not measured; cost: vast $0 (the credit fell by $0.0104 from late C6 billing), EC2 about $0.014 | `up` failed after 8 min 11 s, before the head container started. It rented no vast instance. `c8/up.log`, `c8/head/image_info_evidence.txt` |
| Teardown | Pass | `down` 35 s, `left: nothing`, exit 0; sweep `0 left`; `aws left: nothing` | Secret scan: no output. No cluster token or join token existed. |

**Product bug: `up` reads the output of the image entrypoint.** `commands.py:215` runs `docker run --rm <image> python -c IMAGE_INFO`. It expects exactly 2 words on stdout: the Ray version and the shim path. The `-gpu` image has `ENTRYPOINT ["/opt/nvidia/nvidia_entrypoint.sh"]`. Before `exec "$@"`, this script writes the CUDA banner, the license text and `WARNING: The NVIDIA Driver was not detected` to stdout. The result is `ValueError: too many values to unpack (expected 2)`. With `--entrypoint ""`, stdout has only the 2 lines. The `-cpu` image has no entrypoint, so C5 and C6 did not find the bug. Effect: `up` cannot use `rayproject/ray:*-gpu`. A fix can read only the last 2 lines of stdout, or run the check with `--entrypoint ""`. The plan lists "images with an ENTRYPOINT" as "later", but the GPU image of `rayproject/ray` has one. Also check the other `docker run` commands of `up` and the worker start on vast with this entrypoint.

Other findings:
- `up` took 8 min 11 s with the 18.4 GB GPU image. The C5 and C6 runs with the CPU image took 2.5 min. Most of the time is the pull of the image on the head.
- The vast credit fell by $0.0104 during C8 with no C8 rental. vast books the end of a rental some minutes after the destroy.

## Live cycle C5 re-run (2026-10-14)

Setup: as the first C5 run, from commit 4441ab9 (GPU count from `/dev/nvidia<N>`, head refuses a GPU mismatch, head `--shm-size` 0.33 × RAM, one ending function `VastNodeProvider._end`, one `vast_api.sweep`, `RAYVAST_IMAGE_INFO` marker line). Data: `archive/results/2026-10-14/` (`progress.md` "C5 re-run", `c5-rerun/`, `spend.md`, `resources.csv`).

| # | Result | Key numbers | Notes and links |
|---|---|---|---|
| T15 | Pass | `up` 2 min 22 s (exit 0). Second wave: create → `running` ≤ 20 s → enrolled 35.7 s after the create → Ray node running 9.0 s later. | The marker line of `up` works with the `-cpu` image. `c5-rerun/up.log`, `c5-rerun/head/monitor.log` |
| Join | Pass (diff) | 4 rentals in 2 waves. Wave 1: both candidates called `/enroll` at the same time. The head refused the second one (`no free slot`). The provider ended it 0.08 s after the enrollment. Wave 2: the provider ended the extra candidate 4.5 s after the enrollment. | In the first run, the extra candidate never called `/enroll`. The plan allows both paths. |
| Fields | Pass | `actual_status` values: `null`, `loading`, `running`, `exited`. | All 10 names exist. `public_ipaddr` equals the worker's public IP (143.55.45.86). `ports` has 64 entries. `exited` is in the failed set. No `unknown vast actual_status` line. `c5-rerun/instance_fields.json` |
| CapBnd | Pass | `00000000a80405fb` | Host checks all `True` on both enrolled workers. `c5-rerun/worker_*.log` |
| Task (C5.3) | Pass | Job `SUCCEEDED` in 13 s | Same values as the first run. `c5-rerun/task_probe.out` |
| GPU count | Pass | GPU 1.0 in Ray on the P100 host m3969 | The first run showed GPU 4.0 on this host. `/proc/driver/nvidia/gpus` still lists 4 GPUs. `/dev` has only `nvidia3`. `c5-rerun/gpu_probe.out` |
| Head shm | Pass | `/dev/shm` 2.6 GB (ShmSize 2704650240 B = 0.33 × 8003820 kB) | The first run had 64 MB. `c5-rerun/head_shm.txt` |
| Teardown | Pass | `down` 25.6 s, `left: nothing`, exit 0; sweep `0 left`; `aws left: nothing` | `down` used the new sweep line (`sweep: destroying instance …`). Secret scan with the cluster token and all 4 join tokens: no output. |
| Cost | | vast $0.0085 (credit 11.3661 → 11.3576); EC2 about $0.01 | `spend.md` |

Differences from the first run:
- vast stopped both wave-1 candidates by itself. `intended_status` became `stopped` while `actual_status` was `running`, before any enrollment. The enrolled worker (55886615) ran Ray and then showed `exited`. The provider ended it (`instance status 'exited'`), and v2 rented a new wave. The new worker joined 46 s after the end. The code has no stop call. C6 had the same event once (55870639). The first C5 run had none.
- Both wave-1 candidates reached `/enroll` within 0.13 s. The head refused one with `no free slot`. The worker also sent a destroy report.
- A field fetch right after a rental gives `actual_status`, `status_msg` and `ports` as `null` (`c5-rerun/instance_fields_early.json`).
- The 86 `Dropping the empty legacy field` warnings are still in `monitor.log`.

## Live cycle C6 re-run (2026-10-14)

Setup: as the first C6 run, from commit 622c7c1 (one ending function `VastNodeProvider._end` for the provider and the head server, one `vast_api.sweep`, the `RAYVAST_IMAGE_INFO` marker line). Data: `archive/results/2026-10-14/` (`progress.md` "C6 re-run", `c6-rerun/`, `spend.md`, `resources.csv`).

Workers: A = 55888563 (RTX A5000, 5.2.174.20), B = 55888582 (RTX 3090, 185.62.108.226), C = 55888774 (P100, 143.55.45.86). B had the only IP of its host. The order was C6.1, C6.2, C6.3.

| # | Result | Key numbers | Notes and links |
|---|---|---|---|
| T13 | Pass | Five signals: no effect after 120 s. `SIGTERM`: report 0.52 s after the signal, gone from vast ≤ 5.7 s after the signal (limit 20 s). Ray node DEAD 28.5 s after the signal. | The same PID 1 before and after. 50 orphans went to PID 1. Zombies of PID 1: 0. vast never showed `exited`. The provider logged `instance is gone from vast` through `_end`. v2 rented a replacement. `c6-rerun/signals.out`, `c6-rerun/sigterm.out` |
| T14 | Pass | Provider destroy at `t_last` + 307.4 s (limit 270–330 s). | `t_last` comes from a header-only SYN capture on `ens5`. After the cut, B made 10 heartbeat attempts. The provider acted first (`no heartbeat for 5 min`). The watchdog would act about 3 s later. B's Ray node was ALIVE in all 18 checks. `c6-rerun/b_syn_capture.txt`, `c6-rerun/b_watch.txt`, `c6-rerun/logloop_B/` |
| Restart | Pass | Reboot → refusal 12.8 s; refusal → gone from vast ≤ 2.0 s | Head log: `enroll refused … already enrolled`, no ending by the head for it. The worker's report reached port A. The worker log of the second run: `head refused enrollment: already enrolled`, then `vast accepted the delete`. `c6-rerun/t_r.txt`, `c6-rerun/logloop_C/` |
| GPU count | Pass | GPU 1.0 on each of the 6 Ray workers | `c6-rerun/ray_nodes_end.json` |
| Head shm | Pass | `/dev/shm` 2.6 GB | `c6-rerun/head_shm.txt` |
| Teardown | Pass | `down` 26 s, `left: nothing`, exit 0; sweep `0 left`; `aws left: nothing` | `down` used the new sweep line for the 3 instances. Secret scan with the cluster token and all 7 join tokens: no output. |
| Cost | | vast $0.1805 (credit 11.3561 → 11.1756, 0 top-ups); EC2 about $0.035 | 7 rentals, peak $0.672/h. `spend.md` |

Differences from the first run:
- `up` took 6 min 19 s. The first run took 2 min 27 s. `up` still ended with exit code 0.
- The provider logged `unknown vast actual_status 'rebooting' (treated as pending)` once during C6.3. The first run did not sample `rebooting`. This is the expected behavior.
- The log of the rebooted container was fetched this time (fetches 2–13 s after the reboot call). It shows a new fact: the vast reboot sends `SIGTERM` to PID 1. The old bootstrap logged `destroying this instance: received SIGTERM`. Its report and its delete did not complete: vast killed the container about 10 s later. The second run then destroyed the instance. The first run's log has the same `SIGTERM` line. The result is the same: the instance is destroyed.
- The log of A after the `SIGTERM` was not caught (`log download: not ready after 60 s`). The head log proves the report.
- vast stopped one new candidate (55888548) by itself again (`intended_status` `stopped`). The provider ended it through `_end` (`instance status 'exited'`), and v2 rented a replacement.

## Live cycle C8 re-run (2026-10-14)

Setup: `ray-vast up` from commit 7dd15c2 (0.1.0rc1) on the dev box (the operator machine), EC2 `t3.large` head, us-west-2, image `rayproject/ray:2.59.0-py311-gpu`, node types `gpu-a` and `gpu-b` `{CPU: 4, GPU: 1}`, `max_workers` 2 each, `candidate_factor` 2, `max_dollars_per_hour: 3.0`, `idle_timeout_minutes: 2`. `up` installed ray-vast on the head from a wheel of the source tree. Data: `archive/results/2026-10-14/` (`progress.md` "C8 re-run", `c8-rerun/`, `spend.md`, `resources.csv`).

Node types. The YAML did not change. At 08:14Z, `gpu-a` had only 4 offers. One minute later, it had 6 offers again. The market changes quickly.

| Type | `gpu_ram` (MB) | Offers | Machines | Cheapest 4 ($/h) |
|---|---|---|---|---|
| `gpu-a` | ≥ 8000, < 24570 | 6 | 6 | P100 0.097, A5000 0.233, A5000 0.233, RTX 5070 Ti 0.671 |
| `gpu-b` | ≥ 24570, < 70000 | 6 | 6 | RTX 3090 0.324, CMP 170HX 0.536, CMP 170HX 0.603, RTX 5090 1.403 |

| # | Result | Key numbers | Notes and links |
|---|---|---|---|
| Up | Pass | `up` 8 min 14 s, exit 0 | The head container keeps the NVIDIA ENTRYPOINT. Its log starts with the CUDA banner. The marker line works. `c8-rerun/up.log`, `c8-rerun/head/docker_logs.txt` |
| L6 | Pass | Submit → all 4 placed: 3 min 33.6 s. Rentals 6, enrolled 4, ended unused 2. Peak $2.708/h (limit 3.0). Idle scale-down to 0: 2 min 11 s after the job end (rules 0 at ≤ 2 min 14 s). GPU 1.0 on each worker. Cost: vast $0.2074 for C8 and the PID 1 probe (credit 11.1756 → 10.9682, 0 top-ups), EC2 about $0.03. | See below. `c8-rerun/c8_job.out`, `c8-rerun/head/monitor.log`, `c8-rerun/c8_poll.jsonl` |
| GPU image | Pass | 4 of 4 workers enrolled. PID 1 is the bootstrap. | Each worker log starts with the CUDA banner, then `bootstrap: host checks` (all `True`). An extra probe found PID 1 `python3 /tmp/rayvast-bootstrap.py`. `c8-rerun/workers/`, `c8-rerun/pid1/` |
| Teardown | Pass | `down` 50.4 s, `left: nothing`, exit 0; sweep `0 left`; `aws left: nothing` | Secret scan with the cluster token and all 8 join tokens: no output. |

C8.1 timeline (UTC). `T_submit` 08:24:45.8. First rental 08:24:55.1 (+9.3 s). Sixth rental 08:25:08.7. First `running` 08:25:29. First node ALIVE 08:25:50.0. `C8_ALL_PLACED` 08:28:19.4. Fourth node ALIVE 08:28:20.7. Job end 08:28:57.8, `SUCCEEDED`.

| Worker | Type | GPU | Create → enroll | `nvidia-smi -L` | `ray.get_gpu_ids()` |
|---|---|---|---|---|---|
| 55894125 | `gpu-b` | RTX 3090 | 46.4 s | 1 GPU | `[0]` |
| 55894123 | `gpu-a` | Tesla P100 | 83.9 s | 1 GPU | `[0]` |
| 55894139 | `gpu-a` | RTX 4090 | 79.9 s | 1 GPU | `[0]` |
| 55894153 | `gpu-b` | CMP 170HX | 179.8 s | 1 GPU | `[0]` |

Extra candidates: 55894128 ended 4.3 s after its slot was filled. 55894124 ended 8.8 s after its slot was filled. Both were still `loading`. Both ended before the last slot was filled. There were no refusals at `/enroll`.

Differences from the plan and other findings:
- The plan expects up to 4 candidates for each type. The provider rented 3 for each type. Ray asked for 2 slots of each type in 2 create calls. The first call of each type rented 2 candidates. The second call rented 1. Then the next offer would pass `max_dollars_per_hour` (sum $2.708/h after 6 rentals). The plan allows this limit.
- The second create calls got `no_such_ask` (HTTP 400) 5 times. 4 of these offers were the offers that the first calls had just rented, so the search was stale. The provider took the next offer. Also 3 HTTP 429 retries. The two pollers of the operator use the same key.
- Ray uses the real core count: CPU 24, 30, 7 and 6 (4 declared). This is the same as in C5.
- The plan does not check PID 1 in C8. An extra job with `num_gpus=1` (`c8-rerun/pid1/pid1_probe.py`) rented 2 candidates (1 enrolled in 29.4 s: the image was in the host cache). PID 1 was `/home/ray/anaconda3/bin/python3 /tmp/rayvast-bootstrap.py`. Its child was `ray start --block`. No `nvidia_entrypoint.sh` or shell was left. uid 1000. `/dev/nvidia1` only. `CUDA_VISIBLE_DEVICES` 0.
- vast did not stop a new candidate by itself in this cycle.
- `monitor.log` has 0 `ERROR` lines and 0 `unknown vast actual_status` lines. It has 286 `Dropping the empty legacy field` warnings (not errors).

## TestPyPI release test 0.1.0rc1 (2026-10-14)

Release: https://test.pypi.org/project/ray-vast/0.1.0rc1/ (wheel and sdist, built from `9aa4576`). Data: `archive/results/2026-10-14/l4-testpypi/`.

| # | Result | Key numbers | Notes and links |
|---|---|---|---|
| Build | Pass | `twine check --strict`: 2 of 2 passed | The wheel has `shim/librayvast_shim.so` (SHA-256 `79055dea…17fd`, the recorded value), `worker/*` and the 3 example YAMLs. Packaging tests: 7 passed. `build_listing.txt`, `twine_check.txt` |
| Scan | Pass | Wheel: no output | The sdist matches only fake fixture values in 4 test files (`Bearer ACCOUNTKEY`, `PRIVATE KEY` checks). The real vast key is in no file. The IP addresses are loopback, private ranges and TEST-NET only. |
| Upload | Pass | 08:40:57 UTC | The name was free on TestPyPI. `upload.txt` |
| Install | Pass | 37 s, new venv `~/rvtest-venv` | The command of `reference/cli.md`. `ray_vast` imports from site-packages. `install.log` |
| Up | Pass | 550 s, exit 0 | GPU image. First line `installing ray-vast==0.1.0rc1 from https://test.pypi.org/simple/`. The head container has `0.1.0rc1` in site-packages. `up.log`, `head_check.log` |
| Job | Pass | 8.7 s | `ray job submit --working-dir . -- python -c "print(1)"` through the tunnel. The head has CPU 0. `job.log` |
| Down | Pass | 35 s, `left: nothing`, exit 0 | `aws left: nothing`. vast count 0. No vast rental. `down.log`, `teardown_check.log` |

Cost: EC2 only, `t3.large` for about 12 min (less than $0.02). vast $0.

Findings (details in `walkthrough.md`):
- `--extra-index-url` mixes the two indexes. On the laptop, pip took `opentelemetry-exporter-prometheus 1.12.0rc1` from TestPyPI (PyPI has `0.66b1`). The docs say that the dependencies come from PyPI. The head was not affected.
- Ctrl-C on `ray-vast dashboard` prints a `KeyboardInterrupt` traceback.
- `ray-vast --version` does not exist.
- `getting-started.md` uses `python -m venv`. Ubuntu 24.04 has only `python3` outside a venv.
- The `up` progress lines show only at the end when stdout is a pipe or a file.

## Live cycle C9: Ray libraries (2026-10-14)

Setup: `ray-vast up` from commit 483cc52 (0.1.0rc1) on the dev box (the operator machine), EC2 `t3.large` head, us-west-2, image `rayproject/ray:2.59.0-py311-gpu`. Node type `gpu` `{CPU: 4, GPU: 1}`, `min_workers` 2; node type `gpu2` `{CPU: 4, GPU: 2}`, `min_workers` 0. `candidate_factor` 2, `max_dollars_per_hour: 2.5`, `compute_cap` 600–899. All jobs used `ray job submit` through the `dashboard` tunnel. Torch jobs got `torch==2.7.1` from `runtime_env` `pip`. Data: `archive/results/2026-10-14/` (`progress.md` "C9", `c9/`, `spend.md`, `resources.csv`). `c9/results.jsonl` has one row for each run.

The first workers: a Tesla P100 host (24 Ray CPUs, machine 3969) and an RTX 3060 host (48 Ray CPUs, machine 32816). The autoscaler also added a `gpu2` node (2x CMP 170HX, 13 Ray CPUs) for the CPU demand of C9.1.1. It ended by the idle timeout at 10:22Z.

### Main finding: the pids limit of vast containers

The first Ray Data jobs killed 4 workers in 10 min. The cause is the pids (task) limit of the vast container:

- `pids.max` is 1280 on the two 1-GPU hosts (cgroup v1, Docker `--pids-limit`). It is 3328 on the 2-GPU host (cgroup v2). `RLIMIT_NPROC` is unlimited.
- Each Ray worker process has 45–50 threads (16 `event_engine`, 12–16 `nexting_thread`, 4 `client.poll0`, and others). The raylet has 64–75 threads. An idle node has about 300 threads.
- ray-vast starts Ray with `--num-cpus` = the real core count (24 and 48 here). A job that uses all CPUs starts 24–48 worker processes, that is 1200–2400 threads.
- Above 1280, `pthread_create` fails with `EAGAIN`. The logs show `RuntimeError: can't start new thread` (Ray Data `MemoryProfiler`), `pthread_create failed: Resource temporarily unavailable`, and a raylet abort (`terminate called after throwing an instance of 'std::system_error'`). The node dies. The bootstrap reports `ray start exited with code 1` and destroys the instance. v2 rents a replacement.
- The bootstrap itself also fails: its `call()` runs each HTTP call in a thread. One worker reported `unexpected error: ... line 367, in main ... call(f"{base_b}/heartbeat", ...)`. `RuntimeError` is not in `NET_ERRORS`, so the bootstrap destroyed the worker.

Evidence: `c9/limits/limits_*.log`, `c9/limits/threads_1.log`, `c9/C9.1.1-run1/`, `c9/C9.1.3-run1/`, `c9/C9.1.4-run1/`, `c9/head/monitor.log` (09:23Z–09:34Z).

Workaround in the tests: `C9_DATA_CPU=12` sets the Ray Data `execution_options.resource_limits` (cpu 12) and `max_hash_shuffle_aggregators` 4. With it, all Data jobs except C9.1.6 passed. A product fix is necessary (see the summary below).

### Results

| ID | Result | Key numbers | Evidence |
|---|---|---|---|
| C9.0.1 | PASS | `up` 7 min 45 s. 2 workers 2 min 11 s after `up`. 4 rentals, 2 ended unused (7.7 s and 3.0 s after the slot fill). | `c9/up.log` |
| C9.0.2 | PASS | 4 tasks pinned to each of 3 nodes read `data/hello.txt`. Same package path on all nodes. (First run: test error, SPREAD put all 8 tasks in one worker process.) | `c9/C9.0.2/` |
| C9.0.3 | PASS | `torch 2.7.1+cu126` from pip on both workers and the head driver: 263.6 s for the first install. P100 and RTX 3060: CUDA available, 1 device. | `c9/C9.0.3/` |
| C9.0.4 | PASS | `ray job list`, `logs --follow` (17 ticks in 30 s), `stop` (exit 0, `STOPPED`). | `c9/C9.0.4/job_cli.txt` |
| C9.1.1 | PASS with workaround | 10M rows, 31.2 s, 640 728 rows/s, tasks on 3 nodes. Without `C9_DATA_CPU`: FAIL, pids limit, node A died. | `c9/C9.1.1/`, `c9/C9.1.1-run1/` |
| C9.1.2 | PASS | 3.0 GB `range_tensor`: `materialize` 19.7 s, driver `iter_batches` 17.2 MB/s, no spill. | `c9/C9.1.2/` |
| C9.1.3 | PASS with workaround | `random_shuffle` 186.2 s, `groupby().count()` 69.7 s on 3.0 GB. Without the workaround: FAIL (pids limit). | `c9/C9.1.3/`, `c9/C9.1.3-run1/` |
| C9.1.4 | PASS with workaround | GPU inference, 2 actors on 2 nodes, 200 000 rows in 39.5 s (5059 rows/s), `cuda`. Without the workaround: FAIL (a `HashShuffleAggregator` died). | `c9/C9.1.4/`, `c9/C9.1.4-run1/` |
| C9.1.5 | PASS | `write_parquet` to `/tmp/c9_out`: files on each worker (6/5/5). The read-back from the head finds no files. Public S3 parquet: 150 rows in 3.9 s. CSV from the working dir: 100 rows. | `c9/C9.1.5/` |
| C9.1.6 | FAIL | 400 blocks with `num_cpus=0.05`. Without a limit: pids limit (`pthread_create failed`), 13/400 when stopped. With `C9_DATA_CPU=12`: no crash, but 96/400 in 12.5 min (ReadRange held the CPU budget). More than about 20 worker processes per node is not possible. | `c9/C9.1.6-run1/`, `c9/C9.1.6-run2/` |
| C9.2.1 | PASS | 8 trials, 0 errors, 34.4 s, trials on 3 nodes (trial CPU = the smallest node's CPU count; first run with 1 CPU: all trials on 1 node, a test error). | `c9/C9.2.1/` |
| C9.2.2 | PASS | ASHA: 7 of 8 trials stopped at iteration 2, 1 ran to 20. | `c9/C9.2.2/` |
| C9.2.3 | PASS | 2 GPU trials on 2 nodes, 1 GPU each. | `c9/C9.2.3/` |
| C9.2.4 | LIMIT (Ray), PASS with shared storage | Default `storage_path`: `RuntimeError: Unable to set up cluster storage ... A local path on the head node is not accessible by worker nodes.` With an S3 bucket: 4 trials, checkpoints readable on the driver. | `c9/C9.2.4-run1/`, `c9/C9.2.4/` |
| C9.3.1 | LIMIT (Ray), PASS with shared storage | Same storage error at the first checkpoint. With the S3 bucket: 1 GPU, `nccl`, 2 epochs, checkpoint saved, 141.4 s. | `c9/C9.3.1-run1/`, `c9/C9.3.1/` |
| C9.3.2 | LIMIT (design) | NCCL, 2 nodes: `DistNetworkError: The client socket has timed out after 180000ms while trying to connect to (81.183.231.113, 57835)`. Error after 200.5 s. | `c9/C9.3.2/job.log` |
| C9.3.3 | LIMIT (design) | gloo, 2 nodes: `DistStoreError: Timed out after 181 seconds waiting for clients. 1/2 clients joined.` Rank 1: `TCP client failed to connect ... 185.62.108.226:40929`. Error after 201 s. | `c9/C9.3.3/job.log` |
| C9.3.5 (extra) | LIMIT (design) | TCPStore by hand on a free identity port (45084): rank 1 connected in 2.0 s. Then gloo: `Gloo connectFullMesh failed with [gloo/transport/tcp/pair.cc:144]`; the other rank hangs. | `c9/C9.3.5/` |
| C9.3.4 | PASS | 2 GPUs on one `gpu2` node (2x CMP 170HX), `nccl`, `cuda:0` and `cuda:1`, 162.5 s incl. the rental. | `c9/C9.3.4/` |
| C9.4.1 | PASS | 2 replicas on 2 nodes; 40 concurrent handle calls (28 + 12). (First run: test error, 20 sequential calls went to one replica.) | `c9/C9.4.1/` |
| C9.4.2 | PASS | `curl http://127.0.0.1:8000/` on the head host: HTTP 200. | `c9/C9.4.2/curl_head.txt` |
| C9.4.3 | PASS | `ssh -i <key> -N -L 18000:127.0.0.1:8000 ubuntu@<head ip>`, then `http://127.0.0.1:18000/`: HTTP 200, 0.21 s. | `c9/C9.4.3/tunnel.txt` |
| C9.4.4 | PASS | 2 GPU replicas on 2 nodes, `cuda`, 1 GPU id each. | `c9/C9.4.4/` |
| C9.5.1 | PASS | `PACK` 4 CPU bundles on 1 node; `STRICT_SPREAD` 2 GPU bundles on 2 nodes. | `c9/C9.5.1/` |
| C9.5.2 | PASS | `os._exit`: restart in 5.9 s; `ray.kill`: restart in 8.0 s (`max_restarts=1`). | `c9/C9.5.2/` |
| C9.5.3 | PASS | 2.0 GB `ray.put` on the CZ host, `ray.get` on the TX host: 67.1 s, 29.8 MB/s, checksum ok. | `c9/C9.5.3/` |
| Teardown | PASS | `down` 43.2 s, `left: nothing`, exit 0. Sweep `0 left`. `aws left: nothing`. S3 test bucket deleted (head-bucket 404). Secret scan with the cluster token and 15 join tokens: no output. | `c9/down.log`, `c9/sweep.log`, `c9/aws_check.log`, `c9/s3_bucket.log` |

Cost: vast $2.6868 (credit 10.9682 → 8.2814, 0 top-ups; the poll estimate is $1.70, the rest is probably bandwidth and storage). 15 rentals, peak $2.211/h. EC2 about $0.12.

Diagnosis of the LIMIT rows:
- C9.3.2, C9.3.3, C9.3.5: ray-vast's design. Ray Train sets `MASTER_PORT` to a random free port (`get_address_and_port`, bind to port 0). vast maps only the 64 identity ports, and Ray uses all 64. Ray Train has no setting for this port. A store on an identity port works (C9.3.5), but gloo and NCCL then open more connections on random ports, and they advertise the container's private address (172.17.0.x). Multi-node collectives cannot work behind this NAT.
- C9.2.4, C9.3.1 (default storage): Ray itself. Ray 2.59 refuses a local `storage_path` on a multi-node cluster. Shared storage fixes it.
- C9.2.4 with `s3://anonymous@...` (pyarrow): S3 refuses multipart uploads from anonymous users. The test then used `s3fs` (`anon=True`) through `pyarrow.fs.FSSpecHandler`. The bucket policy allowed only the head and worker IPs. A real user gives the workers credentials (for example `env_vars` in `runtime_env`).

Test script changes (`tests/manual/c9/`): C9.0.2 pins tasks to each node; C9.2.1 trial CPU = the smallest node's CPU count; C9.4.1 and C9.4.4 send 40 concurrent calls and check the replica actor nodes; new C9.3.5 (`c9_3_5_store_port.py`); `C9_DATA_CPU`, `C9_STORAGE=s3anon://...` (`storage_settings()` in `c9common.py`) and `C9_EXTRA_PIP` (`run_c9.py`).

### Ray library support

| Library | What works | What does not work | What the user must do |
|---|---|---|---|
| Jobs | `ray job submit/list/logs --follow/stop` through the `dashboard` tunnel; `--working-dir`; `runtime_env` `pip` (torch 2.7.1 with CUDA, about 4.5 min for the first install). | | Keep the tunnel open. The driver runs on the head, so pip also installs there. |
| Ray Core | Tasks, actors, actor restarts, placement groups (`PACK`, `STRICT_SPREAD`), 2 GB objects between hosts (about 30 MB/s). | More than about 20 worker processes on one node (pids limit 1280). | Until ray-vast caps the CPUs: limit the parallel tasks or actors on each node. |
| Ray Data | Pipelines, shuffles, groupby, GPU batch inference, `read_parquet` from S3, `read_csv` from the working dir, `write_parquet`. | Data with the default resources (it uses all CPUs and hits the pids limit). Output to a local path stays on the workers. | Set `DataContext.get_current().execution_options.resource_limits = ExecutionResources(cpu=N)` (N about 12 for 2 nodes). Write output to a cloud bucket. |
| Ray Tune | Trials on many nodes, ASHA, GPU trials, checkpoints to shared storage. | Checkpoints with the default local `storage_path`. | Set `RunConfig(storage_path="s3://...")` and give the workers credentials. |
| Ray Train | One node: 1 GPU, and 2 GPUs on one host (NCCL). Checkpoints to shared storage. | Training over 2 or more nodes (gloo and NCCL): the rendezvous and the collectives need ports that vast does not map. | Use one node with many GPUs. Set a shared `storage_path`. |
| Ray Serve | Replicas on many nodes, GPU replicas, `DeploymentHandle` calls, HTTP on the head port 8000. | HTTP from outside without a tunnel (no new security group rule). | Open an SSH tunnel: `ssh -i <key> -N -L 18000:127.0.0.1:8000 ubuntu@<head ip>`, then use `http://127.0.0.1:18000/`. |

Product bugs from C9:
1. `--num-cpus` = the real core count ignores the container's pids limit. On hosts with many cores, a normal Data job kills the node. A possible fix: cap `--num-cpus` (or the worker count) by `pids.max`, for example (pids.max − 400) / 50.
2. The bootstrap destroys the worker when it cannot start a thread for a heartbeat call (`RuntimeError` is not in `NET_ERRORS`).
3. The head log cuts a worker's destroy reason at 300 characters, so the traceback of bug 2 is cut before the error line.

## Live cycle C10: process limits of the Ray libraries (2026-10-16)

Setup: `ray-vast up --wheel` of main f258c78 (design (a) with the `pids-tuning` constants: 60 threads for each worker port, reserve 400, soft limit = ports // 2) from the dev box. EC2 `t3.large` heads, us-west-2, image `rayproject/ray:2.59.0-py311-gpu`. Phase A: cluster `rvtest-c10a`, one worker. Phase B: cluster `rvtest-c10b` (2 `gpu` workers, `gpu2` for R2). All jobs used `ray job submit --working-dir` through the `dashboard` tunnel and `run_c10.py`. Data: `archive/results/2026-10-16/` (`progress.md`, `spend.md`, `c10/`; `c10/results.jsonl` has one row for each run).

Hosts and limits (from `c10_probe.py` and the bootstrap log):

| Host | Where | pids.max | Ray CPUs | Worker ports | Soft limit |
|---|---|---|---|---|---|
| m33638 RTX 5060 Ti (phase A, host retry, rented 3 times) | TX | 7680 | 128 | 58 | 29 |
| m3969 Tesla P100 (phase A worker; phase B worker) | TX | 1280 (cgroup v1) | 24 | 14 | 7 |
| m8966 RTX 3090 (phase B worker) | CZ | 1280 | 32 | 14 | 7 |
| m55704 2x CMP 170HX (phase B `gpu2`, R2) | HU | not probed (it came after the probes) | | | |

### Phase A: the pairs (m3969, pids.max 1280, 14 ports, budget 10)

"WARNING" is the head's process-limit line for the worker (`... is at its process limit: 0 refused forks/threads (+0), pids N of 1280, worker ports in use 14 of 14 ...`). "pids" is the highest value in those lines (cgroup v1 has no `pids.peak`). Every run: the worker stayed enrolled, `pids.events` stayed `max 0`, no node crash.

| Test | Result | Settings | Time | WARNING lines (pids) | Evidence |
|---|---|---|---|---|---|
| C10.core.tuned | PASS | 200 items on 5 actors, `num_cpus` 2.4 | 17 s | 0 | `c10/C10.core.tuned/` |
| C10.core.many | PASS (HUNG at the 600 s guard, 0 of 200 done) | 200 actors, `num_cpus` 0.06 | 600 s | 2 (496) | `c10/C10.core.many/` |
| C10.data.tuned | **FAIL**: `RaySystemError: Failed to startup worker after retrying 5 times ... name=_shuffle_block` | aggregators 2, read 2, map 2, `resource_limits` cpu 2, 400 blocks | 560 s | 2 (511) | `c10/C10.data.tuned/` |
| C10.data.many | PASS (HUNG at the 900 s guard) | Ray Data defaults, 400 blocks | 900 s | 3 (527) | `c10/C10.data.many/` |
| C10.data.tuned2 (added) | PASS | as tuned, plus `override_num_blocks` = budget // 2 = 5 | 16 s | 0 | `c10/C10.data.tuned2/` |
| C10.tune.tuned | PASS (60 trials, 0 errors) | `max_concurrent_trials` 5 | 119 s | 0 | `c10/C10.tune.tuned/` |
| C10.tune.many | PASS (HUNG) | 60 trials at once | 600 s | 2 (546) | `c10/C10.tune.many/` |
| C10.serve.tuned | PASS (40 answers) | 5 replicas, `num_cpus` 1 | 23 s | 0 | `c10/C10.serve.tuned/` |
| C10.serve.many | PASS (HUNG in `serve.run`, no crash) | 60 replicas, `num_cpus` 0.1 | 600 s | 2 (812) | `c10/C10.serve.many/` |
| C10.train.tuned | PASS | 5 workers, 1 CPU, DataLoader 2 | 133 s (incl. the torch install) | 0 | `c10/C10.train.tuned/` |
| C10.train.many | PASS (HUNG) | 40 workers, 0.59 CPU | 900 s | 3 (533) | `c10/C10.train.many/` |
| C10.rllib.tuned | PASS (1 iteration) | 2 env runners × 20 envs | 21 s | 0 | `c10/C10.rllib.tuned/` |
| C10.rllib.many | PASS (HUNG in the build) | 40 env runners, 0.575 CPU | 900 s | 3 (500) | `c10/C10.rllib.many/` |

Cause of C10.data.tuned: the hash shuffle starts one `_shuffle_block` task (1 CPU) for each input block that arrives. `execution_options.resource_limits` does not cap these tasks: the job log shows 16 of them at once ("Resources: 16.5 CPU") with a limit of 2. With 24 Ray CPUs they needed more processes than the 14 ports. A task that cannot get a worker process fails after 5 start tries. So a port shortage can make a task fail, not only wait. The local check (4 Ray CPUs) did not show this. With fewer, larger blocks (`override_num_blocks` = budget // 2), at most 5 shuffle tasks run at once, and the job passed.

### Phase B: C9 Data re-runs without `C9_DATA_CPU`, RLlib (m3969 and m8966, both pids.max 1280, 14 ports)

| Test | Result | Key numbers | Evidence |
|---|---|---|---|
| C10.c9.1.1 (C9.1.1) | PASS | 10M rows, 24 s, 843 634 rows/s, tasks on 3 nodes | `c10/C10.c9.1.1/` |
| C10.c9.1.3 (C9.1.3) | FAIL, no crash | `RaySystemError: Failed to startup worker after retrying 5 times ... _shuffle_block` after 695 s; WARNING on both workers (ports 14 of 14) | `c10/C10.c9.1.3/` |
| C10.c9.1.4 (C9.1.4) | FAIL, no crash | the same error in its `groupby` after 838 s | `c10/C10.c9.1.4/` |
| C10.c9.1.6 (C9.1.6) | PASS (FAIL in C9) | 400 blocks in 45 s, 13 processes per node, 24 tasks at once | `c10/C10.c9.1.6/` |
| C10.rllib.R1 | PASS | PPO CartPole, 4 env runners (12 CPUs each), 2 on each node, 2 iterations, return 24.1 → 32.5 | `c10/C10.rllib.R1/` |
| C10.rllib.R3 | LIMIT | 2 learners (1 GPU each) on 2 nodes; no answer in 900 s at the build stage. Run 1 log: `[c10d] The server socket on [...]:54603 has timed out, will retry` (a random port, as in C9.3.2) | `c10/C10.rllib.R3/`, `c10/C10.rllib.R3-run1/` |
| C10.rllib.R2 | PASS (run 3) | Tune + `placement_strategy="STRICT_PACK"`: the autoscaler started `gpu2` (m55704); 2 learners on 1 node, 2 iterations, 0 errors, 115 s | `c10/C10.rllib.R2/`, `-run1/`, `-run2/` |

No node crashed in C10 (no "asks to be destroyed" line, no ERROR line in the head logs). In C9 the same Data jobs killed nodes.

### Every workaround, test-script change, and failed or INCONCLUSIVE run

1. **Offer query (phase A).** The spec query (`compute_cap` 600–899, `gpu_ram`, `dph_total` ≤ 0.45, CPU 32) had 0 offers. First change: `num_gpus` = 1, `dph_total` ≤ 0.70 (4 offers).
2. **Host retry did not find a limited host.** The first `big` worker (m33638) had pids.max 7680. The retry destroyed it, and the provider rented the same machine again (twice): the provider does not block a destroyed host, so the retry rents the cheapest offer again. I stopped `run_c10.py` and ran `down` (`c10/a1_host_retry/`).
3. **Phase A node type changed** to `offer_query: {machine_id: {in: [3969, 32816]}}`, `resources: {CPU: 4, GPU: 1}`: the 2 hosts with pids.max 1280 in C9. So phase A ran on a 24-CPU host, not a 32+ core host.
4. **Runner bug: lost job logs.** `run_c10.py` wrote `job.log`, then the cleanup job overwrote it. For core.tuned, core.many, data.tuned and data.many, I fetched the logs again with `ray job logs`. Fixed: one file name for each job.
5. **C10.data.tuned FAIL** (above). Added sub-test C10.data.tuned2 (`override_num_blocks` = budget // 2): PASS.
6. **Runner restarted** after data.many with the fixed script and the new test. A started tune.tuned job was stopped (`c10/C10.tune.tuned-stopped/`), then tune.tuned ran again in full.
7. **"No available ports" count is not a delta.** Ray removes old `worker-*.err` files, so post − pre is sometimes negative (serve.many −27, train.many −5, rllib.many −11). The verdict on vast does not use it (it uses the head WARNING line).
8. **cgroup v1 hosts have no `pids.peak`.** The pids numbers come from the WARNING lines (one sample per heartbeat).
9. **R3 run 1 stopped by hand:** the runner timeout (1500 s) was equal to the R3 guard (1500 s), so the runner would stop the job first. Re-run with `C10_GUARD_S=900`: LIMIT. The runner timeout for R3 is now 2400 s.
10. **R2 run 1 FAIL (test bug):** the job waited for a 2-GPU node, but nothing asked for one, so the autoscaler never started `gpu2`; the guard printed HUNG and no `C10_RESULT` line. Change: run PPO through Tune (`config.algo_class`) with `placement_strategy="STRICT_PACK"`, no remote env runners; checked locally first.
11. **R2 run 2 FAIL (Ray limit, as C9.2.4):** 2 iterations ran on 2 GPUs of one node, then Tune's checkpoint at the end failed: `Unable to set up cluster storage` (local storage on a multi-node cluster). Change: `CheckpointConfig(checkpoint_at_end=False)`. Run 3: PASS.
12. **C10.c9.1.3 and C10.c9.1.4 FAIL** (the hash shuffle error of item 5; no crash).
13. **C10.rllib.R3 LIMIT** (expected).
14. No many test was INCONCLUSIVE in the final runs. The 3 rentals of item 2 were the host retry, not results.

Cost: vast $1.5618 (credit 7.7294 → 6.1676, no top-up; the poll estimate is $1.19). 14 rentals, peak about $1.48/h. EC2 about $0.35 (3 heads, about 4 h).

Product bugs: none found in ray-vast. Findings for the design and the docs:
- The port cap works: every many test hung or failed, the worker stayed alive with 0 refused forks, and the head WARNING named the worker. pids stayed at 466–812 of 1280.
- A port shortage can also make a task fail: after 5 failed worker starts, Ray fails the task (`RaySystemError: Failed to startup worker after retrying 5 times`). The user docs say "queue, hang or fail"; this is the "fail" case.
- Ray Data's hash shuffle (`groupby`, and C9's `random_shuffle` and `groupby`) does not respect `execution_options.resource_limits` for its `_shuffle_block` tasks. On a node with more CPUs than ports, a `groupby` fails unless the user also limits the blocks (`override_num_blocks`).
- The host retry of the test cannot avoid the same machine (the provider does not block a destroyed host). A test that needs a property of a host (here a pids limit) must name the machines.

## Live IAM test: a restricted user (2026-10-16)

Goal: prove the policy `ray-vast-user` live, with the generated SSH key and the saved profile. No vast rental (all node types `min_workers: 0`). AWS profile `personal`, `us-west-2`. EC2 cost about $0.02.

Setup (as `personal`):

- The account had no `ray-vast-*` policy before. The test created `ray-vast-head-boundary` and `ray-vast-user` from `src/ray_vast/iam/`.
- The first `create-policy` of `ray-vast-user` failed: `MalformedPolicyDocument: The policy failed legacy parsing`. Cause: `ArnEquals` (and `ArnLike`) with the policy variable `${aws:PrincipalAccount}`. Fix: `StringEquals` with the variable. IAM accepted it. `iam:PermissionsBoundary` has the type ARN in the Service Authorization Reference.
- The IAM user `ray-vast-iamtest-20261016` with only `ray-vast-user` attached, and an access key in a temporary credentials file (mode 600, deleted at the end).

Negative tests, as the test user:

| Call | Result |
|---|---|
| `iam:CreateRole` `ray-vast-negtest` without a boundary | `AccessDenied` |
| `iam:CreateRole` `ray-vast-negtest` with `AdministratorAccess` as the boundary | `AccessDenied` |
| `iam:CreateRole` `negtest-other` (name not `ray-vast-*`) with the right boundary | `AccessDenied` |
| `iam:CreatePolicy` | `AccessDenied` |
| `iam:DeleteRolePermissionsBoundary` | `AccessDenied` |
| `ec2:CreateSecurityGroup` without the `ray-vast-cluster` tag | `UnauthorizedOperation` |

`AWS_SHARED_CREDENTIALS_FILE` in the test process hid the `personal` profile of the final leftover check. The test now takes `RAY_VAST_LIVE_CREDENTIALS_FILE` for the `ray-vast` commands only.

Runs of `tests/live/test_up_down.py` as the test user (`up --profile ray-vast-iamtest`, `dashboard` without `--profile`, `ray job submit`, `down` without `--profile` and without `AWS_PROFILE`):

| Run | Cluster | Result |
|---|---|---|
| 1 | `rvtest-b97a` | `up` (about 2.5 min), `dashboard` over SSH with the generated key, job printed `12345`, `down` printed `left: nothing` (key pair, 5 groups, instance profile, role). The test then failed in its own leftover check (`NoCredentialsError`, the credentials file problem above). A manual check as `personal` found nothing left. |
| 2 | `rvtest-532a` | Interrupted by an accidental stop after `up`. The cluster stayed. Later, `ray-vast down` from the run's state directory, without `--profile`, used the saved profile `ray-vast-iamtest` and printed `left: nothing`. |
| 3 | `rvtest-54c0` | PASS in 205 s: `up`, `dashboard`, job `12345`, `down` `left: nothing`, and the test's own check as `personal` (no instance, group, volume, key pair, role or instance profile). |

So `iam:DeleteRole` with the boundary condition works live: `down` deleted the role as the restricted user in all three runs. `up` with the generated key, `dashboard` with the generated key, and `down` with the saved profile all worked.

Teardown: the test user and its access key were deleted. No `ray-vast-rvtest-*` instance, security group, volume, key pair, role or instance profile is left. vast: 0 instances. The two policies `ray-vast-head-boundary` and `ray-vast-user` stay in the account (it had none before), so the account is set up for ray-vast.

## Local check: the `num_cpus` = CPUS / B rule (2026-10-16)

Setup: the C10 local check with the real ray-vast head and worker bootstrap (`tests/manual/c10/test_c10_local_e2e.py`, the step 11 harness), on the dev box. One worker container with `--pids-limit 1280`: 14 worker ports, so B = 10. The box has 4 cores, so CPUS = 4 and CPUS / B = 0.4. Image `c10-local:torch`. Cost $0.

Command: `PYTHONPATH=src .venv/bin/python -m pytest -o addopts="" -p no:cacheprovider -s ../tests/manual/c10/test_c10_local_e2e.py -k full`.

| Test | Result | Settings | Time | pids.peak | Worker processes (peak) | `pids.events` |
|---|---|---|---|---|---|---|
| C10.serve.full | PASS (40 answers) | 10 replicas (B), `num_cpus` 0.4 (CPUS / B), no `max_replicas_per_node` | 27 s | 545 | 13 | `max 0` |
| C10.core.full | PASS (20 items) | 10 actors (B), `num_cpus` 0.4 (CPUS / B) | 15 s | 390 | 12 | `max 0` |

The worker stayed alive in both runs, and the head logged no process-limit WARNING. So the library pages give B (not B // 2) replicas, and `max_replicas_per_node` is not needed: `num_cpus` = CPUS / B lets Ray put at most B replicas on one worker.
Not tested here: the rule on Tune trials, Train workers and RLlib env runners. Their caps (B // 2) are the ones that C10 tested.

## Measurements that are recorded in other files

The user docs give some numbers that no live cycle of this file measured. Their records are:

| Doc claim | Where it is recorded |
|---|---|
| TUN and NET_ADMIN on 0 of 146 hosts; about 8 % of hosts with an unreachable port, about 8 % with another egress IP (design-decisions.md) | design.md section 1 ("Measured on 2026-10-06/07"), from the host survey before C0 |
| "In a local spike, Ray started 2 workers for 4 tasks" (node-types.md) | spikes.md, finding B (check 3b) |
| 25 to 29 threads for each worker process, 50 for a Serve replica with FastAPI (limits.md, serve.md) | decisions.md (2026-10-16, the simulated 48-core host). The live suite records the threads of each worker process on real hosts (L03, LB00) |
| "HTTP calls worked after the job" (serve.md) | LB09 of the live suite |
| "tested with `num_cpus_per_learner=1`" (rllib.md) | LB10 of the live suite records the value |

<!-- live-suite 2026-10-16 start -->
## Live suite 2026-10-16

Written by `tests/live/summarize.py` from `archive/results/2026-10-16/`.

| ID | Result | Time (s) | Line | Evidence |
|---|---|---|---|---|
| F01 | PASS | 144.0 | committed shim is the step 10c build: 79055dea6e913180c362c81e2b0c9903dd7a39bac5e3d5bab0e491bb9b3817fd; shim tests: exit 0: {'passed': 14}; shim tests: no failure or error: {'passed': 14}; 14 shim tests: 14; mutation run: the unchanged source builds the committed .so: _overwritten_environment_memory | `2026-10-16/F01/` |
| F02 | PASS | 201.3 | shim: node ALIVE: ['NODES ["ALIVE"]']; shim: job ran a task; shim: no Errno 99: ['ERRNO99 0']; control without the shim: Errno 99 or no job: ['ray start exit 124', 'NODES none', 'ERRNO99 0'] | `2026-10-16/F02/` |
| F03 | PASS | 25.2 | config and CLI unit tests: exit 0: {'passed': 174}; config and CLI unit tests: no failure or error: {'passed': 174}; config and CLI unit tests: a passing test '0_1_0': ['test_main_down_removes_a_cluster_that_version_0_1_0_made', 'test_main_down_of_a_0_1_0_yaml_without_the_state_file_takes_the_region | `2026-10-16/F03/` |
| F04 | PASS | 13.0 | provider unit tests: exit 0: {'passed': 124}; provider unit tests: no failure or error: {'passed': 124}; provider unit tests: a passing test 'missing_from_vast_counts_only_after_the_grace': ['test_instance_missing_from_vast_counts_only_after_the_grace_period']; provider unit tests: a passing test 'o | `2026-10-16/F04/` |
| F05 | PASS | 114.9 | bootstrap tests: exit 0: {'passed': 55}; bootstrap tests: no failure or error: {'passed': 55}; bootstrap tests: a passing test 'cgroup_v1_cpu_quota': ['test_cgroup_v1_cpu_quota_sets_the_cpu_count_and_minus_1_is_no_limit[150000\\n-100000\\n-1]', 'test_cgroup_v1_cpu_quota_sets_the_cpu_count_and_minus_ | `2026-10-16/F05/` |
| F06 | PASS | 56.1 | head server and PKI tests: exit 0: {'passed': 72}; head server and PKI tests: no failure or error: {'passed': 72}; a test asserts the exact enroll answer keys: {"slot_id", "node_type", "cert_pem", "key_pem", "token", "shim_b64", "gcs_address"} | `2026-10-16/F06/` |
| F07 | PASS | 83.9 | bootloader and bootstrap container tests: exit 0: {'passed': 16}; bootloader and bootstrap container tests: no failure or error: {'passed': 16}; bootloader and bootstrap container tests: a passing test 'one_changed_byte': ['test_bootstrap_with_one_changed_byte_is_refused_by_the_bootloader_which_dest | `2026-10-16/F07/` |
| F08 | PASS | 118.6 | test_scale_up_three_workers_with_k_2_runs_the_battery_then_idle_scale_down_to_zero passed: (0, {'test_scale_up_three_workers_with_k_2_runs_the_battery_then_idle_scale_down_to_zero': 'passed'}) | `2026-10-16/F08/` |
| F09 | PASS | 100.0 | test_two_workers_with_the_same_public_ip_both_join_and_the_idle_one_is_removed passed: (0, {'test_two_workers_with_the_same_public_ip_both_join_and_the_idle_one_is_removed': 'passed'}) | `2026-10-16/F09/` |
| F10 | PASS | 27.1 | test_host_with_more_cores_than_declared_runs_with_the_real_core_count passed: (0, {'test_host_with_more_cores_than_declared_runs_with_the_real_core_count': 'passed'}) | `2026-10-16/F10/` |
| F11 | PASS | 96.5 | test_a_candidate_that_never_enrolls_is_ended_and_its_slot_is_asked_for_again passed: (0, {'test_a_candidate_that_never_enrolls_is_ended_and_its_slot_is_asked_for_again': 'passed'}) | `2026-10-16/F11/` |
| F12 | PASS | 109.6 | test_head_silence_makes_each_worker_report_and_destroy_itself passed: (0, {'test_head_silence_makes_each_worker_report_and_destroy_itself': 'passed'}) | `2026-10-16/F12/` |
| F13 | PASS | 25.6 | test_container_restart_is_refused_a_second_enrollment_and_destroys_itself passed: (0, {'test_container_restart_is_refused_a_second_enrollment_and_destroys_itself': 'passed'}) | `2026-10-16/F13/` |
| F14 | PASS | 101.2 | test_monitor_crash_stops_the_head_and_every_worker_destroys_itself passed: (0, {'test_monitor_crash_stops_the_head_and_every_worker_destroys_itself': 'passed'}) | `2026-10-16/F14/` |
| F15 | PASS | 152.0 | test_three_enrollment_failures_in_a_row_pause_the_node_type_then_it_launches_again passed: (0, {'test_three_enrollment_failures_in_a_row_pause_the_node_type_then_it_launches_again': 'passed'}) | `2026-10-16/F15/` |
| F16 | PASS | 235.6 | C10 local pairs: exit 0: {'passed': 5}; C10 local pairs: no failure or error: {'passed': 5}; 5 pair runs: ['test_pair[c10_core.py-full-1280-300]', 'test_pair[c10_core.py-many-1280-150]', 'test_pair[c10_core.py-tuned-1280-150]', 'test_pair[c10_data.py-many-560-150]', 'test_pair[c10_serve.py-full-1280 | `2026-10-16/F16/` |
| F17 | PASS | 0.4 | twine check --strict: ['Checking /tmp/pytest-of-ubuntu/pytest-186/popen-gw1/dist0/ray_vast-0.1.0.tar.gz:\x1b[32mPASSED\x1b[0m']; wheel has ray_vast/shim/librayvast_shim.so; wheel has ray_vast/worker/bootstrap.py; wheel has ray_vast/worker/bootloader.sh; wheel has ray_vast/examples/gpu-24gb.yaml; whe | `2026-10-16/F17/` |
| F18 | PASS | 155.8 | Python 3.10: ['Python 3.10.22']; ray-vast --help works; fast unit run passes on 3.10: ['445 passed, 52 deselected in 90.79s (0:01:30)'] | `2026-10-16/F18/` |
| F19 | PASS | 0.7 | cpu-heavy.yaml cpu32: >= 4 offers on >= 3 machines: {'source': 'cpu-heavy.yaml', 'type': 'cpu32', 'offers': 22, 'machines': 22, 'cheapest': [0.337, 0.671], 'budget': 4.0}; cpu-heavy.yaml cpu32: the first candidates fit the budget: {'source': 'cpu-heavy.yaml', 'type': 'cpu32', 'offers': 22, 'machines | `2026-10-16/F19/` |
| F20 | PASS | 31.7 | core_0 compiles; core_1 compiles; data_0 compiles; rllib_0 compiles; rllib_1 compiles; serve_0 compiles; train_0 compiles; tune_0 compiles; tune_1 compiles; all snippet imports work in the Ray 2.59 image (torch from pip); the core snippets run on a local node: ing '--shm-size=10.00gb' to 'docker run | `2026-10-16/F20/` |
| A01 | PASS | 41.5 | up: first line names the source: installing ray-vast from a wheel built from the source tree /home/ubuntu/ray-vast/ray-vast-pypi; up output line ^head instance i-\w+ starting$; up output line ^head IP [\d.]+$; up output line ^cluster rvtest-aws-455d is up\. Head IP [\d.]+\.$; up output line ^ dashb | `2026-10-16/A01/` |
| A02 | PASS | 5.9 | no ray-vast role uses the boundary now: []; up exits 1 with the message and the link: ray-vast up: the IAM policy ray-vast-head-boundary does not exist in AWS account 491085427508. Create it once (one command): https://duc-ph.github.io/ray-vast/getting-started/#aws-setup. Nothing was created. ; noth | `2026-10-16/A02/` |
| A03 | PASS | 43.6 | (a) up was stopped after the key pair: fer) ^^^^^^^^^^^^^^^^^^^^^^^^^ File "/usr/lib/python3.12/ssl.py", line 1104, in read return self._sslobj.read(len, buffer) ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ KeyboardInterrupt ; (a) down: left nothing: deleted security group sg-0b31a7c85 | `2026-10-16/A03/` |
| A04 | PASS | 6.0 | a second up stops (head tag): ray-vast up: cluster rvtest-aws-455d already exists; use `ray-vast down` first ; it made nothing (no head line): installing ray-vast from a wheel built from the source tree /home/ubuntu/ray-vast/ray-vast-pypi ; up stops on the state file alone: ray-vast up: cluster rvte | `2026-10-16/A04/` |
| A05 | PASS | 96.6 | port 6379 is closed from the operator machine: TimeoutError; port 7443 is closed from the operator machine: TimeoutError; port 7444 is closed from the operator machine: TimeoutError; port 8265 is closed from the operator machine: TimeoutError; port 10001 is closed from the operator machine: TimeoutE | `2026-10-16/A05/` |
| A06 | PASS | 14.8 | CA key mode 600: 600; vast key mode 600: 600; the job ran: SUCCEEDED; a task on the head can read the vast key and the CA key: {'/home/ubuntu/ray-vast-rvtest-aws-455d/secrets/vast_api_key': True, '/home/ubuntu/ray-vast-rvtest-aws-455d/secrets/ca.key': True} | `2026-10-16/A06/` |
| A07 | PASS | 24.7 | s3 ListAllMyBuckets is denied: AccessDenied; ec2 CreateSecurityGroup is denied: UnauthorizedOperation; authorize on an untagged group is denied: UnauthorizedOperation; authorize on cluster group 1 works: ALLOWED; revoke on cluster group 1 works: ALLOWED | `2026-10-16/A07/` |
| A08 | PASS | 24.3 | monitor.log: no rentable offer matches the node type query: 2026-10-16 16:29:14,365 INFO node_provider.py:491 -- Failed to launch 1 nodes of type impossible: Node Launch Exception (no offers): node type 'impossible': no rentable offer matches the node type query; monitor.log: every matching offer wo | `2026-10-16/A08/` |
| A09 | PASS | 4.5 | ray job submit without RAY_AUTH_* fails: t variable. To generate a token for local development, use `ray get-auth-token --generate` For remote clusters, ensure that the token is propagated to all nodes of the cluster when token authentication is enabled. For more information, see: https://docs.ray.i | `2026-10-16/A09/` |
| A10 | PASS | 0.9 | CreateRole ray-vast-negtest without a boundary is denied: AccessDenied; CreateRole ray-vast-negtest with AdministratorAccess as the boundary is denied: AccessDenied; CreateRole negtest-other with the right boundary is denied: AccessDenied; PutRolePolicy on a role not named ray-vast-* is denied: Acce | `2026-10-16/A10/` |
| A11 | PASS | 383.8 | pip install ray-vast==0.1.0; with_state_file: 0.1.0 up: a11-81ed is up. Head IP 35.167.164.61. dashboard: ray-vast dashboard /home/ubuntu/ray-vast/archive/results/2026-10-16/A11/with_state_file/cluster.yaml (then open http://127.0.0.1:8265) shell: ray-vast attach /home/ubuntu/ray-vast/arch | `2026-10-16/A11/` |
| A12 | PASS | 3.1 | ray exec fails: exit 1: ["KeyError: 'type'"]; ray attach fails: exit 1: ["KeyError: 'type'"]; ray rsync-up fails: exit 1: ["KeyError: 'type'"]; ray up fails: exit 1: ["KeyError: 'type'"]; they made no EC2 instance: set(); they rented nothing on vast | `2026-10-16/A12/` |
| A13 | PASS | 32.4 | up with t4g.small fails with an AWS error: cified instance type does not match the architecture 'x86_64' of the specified AMI. Specify an instance type and an AMI that have matching architectures, and try again. You can use 'describe-instance-types' or 'describe-images' to discover the architecture | `2026-10-16/A13/` |
| L01 | NOT RUN | | | |
| L02 | NOT RUN | | | |
| L03 | NOT RUN | | | |
| L04 | NOT RUN | | | |
| L05 | NOT RUN | | | |
| L06 | NOT RUN | | | |
| L07 | NOT RUN | | | |
| L08 | NOT RUN | | | |
| L09 | NOT RUN | | | |
| L10 | NOT RUN | | | |
| L11 | NOT RUN | | | |
| LB00 | NOT RUN | | | |
| LB01 | NOT RUN | | | |
| LB02 | NOT RUN | | | |
| LB03 | NOT RUN | | | |
| LB04 | NOT RUN | | | |
| LB05 | NOT RUN | | | |
| LB06 | NOT RUN | | | |
| LB07 | NOT RUN | | | |
| LB08 | NOT RUN | | | |
| LB09 | NOT RUN | | | |
| LB10 | NOT RUN | | | |
| LB11 | NOT RUN | | | |
| LB12 | NOT RUN | | | |
| LB13 | NOT RUN | | | |
| LB14 | NOT RUN | | | |
| R01 | NOT RUN | | | |
| R02 | NOT RUN | | | |

Spend for each cluster group (vast: the dph_total of its instances over time, from its timeline; EC2: an estimate from the hours):

| Group | Cluster | vast | Hours | EC2 |
|---|---|---|---|---|
| aws_only | rvtest-aws-455d | $0.0000 | 0.19 h | $0.02 |

vast spend of each run (the credit before and after, plus the auto top-ups in between):

| Run | vast | Top-ups |
|---|---|---|
| 231904-264721 | $0.0000 | $0.00 |
| 233326-266447 | $0.0000 | $0.00 |
<!-- live-suite 2026-10-16 end -->
### Notes on the live suite run of 2026-10-16 (free and aws groups only)

The first run stopped early: the dev box was resized. The second run (after the dev box resize, in parallel with `-n`) ran the free and aws groups again. All 33 scenarios pass. No vast group ran.

- Wall time of the free group: 38 min one after the other (2300 s, the code before the parallel suite), 5 min 52 s with `-n 12` (352 s). The aws group: 12 min 13 s with `-n 8`.
- F03, F05, F07: the test looked for a unit test name that does not exist. Fixed to the real names; pass.
- F05 (parallel run 1 and 2): the bootstrap tests and the container tests of F07 took the same ports at the same time (random port blocks of the fake worker world). Fixed: each fake world reserves a port block with a lock file (`tests/fakes/worker_world.py`, red test `test_worlds_that_exist_at_the_same_time_get_port_blocks_that_do_not_overlap`).
- F17: the secret patterns matched the fake keys in the package's own test files. Fixed earlier; pass.
- F18: no product bug on Python 3.10. The 3 failures came from the layout: the unit tests ran against the installed wheel, outside a source tree and without `docs/test-plan.md`. F18 now runs them in an editable source tree next to the plan; 445 pass on 3.10.
- F19: the example queries of `cpu-heavy.yaml`, `gpu-24gb.yaml` and `gpu-80gb.yaml` matched 1 offer each on 2026-10-16. The examples are wider now (decisions.md); each type has 8 or more offers.
- F20: the first try failed because the dev box disk was full (`pip install torch`: "No space left on device"; the test hid the pip output). Space was freed, the test shows the pip output now, and F20 runs after F18 (both fill about 1.5 GB). Pass.
- A01: the AWS check waits up to 180 s while only the head's volume is left (`cluster.aws_leftovers`). Pass.
- A03 (parallel run): the boto3 `instance_terminated` waiter fails at once when it sees the head still `pending`. Fixed with a poll loop; pass. The failed run left the IAM role, key pair and groups of `rvtest-a03b-a6f2`: the end-of-run check did not know clusters without a fixture. Removed with `ray-vast down --profile` (left: nothing). Now every `Cluster` of a run is noted for the end-of-run check.
- A05, A06, A09, A11: fixed in the first run; pass.

Product changes found by this work (both with a red test first):
- `down`, `attach` and `dashboard` of a 0.1.0 YAML without the state file failed with "the YAML needs node_config.region". They now take `provider.region` (test `test_main_down_of_a_0_1_0_yaml_without_the_state_file_takes_the_region_from_provider_region`).
- An unknown AWS profile or missing credentials printed a traceback. Now one line (tests `test_main_with_an_unknown_profile_prints_one_line_without_a_traceback`, `test_main_down_without_aws_credentials_prints_one_line_without_a_traceback`).

Doc bug found by the local runs of the libraries jobs (fixed): tune.md, Settings. A trial that asks `{"cpu": CPUS / B}` and starts a task with `num_cpus = CPUS / B` fails with "No trial resources are available" (local C10 check, `lb_tuned.py tune` at pids.max 1280, 14 ports: ERROR). tune.md now gives such a trial `tune.PlacementGroupFactory([{"CPU": CPUS / B}, {"CPU": CPUS / B}])`, and LB11 `tune.tuned` runs that code: PASS in the local C10 check (18 trials, 25 s). The same check ran `lb_tuned.py train` and `rllib` with the train.md and rllib.md values: both PASS, so those pages need no change.

<!-- live-suite 2026-10-17 start -->
## Live suite 2026-10-17

Written by `tests/live/summarize.py` from `archive/results/2026-10-17/`.

| ID | Result | Time (s) | Line | Evidence |
|---|---|---|---|---|
| F01 | NOT RUN | | | |
| F02 | NOT RUN | | | |
| F03 | NOT RUN | | | |
| F04 | NOT RUN | | | |
| F05 | NOT RUN | | | |
| F06 | NOT RUN | | | |
| F07 | NOT RUN | | | |
| F08 | NOT RUN | | | |
| F09 | NOT RUN | | | |
| F10 | NOT RUN | | | |
| F11 | NOT RUN | | | |
| F12 | NOT RUN | | | |
| F13 | NOT RUN | | | |
| F14 | NOT RUN | | | |
| F15 | NOT RUN | | | |
| F16 | NOT RUN | | | |
| F17 | NOT RUN | | | |
| F18 | NOT RUN | | | |
| F19 | NOT RUN | | | |
| F20 | NOT RUN | | | |
| A01 | NOT RUN | | | |
| A02 | NOT RUN | | | |
| A03 | NOT RUN | | | |
| A04 | NOT RUN | | | |
| A05 | NOT RUN | | | |
| A06 | NOT RUN | | | |
| A07 | NOT RUN | | | |
| A08 | NOT RUN | | | |
| A09 | NOT RUN | | | |
| A10 | NOT RUN | | | |
| A11 | NOT RUN | | | |
| A12 | NOT RUN | | | |
| A13 | NOT RUN | | | |
| L01 | PASS | 0.7 | up as the restricted user exits 0: 559.9; the head runs in us-east-2 with t3.medium: ('us-east-2b', 't3.medium'); the head role has the boundary; the state file saved the restricted profile | `2026-10-17/L01/` |
| L02 | PASS | 316.4 | c8_job SUCCEEDED with 4 containers: SUCCEEDED; both node types: ['gpu-a', 'gpu-b']; bundle 0: 1 GPU, gpu_ids [0]: ([0], 'GPU 0: NVIDIA GeForce RTX 3090 (UUID: GPU-d1929428-eb3c-777d-f101-91cfd03f34ea)'); bundle 1: 1 GPU, gpu_ids [0]: ([0], 'GPU 0: NVIDIA GeForce RTX 5060 Ti (UUID: GPU-407d0c99-ab97- | `2026-10-17/L02/` |
| L03 | PASS | 26.3 | probe job SUCCEEDED on 4 workers: ('SUCCEEDED', 4); 56321651: uid 1000, Ray 2.59.0: (1000, '2.59.0'); 56321651: no CONTAINER_API_KEY or RAYVAST_JOIN_TOKEN in os.environ; 56321651: PID 1 is the bootstrap (GPU image ENTRYPOINT kept); 56321651: /tmp/rayvast 700, files 600 or 644 for certs: {'ca.pem': ' | `2026-10-17/L03/` |
| L04 | FAIL | 14.4 | check failed: 56321651: agent port answers 401 without the token (InvalidURL) | `2026-10-17/L04/` |
| L05 | PASS | 3.4 | monitor.log has the rent line; monitor.log has the enrolled line; monitor.log has the ending line; the `Dropping the empty legacy field` lines are not errors: ['2026-10-16 17:35:50,043\tWARNING util.py:279 -- Dropping the empty legacy field head_node. head_nodeis not supported for ray>=2.0.0. It is | `2026-10-17/L05/` |
| L06 | PASS | 14.3 | 56321651: /proc/1/environ holds CONTAINER_API_KEY and RAYVAST_JOIN_TOKEN (a task can read them); 56321651: the key reads its own instance (200) and not another (401 or 403): {'other': 401, 'own': 200}; 56321651: uid 1000 cannot read /root/.ssh; 56321632: /proc/1/environ holds CONTAINER_API_KEY and R | `2026-10-17/L06/` |
| L07 | FAIL | 166.2 | check failed: TERM: the report reaches the head within 1 s (1.77) | `2026-10-17/L07/` |
| L08 | FAIL | 25.5 | IndexError: list index out of range | `2026-10-17/L08/` |
| L09 | FAIL | 29.8 | check failed: `rebooting` is not an unknown status () | `2026-10-17/L09/` |
| L10 | PASS | 860.7 | head stopped with a worker enrolled and a candidate loading; worker 56322499 destroys itself within 300 s + 30 s heartbeat + 15 s: 65; candidate 56322500 is gone within 10 min plus its pull: 65; candidate 56322510 is gone within 10 min plus its pull: 65; candidate 56322513 is gone within 10 min plus | `2026-10-17/L10/` |
| L11 | FAIL | 0.4 | check failed: down: left nothing () | `2026-10-17/L11/` |
| LB00[H1] | FAIL | 975.5 | livekit.HostNotFound: no host matched {'pids_max': 1280, 'cgroup': 'v1', 'cpus': {'gte': 36}} in 3 rentals (machines 148655, 58013, 36376) | `2026-10-17/LB00[H1]/` |
| LB00[H2] | PASS | 600.3 | H2: cgroup v2: {'machine_id': 51706, 'pids.max': '1536', 'cgroup': 'v2'}; H2 56320907: worker ports = (pids.max - 400) // 60 in 4..58 (pids.max 1536): (18, 18); H2 56320907: soft limit min(CPUs, ports // 2), grpc threads 4, startup concurrency 8: {'RAY_num_grpc_internal_threads': '4', 'RAY_num_worke | `2026-10-17/LB00[H2]/` |
| LB00[H3] | PASS | 578.6 | H3 56320908: worker ports = (pids.max - 400) // 60 in 4..58 (pids.max 1280): (14, 14); H3 56320908: soft limit min(CPUs, ports // 2), grpc threads 4, startup concurrency 8: {'RAY_num_grpc_internal_threads': '4', 'RAY_num_workers_soft_limit': '5', 'RAY_worker_maximum_startup_concurrency': '8'}; H3 56 | `2026-10-17/LB00[H3]/` |
| LB00[gpu2] | PASS | 716.8 | gpu2 56321071: worker ports = (pids.max - 400) // 60 in 4..58 (pids.max 3072): (44, 44); gpu2 56321071: soft limit min(CPUs, ports // 2), grpc threads 4, startup concurrency 8: {'RAY_num_grpc_internal_threads': '4', 'RAY_num_workers_soft_limit': '12', 'RAY_worker_maximum_startup_concurrency': '8'}; | `2026-10-17/LB00[gpu2]/` |
| LB01 | PASS | 472.7 | --working-dir: data/hello.txt read by tasks on both workers: {'cwd_example': '/tmp/ray/session_2026-10-16_17-33-40_263304_7/runtime_resources/working_dir_files/_ray_pkg_97ba245c9a73e896', 'elapsed_s': 7.7, 'file_ok': True, 'ips': ['143.55.45.86', '185.62.108.226'], 'tasks_per_node': {'e4cf6aaa': 4, | `2026-10-17/LB01/` |
| LB02 | PASS | 174.7 | PACK on one worker, STRICT_SPREAD on both: {'elapsed_s': 12.2, 'pack_bundles': {'0': 'e4cf6aaa', '1': 'e4cf6aaa', '2': 'e4cf6aaa', '3': 'e4cf6aaa'}, 'pack_task_nodes': ['e4cf6aaa', 'e4cf6aaa', 'e4cf6aaa', 'e4cf6aaa'], 'spread_bundles': {'0': 'fa6c41ea', '1': 'e4cf6aaa'}, 'spread_task_nodes': ['fa6c4 | `2026-10-17/LB02/` |
| LB03[H1] | FAIL | 1831.8 | AssertionError: the LB03 input was not written: FAILED ('make_parquet', 'FAIL', {'error_class': 'ExecutionTimeoutError', 'error_head': ["Dataset execution made no progress for at least 1800s of scheduling time (timeout: 1800s). No output was taken and no blocks moved through any operator's queues in | `2026-10-17/LB03[H1]/` |
| LB04 | PASS | 232.5 | C9.1.3 with the data.md settings at B 10: SUCCEEDED: ('SUCCEEDED', {'dataset_gb': 3.0, 'elapsed_s': 124.6, 'group_total': 1430, 'groupby_s': 55.5, 'groups': 16, 'rows': 1430, 'settings': {'B': 10, 'max_hash_shuffle_aggregators': 2, 'override_num_blocks': 5, 'read_concurrency': 2, 'resource_limits_cp | `2026-10-17/LB04/` |
| LB05 | PASS | 363.5 | c9_1_1_data_cpu.py with the defaults: {'count': 3333334, 'elapsed_s': 23.5, 'expected': 3333334, 'first': [{'id': 0}, {'id': 6}, {'id': 12}], 'rows': 10000000, 'rows_per_s': 856884, 'seconds': 23.34, 'task_nodes': {'e4cf6aaa': 38, 'fa6c41ea': 46}}; c9_1_2_data_objstore.py with the defaults: {'checks | `2026-10-17/LB05/` |
| LB06 | FAIL | 406.1 | check failed: the default storage_path: `Unable to set up cluster storage ... not accessible by worker nodes` (tune.md:83) ({'elapsed_s': 10.1, 'errors': 4, 'experiment_path': '/home/ray/ray_results/c9_2_4', 'storage': 'default', 'trials': [{'checkpoint': None, 'error': '\x1b[36mray::ImplicitFunc.tr | `2026-10-17/LB06/` |
| LB07 | FAIL | 284.5 | interrupted: the test did not end (no call report) | `2026-10-17/LB07/` |
| LB08 | FAIL | 821.1 | interrupted: the test did not end (no call report) | `2026-10-17/LB08/` |
| LB09 | NOT RUN | | | |
| LB10[gpu2] | FAIL | 2048.0 | check failed: the learner snippet on gpu2 (num_cpus_per_learner 0.3): 2 iterations, 0 errors ({'elapsed_s': 1912.7, 'errors': 1, 'iterations': None}) | `2026-10-17/LB10[gpu2]/` |
| LB11[H1] | FAIL | 242.0 | interrupted: the test did not end (no call report) | `2026-10-17/LB11[H1]/` |
| LB11[H2] | FAIL | 2449.0 | interrupted: the test did not end (no call report) | `2026-10-17/LB11[H2]/` |
| LB12 | NOT RUN | | | |
| LB13 | NOT RUN | | | |
| LB14 | NOT RUN | | | |
| R01 | NOT RUN | | | |
| R02 | NOT RUN | | | |

Spend for each cluster group (vast: the dph_total of its instances over time, from its timeline; EC2: an estimate from the hours):

| Group | Cluster | vast | Hours | EC2 |
|---|---|---|---|---|
| lifecycle | rvtest-life-f9e2 | $0.4077 | 0.6 h | $0.03 |

vast spend of each run (the credit before and after, plus the auto top-ups in between):

| Run | vast | Top-ups |
|---|---|---|
| 002539-287730 | $2.1200 | $10.00 |
<!-- live-suite 2026-10-17 end -->
### Notes on the live suite run of 2026-10-17 (lifecycle and libraries, stopped)

The run stopped early on the user's decision: the suite gets a refactor before the next run. No scenario of this run is a final record. The records of the smoke run (`--keep-cluster`, L01 and LB00) are in `archive/results/2026-10-16/`.

Last state of each scenario (run `002539-287730`, `-n 8`, fresh clusters):

- PASS: L01, L02, L03, L05, L06, L10, LB00[H2], LB00[H3], LB00[gpu2], LB01, LB02, LB04, LB05.
- L04 FAIL (test bug, fixed, not re-run): `node_probe.py` read the agent HTTP port from the raylet command line. Ray 2.59 gives it to the dashboard agent process (`--listen-port`).
- L07 FAIL (test bug, fixed, not re-run): the 1.5 s limit counted from t0, before the driver schedules the signal task. The report came 0.05 s after the signal. Also the limit had no source in the spec (see "Timing bounds").
- L08 FAIL (follow-on of L07): L07 stopped before its replacement joined. The probe of L08 hit the dead node (`TaskUnschedulableError`), so it had no nodes (IndexError).
- L09 FAIL (product bug, fixed with a red test): the provider logged `unknown vast actual_status 'rebooting'`. `rebooting` is now a pending status. The refusal and the destroy passed (gone 2.9 s after the refusal).
- L11 FAIL (test bug, fixed, not re-run): after L10 the state file is gone, and `down` needs `--profile` (cli.md). `Cluster.down` now adds it.
- LB00[H1], LB03[H1] FAIL (host spec, fixed, not re-run): see "H1". LB03[H1] then failed in its fixture: the parquet input job (`lb_make_parquet.py`) made no progress for 1800 s on the 38-CPU host 36376 that `find` left (cause not analysed).
- LB06 FAIL (test bug, fixed, not re-run): the 60-trial snippet, ASHA and GPU trials passed. The default-storage check read the result line, which cuts each error to 300 characters. The job log has the documented error. The check reads the log now.
- LB10[gpu2] FAIL (not analysed): the learner snippet gave 1 error and no iterations after 1913 s.
- LB07, LB08, LB11[H1], LB11[H2]: interrupted by the stop (records say "interrupted").
- Not run: LB03[H3], LB09, LB10[H3], LB12, LB13, LB14.

H1. In C9, m32816 gave 48 Ray CPUs. Since the cgroup v1 quota fix it gives 5 (quota 5.76 cores). The probes of 2026-10-17 show that vast sets `pids.max` = 256 × the CPU quota, rounded down (1280 at 5.76, 1536 at 6.72, 1792 at 7.68, 2816 at 11.52, 9728 at 38.4, 11776 at 46.08, 15616 at 61.44). So no host has many Ray CPUs and `pids.max` 1280. The first try (want 36 or more CPUs, query `cpu_cores_effective` ≥ 40) rented m148655 (61 CPUs, 15616, v1), m58013 (46, 11776, v2) and m36376 (38, 9728, v1): no match. H1 is now `pids.max` 1280 and cgroup v1, with m32816 first and `cpu_cores_effective` 5.5 to 6.5 as the fallback.

Hosts of the run (pids.max, cgroup, Ray CPUs, CPU quota in cores, enroll time from the suite's first `running` sample): H2 m51706 (1536, v2, 6, 6.912, 4.5 s); H3 m8966 (1280, v1, 5, 5.12, 8.4 s) and m3969 (1280, v1, 5, 5.76, 9.5 s); gpu2 m29278 (3072, v1, 12, 12.288, 2.5 s); the 4 lifecycle workers (1792, 2816, 1280, 1536, all v1; enroll −2.6 to 7.0 s). The smoke run had the enroll times wrong by −25190 s: see the test fixes.

Test-script changes (all in `ray-vast-pypi/tests/live/`):

- `livekit.log_epoch`: monitor.log times are in the head container's time zone. The rayproject/ray images set `TZ=America/Los_Angeles`. Red: `test_log_epoch_reads_a_monitor_log_time_in_the_time_zone_of_the_head_container` (no such function).
- `livekit.outcome_of`: a test that SIGTERM stopped was recorded as passed (no call report). Red: `test_a_test_without_a_call_report_is_not_recorded_as_passed` (no such function); mutation: without the branch the test fails with `'passed' == 'failed'`.
- `Cluster.down` adds `--profile` without a state file; `node_probe.py` reads the agent port from the agent process; LB06 and LB07 read the storage errors from the job log; L07 uses the signal time.

Timing bounds (each bound now cites the spec or a user doc, or is a safety bound with the time recorded as data):

| Scenario | Before | Now |
|---|---|---|
| L02 enroll | ≤ 45 s | −15 to 75 s (design.md:56, :86: 1 min; the suite's 10 s samples) |
| L02 idle to 0 | ≤ idle_timeout + 120 s | ≤ idle_timeout + 300 s (control-cost.md:24; safety bound) |
| L07 report | ≤ 1.5 s after t0 | the head logs the report (design.md:262 has no time) |
| L07 gone from vast | ≤ 20 s | ≤ 60 s (safety bound) |
| L07 Ray node DEAD | ≤ 60 s | DEAD within 300 s (safety bound) |
| L08 destroy after the cut | 240 to 345 s | 240 to 400 s (design.md:246, :258, :265: 300 s) |
| L09 gone after the refusal | ≤ 20 s | ≤ 60 s (safety bound) |
| L10 worker gone | ≤ 345 s | ≤ 360 s (design.md:246, :258, :261) |
| L10 candidate gone | ≤ 20 min | ≤ 30 min (design.md:144, :155, :260, after its pull) |
| LB00 enroll | ≤ 45 s | −15 to 75 s (design.md:86) |
| LB08 gloo error | ≤ 300 s | unchanged, cites train.md:91 |
| R02 tutorial | ≤ 30 min | unchanged, cites getting-started.md:4 |

worker-lifecycle.md now says that the 2026-10-14 times are one measured run, not limits.

Workarounds: the 4 libraries clusters survived the stop (SIGINT ended the xdist workers before their teardown). They were ended by hand with `ray-vast down` (`_session/libraries-*/down_by_hand.log`, each `left: nothing`). The smoke run's restricted IAM user `ray-vast-livetest-20261016` (kept by `--keep-cluster`) was deleted by hand.

Spend: smoke run vast $0.52, EC2 about $0.20 (5 heads, 0.6 h). Run `002539-287730`: vast $2.12 (credit diff corrected for one $10 top-up), EC2 about $0.40 (4 × t3.large and 1 × t3.medium, about 1 h). Cleanup at 01:26Z: vast label sweep 0 left; AWS check empty for every cluster of both runs in us-west-2 and us-east-2; no ray-vast role, test user, key pair, test group or bucket left; the secret scan of `archive/results/2026-10-16` and `2026-10-17` found no file.
