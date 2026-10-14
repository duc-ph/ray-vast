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
