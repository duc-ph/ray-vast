# Test plan

This plan tests the parts of [design.md](design.md) that are not proven yet. Read it from top to bottom. Each cycle has setup, steps, expected output, data to capture, and teardown. A person who reads only this file can run the tests.

## Test items

| # | Item | Cycle |
|---|---|---|
| T1 | `LD_PRELOAD` shim: bind and connect rewrite in all Ray processes. Hairpin NAT is not necessary. | C0 (local), C2 |
| T2 | Localhost bind: the dashboard-agent job port and the runtime-env agent port listen on 127.0.0.1 only, and the cluster still works. | C2 |
| T3 | A cluster with TLS and token auth operates correctly. | C2 |
| T4 | Plain HTTP hop: the token does not go over the network. | C2 |
| T5 | Token check: an agent port without a token gives 401. | C1, C2 |
| T6 | Secret push over SSH, with the image ENTRYPOINT overridden. | C2 |
| T7 | Worker loss: how long the head takes to mark a lost worker DEAD. (The full IP-change flow needs the provider. Test it after the provider exists.) | C3 |
| T8 | Image pull time against `inet_down` and the image size. | C2, C3 (all rentals) |
| T9 | Shim on the EC2 head (Docker, `--network host`, public IP advertised). | C0 (local), C1 |
| T10 | Worker watchdog and a vast key scoped to one instance. | C3 |
| T11 | Worker port exhaustion: a task fails with "No available ports" after 5 retries, and the raylet stays alive. Drivers use the same port pool. | C2 |
| T12 | Cert reload: does a running Ray node use a new cert file without a restart? (Replace `node.crt` on W-hp with a new cert from the same CA, then open a new connection to it.) This decides if a rolling renewal needs restarts. | deferred (certs are valid for 365 days) |
| T13 | Watchdog cannot be stopped by a task: it is PID 1 and runs as root, and user `ray` has no `sudo`. If it stops anyway, the container stops. | C4 |
| T14 | Heartbeat watchdog: a pause of 2 min gives no destroy; a stop of the heartbeat gives a self-destroy 5 min (± 30 s) after the last heartbeat, while the head GCS stays up. | C4 |

## Fixed values

### Machines

- **Head:** a **new** EC2 instance, only for these tests. Do not use the existing dev box (`50.112.112.27`) for anything except as the place where you run the AWS CLI if necessary.
  - Account and region: the personal AWS account, `us-west-2`.
  - Instance type: `t3.large` (2 vCPU, 8 GiB). The head has `CPU: 0` and runs only GCS, the dashboard and the agents.
  - AMI: the latest Ubuntu 24.04 LTS amd64. Find it with `describe-images` (owner 099720109477, name `ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*`, newest).
  - Disk: 40 GiB gp3 (the Ray image is 0.95 GB compressed and 3.5 GB unpacked, plus logs and pcaps).
  - Key pair: the existing personal key pair (`~/phduc-personal.pem`).
  - IMDSv2 required (`HttpTokens=required`).
  - Security group: a new group `rayvast-test-sg` (see the safety rules).
  - Tags: `Name=rayvast-test-head`, `rayvast-test=<YYYYMMDD>`.
  - Elastic IP: none. Use the public IP that EC2 gives. In this file, `HEAD_IP` means this public IP, and `HEAD_PRIVATE_IP` means the private IP from IMDS (`/latest/meta-data/local-ipv4`).
  - Cost: about $0.08/hour.
- **Workers:** vast datacenter offers. Use the cheapest offers that match this query:
  `datacenter=true verified=true rentable=true direct_port_count>=64 num_gpus=1 dph<=0.4 inet_down>=500 inet_up>=250`
  The network filter removes slow hosts, because a slow image pull makes each test cycle longer. On 2026-10-12 this query matched 11 offers on 11 different hosts. Without the price limit of $0.4, most cheap datacenter offers are on very few hosts.
  Show the chosen offers to the user (id, GPU, `dph_total`, `inet_down`, `inet_up`, location, `static_ip`), and get approval **before** you rent.

### Software

- Image: `rayproject/ray:2.59.0-py311-cpu` on all nodes. 2.59.0 is the latest stable Ray release (2026-10-02). Version 2.60 exists only as nightly builds. Check PyPI on the test day. If a newer stable release exists, use it and record the version.
  - Important for later versions: the Ray docs say that Ray 2.61 makes token auth the default for all clusters, including multi-node clusters. Our design already sets `RAY_AUTH_MODE=token` on every node, so this change does not affect it. But in 2.61, a node without the token cannot join. Run this test plan again for each new Ray version (see design.md). Record the image digest (`docker inspect --format '{{index .RepoDigests 0}}'`) and the compressed size (sum of the layer sizes from `docker manifest inspect -v`).
- Ray 2.59.0, Python 3.11. On each node, run `python -c "import ray,sys; print(ray.__version__, sys.version)"`. All nodes must print the same values.

### Ports

On the head (EC2, no port mapping):

| Use | Port | Bind |
|---|---|---|
| GCS | 6379 | 0.0.0.0 |
| Dashboard | 8265 | 127.0.0.1 (`--dashboard-host=127.0.0.1`) |
| Node manager | 6380 | 0.0.0.0 |
| Object manager | 6381 | 0.0.0.0 |
| Metrics export | 6382 | 0.0.0.0 |
| Dashboard-agent gRPC | 6383 | 0.0.0.0 |
| Dashboard-agent HTTP (job port) | 6384 | 127.0.0.1 (shim) |
| Runtime-env agent | 6385 | per test |
| Workers | 10002–10100 | 0.0.0.0 |

On a vast worker, request the identity ports 70000–70063 (`-p 70000:70000` ... `-p 70063:70063`). vast gives a host port P and maps P→P. The container sees P in `$VAST_TCP_PORT_700xx`. In this file, `P(700xx)` means the value of `$VAST_TCP_PORT_700xx`.

| Use | Request | Ray flag |
|---|---|---|
| Node manager | 70000 | `--node-manager-port=P(70000)` |
| Object manager | 70001 | `--object-manager-port=P(70001)` |
| Metrics export | 70002 | `--metrics-export-port=P(70002)` |
| Dashboard-agent gRPC | 70003 | `--dashboard-agent-grpc-port=P(70003)` |
| Dashboard-agent HTTP (job port) | 70004 | `--dashboard-agent-listen-port=P(70004)` |
| Runtime-env agent | 70005 | `--runtime-env-agent-port=P(70005)` |
| sshd | 70040 | — |
| Worker processes (57 ports) | 70006–70039, 70041–70063 | `--worker-port-list=<comma list>` |

### Shim

- Source: `shim/rayvast_shim.c`. It wraps `bind()` and `connect()` with `dlsym(RTLD_NEXT, ...)`. It handles only IPv4 (`AF_INET`).
- Build: in `quay.io/pypa/manylinux2014_x86_64` (glibc 2.17), run `gcc -shared -fPIC -O2 -o librayvast_shim.so rayvast_shim.c -ldl`.
- Environment variables:
  - `RAYVAST_PUBLIC_IP`: the IP that Ray advertises.
  - `RAYVAST_PRIVATE_IP`: the interface IP.
  - `RAYVAST_LOCALHOST_PORTS`: a comma list of ports that must listen on 127.0.0.1 only.
  - `RAYVAST_SHIM_LOG`: a file path. The shim writes one line for each rewrite: `time pid comm call old_addr:port -> new_addr:port`.
- Rules:
  1. `bind(addr, port)` with `port` in `LOCALHOST_PORTS` → `bind(127.0.0.1, port)`.
  2. `bind(PUBLIC_IP, port)` → `bind(PRIVATE_IP, port)`.
  3. `connect(PUBLIC_IP, port)` with `port` in `LOCALHOST_PORTS` → `connect(127.0.0.1, port)`.
  4. All other `connect(PUBLIC_IP, port)` → `connect(PRIVATE_IP, port)`. A node never needs to reach its own public IP through the NAT, so this rule needs no list of the ports that it listens on.
  5. All other calls go through without a change.
- Also (found in C0/C1, shim v3):
  - Rule 1 sets `SO_REUSEPORT`. The dashboard agent binds the job port twice in one process (to the node IP and to 127.0.0.1), so without it the second bind fails.
  - The shim handles `AF_INET6` sockets with `::ffff:a.b.c.d` addresses, because gRPC uses dual-stack sockets.
  - The shim copies its environment values at start, because Ray's setproctitle overwrites the environment memory.

### Cluster secrets

- On the head: make a new CA (`openssl req -x509 -newkey rsa:2048 -days 2 -nodes`) and a 32-byte random token (`openssl rand -hex 32`) for each test day. Keep them in `/opt/rayvast-test/secrets/` on the EC2 box, with mode 600.
- For each node: a key and a cert signed by the CA. SAN = `IP:<public IP>, IP:127.0.0.1`.
- Ray environment on every node:
  `RAY_USE_TLS=1 RAY_TLS_SERVER_CERT=... RAY_TLS_SERVER_KEY=... RAY_TLS_CA_CERT=... RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=...`

### Safety rules (all cycles)

- Give every vast instance the label `rayvast-test-<YYYYMMDD>`.
- Before each cycle, record the vast credit (`GET /api/v0/users/current` → `credit`). Stop the cycle if the spend for the day goes above **$8**.
- Teardown always ends with a **label sweep**: list the instances, destroy each one that has the test label, and list again until none are left. Retry on HTTP 429.
- The security group `rayvast-test-sg` starts with one inbound rule: `TCP 22 from <operator's public IP>/32`. Add each worker as `TCP 0-65535 from <worker public IP>/32` with the description `rayvast-test`. Remove each worker rule when the worker is destroyed.
- Do not change the existing dev box, its security group, or its services.
- The final teardown terminates the test EC2 instance and deletes `rayvast-test-sg`.

### Resource log

For each step that runs longer than 1 minute, record the duration, the CPU and RAM on the head, and the vast cost. Use one CSV file for each day: `results/<date>/resources.csv`.

---

## Cycle C0 — Local shim tests (no cost)

**Tests:** T1 (local part), T9 (local part).
**Machines:** the new test EC2 instance only.

### Setup

1. Create `rayvast-test-sg` in the default VPC of `us-west-2`, with the SSH rule only.
2. Launch the test EC2 instance (see "Machines"). Wait until the status checks pass. Record `HEAD_IP`, `HEAD_PRIVATE_IP` and the instance ID.
3. SSH to it as `ubuntu`. Install Docker from Ubuntu's `docker.io` package, and add `ubuntu` to the `docker` group. Install `tcpdump`, `jq` and `openssl`. Expected: `docker version` shows a server version.
4. Make the directory `/opt/rayvast-test/`.
5. Build the shim (see "Shim"). Expected: `librayvast_shim.so` exists, and `nm -D` shows `bind` and `connect`.
6. Pull the image. Record the pull time and the size (the T8 baseline from AWS).

### Steps

**C0.1 — Shim unit test with a fake public IP.**
Use `203.0.113.10` (TEST-NET-3, not on any interface) as `RAYVAST_PUBLIC_IP`, and use `127.0.0.1` as `RAYVAST_PRIVATE_IP`.
1. Without the shim, run `python -c "import socket; s=socket.socket(); s.bind(('203.0.113.10', 5000))"`.
   Expected: `OSError: [Errno 99] Cannot assign requested address`.
2. With `LD_PRELOAD`, run the same command, then `s.listen()`, and keep the socket open.
   Expected: no error. `ss -ltn` shows `127.0.0.1:5000`. The shim log has one `bind` line.
3. With the shim, start `python -m http.server 5000 --bind 203.0.113.10`. Then, with the shim, run `curl -s -o /dev/null -w '%{http_code}' http://203.0.113.10:5000/`. curl is C, so this tests the C path.
   Expected: `200`. The shim log has one `connect` line.
4. Put `5001` in `RAYVAST_LOCALHOST_PORTS`. With the shim, bind `0.0.0.0:5001`.
   Expected: `ss -ltn` shows `127.0.0.1:5001`, not `0.0.0.0:5001`.

**C0.2 — Ray single node with a fake public IP (T9 local).**
1. In the container (`--network host`, shim mounted at `/opt/rayvast/`), without the shim, run `ray start --head --node-ip-address=203.0.113.10 --port=6379 ...`.
   Expected: failure. Capture the error (Errno 99 in `runtime_env_agent.log` or `dashboard_agent.log`, and the raylet exits).
2. Run `ray stop --force`. Start again with `LD_PRELOAD` and the shim variables (`PRIVATE_IP` = `HEAD_PRIVATE_IP`).
   Expected: `ray status` shows 1 node. Capture `ps -eo pid,comm,args | grep -E 'raylet|gcs_server|agent'`. Check that `/proc/<pid>/maps` of each of these processes contains `librayvast_shim.so`. (Do not use `/proc/<pid>/environ`: setproctitle overwrites it.)
3. Run a small driver: 20 tasks, 1 actor with 10 calls, and 1 task with `runtime_env={"pip": ["emoji==2.12.1"]}`.
   Expected: all of them finish without errors.
4. Capture the shim log (count the lines for each `comm` and `call`) and `ss -ltnp`.

**Pass:** C0.1 steps 2–4 and C0.2 steps 2–3 pass. If C0.2 fails, stop here. The shim design must change before any rental.

### Teardown

`ray stop --force`, then stop the container. Keep the built shim and the image on the box.

---

## Cycle C1 — EC2 head with its real public IP

**Tests:** T9, T5 (head).
**Machines:** the test EC2 instance. No vast cost.

### Setup

1. Make the CA, the token, and the head cert (SAN `IP:<HEAD_IP>, IP:127.0.0.1`).
2. Write `head.env` with `LD_PRELOAD=/opt/rayvast/librayvast_shim.so` and the TLS, token and shim variables: `RAYVAST_PUBLIC_IP=<HEAD_IP>`, `RAYVAST_PRIVATE_IP=<HEAD_PRIVATE_IP>`, `RAYVAST_LOCALHOST_PORTS=6384`.

### Steps

1. Start the head container: `docker run -d --network host --name rayvast-head --env-file head.env -v /opt/rayvast-test:/opt/rayvast-test rayproject/ray:2.59.0-py311-cpu sleep infinity`.
2. In the container: `ray start --head --node-ip-address=<HEAD_IP> --port=6379 --dashboard-host=127.0.0.1 --num-cpus=0 --node-manager-port=6380 --object-manager-port=6381 --metrics-export-port=6382 --dashboard-agent-grpc-port=6383 --dashboard-agent-listen-port=6384 --runtime-env-agent-port=6385 --min-worker-port=10002 --max-worker-port=10100`.
   Expected: `ray status` shows 1 node and no CPU row.
3. Capture `ss -ltnp`. Expected: 6384 and 8265 on 127.0.0.1. The other Ray ports are on 0.0.0.0 or on the private IP. No process fails with Errno 99.
4. In the container: `curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:6384/api/jobs/`. Expected: `401`. This proves that token auth is on (T5 for the head).
5. Run the C0.2 step 3 driver on the head. The head has `num_cpus=0`, so for this check only, run the tasks and the actor with `num_cpus=0`.
   Expected: they finish. In the shim log, every `connect` line to `<HEAD_IP>` shows a rewrite to the private IP or to 127.0.0.1.

**Pass:** steps 2–5 pass. Keep the head running for C2.

---

## Cycle C2 — Head + 2 workers: shim, TLS, token, localhost, ports

**Tests:** T1, T2, T3, T4, T5, T6, T8, T11.
**Machines:** the C1 head + 2 vast workers:
- **W-hp:** a host where hairpin works.
- **W-nohp:** a host where hairpin does not work. This worker is the proof for T1.

### Setup — rent and check candidates

1. Search the offers (see "Machines"). Show the user the 6 cheapest. Get approval.
2. Rent 3 candidates. Use this create request for each:
   - `image`: `rayproject/ray:2.59.0-py311-cpu`
   - `label`: `rayvast-test-<date>`
   - `runtype`: `args`
   - `env`: the 64 `-p 700xx:700xx` mappings, plus `RAYVAST_HEAD_SSH_PUBKEY=<head public key>`. **No secrets.**
   - `args` (overrides the image command): a bash script that installs `openssh-server` if it is missing, writes `RAYVAST_HEAD_SSH_PUBKEY` to `/root/.ssh/authorized_keys`, starts `sshd -p $VAST_TCP_PORT_70040 -o PasswordAuthentication=no`, and then runs `sleep infinity`.
3. Poll each instance every 10 s. Record for T8: the creation time, the time when the status becomes `running`, the time when the SSH port answers, `inet_down` from the offer, and the image size. Apply the time limits: pull `2 × size / inet_down` (between 5 and 15 min), sshd 1 min. Destroy a candidate that does not meet a limit, and record why.
4. On each candidate, over SSH, run the host checks. Record each result:
   - **Identity ports:** in the container, start a listener on each P(700xx) except sshd. From the head, connect to `PUBLIC_IPADDR:P` for each port. Expected: 63/63 connect.
   - **Egress IP:** `curl -s https://api.ipify.org` equals `$PUBLIC_IPADDR`.
   - **Hairpin:** in the container, connect to `PUBLIC_IPADDR:P(70006)` while a listener is on it. Record pass or fail. (This is data only. It is not a reason to reject the host.)
   - **Capabilities:** `grep CapEff /proc/self/status`. Bit 12 (NET_ADMIN) and bit 13 (NET_RAW) must be 0.
   - **Own network:** `hostname -I` shows an RFC 1918 address that is not `PUBLIC_IPADDR`.
   - **/dev/shm:** `ls -A /dev/shm` is empty.
5. Choose W-hp and W-nohp. If none of the 3 fails hairpin, rent 3 more (with approval). If there is still no host without hairpin after 6 rentals, continue with W-hp only, and mark T1-nohp as "not tested". Destroy the other candidates at once.

### Steps

**C2.1 — Secret push and join (T6).**
1. On the head: for each worker, sign a cert with SAN `IP:<worker PUBLIC_IPADDR>, IP:127.0.0.1`. Add the security group rule for the worker IP.
2. Over SSH, copy these files to `/opt/rayvast/` on the worker: `ca.crt`, `node.crt`, `node.key`, `token`, `librayvast_shim.so`, `worker.env`. Use mode 600 for the secret files.
3. Over SSH, run `ray start --address=<HEAD_IP>:6379 --node-ip-address=$PUBLIC_IPADDR --num-cpus=8 <port flags from the table>`, with the TLS, token and shim environment. Set `RAYVAST_LOCALHOST_PORTS=P(70004)` (only the job port, for now).
4. Expected: `ray status` on the head shows 3 nodes. Capture `ray list nodes` (state, IP, resources).
5. Check that no secret is in the vast instance metadata: `GET /api/v0/instances/<id>/` must not contain the token or a key.

**C2.2 — Workload battery (T1, T3).** Run `battery.py` on the head as a driver. It does these things and prints a JSON result:
1. 200 tasks with `scheduling_strategy="SPREAD"`. Record which node ran each task. Expected: both workers ran tasks.
2. One actor on each worker (`NodeAffinitySchedulingStrategy`). Make 200 calls to each actor. Record the median and the p95 RTT.
3. One task on each worker with `runtime_env={"pip": ["emoji==2.12.1"]}`. Expected: it returns `emoji.__version__`.
4. Objects of 50 MB: head→worker, worker→head, W-hp→W-nohp, W-nohp→W-hp. Record the MB/s for each direction.
5. `ray list actors` and `ray list tasks --limit 10`. Expected: both work.
6. Through the head dashboard: `ray job submit --address http://127.0.0.1:8265 -- python -c "import ray; ray.init(); print(ray.cluster_resources())"`. Expected: `SUCCEEDED`.

**Pass for T1:** the battery passes on W-nohp. On both workers, the shim log has `connect` rewrites from `raylet` to the node's own public IP.
**Pass for T3:** the battery passes on both workers.

**C2.3 — Negative TLS and token checks (T3, T5).** Run these from the head host, outside the container, so that there are no certs and no token:
1. A plaintext gRPC connection to `W:P(70000)`. Expected: the connection is refused or the handshake fails.
2. A TLS connection with a cert from a second, unknown CA. Expected: the handshake fails.
3. `curl http://W:P(70005)/` (the runtime-env agent, still public). Expected: `401`. With a wrong token: `403`.
4. `curl http://W:P(70004)/api/jobs/`. Expected: the connection is refused (the port is on 127.0.0.1).
5. Over SSH on the worker (the image has no curl, so use Python): `python -c "import urllib.request as u; u.urlopen('http://127.0.0.1:P(70004)/api/jobs/')"`. Expected: `HTTPError 401`. **The provider will use this check for T5.**
6. `curl http://W:P(70002)/metrics`. Expected: `200` (a known gap; the data is read-only). Record it.

**C2.4 — Plain HTTP hop (T4).**
1. On the head host, while battery step 3 runs again with a new pip package (`humanize==4.10.0`), run `sudo tcpdump -i any -s0 'tcp port 6385 or tcp port 6384' -w head-hop.pcap`. Then search the capture for the token, and record the interface of each match.
   Expected: the token appears only on the loopback interface (`lo`), not on `ens5`.
2. tcpdump does not work on the workers (no NET_RAW). Use these instead:
   - The shim log: each `connect` from `raylet` to `PUBLIC_IP:P(70005)` has a rewrite to `PRIVATE_IP` or `127.0.0.1`.
   - `ss -tnp` during the pip install: the raylet's connection to the runtime-env agent has a private or loopback peer address, not `PUBLIC_IPADDR`.

**Pass:** no token on `ens5`, and no connection from a worker to its own public IP.

**C2.5 — Runtime-env agent on localhost (T2).** Do this in place, without a new rental:
1. On each worker: `ray stop --force`. Start again with `RAYVAST_LOCALHOST_PORTS=P(70004),P(70005)`.
2. Run the C2.2 battery again.
3. Expected: the battery passes. `ss -ltn` shows P(70005) on 127.0.0.1. From the head, `curl http://W:P(70005)/` is refused.
4. If the raylet stops or the head marks the node DEAD, capture `raylet.out` and `raylet.err`. Then T2 for the runtime-env port fails, and that port stays public with token auth.

**C2.6 — Port exhaustion (T11).** Do this in place, on W-hp only:
1. `ray stop --force`. Start again with `--num-cpus=8 --worker-port-list=<4 ports>` and `RAY_num_workers_soft_limit=4`.
2. Run 8 tasks, each with `num_cpus=1` and `time.sleep(60)`, pinned to W-hp.
   Expected: 4 tasks run. For the other 4, record whether they wait and then run, or fail. Capture the exception text (expected: "No available ports"), the time until the failure, and `raylet.out`. Check that the raylet stays alive (`ray list nodes` shows ALIVE).
3. Run 8 actors with `num_cpus=0`, pinned to W-hp. Record the same data.
4. While 4 actors hold all the ports, start a driver on W-hp (`python -c "import ray; ray.init(address='auto')"`). Record whether it fails. This checks that drivers use the same port pool.
5. Restore: `ray stop --force`, then start with all 57 ports.

### Teardown

Keep W-hp and the head for C3. Destroy W-nohp and remove its security group rule. Record the cost of C2.

---

## Cycle C3 — Worker loss and watchdog

**Tests:** T7 (Ray part), T10, T8.
**Machines:** the head + W-hp + 1 new cheap worker, **W-loss**. Rent W-loss as in C2, with approval.

### Steps

**C3.1 — Scoped vast key (T10).**
1. From the laptop (with the full key), create a key with `instance_write` limited to W-hp's instance ID only. Use the constraint format from the vast permissions docs (https://docs.vast.ai/cli/permissions).
2. With this key, try to destroy W-loss. Expected: HTTP 401 or 403, and W-loss still runs.
3. With this key, try `GET /api/v0/users/current` and an offer search. Expected: both are refused. Record the response codes.

**C3.2 — Worker loss detection (T7).**
1. Destroy W-loss with the full key. Record the time `t0`.
2. On the head, run `ray list nodes` every 5 s. Record when W-loss changes from ALIVE to DEAD (`t1`). Expected: `t1 - t0` is about the GCS health check time (about 30 s with the Ray defaults). Record the exact value.
3. Remove W-loss's security group rule.

**C3.3 — Watchdog (T10).**
1. Copy the scoped key and a watchdog script to W-hp. Every 15 s, the script tries a TCP connection to `<HEAD_IP>:6379`. After **3 min** without a connection (shorter than in production, for the test), it destroys its own instance with the scoped key.
2. Start the watchdog. Wait 5 min. Expected: W-hp still runs, because the head is up.
3. On the head: `ray stop --force`, and stop the head container. Record the time `t0`.
4. Get the vast instance list every 15 s. Expected: W-hp is destroyed 3–4 min after `t0`. Record the time.

### Teardown (final)

1. Label sweep (see the safety rules). Expected: 0 instances with the test label.
2. Remove all security group rules with the description `rayvast-test`.
3. Delete the scoped vast key.
4. Copy `/opt/rayvast-test/results/`, the shim source and the shim build to the laptop (`results/<date>/` in this repo). Do not copy `secrets/`.
5. Terminate the test EC2 instance. Wait until its state is `terminated`.
6. Delete `rayvast-test-sg`.
7. Check that nothing is left: no instance with the tag `rayvast-test`, no group `rayvast-test-sg`, and no vast instance with the test label.
8. Record the total vast spend from the difference in credit, and the EC2 hours.

---

## Cycle C4 — Watchdog hardening and heartbeat

**Tests:** T13, T14, T8 (record the pull times again).
**Machines:** a new test EC2 head (as in C0 setup steps 1–4, and C1 setup and steps 1–2) + 2 vast workers, **W-kill** (T13) and **W-hb** (T14). Use the worker query from "Machines". The user approved the AWS calls for this cycle (security group, instance, worker rules, teardown).
**Budget:** less than $1 on vast.

### The watchdog (write it before the rentals)

`tests/manual/watchdog.py`, for the system Python 3 of the image. It does only these things:
1. Every 15 s, read the modification time of `/opt/rayvast/heartbeat`.
2. **Grace:** if the file does not exist 10 min after the watchdog started, destroy the instance. (This also removes a candidate that the provider forgot.)
3. If the file is older than **5 min**, destroy the instance.
4. Destroy = `DELETE https://console.vast.ai/api/v0/instances/$CONTAINER_ID/` with `Authorization: Bearer $CONTAINER_API_KEY`. Retry every 10 s for 2 min if the API does not answer. Never log the key.
5. On any unexpected exception, destroy the instance, then exit.
6. Write one line for each check and each decision to `/opt/rayvast/watchdog.log` and to stdout.

### Start script changes (the `args` of the create request)

- Start sshd as root, as in C2. Also allow the head key for `root` (`PermitRootLogin prohibit-password`). This is for the test only, so that the test can kill the watchdog as root.
- End with `exec sudo -E /usr/bin/python3 /opt/rayvast/watchdog.py`, with the watchdog source written by the script itself (it is not a secret). PID 1 is then `sudo`, which runs as root, and its child is the watchdog. Record `ps -o pid,ppid,user,args` for PID 1 and the watchdog.

### Head heartbeat loop (in place of the provider)

`tests/manual/heartbeat.sh` on the head: every 30 s, for each worker, `ssh ray@W touch /opt/rayvast/heartbeat`, then copy `/opt/rayvast/watchdog.log` back to `results/<date>/c4/<worker>/`. Log each attempt with the time and the exit code.

### Setup

1. Make the head (EC2, security group, Docker, shim v3 from `results/2026-10-12/shim_builds/`, new CA and token, Ray head container). Record `HEAD_IP`.
2. Rent W-kill and W-hb (cheapest offers on 2 different public IPs). Run the host checks, add the security group rules, do the secret push and `ray start` as in C2.1. Then, on each worker, remove passwordless `sudo` for user `ray` (`sudo rm` the matching file in `/etc/sudoers.d/`, or edit `/etc/sudoers` with `visudo -c` after). Check: as `ray`, `sudo -n true` fails.
3. Start the heartbeat loop. Expected: the watchdog log on both workers shows a fresh heartbeat.

### Steps

**C4.1 — Watchdog cannot be stopped by a task (T13, W-kill).**
1. As user `ray` over SSH: `kill -9 <watchdog pid>` and `kill -9 1`. Expected: `Operation not permitted` for both.
2. As user `ray`: `sudo -n kill -9 <watchdog pid>`. Expected: refused ("a password is required").
3. From a Ray task pinned to W-kill: `os.kill(<watchdog pid>, 9)` and `os.kill(1, 9)`. Expected: `PermissionError` for both. The watchdog is still running.
4. As root (SSH as `root`): `kill -9 <watchdog pid>`. Expected: PID 1 (`sudo`) exits, so the container stops. Poll the vast API every 10 s. Record the status (`exited` or `stopped`) and the time. Record whether vast still reports a GPU cost for the instance (`dph_total`) or only a storage cost.
5. Destroy W-kill with the full key from the laptop. Remove its security group rule.

**C4.2 — Heartbeat watchdog (T14, W-hb).**
1. Let the heartbeat run for 3 min. Expected: no destroy. The log shows heartbeat ages below 45 s.
2. **Short pause:** stop the heartbeat loop for 2 min, then start it again. Expected: no destroy. The largest heartbeat age in the log is about 2.5 min.
3. **Provider stop:** stop the heartbeat loop for good. The Ray head (GCS) stays up. Record `t0` = the time of the last successful heartbeat.
4. Poll the vast API every 15 s. Expected: W-hb is destroyed between `t0 + 5 min` and `t0 + 5.5 min`. Record the time.
5. Try to get the container log from the vast API (`request_logs`) after the destroy. Record whether the watchdog's last lines (stdout) are still available.
6. Remove W-hb's security group rule.

### Teardown

As in "Teardown (final)", plus: check that no instance with the C4 label remains, and that `ray` had no `sudo` on the destroyed workers (from the logs).

---

## Results

After the run, fill in one row for each test. Keep the raw logs in `results/<date>/`.

| # | Result (pass / fail / not tested) | Key numbers | Notes and links to logs |
|---|---|---|---|
| T1 | | rewrites per process; battery on W-nohp | |
| T2 | job port: / runtime-env port: | | |
| T3 | | actor RTT median/p95; MB/s per direction | |
| T4 | | token matches on `ens5`: | |
| T5 | | HTTP codes | |
| T6 | | secrets in metadata: yes/no | |
| T7 | | ALIVE→DEAD seconds | |
| T8 | | pull time, size, `inet_down` per host | |
| T9 | | | |
| T10 | | refused calls; self-destroy delay | |
| T11 | | behavior for tasks, actors, driver | |
| T13 | | refused kills; container status after root kill | |
| T14 | | max age in the pause; destroy delay after the stop | |
