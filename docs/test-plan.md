# Test plan

Open tests for [design.md](design.md). The cycles C0–C4 ran on 2026-10-12/13. Their setup, results and findings are in [test-results.md](test-results.md). The raw data and the plan that ran are in `archive/results/`. C2–C4 used SSH only as a test tool. The design does not use SSH.

Before a cycle runs, write it here with the same detail as the archived plan: setup, steps, expected output, data to capture, and teardown.

The cycles C5–C8 are the paid live tests of the real package (plan step 12, [implementation-plan.md](../ray-vast-pypi/docs/implementation-plan.md) part 4). They use the real CLI: `ray-vast up` makes the head, `ray-vast down` removes it. The old manual SSH setup is not used.

## Proven (C0–C4)

- The shim makes Ray work across NAT, also on hosts without hairpin NAT, on the head and on workers.
- TLS + token: plaintext, a missing cert and an unknown CA are refused; HTTP without a token gives 401.
- Both agent HTTP ports can listen on 127.0.0.1 only. The token never goes over the network.
- `CONTAINER_API_KEY` can destroy its own instance, and cannot read or destroy other instances.
- A lost worker is DEAD on the head after about 41 s.
- Port exhaustion: tasks wait, actors fail after about 6 min, drivers fail at once.
- When PID 1 exits, vast starts the container again (the instance keeps its cost).
- Heartbeat watchdog (C4, test version over SSH): heartbeat age stayed at 40 s or less for 13 min. A pause of 2 min 18 s gave no destroy. After the last heartbeat, the instance destroyed itself after 5 min 11 s.
- After a destroy, vast keeps no container log. The watchdog must send its destroy reason to the head first.
- From inside the container, `kill -9 1` gives no error, but the kernel ignores it.

## Open tests

| # | Item | Cycle |
|---|---|---|
| T15 | **Bootloader on different images.** The bootloader finds the Python with Ray, downloads the bootstrap, checks its SHA-256, and `exec`s it as PID 1. Use 3 images: `rayproject/ray`, a slim image with Ray that runs as non-root, and an image with an ENTRYPOINT (expect a clear failure). This also shows that vast accepts the bootloader in `args`. | C5 |
| T16 | **Enrollment security.** Refuse: another instance's `CONTAINER_API_KEY`, a second enrollment of the same instance (also after a container restart), a request from a different IP, and an instance that is not a candidate. Also check whether vast's instance list shows the `instance_api_key` of other instances. | C5 |
| T13 | **Watchdog as PID 1.** Ray runs as the image's user (same user as PID 1). `kill -TERM/-INT/-HUP 1` from a Ray task gives a destroy, never an exit. `kill -9 1` has no effect. A task's `os.environ` has no `CONTAINER_API_KEY`. Test on a root image and on a non-root image. The watchdog collects its finished child processes. | C6 |
| T14 | **Heartbeat to the head.** Stop the head's heartbeat endpoint: destroy after 5 min (± 30 s). Stop it for 2 min only: no destroy. No enrollment within 10 min: destroy. The head gets the destroy reason before the destroy. (C4 proved the timing with a test watchdog; repeat with the real bootstrap.) | C6 |
| T7 | **IP change, full flow:** the provider destroys the worker, removes its rule (only if no other worker uses the IP), and the autoscaler gets a new worker. | C7 |
| L6 | **Full scale test:** 2 node types, a job that needs 4 GPU workers, k=2, scale up, idle scale-down, `ray job submit` from the laptop, `ray-vast down` leaves nothing. | C8 |
| T12 | Cert reload without a restart. | Deferred (certs are valid for 365 days) |

C5 also proves three things about the real vast API: the field names `public_ipaddr` and `ports` of an instance, the values of `actual_status` that the provider treats as pending or failed, and the `CapBnd` host check on real hosts.

## Common setup for C5–C8

### Fixed values

- **Operator machine:** the laptop. It needs a venv with `ray[default]==2.59.0`, `build`, `boto3`, and this package (`pip install -e ray-vast-pypi`). The `ray` CLI of the venv must be 2.59.0, the same as the image. Docker is not needed on the laptop. Build the C5 test images on the dev box (`ray-vast-dev`), which has Docker.
- **AWS:** profile `personal`, region `us-west-2`, EC2 key pair `phduc-personal`, key file `~/phduc-personal.pem`. The head is `t3.large` (the default, about $0.083/h; the CLI sets a 60 GiB disk). Never touch the badminton box (50.112.112.27), the dev box's services, or anything with the name `badminton`.
- **Shell variables** (set them in each shell):
  ```
  OPS="--profile personal --key-file $HOME/phduc-personal.pem"   # up, attach, dashboard
  export VAST_API_KEY=$(cat ~/.config/vastai/vast_api_key)       # never echo it
  DATE=$(date -u +%F); D8=$(date -u +%Y%m%d)
  ```
  `up` also takes `--key-name phduc-personal`. `down` takes only `--profile personal`.
- **Image:** `rayproject/ray:2.59.0-py311-cpu` for C5–C7. C8 uses `rayproject/ray:2.59.0-py311-gpu`, because C8 needs GPU workers. Record `ray --version` on the head and the image digest.
- **Day:** `DATE` is the UTC day of the run. All data goes to `archive/results/$DATE/c5/`, `c6/`, `c7/`, `c8/`, plus `resources.csv` and `spend.md` in `archive/results/$DATE/`.
- **Names.** The cluster of a cycle is `rvtest-c5`, `rvtest-c6`, `rvtest-c7` or `rvtest-c8`. The provider labels its instances `ray-vast:<cluster>`. The test harness (below) runs a second provider with the cluster names `rvtest-c5h`, `rvtest-c5t` and `rvtest-c6h`. Instances that the harness rents without a provider have the label `ray-vast-test-<D8>`.
- **Paths on the head.** WORK is `/home/ubuntu/ray-vast-<cluster>`. It holds `cluster.yaml` and `secrets/`. The setup adds `h/` (harness scripts) and `results/`. The container `ray-vast-head` mounts WORK at the same path. The container user `ray` and the host user `ubuntu` both have uid 1000, so the container can write `WORK/results/`.
- **Log of the provider.** The provider and the head server log through Python logging into the Ray monitor log in the container. Find the file once: `sudo docker exec ray-vast-head sh -c 'ls /tmp/ray/session_latest/logs | grep -i monitor'` (the README says: confirm this path on a live head). The log lines that this plan needs are at level WARNING: `enroll refused for instance`, `unverified report from`, `destroying instance`, `ending instance`, `worker N asks to be destroyed`.

### Scripts to write before the first rental

All are in `tests/manual/`. Commit them before the run. They hold no secrets.

| Script | Where it runs | What it does |
|---|---|---|
| `vast_tool.py` | laptop | Subcommands. `credit`: print the vast credit. `count`: count the instances whose label is `ray-vast-test-*` or starts with `ray-vast:rvtest-`. `sweep`: destroy those instances, list again until 0, retry on HTTP 429, print only counts. `poll <label> <period>`: print one JSON line per sample with `id`, `actual_status`, `intended_status`, `status_msg`, `public_ipaddr`, `dph_total`, `label` for each instance with the label. `logs <id>`: ask `request_logs` for the container log and print it. It never prints the key. It never touches an instance with another label. |
| `scan_secrets.sh DIR` | laptop | Print the names of files (never the text) under DIR that hold: `PRIVATE KEY`, the vast key, the cluster token (read from a mode-600 temp file), `"instance_api_key": "` followed by 8 or more characters, `CONTAINER_API_KEY=` followed by a value, or `Bearer ` followed by 16 or more characters. Expected: no output. |
| `resources.sh CYCLE STEP` | laptop | Append one line to `resources.csv`: UTC time, cycle, step, CPU % and memory of `ray-vast-head` (`docker stats --no-stream` over SSH), the vast instances with the cycle label and the sum of their `dph_total`. |
| `c5_offers.py` | laptop | Offer search (see below). |
| `harness_head.py` | head container | A module for the cycle scripts. It makes a second provider H: `cfg` is the provider section of `WORK/cluster.yaml` with `port_a`, `port_b`, `image`, `candidate_factor` and `max_dollars_per_hour` replaced; `H = VastNodeProvider(cfg, "<harness cluster>", start_threads=False)`; `H.head_server.start()`. It runs `H.reconcile_once()` every 10 s in a thread that has a `pause` switch. It wraps `H.head_server.enroll` and `worker_request` to write one JSON line per call (time, route, instance ID, HTTP status; never the body), and to answer 503 on demand: `hold_enroll(id)` for `/enroll`, `fail_heartbeat(id)` for `/heartbeat`. `hold(id)` also moves `running_at` of that candidate one hour into the future, so the 60 s enrollment limit of the provider does not apply. It wraps `H._vast.destroy_instance` to log each call. It has `fetch_logs(id)`, `ray_nodes()` (a driver: `ray.init(address="auto")`, `ray.nodes()`) and `emit(kind, **fields)`, which appends one JSON line to `WORK/results/<script>.jsonl`. **Why H:** the real provider in the Ray monitor cannot be paused or held, and C5–C7 need that control. H is the real `VastNodeProvider` and the real `HeadServer` on other ports (8443/8444 and 8453/8454), with the same CA, token and secrets. The main cluster of C5 and C6 has `max_workers: 0`, so its autoscaler does not rent. Its `idle_timeout_minutes` is long, so it does not drain the test workers that join the Ray cluster. |
| `c5_images/Dockerfile`, `c5_probe.py`, `c5_t15.py`, `c5_t16.py` | dev box; probe: in a rented container; the others: head container | C5. |
| `c6_signal_task.py`, `c6_run.py` | head container | C6. |
| `c7_vast_proxy.py` | head | C7. |
| `c8_job.py`, `c8_poll.py` | job: on the head through `ray job submit`; poller: laptop | C8. |

### Offer search (all cycles)

`c5_offers.py` (laptop; the offer search needs no key) builds the query with `ray_vast.config.build_offer_query` from the node type of the cycle. So it uses the same filters as the provider: `datacenter`, `verified`, `rentable`, `direct_port_count >= 64`, `cpu_cores_effective >= CPU`, `num_gpus`. It prints the 12 cheapest offers: `id`, `machine_id`, `host_id`, `public_ipaddr`, `geolocation`, `gpu_name`, `dph_total`, `inet_down`, `inet_up`, `static_ip`.

- For C5–C7, try `num_gpus = 0` first (a CPU-only host is enough). If vast returns no such offer, use `num_gpus = 1` with `dph_total <= 0.4` and `inet_down >= 500` (the C2 query). Record which one you used. Set `GPU` in the YAML to match.
- Choose the cheapest offers on different `machine_id`s and, where the cycle says so, on different `public_ipaddr`s.
- Save the search output to `<cycle dir>/offers.json`.

### Rules for every cycle

1. **Approval.** Show the user the EC2 type, the offers (id, GPU, `dph_total`, `inet_down`, location, `static_ip`), the budget and the `max_dollars_per_hour` value. Wait for approval before the first rental.
2. **Credit.** Run `vast_tool.py credit` before and after the cycle. Write both values and the day total into `spend.md`. If the spend of the UTC day is above **$8**, stop and run the teardown.
3. **Budget.** Each cycle has a vast budget (below) and a `max_dollars_per_hour` in its YAML. The provider does not rent above `max_dollars_per_hour`. If the cycle spend goes above the cycle budget, stop and run the teardown.
4. **Version record.** Write to `<cycle dir>/versions.txt`: `git rev-parse HEAD` and `git status --short`, SHA-256 of `worker/bootstrap.py`, `worker/bootloader.sh` and the shim, `ray --version` on the head and on the laptop, and the image digest.
5. **Resource log.** Run `resources.sh` at the end of each step that takes more than 1 minute.
6. **Logs from workers.** `request_logs` returns nothing after a destroy (C4). Fetch the log of each worker with `vast_tool.py logs <id>` before its destroy. The cycle scripts do this at the steps that say so.
7. **Cost of one day.** C5 + C6 + C7 cost about $2.5 together. Run C8 on the same day only if the spend so far is below $4.

### Teardown (all cycles, in this order)

1. Stop the harness script. It ends itself; if not, `sudo docker exec ray-vast-head pkill -f <script name>`.
2. Copy the results from the head: `scp -r ubuntu@$HEAD_IP:WORK/results/ <cycle dir>/head/`. Copy the monitor logs and `sudo docker logs ray-vast-head` into `<cycle dir>/head/`. Never copy `WORK/secrets/`. Never copy a capture of the loopback interface.
3. Copy the cluster token to a mode-600 temp file in the scratch directory (`~/.ray-vast/<cluster>/token` exists after `ray-vast dashboard`; otherwise read `WORK/secrets/token` over SSH). The scan needs it. `down` deletes `~/.ray-vast/<cluster>/`.
4. Run `scan_secrets.sh <cycle dir>`. Expected: no output. If a file is listed, remove the match or the file before any commit. Then delete the temp token file.
5. `vast_tool.py sweep`. Expected: `0 left`.
6. `ray-vast down <yaml> --profile personal`. Expected output: `terminating head i-…`, `deleted security group sg-…`, `deleted instance profile ray-vast-<cluster>`, `deleted role ray-vast-<cluster>`, `left: nothing`; exit code 0. If `left:` lists something, run `down` again.
7. Independent check (not the CLI):
   - `aws ec2 describe-instances --profile personal --region us-west-2 --filters Name=tag:ray-vast-cluster,Values=<cluster> Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down --query 'Reservations[].Instances[].InstanceId'` gives `[]`.
   - `aws ec2 describe-security-groups --filters Name=group-name,Values=ray-vast-<cluster>` gives `[]`. `aws ec2 describe-volumes --filters Name=tag:ray-vast-cluster,Values=<cluster>` gives `[]`.
   - `aws iam get-role --role-name ray-vast-<cluster>` and `aws iam get-instance-profile --instance-profile-name ray-vast-<cluster>` both give `NoSuchEntity`.
   - `vast_tool.py count` gives `0`.
   - If you added your own SSH rule to the group (laptop IP change), it went with the group.
8. Write `spend.md` (credit before and after, EC2 hours). Fill the rows in the Results table at the end of this file. Add the findings to `test-results.md`.
9. Commit the scripts and the cleaned results to `main` and push. Do not commit before step 4 is clean.

If two cycles run on one day with the same configuration (C5 and C6), you can keep the head. Skip `down` and `up` between them and run teardown steps 1–5 only. Run `down` after the last cycle. A new harness provider removes the old rules of its own cluster name when it starts.

---

## Cycle C5: bootloader on real images, enrollment refusals, real vast fields (T15, T16)

**Tests:** T15, T16, the vast field names, the `actual_status` values, the `CapBnd` host check.
**Machines:** one EC2 head (`t3.large`) from `ray-vast up`, and 6 rentals: 4 workers for T15 (one of them, E, is also used for the restart test of T16) and 2 probes for T16.
**Budget:** vast $0.75; EC2 about $0.20.
**Duration:** about 2.5 h, of which `up` and `down` are about 25 min.

### Setup

1. Approval and credit (rules 1 and 2).
2. **Test images** (on the dev box). Push them to `ttl.sh`, an anonymous public registry that deletes an image after the time in its tag. It needs no account. Use a random name.
   ```
   # tests/manual/c5_images/Dockerfile
   FROM python:3.11-slim AS slim-root
   RUN pip install --no-cache-dir "ray[default]==2.59.0"
   FROM slim-root AS slim-user
   RUN useradd -m -u 1000 app
   USER app
   FROM slim-root AS entrypoint
   ENTRYPOINT ["python3"]
   ```
   ```
   R=rvtest-$(openssl rand -hex 6)
   for t in slim-root slim-user entrypoint; do
     docker build --target $t -t ttl.sh/$R-$t:6h tests/manual/c5_images && docker push ttl.sh/$R-$t:6h
   done
   ```
   `python:3.11-slim` has `sh` and `python3` in `PATH`, and no `curl`, no sshd and no `sudo`. Check each image: `docker run --rm --entrypoint sh <image> -c 'id -u; python3 -c "import ray; print(ray.__version__)"'`. Expected: uid `0` for `slim-root` and `entrypoint`, `1000` for `slim-user`, and `2.59.0` for all. Write the names, digests and sizes (`docker image inspect --format '{{.Size}}'`) to `c5/images.txt`. The tag lives 6 h; push again if the cycle runs longer. If vast cannot pull from `ttl.sh`, use a public Docker Hub repository of the user instead (open question).
3. Check the size of the bootloader: `wc -c ray-vast-pypi/src/ray_vast/worker/bootloader.sh`. It is 1456 bytes on 2026-10-13. The implementation plan says "less than 1 KB". T15 shows that vast accepts 1456 bytes in `args`. Record the size.
4. Offer search (see "Offer search") for the node type `w` with `resources {CPU: 4, GPU: 0}`. Choose 6 offers on 6 different `machine_id`s. The two probes (M and V) must have **different public IPs** (check `public_ipaddr` of the offers).
5. Write `tests/manual/c5/cluster.yaml`:
   ```yaml
   cluster_name: rvtest-c5
   max_workers: 0
   idle_timeout_minutes: 60
   provider:
     region: us-west-2
     image: rayproject/ray:2.59.0-py311-cpu
     max_dollars_per_hour: 2.0
   auth: {ssh_user: ray}
   head_node_type: head
   available_node_types:
     head: {resources: {CPU: 0}, node_config: {}, max_workers: 0}
     w:
       resources: {CPU: 4, GPU: 0}      # GPU: 1 if the offer search found no CPU-only offer
       min_workers: 0
       max_workers: 0
       node_config: {disk_gb: 30, offer_query: {inet_down: {gte: 500}}}
   ```
   `max_workers: 0` keeps the main autoscaler from renting. The long `idle_timeout_minutes` keeps it from draining the test workers that H adds to the Ray cluster.
6. `ray-vast up tests/manual/c5/cluster.yaml $OPS --key-name phduc-personal`. Expected: `head instance i-… starting`, `head IP <HEAD_IP>`, and after 8–12 min `cluster rvtest-c5 is up. Head IP <HEAD_IP>.` with exit code 0. Record the duration.
7. Check the head: `HEAD_IP=$(jq -r .head_ip ~/.ray-vast/rvtest-c5.json)`; `ssh -i ~/phduc-personal.pem ubuntu@$HEAD_IP 'sudo docker exec ray-vast-head ray status'`. Expected: 1 active node (the head) and no error. If SSH times out, the laptop IP changed. Add its `/32` for TCP 22 to the group `ray-vast-rvtest-c5` (`aws ec2 authorize-security-group-ingress`).
8. Make `WORK/h/` and `WORK/results/` on the head. Copy `harness_head.py`, `c5_probe.py`, `c5_t15.py` and `c5_t16.py` to `WORK/h/`. Write `versions.txt`.

### Steps

**C5.1 — T15: bootloader on 4 images, enrolled by the real head server.** Run `c5_t15.py` in the head container: `ssh … 'sudo docker exec -d ray-vast-head sh -c "python WORK/h/c5_t15.py > WORK/results/c5_t15.out 2>&1"'`. The script:
1. Makes H (cluster `rvtest-c5h`, ports 8443/8444, `candidate_factor 1`) with reconcile on.
2. For each image, in this order, sets `H._cfg["image"]` and `H._cfg["image_size_gb"]` (from `images.txt`) and calls `H.create_node_with_resources_and_labels(...)` with `count=1`. The images: (a) `rayproject/ray:2.59.0-py311-cpu` (this instance is E), (b) `slim-user`, (c) `slim-root`, (d) `entrypoint`. The create call is the real code path: it builds the env, `args=["sh","-c",<bootloader.sh>]` and the label `ray-vast:rvtest-c5h`.
3. Every 5 s writes the vast status of each instance (all `actual_status` values and `status_msg`) and the H events (enroll calls, heartbeats).
4. When (a), (b) and (c) are enrolled: for each one, a Ray task pinned to its node (`NodeAffinitySchedulingStrategy`) returns `os.getuid()`, `sys.executable`, `sys.version`, `ray.__version__`, and whether `/proc/1/cmdline` holds `bootstrap`. The script also fetches the container log with `fetch_logs`.
5. Waits until (d) is gone, or 180 s after (d) was `running`, and fetches the log of (d) while it exists.

Expected output:
- (a), (b), (c): in the H events, `/enroll` 200, then `/heartbeat` 200 about every 30 s. In `ray_nodes()`, the node is ALIVE with `RAY_NODE_TYPE_NAME=w`. The container log has `bootstrap: host checks: {'egress_ip_ok': True, 'no_net_raw_admin': True, 'own_netns': True, 'shm_empty': True, …}` and `bootstrap: enrolled as slot slot-1-… started ray`. The task returns uid 1000 for (a) and (b), uid 0 for (c), and Ray 2.59.0. The time from create to enrolled is below the pull limit.
- (d): vast shows `exited`, or a flip between `running` and `exited` (as in C4). The container log has an error of the form `python3: can't open file '/sh'`. The instance never enrolls. H ends it with `instance status 'exited'` or `not enrolled 60 s after running`. It is gone from vast within 90 s of the first `running`. The head log has `ending instance … (machine …)` with the reason.

**C5.2 — Real vast fields.** Also in `c5_t15.py`, once each instance is `running`: call `GET /instances/` and `GET /instances/<id>/` with the account key and write `c5/instance_fields.json`. For each test instance keep only these fields: `id, label, actual_status, cur_state, intended_status, status_msg, public_ipaddr, machine_id, host_id, dph_total, ports, geolocation, gpu_name, num_gpus, cpu_cores_effective, start_date`. Drop every other field (the full record holds `instance_api_key`, `jupyter_token` and `extra_env`). Then check:
- The list entry and the single-instance record both have `public_ipaddr` (an IPv4 string), `actual_status`, `status_msg` and `label`. The provider reads exactly these names in `reconcile_once`.
- `ports` is a mapping. For every identity port `70000+i`, `ports["<port>/tcp"][0]["HostPort"]` equals `VAST_TCP_PORT_<port>` in the container (the T16 probe prints them) and equals the port itself. Record the real shape. (The provider does not read `ports`. The bootstrap reads `VAST_TCP_PORT_*`.)
- `public_ipaddr` equals the container's `PUBLIC_IPADDR` and its egress IP.
- The set of all `actual_status` values seen from create to running, and for (d) until it is gone. The provider treats `created`, `loading`, `running` and `scheduling` as pending, and `exited`, `error`, `failed` and `offline` as failed. List each value that is in neither set (the provider logs it once as `unknown vast actual_status`).
- The `status_msg` text for a good start and for (d).

**C5.3 — T16: refusals on real instances.** Run `c5_t16.py` in the head container. It makes H2 (cluster `rvtest-c5t`, ports 8453/8454, reconcile **off**) and rents two probe instances with `VastClient.create_instance`. This is the same create code as the provider. The label is `ray-vast-test-<D8>`, the image is `rayproject/ray:2.59.0-py311-cpu`, and `args=["sh","-c",'exec python3 -c "$RVPROBE_SRC"']`. The probe source is in the env value `RVPROBE_SRC` (public). The env also holds `RVPROBE_HEAD_IP`, `RVPROBE_PORT_A=8453`, and for M `RVPROBE_TARGET_ID=<V id>`. V runs in mode `info`. M runs in mode `attack`. The probe `c5_probe.py` prints one line for each fact, with the prefix `RVPROBE`, and never prints a key:
- `RVPROBE_INFO`: uid and gid, the `Cap*` lines of `/proc/self/status`, the network namespace link, the number of entries in `/dev/shm`, `PUBLIC_IPADDR`, `CONTAINER_ID`, the egress IP from `api.ipify.org` and from `ifconfig.me/ip`, the 64 `VAST_TCP_PORT_*` values, the environment variable **names** (sorted, without values), and `/proc/1/cmdline`.
- `RVPROBE_VASTLIST` (M only), with M's own `CONTAINER_API_KEY`: the HTTP status of `GET /instances/`; the number of instances in it; whether any entry that is not M's own has the key `instance_api_key` (yes or no only); whether M's own entry has it; the status of `GET /instances/<V>/`; the status of `GET /instances/<M>/` (control). After the enrollment attacks: the status of `DELETE /instances/<V>/`.
- `RVPROBE_ENROLL` (M only): every 5 s, four requests to `https://<HEAD_IP>:8453/enroll` (an unverified TLS context is fine for the attacker). A = (V's ID, M's key). B = (V's ID, empty key). C = (M's own ID, M's key). D = (ID 999999999, M's key). Each line has the time, the case, the HTTP status and the `reason`. The body has valid `checks`, `ports` (M's real ports), `cpu_count` and `gpu_count`.

The script adds the security group rule for M's IP in the H2 group (`H2._sg.allow_ip(M_ip)`), and changes the state of H2 in phases of 40 s. It writes each change with its time:

| Phase | State of H2 | Expected for A (V's ID, M's key) | B, C, D |
|---|---|---|---|
| P0 | V is not a candidate | 403 `not a candidate` | B: 403 `empty container key`; C, D: 403 `not a candidate` |
| P1 | V is a candidate with its real public IP | 403 `source IP does not match the instance public IP` | as P0 |
| P2 | V's `public_ip` is set to M's IP (the same-NAT co-tenant case) | 403 `container key is not valid for this instance` (vast answers 401, 403 or 404 to M's key for V) | as P0 |
| P3 | V is also in `enrolled` | 403 `already enrolled` | as P0 |
| P4 | V is removed from `enrolled`, and its `public_ip` is `None` | 503 `the head has not seen this instance running yet, retry` | as P0 |

V is a real instance that sleeps. It is never a worker.

Pass criteria for C5.3:
- All the statuses and reasons in the table appear, in the phase in which they belong.
- The wrapper log of `H2._vast.destroy_instance` has 0 calls for V or M during the phases. V is still `running` at the end. (A bad key must not destroy a candidate.)
- `GET /instances/<M>/` with M's key gives 200. `GET /instances/<V>/` with M's key gives 401, 403 or 404, never 200. `DELETE /instances/<V>/` with M's key gives 401, 403 or 404, and V stays.
- **Instance list.** Record whether `GET /instances/` with M's key gives 200, how many instances it lists, and whether other entries have `instance_api_key`. If they do, mark T16 as **FAIL (design impact)**: a worker could take the key of another worker. Stop, and tell the user before any other cycle. If vast refuses the call for a container key, record that (better).
- **CapBnd.** From the `Cap*` lines of both probes and of the T15 logs: `parse_capbnd` (run on the laptop on the raw text) has bits 12 and 13 clear on every host. The root `CapEff` on C2 hosts was `0xa80405fb`. For a non-root image, `CapEff` is `0` and `CapBnd` is not `0`; this is why the check reads `CapBnd`. The `no_net_raw_admin` value in the bootstrap logs is `True` on every host. No vast host has NET_RAW or NET_ADMIN, so the negative case stays a unit test.

**C5.4 — T16: second enrollment after a container restart (E from C5.1).** This runs in `c5_t15.py` after step 4, with E still enrolled:
1. Pause the reconcile thread of H (so the provider does not end E while it restarts).
2. Restart the container of E with the vast API: `PUT /instances/reboot/<E>/` (the call behind `vastai reboot instance`; check the path in the vast docs at setup). If it fails, use `PUT /instances/<E>/` with `{"state":"stopped"}` and then `{"state":"running"}`. Write the time.
3. Watch the H events and the vast status for 4 min.

Expected: vast shows `exited` or `loading`, then `running` again. The bootloader and the bootstrap start again. H sees `/enroll` for E and answers **403 `already enrolled`**. The head log has `enroll refused for instance <E> from <ip>: already enrolled` and no `destroying instance <E>` from the head. The bootstrap of E reports `head refused enrollment: already enrolled` to port A (`unverified report from … instance <E> kind 'destroy'`) and then deletes its own instance. E is gone from vast within 60 s of the new `/enroll` call. After E is gone, resume the reconcile thread: it removes the slot and the SG rule if no other instance uses the IP. The Ray node of E is DEAD about 41 s after the first container stop.

### Data to capture (`archive/results/$DATE/c5/`)

`offers.json`, `images.txt`, `versions.txt`, `instance_fields.json`, `c5_t15.jsonl` and `c5_t16.jsonl` (events), `probe_M.log` and `probe_V.log` (from `vast_tool.py logs`, before the destroy), `worker_<a|b|c|d>.log` (container logs), `status_timeline.jsonl` (all `actual_status` values with times), `head/` (monitor log, `docker logs ray-vast-head`), `up.log`, `down.log`, `spend.md`. The scan must be clean.

### Pass criteria

- T15: (a), (b) and (c) enroll and run a Ray task. (d) never enrolls, has the entrypoint error in its log, and is destroyed within 90 s. Vast accepts the 1456-byte bootloader in `args`.
- T16: the refusals give the statuses in the table. The head destroys nothing for a refused request. The restart case ends with the worker's own destroy. The instance list does not show the `instance_api_key` of other instances (or vast refuses the call).
- Fields: `public_ipaddr`, `ports`, `actual_status`, `status_msg` and `label` exist as the code reads them. Unknown `actual_status` values are listed.
- Teardown ends clean: 0 vast instances, 0 AWS objects.

---

## Cycle C6: signals to PID 1 and the heartbeat (T13, T14)

**Tests:** T13, T14.
**Machines:** one EC2 head from `ray-vast up`, and 11 short rentals: 7 for T13 and 4 for T14. If the budget is tight, T13 can skip `r-int` and `r-hup`. That saves 2 rentals.
**Budget:** vast $1.50; EC2 about $0.10.
**Duration:** about 1.5 h, plus `up` and `down`.

### Setup

1. Approval and credit. Offer search for the node type `w` (as C5). Choose 11 offers on 11 different `machine_id`s. Show them to the user.
2. The images: `slim-root` (root; from C5, push again if the 6 h tag expired) and `rayproject/ray:2.59.0-py311-cpu` (user `ray`, non-root). The head uses the `rayproject/ray` image.
3. `tests/manual/c6/cluster.yaml`: as C5, with `cluster_name: rvtest-c6`. Run `ray-vast up … $OPS --key-name phduc-personal`. Check `ray status`. Make `WORK/h/` and `WORK/results/`. Copy `harness_head.py`, `c6_run.py` and `c6_signal_task.py` to `WORK/h/`. Write `versions.txt`.

### Scripts

`c6_signal_task.py` defines the Ray tasks. Each one is pinned to one node with `NodeAffinitySchedulingStrategy(node_id, soft=False)`.
- `send(sig)`: runs `os.kill(1, sig)`. It returns if the worker survives.
- `inspect()`: returns whether `CONTAINER_API_KEY` is in the task's `os.environ` (yes or no only); `os.getuid()`; the uid of PID 1 (`/proc/1/status`); whether `/proc/1/environ` is readable and holds the text `CONTAINER_API_KEY=` (yes or no only, never the value); the same for `/proc/<raylet pid>/environ`; the file modes in `/tmp/rayvast`; the number of zombie processes (state `Z` in `/proc/*/stat`).
- `orphans(n)`: starts `n` short processes whose parent exits at once (`subprocess.Popen(["sh","-c","(sleep 0.2 &)"])`), so PID 1 gets them. It waits 5 s and returns the number of zombies with parent PID 1.

`c6_run.py` runs in the head container. It makes H (cluster `rvtest-c6h`, ports 8443/8444, `candidate_factor 1`, `max_dollars_per_hour 2.5`) with reconcile on, and runs the two steps below.

### Steps

**C6.1 — T13: signals from a Ray task (7 workers).**
1. Create the workers, one `create` call (count 1) for each, and set `H._cfg["image"]` before each call. Root workers on `slim-root`: `r-term`, `r-int`, `r-hup`. Non-root workers on `rayproject/ray`: `u-term`, `u-int`, `u-hup`, and `u-keep`. Wait until all 7 are enrolled and their Ray nodes are ALIVE. The limit is the pull limit plus 2 min. Replace a worker that misses it, and record that.
2. On `u-keep`, run `inspect()` and `orphans(50)`. Then run `send(SIGKILL)`, `send(SIGQUIT)`, `send(SIGUSR1)`, `send(SIGUSR2)` and `send(SIGCONT)`. Wait 3 min.
   Expected:
   - `CONTAINER_API_KEY` is **not** in the task's `os.environ`, and not in the raylet's environ.
   - The task can read `/proc/1/environ` (same user), and it holds the key. This is the documented risk D7. Record yes or no.
   - The files in `/tmp/rayvast` have mode 600.
   - All five `send` calls return without effect. The kernel ignores `SIGKILL` for PID 1, and PID 1 has no handler for the other four.
   - `u-keep` stays enrolled, and H shows `/heartbeat` 200 every 30 s.
   - The zombie count with parent 1 is 0 after `orphans(50)`.
   - The uid of the Ray task equals the uid of PID 1 (1000).
3. On each of the other 6 workers, write `t0` and run `send(SIGTERM)`, `send(SIGINT)` or `send(SIGHUP)` in a Ray task on that node, as the worker's name says. Run them one after the other, 20 s apart. For `u-term`, send `SIGTERM` twice, 0.5 s apart. Poll the vast status of each worker every 2 s.
   Expected for each:
   - The head log has `worker <id> asks to be destroyed: 'received SIGTERM'` (or `SIGINT`, `SIGHUP`) **before** the instance is gone from vast.
   - The instance is gone from vast (404 or not in the list) within 20 s of `t0`. It never shows `exited` and a restart, so PID 1 did not exit.
   - The driver's `ray.get` of the task raises an error, because the node died. The Ray node is DEAD within about 45 s.
   - H removes the slot, and the SG rule if no other instance uses the IP.
   - For the double `SIGTERM`: one destroy only, and no traceback in the container log (if the log is lost, record that).
   - The result is the same on the root image and on the non-root image.

**C6.2 — T14: heartbeat and enrollment limits (4 workers).** Create 4 workers on `rayproject/ray`, one `create` call for each. Use these roles. The H wrapper does the rest:
- `hb-503` (the heartbeat stops, the reason reaches the head): after enrollment and 2 min of normal heartbeats, **pause the reconcile thread of H**. Then call `fail_heartbeat(id)`. H now answers 503 to `/heartbeat` only; `/report` still works. `t_last` is the time of the last `/heartbeat` 200 in the events.
- `hb-cut` (the head is unreachable): same pause. At `t_cut`, remove the SG rule of its IP (`H._sg.revoke_ip(ip)`). `t_last` is the last 200 before `t_cut`. Do not put the rule back.
- `hb-pause` (a stop of 2 min): at `t_p`, call `fail_heartbeat(id)` for 120 s, then stop the 503 answers. Keep the worker for 3 more min.
- `no-enroll`: call `hold_enroll(id)` and `hold(id)` as soon as H has recorded the public IP and the rule (the worker is `running`). From then on `/enroll` answers 503 `the head has not seen this instance running yet`, and the provider does not end the candidate. `t_first` is the time of the first 503 in the events.

Keep the reconcile thread paused until `hb-503` and `hb-cut` are gone, then resume it.

Expected:

| Worker | Expected |
|---|---|
| `hb-503` | Destroy at `t_last` + 300 s (limit 270–330 s). The container log (fetched before) has `heartbeat answered HTTP 503` lines. The head log has `worker <id> asks to be destroyed: 'no successful heartbeat for 300 s'` **before** the instance is gone. |
| `hb-cut` | Destroy at `t_last` + 300 s, plus up to 12 s for the report that cannot be sent (it times out after 10 s). Limit 270–330 s. The reason is **not** in the head log (expected: the head was unreachable). |
| `hb-pause` | No destroy. The largest heartbeat age in H (time since the last 200) is about 150 s (120 s pause + 30 s period) and is below 300 s. After the pause, `/heartbeat` 200 comes again within 30 s. |
| `no-enroll` | Destroy at `t_first` + 600 s (limit 570–630 s; the bootstrap checks every 10 s). The head log has `unverified report from <ip>: instance <id> kind 'destroy' reason 'not enrolled before the enrollment deadline'` before the instance is gone. |

The provider side of the heartbeat limit (`HEARTBEAT_TIMEOUT_S` in `reconcile_once`) is unit tested. This cycle tests that the **worker** destroys itself when the provider cannot. So the reconcile thread is paused in these tests.

### Data to capture (`archive/results/$DATE/c6/`)

`offers.json`, `versions.txt`, `c6_run.jsonl` (events with times: creates, enrolls, heartbeats, signal sends, reports, destroys), `status_timeline.jsonl`, `task_results.json` (the output of `inspect()` and `orphans()`), `worker_<name>.log` (container logs fetched before the destroy, where possible), `head/`, `up.log`, `down.log`, `spend.md`, and `c6_timings.csv` (worker, `t0` or `t_last`, time gone from vast, difference in seconds).

### Pass criteria

- T13: all 6 signals give a destroy within 20 s and never an exit. The reason is in the head log before the instance is gone. `SIGKILL`, `SIGQUIT`, `SIGUSR1`, `SIGUSR2` and `SIGCONT` have no effect. There is no `CONTAINER_API_KEY` in a task's `os.environ`. Zombies: 0. The root and the non-root image give the same results.
- T14: `hb-503` and `hb-cut` destroy in 270–330 s. `hb-pause` does not destroy. `no-enroll` destroys in 570–630 s. The head gets the reason in the cases `hb-503` and `no-enroll`.
- Teardown ends clean.

---

## Cycle C7: IP change, full flow (T7)

**Tests:** T7.
**Machines:** one EC2 head from `ray-vast up`, and 3 to 5 rentals. The first two must share one public IP.
**Budget:** vast $1.25; EC2 about $0.15.
**Duration:** about 2 h.

**How to cause an IP change.** Real vast has no way to do it: a host changes its IP only by itself. C7 changes what the head sees. A small proxy on the head forwards every vast API call of the provider to `console.vast.ai`. For the instance IDs in a file, it replaces `public_ipaddr` in the answer of `GET /instances/` with `198.51.100.23` (TEST-NET-2). The provider sees an IP that differs from the one it recorded. This is the same event as a real change. The workers still use the real vast API. Everything else is real: the provider in the Ray monitor, the autoscaler (v2), the destroy, the security group.

### Setup

1. Approval and credit.
2. **Find a shared IP.** Search offers for the node type `w` (`CPU: 2`, GPU as C5 found). Group the result by `public_ipaddr`. Choose the cheapest IP that has at least 2 offers on different `machine_id`s. (On 2026-10-12, host 62819 had the machine IDs 148075, 142607 and 142609 on the IP 85.10.218.46.) Save the groups to `c7/offers.json`. If no IP has 2 offers, run only C7.4, and mark "rule kept while shared" as "not tested".
3. `tests/manual/c7/cluster.yaml`:
   ```yaml
   cluster_name: rvtest-c7
   max_workers: 2
   idle_timeout_minutes: 60
   provider:
     region: us-west-2
     image: rayproject/ray:2.59.0-py311-cpu
     max_dollars_per_hour: 1.0
     candidate_factor: 1
   auth: {ssh_user: ray}
   head_node_type: head
   available_node_types:
     head: {resources: {CPU: 0}, node_config: {}, max_workers: 0}
     w:
       resources: {CPU: 2, GPU: 1}      # the GPU count of the chosen offers
       min_workers: 0
       max_workers: 2
       node_config:
         disk_gb: 30
         offer_query: {machine_id: {in: [<m1>, <m2>, <m3>]}}   # the machines on the shared IP
   ```
   `candidate_factor: 1` rents one candidate for each slot. The `in` list holds the machines on the shared IP, so two workers share the IP. A third machine in the list lets a replacement arrive.
4. `ray-vast up … $OPS --key-name phduc-personal`. Then start the proxy on the head and point the provider at it:
   ```
   scp tests/manual/c7_vast_proxy.py ubuntu@$HEAD_IP:WORK/
   ssh … "sudo docker run -d --name rv-proxy --network host -v WORK:WORK ray-vast-rvtest-c7:head python WORK/c7_vast_proxy.py"
   ssh … "sed -i '/^  region:/a\  vast_api_url: http://127.0.0.1:8099/api/v0' WORK/cluster.yaml && sudo docker restart ray-vast-head"
   ```
   `c7_vast_proxy.py` uses the stdlib only. It listens on 127.0.0.1:8099 and forwards every method and path, with the same headers and body, to `https://console.vast.ai`. On each `GET /api/v0/instances/` it reads `WORK/flip_ids` (one instance ID for each line) and rewrites `public_ipaddr` for those IDs. It prints one line for each call (method, path without query, status) and never the headers or bodies. The restart makes the head start again with the new setting. (This also shows that a restarted head reuses its CA and token.) Wait for `ray status`.
5. Start the samplers on the laptop. The poller: `vast_tool.py poll ray-vast:rvtest-c7 5 > c7/vast_timeline.jsonl`. The rule sampler: every 2 s, `aws ec2 describe-security-groups --group-ids <sg> --query 'SecurityGroups[0].IpPermissions[].IpRanges[].[CidrIp,Description]'` into `c7/sg_timeline.jsonl` (`<sg>` is in `~/.ray-vast/rvtest-c7.json`).

### Steps

**C7.1 — Two workers on one IP.**
1. Ask for 2 workers: `ssh … "sudo docker exec ray-vast-head python -c \"from ray.autoscaler.sdk import request_resources; request_resources(bundles=[{'CPU': 2}]*2)\""`. This keeps a demand of 2 nodes until the call is made again with other values.
2. Wait until `ray status` shows 2 workers. Record the instance IDs, the `public_ipaddr` of each (it must be one IP, S), the SG rules, and the times.
   Expected: both workers enroll. The SG list has 1 worker rule, `S/32`, with the description `ray-vast:rvtest-c7`.

**C7.2 — IP change of worker W1; the rule stays (W2 uses S).**
1. Write the ID of W1 to `WORK/flip_ids`. Write `t0`.
2. Watch the vast poll, the SG sampler, `ray status` and the monitor log.
   Expected, within 40 s of `t0` (one reconcile pass is 10 s):
   - The head log has `ending instance <W1> … : public IP changed`.
   - W1 is destroyed on the real vast (gone from the poll). Its Ray node is DEAD about 41 s later.
   - The SG rule for S is **still there**, because W2 uses S. The SG sampler never shows it absent.
   - W2 stays ALIVE and keeps its heartbeat.
3. Expected within 4 min of the destroy: the autoscaler sees 1 node for a demand of 2, the provider rents a candidate, and the new worker W3 enrolls and is ALIVE. `ray status` shows 2 workers again. The ID of W3 is not in `flip_ids`. Record the time from `t0` to W3 ALIVE. If the create fails with `no offers` (the freed machine is not listed yet), record the number of retries and the delay.

**C7.3 — IP change of the last user; the rule goes.**
1. Add W2 to `flip_ids`. Write `t1`. Wait for the destroy of W2.
2. Then add W3 to `flip_ids` and write `t2`. Wait for the destroy of W3.
3. Expected: after W2 is gone, the rule S stays (W3 uses S). After W3 is gone and **no worker or candidate uses S**, the provider removes the rule S within one reconcile pass (the sampler shows it absent), unless a replacement already has S. A replacement arrives after 30 s or more, so the sampler should show the rule absent for some time. Record the gap. When the replacement W4 arrives, the rule is added for its IP.
4. If the machines on S are not free again (no offer), the replacement uses another IP U. Record it. Then the rule for U exists and the rule for S does not.

**C7.4 — Unique IP (the only part if no shared IP was found; otherwise an extra).** If a worker has a unique IP U, put it in `flip_ids`. Expected: it is destroyed, the rule for U is removed at once (nobody else uses it), and a new worker arrives.

**C7.5 — End.** Clear the demand: `request_resources(num_cpus=0)`. Expected: the autoscaler does not remove the workers before `idle_timeout_minutes` (60). Run the teardown. It destroys them.

### Data to capture (`archive/results/$DATE/c7/`)

`offers.json` (IP groups), `versions.txt`, `vast_timeline.jsonl`, `sg_timeline.jsonl`, `flip_log.txt` (each time an ID was added), `ray_status_timeline.txt` (every 10 s), `proxy.log` (calls only), `head/` (monitor log), `up.log`, `down.log`, `spend.md`, and `c7_timings.csv` (step, `t0`, time of destroy, time of rule change, time of new worker ALIVE).

### Pass criteria

- Each IP change destroys exactly that worker within 40 s, with the reason `public IP changed` in the head log. Other workers are not touched.
- The rule for a shared IP stays while another worker uses it, and goes when the last user is gone (SG sampler). Rules for other IPs are not touched.
- The autoscaler gets a new worker for each destroyed one. The new worker enrolls and Ray shows it ALIVE.
- The provider blocklists nothing for an IP change (the replacement can use the same machine; record whether it did).
- Teardown ends clean.

---

## Cycle C8: full scale test (L6)

**Tests:** L6, with the real CLI only: `up`, `dashboard`, `ray job submit` from the laptop, autoscaling with `candidate_factor: 2`, idle scale-down, `down`.
**Machines:** one EC2 head, and 4 GPU workers of 2 node types. Up to 4 extra candidates (k = 2) are rented and destroyed when the slots are full.
**Budget:** vast $3.00; EC2 about $0.15.
**Duration:** about 1.5 h, plus `up` (10–15 min; the GPU image is bigger) and `down`.

### Setup

1. Approval and credit. Run C8 only if the spend of the day so far is below $4.
2. **Choose two node types by VRAM.** Run the offer search for each (`GPU: 1`, `CPU: 4`, `inet_down >= 500`, sorted by `dph_total`). Example: `gpu-a` with `gpu_ram: {gte: 8000, lt: 16000}` (MB) and `gpu-b` with `gpu_ram: {gte: 16000, lt: 32000}`. Each type needs at least 6 offers on at least 4 different `machine_id`s. Change the ranges until this holds. Show both lists to the user.
3. `tests/manual/c8/cluster.yaml`:
   ```yaml
   cluster_name: rvtest-c8
   max_workers: 4
   idle_timeout_minutes: 2
   provider:
     region: us-west-2
     image: rayproject/ray:2.59.0-py311-gpu
     image_size_gb: 20          # pull limit = 2 x 20 x 8000 / inet_down, between 5 and 15 min
     max_dollars_per_hour: 3.0
     candidate_factor: 2
   auth: {ssh_user: ray}
   head_node_type: head
   available_node_types:
     head: {resources: {CPU: 0}, node_config: {}, max_workers: 0}
     gpu-a:
       resources: {CPU: 4, GPU: 1}
       min_workers: 0
       max_workers: 2
       node_config: {disk_gb: 40, offer_query: {gpu_ram: {gte: 8000, lt: 16000}, inet_down: {gte: 500}}}
     gpu-b:
       resources: {CPU: 4, GPU: 1}
       min_workers: 0
       max_workers: 2
       node_config: {disk_gb: 40, offer_query: {gpu_ram: {gte: 16000, lt: 32000}, inet_down: {gte: 500}}}
   ```
   Each type allows 2 workers, so a job of 4 GPU workers must use both types. `image_size_gb: 20` is set by hand. Without it, `up` sets the unpacked size (about 9 GB), which gives the 5 min minimum limit; the GPU image is about 3.4 GB compressed and pulls more slowly. Record the real pull times. If more than one candidate misses the pull limit, record it and run again with the CPU image (`--num-gpus` comes from `/proc/driver/nvidia`, not from the image).
4. `tests/manual/c8_job.py` is the job driver. It does `ray.init(address="auto")`, makes a placement group of 4 bundles `{GPU: 1, CPU: 1}` with the strategy `STRICT_SPREAD`, waits for `pg.ready()` and prints `C8_ALL_PLACED <epoch>`. Then it starts 4 tasks, one in each bundle, with `num_gpus=1`. Each task prints `nvidia-smi -L` (the GPU name), `ray.get_gpu_ids()`, `CONTAINER_ID` and `RAY_NODE_TYPE_NAME`, and sleeps 30 s, so all 4 run at once. At the end it prints one JSON result. It fails if there are not 4 different `CONTAINER_ID` values.
5. `tests/manual/c8_poll.py` (laptop) writes, every 5 s, one JSON line: the UTC time; the instances with the label `ray-vast:rvtest-c8` (ID, `actual_status`, `dph_total`, `machine_id`); the sum of `dph_total`; the number of worker rules in the security group (the rules with the description `ray-vast:rvtest-c8`); and, over SSH to the head (`ray list nodes --format json` in the container), the count and the states of the Ray nodes.
6. `ray-vast up tests/manual/c8/cluster.yaml $OPS --key-name phduc-personal`. Record the duration. Then run `ray-vast dashboard tests/manual/c8/cluster.yaml $OPS` in a second terminal. It blocks, because it is the tunnel. It prints `export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=~/.ray-vast/rvtest-c8/token`. Run that export in the shell that submits the job. (The README still says that the CLI does not give the token to the laptop. That text is old: `dashboard` copies the token. Fix the README after the run.)
7. Baseline: `ray status --address http://127.0.0.1:8265` shows no workers. `c8_poll.py` shows 0 instances and 0 worker rules. Start the poller.

### Steps

**C8.1 — Scale up.**
1. Copy `c8_job.py` to a clean directory (`$SCRATCH/c8job`). Write `T_submit` (laptop clock, UTC, ms). Run `ray job submit --address http://127.0.0.1:8265 --working-dir $SCRATCH/c8job --no-wait -- python c8_job.py`. Record the job ID.
2. Watch the poller and `ray job logs <id> -f`.
   Expected:
   - Within 1 min the provider rents candidates: for each type, 2 slots and up to 4 candidates (k = 2), as far as `max_dollars_per_hour` allows. The poller shows the instances in `loading`, then `running`.
   - The first candidates that enroll fill the slots. **Every candidate beyond the slots is destroyed within 60 s of the last slot being filled.**
   - 4 Ray nodes become ALIVE (2 of type `gpu-a`, 2 of type `gpu-b`). The job prints `C8_ALL_PLACED`, then the JSON result, and ends with `SUCCEEDED`.
3. Record: `T_submit`; the time of the first rental; the first `running`; the first enrollment (first Ray node ALIVE); the 4th Ray node ALIVE (this is **submit to all workers running**); `C8_ALL_PLACED`; the end of the job; the pull time of each worker (create to `running`); the number of candidates rented and destroyed unused; the peak sum of `dph_total` (it must stay at or below 3.0).

**C8.2 — Idle scale-down.**
1. After the job ends, write `T_idle`. Watch the poller for 8 min.
2. Expected: Ray drains each idle worker after `idle_timeout_minutes` (2), the provider destroys it, and its rule is removed. All 4 workers are gone within 6 min of `T_idle`. The poller then shows 0 instances and 0 worker rules (only the SSH rule is left). Record the time of each removal.

**C8.3 — Down.** Stop the poller and the dashboard tunnel. Run the common teardown. Here `ray-vast down` is the main test. It must leave 0 vast instances, 0 rules (the group is deleted) and 0 IAM objects (role, role policy, instance profile). The independent check in teardown step 7 is the proof.

### Data to capture (`archive/results/$DATE/c8/`)

`offers_a.json`, `offers_b.json`, `versions.txt`, `timeline.jsonl` (poller), `job_logs.txt`, `job_result.json`, `c8_timings.md` (all times above, with the differences in seconds), `head/` (monitor log, `docker logs`), `up.log`, `down.log`, `spend.md`, `cost.md` (credit before and after; for each instance, `dph_total` times hours from the poll).

### Pass criteria

- The job ends with `SUCCEEDED`, with 4 different containers and both node types.
- The sum of `dph_total` never goes above `max_dollars_per_hour`.
- Extra candidates are destroyed less than 60 s after the slots are full.
- Idle workers are gone within 6 min of the end of the job. No worker rule remains.
- `ray-vast down` exits with 0 and prints `left: nothing`. The independent check finds 0 instances, 0 rules and 0 IAM objects.
- The scan finds no secret in the kept data.

---

## Results

Fill one row for each test after its cycle. Put the raw data under `archive/results/<date>/` and link it.

| # | Result | Key numbers | Notes and links to logs |
|---|---|---|---|
| T15 | | pull and enroll time for each image; 1456 B bootloader accepted in `args` | |
| T16 | | HTTP status for each case; `instance_api_key` of others in the list: yes/no | |
| Fields | | `public_ipaddr`, `ports`, `actual_status` values seen | |
| CapBnd | | CapBnd root and non-root; hosts checked | |
| T13 | Partly done (C4, old sudo PID 1) | `kill -9` of the watchdog as ray: refused | sudo as PID 1 let ray send TERM/INT; this design was dropped. Repeat with the bootstrap as PID 1 and Ray as the same user (C6). |
| T14 | Timing passed (C4) | max age 40 s; pause 134 s no destroy; destroy at t0+5:11 | `archive/results/2026-10-13/progress.md`; destroy-reason lines lost. Repeat with the real bootstrap (C6). |
| T7 | | | |
| L6 | | submit to all workers running; peak $/h; scale-down time; total cost | |
