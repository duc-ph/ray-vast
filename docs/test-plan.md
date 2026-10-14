# Test plan

The test levels, the safety rules for paid tests, and the live cycles C5, C6 and C8 (implementation step 12). The results of the cycles that ran (C0–C4, C5a/C5d) are in [test-results.md](test-results.md). The raw data is in `archive/results/`.

Before a cycle runs, it must be written here with: setup, steps, expected output, data to capture, and teardown.

## Test levels

| Level | What | Where | Cost | When |
|---|---|---|---|---|
| L1 Unit | pure logic, fake clock, `responses`, `moto` | dev box | $0 | each change (fast run) |
| L2 Integration | provider + Ray v2 `NodeProviderAdapter` + fake vast; head server + bootstrap processes; bootloader in Docker | dev box | $0 | full run, once per step |
| L3 Local end-to-end | real Ray, real bootloader and bootstrap in Docker, a fake vast that runs containers (step 11) | dev box | $0 | from step 11 |
| L4 AWS only | `up`/`down` with no workers | `us-west-2` | ~$0.10 | steps 10, 13 |
| L5 Live vast | C5, C6 | EC2 head + vast | ~$1 | step 12 |
| L6 Live scale | C8 | EC2 head + vast | ~$3 | step 12, before release |

The commands for the fast and the full run are in the [README](../ray-vast-pypi/README.md) ("Tests").

## Not tested live (decided with the user, 2026-10-13)

- An IP change (C7): real vast cannot force one. Unit tests and step 11 cover it.
- Images other than `rayproject/ray` (root images): later. Images with an ENTRYPOINT are in scope: `rayproject/ray:*-gpu` has one (`/opt/nvidia/nvidia_entrypoint.sh`), and C8 uses it (C8 finding, 2026-10-14).
- Attacks on `/enroll` (wrong token, wrong IP, unknown instance): unit and integration tests cover them.
- The worker's 10 min enrollment deadline: step 11 covers it.

## Safety rules for paid tests

1. **Approval.** Before the first rental of a cycle, show the user the EC2 type, the offers (ID, GPU, `dph_total`, `inet_down`, location, `static_ip`), the budget and `max_dollars_per_hour`. (The user approved autonomous rentals for testing on 2026-10-13; still write the list into the cycle data.)
2. **Credit.** Record the vast credit before and after each cycle in `spend.md`. Stop and run the teardown if the spend of one UTC day goes above **$8**, or if the cycle spend goes above the cycle budget.
3. **Labels.** The provider labels its instances `ray-vast:<cluster>`. Instances that a test rents without the provider have the label `ray-vast-test-<YYYYMMDD>`.
4. **Teardown** ends with a label sweep until 0, then the independent AWS check.
5. **Resources.** Record time, CPU, RAM and cost for each step that takes more than 1 min in `resources.csv`.
6. **Logs from workers.** After a destroy, vast keeps no container log (C4). Fetch a worker's log before its destroy.
7. **Secrets.** Scan all kept data for the cluster token, join tokens, `PRIVATE KEY`, the vast key, `instance_api_key` values, `CONTAINER_API_KEY=` values and `Bearer ` values before a commit. Never copy `secrets/` from the head, or a capture of a loopback interface.

## Common setup for C5, C6 and C8

- **Operator machine:** the laptop, with a venv that has `ray[default]==2.59.0`, `build`, `boto3` and this package (`pip install -e ray-vast-pypi`).
- **AWS:** profile `personal`, region `us-west-2`, EC2 key pair `phduc-personal`, key file `~/phduc-personal.pem`. The head is `t3.large`. Never touch the badminton box (50.112.112.27) or anything with the name `badminton`.
- **Shell variables:**
  ```
  OPS="--profile personal --key-file $HOME/phduc-personal.pem"   # up, attach, dashboard
  export VAST_API_KEY=$(cat ~/.config/vastai/vast_api_key)       # never echo it
  DATE=$(date -u +%F)
  ```
  `up` also takes `--key-name phduc-personal`. `down` takes only `--profile personal`.
- **Image:** `rayproject/ray:2.59.0-py311-cpu` for C5 and C6; `rayproject/ray:2.59.0-py311-gpu` for C8.
- **Data:** `archive/results/$DATE/c5/`, `c6/`, `c8/`, plus `resources.csv` and `spend.md` in `archive/results/$DATE/`.
- **Version record** in each cycle directory, `versions.txt`: `git rev-parse HEAD`, `git status --short`, SHA-256 of `bootstrap.py`, `bootloader.sh` and the shim, `ray --version` on the head and on the laptop, the image digest.
- **Head log:** `/tmp/ray/session_latest/logs/monitor.log` in the container `ray-vast-head`.

### Scripts (`tests/manual/`, committed before the first rental, no secrets)

| Script | Where it runs | What it does |
|---|---|---|
| `vast_tool.py` | laptop | `credit`; `count` and `sweep` (instances with a test label or a `ray-vast:rvtest-` label; sweep lists again until 0 and retries on 429); `poll <label> <period>` (one JSON line per sample: `id`, `actual_status`, `intended_status`, `status_msg`, `public_ipaddr`, `dph_total`, `machine_id`); `logs <id>` (the container log); `fields <label>` (the instance fields that the code reads, see C5.4); `env-secret <id> <file>` (writes the instance's join token to a mode-600 file for the scan). It never prints a key or a token. |
| `scan_secrets.sh DIR TOKENFILE...` | laptop | Print the names of the files (never the text) that hold a secret of safety rule 7. Expected: no output. |
| `resources.sh CYCLE STEP` | laptop | Append one line to `resources.csv`: time, cycle, step, CPU and memory of `ray-vast-head`, the cycle's instances and the sum of their `dph_total`. |
| `offers.py` | laptop | The offer search with `ray_vast.config.build_offer_query` for a node type (the same filters as the provider). Prints the 12 cheapest offers. |
| `task_probe.py` | Ray task, submitted from the laptop | C5.3 and C6. |
| `c8_job.py`, `c8_poll.py` | job on the cluster; poller on the laptop | C8. |

### Teardown (all cycles, in this order)

1. Copy the head log and `sudo docker logs ray-vast-head` into `<cycle dir>/head/`.
2. Write the cluster token and the join tokens of the cycle's instances to mode-600 temp files (`~/.ray-vast/<cluster>/token` after `dashboard`; `vast_tool.py env-secret`). Run `scan_secrets.sh`. Expected: no output. Remove a match before any commit. Delete the temp files.
3. `ray-vast down <yaml> --profile personal`. Expected: `left: nothing`, exit code 0. If something is left, run `down` again.
4. `vast_tool.py sweep`. Expected: `0 left`.
5. Independent check: no EC2 instance with the tag `ray-vast-cluster=<cluster>` in a live state; no security group `ray-vast-<cluster>-*`; no volume with the cluster tag; `iam get-role` and `get-instance-profile` give `NoSuchEntity`; `vast_tool.py count` gives 0.
6. Write `spend.md`. Fill the rows of the Results table. Add the findings to `test-results.md`. Commit and push.

---

## Cycle C5: join and task

**Tests:** T15 (the bootloader on `rayproject/ray`; vast accepts it in `args`), the join token on real vast, a Ray task on a real worker, the real vast fields, the `CapBnd` check on real hosts.
**Machines:** one EC2 head from `ray-vast up`; 2 rentals (1 slot, k = 2).
**Budget:** vast $0.30; EC2 about $0.15.

### Setup

`tests/manual/c5/cluster.yaml`:
```yaml
cluster_name: rvtest-c5
max_workers: 1
idle_timeout_minutes: 60
provider:
  region: us-west-2
  image: rayproject/ray:2.59.0-py311-cpu
  max_dollars_per_hour: 1.0
auth: {ssh_user: ray}
head_node_type: head
available_node_types:
  head: {resources: {CPU: 0}, node_config: {}, max_workers: 0}
  w:
    resources: {CPU: 4, GPU: 0}      # GPU: 1 if offers.py finds no CPU-only offer
    min_workers: 1
    max_workers: 1
    node_config: {disk_gb: 30, offer_query: {inet_down: {gte: 500}}}
```
Run `offers.py` for `w` and save the output to `c5/offers.json`. Write `versions.txt`. Record the size of `bootloader.sh` (about 1.5 KB).

### Steps

**C5.1 — Up.** `ray-vast up tests/manual/c5/cluster.yaml $OPS --key-name phduc-personal`. Expected: exit code 0 after 8–12 min. Record the duration. Start `vast_tool.py poll ray-vast:rvtest-c5 5 > c5/vast_timeline.jsonl`.

**C5.2 — Rent and enroll.** `min_workers: 1` makes v2 ask for 1 node at once. Expected:
- The provider rents 2 candidates (the head log: `create 1 x w: rented 2 candidates`).
- Both reach `running`. One enrolls (`enrolled instance … as slot …`). The other is destroyed within 60 s of the enrollment (`its request has no free slot left`), or the head refuses it with "no free slot" and destroys it.
- `ray status` shows 1 worker of type `w`, with the real core count of the host.
- Record: create → `running`, `running` → enrolled, enrolled → Ray node ALIVE, for each instance.

**C5.3 — Task.** Run `ray-vast dashboard … $OPS` in a second terminal, export the line that it prints, then `ray job submit --address http://127.0.0.1:8265 --working-dir tests/manual -- python task_probe.py`. The task runs on the worker and prints one JSON line: `os.getuid()`, `sys.executable`, `ray.__version__`, whether `CONTAINER_API_KEY` and `RAYVAST_JOIN_TOKEN` are in `os.environ` (yes or no only), whether `/proc/1/cmdline` holds `bootstrap`, the `CapBnd` line of `/proc/self/status`, and the modes of the files in `/tmp/rayvast`.
Expected: the job ends `SUCCEEDED`; uid 1000; Ray 2.59.0; both secrets **not** in `os.environ`; PID 1 is the bootstrap; bits 12 and 13 of `CapBnd` are clear; `node.key` and `token` are mode 600.

**C5.4 — vast fields.** `vast_tool.py fields ray-vast:rvtest-c5 > c5/instance_fields.json`. It keeps only `id, label, actual_status, cur_state, intended_status, status_msg, public_ipaddr, machine_id, dph_total, ports`. Expected: the names exist as the provider reads them; `public_ipaddr` equals the worker's `PUBLIC_IPADDR` (from its container log); every `actual_status` value seen in the poll is in the provider's pending or failed set (list each value that is not; the head log has `unknown vast actual_status` for it).

**C5.5 — Logs.** Fetch the worker's container log (`vast_tool.py logs <id>`) into `c5/worker.log`. Expected: the host check line with all four checks `True`, and `enrolled as slot …`. No secret (teardown step 2 checks it).

### Pass criteria

- The bootloader works on real vast, and the worker enrolls with its join token.
- The extra candidate is destroyed within 60 s of the enrollment.
- The task result is as expected in C5.3.
- The fields of C5.4 exist; unknown status values are listed.
- The teardown ends clean.

---

## Cycle C6: self-destroy paths

**Tests:** T13 (a signal to PID 1 from a Ray task gives a destroy, never an exit), T14 (the heartbeat stops: destroy after 5 min; Ray keeps running until then), the restart case (a container restart gives a refused second enrollment and a destroy).
**Machines:** one EC2 head from `ray-vast up`; 3 workers (A, B, C), plus the replacements that v2 asks for.
**Budget:** vast $1.00; EC2 about $0.15.

### Setup

`tests/manual/c6/cluster.yaml`: as C5, with `cluster_name: rvtest-c6`, `min_workers: 3`, `max_workers: 3`, `candidate_factor: 1`, `max_dollars_per_hour: 1.5`. Run `up`. Wait until `ray status` shows 3 workers. Start the poller. Name the workers A, B and C by instance ID.

### Steps

**C6.1 — Signals (A).**
1. A task pinned to A (`NodeAffinitySchedulingStrategy(node_id, soft=False)`) sends `SIGKILL`, `SIGQUIT`, `SIGUSR1`, `SIGUSR2` and `SIGCONT` to PID 1. Wait 2 min. Expected: no effect; A stays enrolled; zombies with parent 1: 0 (the task starts 50 orphan children first and counts).
2. Write `t0`. A task pinned to A sends `SIGTERM` to PID 1. Expected: the head log has `worker <A> asks to be destroyed: 'received SIGTERM'` before A is gone; A is gone from vast within 20 s of `t0`; vast never shows `exited` and a restart; the Ray node is DEAD within about 45 s; v2 rents a replacement.

**C6.2 — Heartbeat block (B).** Write `t_cut`. On the head host: `sudo iptables -I INPUT -p tcp --dport 7444 -s <B ip> -j DROP`. Port A and the Ray ports stay open.
Expected: B's Ray node stays ALIVE until the destroy. B is gone at `t_last` + 300 s (limit 270–330 s; `t_last` is B's last heartbeat in the head log). Both the provider and the watchdog have this limit; record which one acted (the head log has `ending instance <B> … no heartbeat` for the provider; the report cannot reach port B, so the watchdog's reason is not in the log). Then delete the iptables rule.

**C6.3 — Container restart (C).** Write `t_r`. Restart C's container with the vast reboot call (`PUT /instances/reboot/<C>/`; check the path in the vast docs at setup). Expected: the bootloader and the bootstrap run again; the head log has `enroll refused for instance <C> …: already enrolled` and no destroy by the head for that refusal; the worker's report reaches port A; C is gone within 60 s of the new `/enroll`. If the provider ends C first (exit status or heartbeat), record that.

### Pass criteria

- C6.1: the five signals have no effect; `SIGTERM` gives a destroy within 20 s with the reason in the head log, and no exit.
- C6.2: B is destroyed in 270–330 s, and its Ray node is ALIVE until then.
- C6.3: the second enrollment is refused, and C destroys itself.
- The teardown ends clean.

---

## Cycle C8: full scale test (L6)

**Tests:** the real CLI only: `up`, `dashboard`, `ray job submit` from the laptop, autoscaling with k = 2 over 2 node types, idle scale-down, `down`.
**Machines:** one EC2 head; 4 GPU workers of 2 node types, plus up to 4 extra candidates.
**Budget:** vast $3.00; EC2 about $0.15. Run C8 on a day only if the spend of that day so far is below $4.

### Setup

1. Choose two node types by VRAM. Run `offers.py` for each (`GPU: 1`, `CPU: 4`, `inet_down >= 500`). Example: `gpu-a` with `gpu_ram: {gte: 8000, lt: 16000}` and `gpu-b` with `gpu_ram: {gte: 16000, lt: 32000}`. Each type needs at least 6 offers on at least 4 different `machine_id`s.
2. `tests/manual/c8/cluster.yaml`:
   ```yaml
   cluster_name: rvtest-c8
   max_workers: 4
   idle_timeout_minutes: 2
   provider:
     region: us-west-2
     image: rayproject/ray:2.59.0-py311-gpu
     image_size_gb: 20          # the pull limit; the GPU image pulls slowly
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
3. `c8_job.py`: a placement group of 4 bundles `{GPU: 1, CPU: 1}`, `STRICT_SPREAD`; prints `C8_ALL_PLACED <epoch>`; then 4 tasks (one for each bundle) print `nvidia-smi -L`, `ray.get_gpu_ids()`, `CONTAINER_ID` and `RAY_NODE_TYPE_NAME`, and sleep 30 s. It fails if there are not 4 different `CONTAINER_ID` values.
4. `c8_poll.py`: every 5 s, one JSON line: the instances with the label (ID, `actual_status`, `dph_total`, `machine_id`), the sum of `dph_total`, the number of worker rules in the 5 groups, and the Ray nodes (`ray list nodes` on the head).
5. `up`, then `dashboard` in a second terminal, then the export line. Baseline: no workers, 0 instances, 0 worker rules. Start the poller.

### Steps

**C8.1 — Scale up.** Write `T_submit`. `ray job submit --address http://127.0.0.1:8265 --working-dir <dir with c8_job.py> --no-wait -- python c8_job.py`.
Expected: within 1 min, for each type, 2 slots and up to 4 candidates (within `max_dollars_per_hour`); the first candidates that enroll fill the slots; every extra candidate is destroyed within 60 s of the last slot being filled; 4 Ray nodes ALIVE (2 of each type); `C8_ALL_PLACED`; the job ends `SUCCEEDED`.
Record: `T_submit`, first rental, first `running`, first and 4th node ALIVE, `C8_ALL_PLACED`, job end, the pull time of each worker, the candidates rented and destroyed unused, the peak sum of `dph_total`.

**C8.2 — Idle scale-down.** Write `T_idle` at the end of the job. Expected: all 4 workers are gone within 6 min, and the worker rules are gone (only the SSH rule is left).

**C8.3 — Down.** Stop the poller and the tunnel. Run the teardown. `down` is the main test here.

### Pass criteria

- The job ends `SUCCEEDED`, with 4 different containers and both node types.
- The sum of `dph_total` never goes above `max_dollars_per_hour`.
- Extra candidates are destroyed less than 60 s after the slots are full.
- Idle workers and their rules are gone within 6 min of the end of the job.
- `down` exits 0 with `left: nothing`; the independent check finds nothing.

---

## Results

Fill one row for each test after its cycle, with a link to the data in `archive/results/<date>/`.

| # | Result | Key numbers | Notes and links |
|---|---|---|---|
| T15 | | create → running → enrolled; bootloader size accepted | |
| Join | | refusals: none expected; extra candidate destroy time | |
| Fields | | `actual_status` values seen | |
| CapBnd | | `CapBnd` on the hosts | |
| T13 | Partly done (C4, old sudo PID 1) | | Repeat with the bootstrap as PID 1 (C6.1). |
| T14 | Timing passed (C4, test watchdog) | destroy at t0 + 5:11 | Repeat with the real bootstrap (C6.2). |
| Restart | | | C6.3 |
| L6 | | submit → all workers running; peak $/h; scale-down time; cost | C8 |
