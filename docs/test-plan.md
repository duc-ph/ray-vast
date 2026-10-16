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
| L7 Ray libraries | C9 | EC2 head + vast | ~$2 | step 12, before the docs of step 13 |
| L8 Process limits | C10 | EC2 head + vast | ~$3 | after the pids/ports change of ray-vast |

The commands for the fast and the full run are in [CONTRIBUTING.md](../ray-vast-pypi/CONTRIBUTING.md).

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

## Cycle C9: Ray libraries (L7)

**Goal:** prove which Ray libraries work on a ray-vast cluster when the user keeps the normal Ray workflow. ray-vast changes only the cluster commands (`ray-vast up`, `dashboard`, `down`). Everything else is the user's normal workflow: `ray job submit --working-dir <dir> --runtime-env-json <json> -- python <script>` from the operator machine, through the `dashboard` tunnel. The scripts follow the examples in Ray's own docs for Ray 2.59, made small.
**Machines:** one EC2 head; 2 GPU workers of one node type (`GPU: 1`, `CPU: 4`, the cheapest offers with `inet_down`/`inet_up` ≥ 500), `candidate_factor: 2`; for C9.3c, a third worker type with 2 GPUs on one host if offers exist.
**Image:** `rayproject/ray:2.59.0-py311-gpu` (it has CUDA, not PyTorch). Jobs that need PyTorch install it with `runtime_env` `pip` (this also tests pip downloads on the workers).
**Budget:** vast $3.00 (more is allowed; the user can add credit); EC2 about $0.30.
**Data:** `archive/results/$DATE/c9/`, one directory for each sub-test with the job script, the submit command, the job log (`ray job logs`), the result and the timing.
**Rule for each sub-test:** record PASS, FAIL or LIMIT (fails for a reason that ray-vast's design causes, for example NAT or the 64 ports). For a FAIL or a LIMIT, keep the exact error, the place (driver log, worker log, head log) and the probable cause. Do not fix product code during the cycle; report.

### C9.0 Workflow
1. `ray-vast up`, `ray-vast dashboard`, the export line, as in C8. Wait for 2 workers (`min_workers: 2`).
2. `ray job submit --working-dir c9/jobs -- python hello.py`: the working directory is uploaded and reaches both workers (each task prints its node and a file from the working directory).
3. `--runtime-env-json '{"pip": ["torch==<version for CUDA of the image>"]}'`: pip installs on both workers; `torch.cuda.is_available()` is True on each worker, and `torch.cuda.device_count()` is 1.
4. `ray job list`, `ray job stop`, `ray job logs --follow` work through the tunnel.

### C9.1 Ray Data
1. **CPU pipeline:** `ray.data.range(10_000_000)` → `map_batches` (numpy) → `filter` → `count` / `take`. Tasks run on both workers.
2. **Cross-node transfer and the object store:** a dataset about 1.5 × the object store of one worker (`ray.data.range_tensor`), `map_batches`, `materialize`, then `iter_batches` in the driver. Record spilling, if any, and the time.
3. **All-to-all:** `random_shuffle` and `groupby(...).count()` on that dataset. Blocks move between the workers.
4. **GPU batch inference:** `map_batches` with a callable class, `concurrency=2`, `num_gpus=1`, a small PyTorch model on the GPU (for example a linear layer or `torchvision` resnet18 with random weights). Each worker runs one actor; record the rows per second.
5. **Write and read:** `write_parquet` to `/tmp/c9_out` (local to each worker; record where the files land) and `ray.data.read_parquet` with a public dataset URL or a file from the working directory. Record what a user needs for shared output (a cloud bucket).
6. **Many tasks:** a pipeline with more parallel tasks than 58 per node (for example 400 small blocks). It must finish; record whether tasks wait for a port (user docs, Reference → Limits).

### C9.2 Ray Tune
1. **Function trainable:** `tune.Tuner` with 8 trials (`num_samples=8`), 1 CPU each, a short loop that reports a metric. Trials run on both workers. `ResultGrid` has 8 results.
2. **Scheduler:** the same with `ASHAScheduler`; some trials stop early.
3. **GPU trials:** 2 trials with `resources={"gpu": 1}`; each gets its own GPU on a different worker.
4. **Checkpoints and storage:** trials that save a checkpoint with `RunConfig(storage_path=...)` set to a local path on the head (the default). Record what Ray 2.59 does on a multi-node cluster without shared storage (error, warning, or checkpoints that stay on the workers). This sets the docs text about storage.

### C9.3 Ray Train (PyTorch)
1. **One worker, one GPU:** `TorchTrainer` with `ScalingConfig(num_workers=1, use_gpu=True)`, a small model, 2 epochs, a checkpoint. PASS expected.
2. **Two nodes, NCCL (DDP):** `num_workers=2, use_gpu=True`, one worker on each node. Expected: LIMIT (PyTorch and NCCL open their own connections between nodes, on ports that Ray does not manage; vast maps only the 64 identity ports). Record the exact error, the time to the error, and which side fails. Then try once with `NCCL_SOCKET_IFNAME` and `MASTER_PORT`-style settings only if Ray Train exposes them without code changes; record the result.
3. **Two nodes, Gloo (CPU):** `num_workers=2, use_gpu=False`, backend `gloo`. Same records as C9.3.2.
4. **One node, two GPUs:** only if an offer with 2 GPUs on one host (a second node type, `GPU: 2`) costs less than $1/h: `num_workers=2, use_gpu=True` on that node. PASS expected (NCCL inside one host).

### C9.4 Ray Serve
1. **Deploy and call by handle:** `serve.run` of a deployment with `num_replicas=2` (replicas on both workers), called from a driver job through a `DeploymentHandle`. PASS expected.
2. **HTTP on the head:** `curl http://127.0.0.1:8000/` on the head host (through `ray-vast attach` or SSH). Record the result.
3. **HTTP from the operator machine:** record what works with the existing tools (for example an SSH tunnel to port 8000 of the head). This sets the docs text for Serve. Do not open a new port in the security groups.
4. **GPU replica:** a deployment with `ray_actor_options={"num_gpus": 1}`; one replica on each worker.

### C9.5 Ray Core under the normal workflow
1. **Placement groups:** `STRICT_SPREAD` of 2 GPU bundles, and `PACK` of CPU bundles.
2. **Actor fault:** kill an actor process on a worker (`ray.kill` and `os._exit` in the actor); the actor restarts with `max_restarts=1`.
3. **Large object:** `ray.put` of 2 GB on one worker, `ray.get` on the other worker. Record the time (transfer between two vast hosts over the internet).

### Pass criteria and outputs
- Each sub-test has a row in the Results table of `test-results.md`: PASS, FAIL or LIMIT, the key numbers, the evidence path.
- A "Ray library support" table for the user docs: library, what works, what does not, what the user must do (for example shared storage for Tune and Train checkpoints, a tunnel for Serve HTTP).
- Teardown as for all cycles: `down`, sweep to 0, AWS check, secret scan.

## Cycle C10: process limits of the Ray libraries (L8)

**Goal:** find the exact advice for the user. A vast container can have a cgroup pids limit (`pids.max` 1280 on the C9 hosts). Each Ray worker process has threads (about 25 with `RAY_num_grpc_internal_threads=4`). With the pids/ports change of ray-vast (branch `pids-tuning`, 2ab2adb), a worker node gets fewer worker ports when `pids.max` is set: ports = max(4, min(58, (pids.max − 400) // 60)), so 14 ports at 1280 and 48 at 3328; `RAY_num_workers_soft_limit` = min(CPUs, ports // 2). The port count is a hard cap on the worker processes of a node. So the expected failure for "too many processes" is a hang or an error, not a node crash. For each library, C10 has a pair of tests:

- **many**: the library with its default settings, or a normal user setting, makes more worker processes on one node than the node has ports. Expected: the job fails (a hang that the guard catches, or an error); the head writes its pids/ports WARNING line for the node; the node does not crash.
- **tuned**: the same work with the advised settings. Expected: the job completes; no refused fork.

The "tuned" settings of all pairs are the advice table for the user docs.

**Scripts:** `tests/manual/c10/` (see its README). `run_c10.py` runs the tests and decides; `local_check.py` is the local check.
**Image:** `rayproject/ray:2.59.0-py311-gpu`. Train and RLlib need PyTorch: `runtime_env` `pip` `torch==2.7.1`, as in C9. The image has `gymnasium` 1.2.2 for RLlib (checked in the CPU image of the same tag; R1 records it on the GPU image).
**Budget:** vast $5.00 (more is allowed); EC2 about $0.40. Estimate below.
**Data:** `archive/results/$DATE/c10/`, one directory for each test (see the README).
**Rule:** do not fix product code during the cycle. Report.

### C10.0 Terms and the advice rule
- **ports**: the worker ports of a node: the size of the raylet's `--worker_port_list` (the probe reads it from the raylet command line).
- **budget**: ports − 4. The 4 ports are for the worker processes that Ray's libraries start next to the user's work (Serve proxy, Data and Train helpers, the probe).
- **Advice rule:** on each node, keep the worker processes of the user's work (tasks that run at the same time, actors, trials, replicas, Train workers, env runners) at or below the budget. A task or actor that waits for another task needs a process for both.
- **Idle workers:** the raylet keeps idle worker processes up to `RAY_num_workers_soft_limit`, and an idle worker holds its port. In the first local check, with the soft limit equal to the port count, 7 idle workers held 7 of 12 ports and C10.train.tuned hung; with a soft limit of 2 it passed. `pids-tuning` sets the soft limit to half the ports. The tuned results are valid only for the soft limit that the probe records.
- **Sizes of the many tests:** each many test must make more processes than 48 ports (a 3328 host), not only 14. So the many sizes are max(ports + 4, a fixed size) with CPU per process small enough to fit the node's CPUs (see C10.1). Exception: Data many uses Ray Data's defaults, which start min(CPUs, 128) shuffle aggregators. It fails only when the node's CPUs are more than its ports: always at 1280 (14 ports) on a `big` host, but at 3328 (48 ports) only with 49 or more CPUs. On a 3328 host with fewer CPUs, record C10.data.many as INCONCLUSIVE if it completes.

### C10.0 Node types and order
- **Phase A** (`cluster_a.yaml`, cluster `rvtest-c10a`): one worker of type `big`, `max_workers: 1`. `resources: {CPU: 32, GPU: 1}`, so ray-vast adds `cpu_cores_effective >= 32`. Offer query: `num_gpus = 1`, `gpu_ram` 8–70 GB, `compute_cap` 600–899, `dph_total <= 0.45`, and the test-only `inet_down`/`inet_up >= 500`. All pair tests run here. With one worker, every process of a test lands on the node under test.
- **Phase B** (`cluster_b.yaml`, cluster `rvtest-c10b`): the C9 layout. 2 workers of type `gpu` (`{CPU: 4, GPU: 1}`), and type `gpu2` (`{CPU: 4, GPU: 2}`, `min_workers: 0`) for R2. Phase B starts after the teardown of phase A.
- **pids.max is known only after the rental.** `run_c10.py` first runs `c10_probe.py` on each worker: `pids.max`, `pids.current`, `pids.peak`, `pids.events`, the ports, the threads, the Ray worker processes. The probe also records `RAY_num_workers_soft_limit`, `RAY_num_grpc_internal_threads` and `RAY_worker_maximum_startup_concurrency` from the raylet's environment. **Host retry:** if no worker has `pids.max` ≤ 3328, the runner destroys the worker with the vast API (`vast_tool.call`). The cluster label stays, and the autoscaler rents a replacement (`min_workers: 1`). The runner probes the new worker. It does this until it has rented 3 hosts in all (`HOST_RENTALS_MAX`). The provider does not block the destroyed host, so vast can give the same machine again: `rentals_a.json` records each instance with its `pids.max`. If no host of the 3 has a limit, the runner does not run the many tests and records them as INCONCLUSIVE (never a false PASS). It still runs the tuned tests.
- **Live run (2026-10-16):** the host retry rented the same machine (pids.max 7680) 3 times, so phase A ran with `offer_query: {machine_id: {in: [3969, 32816]}}` (the C9 hosts with pids.max 1280) on m3969 (24 Ray CPUs). See test-results.md, C10.
- **Order in phase A:** for each library, tuned first, then many (a many test can leave the node in a bad state): Core, Data, Tune, Serve, Train, RLlib.
- **Order in phase B:** C9.1.1, C9.1.3, C9.1.4, C9.1.6 (C9 jobs without `C9_DATA_CPU`), then R1, R3, R2 (R2 starts `gpu2`, so it is last).

### C10.0 For each test (run_c10.py)
1. Probe the worker nodes (pre). Note the line count of the head log (`monitor.log` in `ray-vast-head`, by SSH).
2. Submit the job with `C10_PORTS` and `C10_CPUS` of the node from the probe. Wait for its end. At the runner timeout, `ray job stop`.
3. Run `c10_cleanup.py` (Serve shutdown). Wait 30 s. Probe again (post; again every 30 s for up to 5 min while the probe fails).
4. Keep the new head log lines (`head_slice.log`). The head's process-limit WARNING lines for the node: lines that match `WARNING_PATTERN` (`is at its process limit`) and name the node by `(instance <id>, ` or `, ip <ip>)` (`NODE_NAMES`). Both are constants in `run_c10.py`. The line (head server, design 5.7a): `worker <slot> (instance <id>, ip <ip>) is at its process limit: <N> refused forks/threads (+<new>), pids <cur> of <max>, worker ports in use <n> of <total>. Ray work on this worker can queue, hang or fail. See <docs URL>`. The head writes it at once for new refusals, then at most once a minute; for full ports or pids above 90 %, at most once in 5 min for each worker. The worker reports these numbers in its heartbeat (every 30 s), so a hang must last more than 30 s to show the line.
5. Decide and print `C10_RESULT <id> PASS|FAIL|LIMIT|INCONCLUSIVE <json>`.

**Guards:** each job has a wall-clock guard (`C10_GUARD_S`; default 600 s, Data and Train 900 s, R1–R3 1500 s). The guard prints `C10_JOB <id> HUNG` with the job's progress. The runner timeout is longer than the guard (900–3600 s).

**Verdict of a many test:** PASS = the job failed as expected (HUNG or ERROR) AND the head pids/ports WARNING line names the node AND the node did not crash (the same Ray node ID is alive in the post probe). FAIL if the job completed (no advice is needed at that size; record the numbers), if the node crashed, or if the WARNING line is missing. INCONCLUSIVE if `pids.max` of the node is unset or above 3328.
**Verdict of a tuned test:** PASS = the job completed AND the node is alive AND `pids.events` ("max N") did not change AND no head WARNING line about refused forks or pids for the node.
**Root cause for the user:** the driver usually shows only a hang. The user finds the cause in two places:
1. The head's `monitor.log` (`ray-vast attach`, then `sudo docker exec ray-vast-head grep "process limit" /tmp/ray/session_latest/logs/monitor.log`): the WARNING line above names the worker and gives the pids and the ports in use.
2. The worker's Ray logs: each worker process that the raylet started but could not give a port ends with `Failed to register worker to Raylet: Invalid: No available ports` in its `worker-*.err` file in `/tmp/ray/session_latest/logs/` on that worker. Through the `dashboard` tunnel: `ray logs cluster 'worker-*.err' --node-id <node id>` lists the files, and `ray logs cluster <file> --node-id <node id>` prints one. The Ray dashboard (Logs tab) shows the same files.
The probe counts these lines (`no_port_errors`, with one sample). Each many test records both. Record also whether the driver's own error names the cause.

### C10.1 Pairs (phase A)

| ID | many (Ray defaults or a normal user setting) | tuned (the advice) |
|---|---|---|
| C10.core | max(ports + 4, 200) work items on as many actors, each actor call waits for a child task; `num_cpus` = min(0.1, CPUs / (2 × actors)) for both | the same items on a pool of budget // 2 actors; `num_cpus` = CPUs / budget for actors and tasks |
| C10.data | `range` (400 blocks) → `map_batches(num_cpus=0.05)` → `groupby().count()`, Ray Data defaults (aggregators = min(CPUs, 128)) | a quarter of the budget each: `DataContext.max_hash_shuffle_aggregators`, read `concurrency`, `map_batches(compute=TaskPoolStrategy(size=...))`, `execution_options.resource_limits = ExecutionResources(cpu=...)` (the shuffle tasks) |
| C10.tune | max(ports + 4, 60) trials, each a placement group of 2 bundles of min(0.1, CPUs / (2 × trials)) CPU (the trial and its evaluation task, `num_cpus=0`), all at once | `TuneConfig(max_concurrent_trials=budget // 2)` |
| C10.serve | max(ports + 4, 60) replicas, `num_cpus` = min(0.1, CPUs / (replicas + 1)) | `num_replicas` = budget // 2, `ray_actor_options={"num_cpus": min(1, CPUs / (replicas + 1))}` |
| C10.train | `TorchTrainer` (gloo, CPU), `num_workers` = max(ports + 4, 40), CPU per worker min(1, (CPUs − 0.5) / workers); each worker has a `DataLoader` with 2 worker processes | `num_workers` = budget // 2, same CPU rule |
| C10.rllib | PPO CartPole, `num_env_runners` = max(ports + 4, 40), 1 env each, CPU per runner min(1, (CPUs − 1) / runners) | 2 env runners with the same number of envs (`num_envs_per_env_runner`) |

Train can go above the limit: Ray Train places one worker process for each worker in one placement group; with 1 CPU each, a 32+ core node accepts more workers than it has ports. So C10.train.many is a real test, not N/A.

### C10.2 C9 Data re-runs (phase B)
C9.1.1, C9.1.3, C9.1.4 and C9.1.6 with the C9 scripts and settings, without `C9_DATA_CPU`. Expected: no node crash (no worker destroyed, no "asks to be destroyed" line). Record PASS or hang for each, the WARNING lines and the refused forks of each node.

### C10.3 RLlib functional (phase B, first RLlib tests)
1. **R1:** PPO CartPole, 4 env runners on 2 nodes (`num_cpus_per_env_runner` = half of the smallest node's CPUs), 2 training iterations. PASS expected.
2. **R3:** 2 learners with 1 GPU each on 2 nodes. Expected: LIMIT (RLlib's learners use `torch.distributed` through Ray Train's `TorchConfig`; the rendezvous uses a random port, as in C9.3.2). RLlib has no setting for the rendezvous timeout (1800 s), so the guard (1500 s) gives LIMIT when the learners are on 2 nodes in the build stage.
3. **R2:** 2 learners with 1 GPU each on one `gpu2` node. PASS expected.

### C10.4 Cost estimate
| Item | Time | Rate | Cost |
|---|---|---|---|
| Phase A worker (`big`), incl. 2 candidates at `up` | about 2 h (6 many tests × up to 15 min, 6 tuned, torch install) | $0.30–0.45/h | $0.60–0.90 |
| Phase B 2 `gpu` workers | about 1.5 h | $0.35/h | $0.55 |
| Phase B `gpu2` for R2 | about 0.3 h | $0.50–1.10/h | $0.15–0.35 |
| EC2 `t3.large` head, 2 clusters | about 4 h | $0.083/h | $0.35 |
| Host retry: up to 2 more `big` rentals (about 15 min each, incl. the image pull) | 0.5 h | $0.30–0.45/h | up to $0.25 |
| Total | | | about $1.70–2.50; re-runs ×1.5: under $4. The many tests now run to their guard (600–900 s) each: 6 × 15 min is already in the phase A row. |

### C10.5 Local check (dev box, before the rental)
First check, before `pids-tuning` (`local_check.py`, a plain Ray node, no ray-vast), 2026-10-16: one container per run, `--pids-limit 1280`, 48 Ray CPUs, `RAY_num_grpc_internal_threads=4`, `RAY_worker_maximum_startup_concurrency=8`, soft limit = ports unless noted. The local node also runs the head and the driver (about 460 pids before the job), so it has about 160 fewer pids for workers than a vast worker with the same limit. The local verdict uses the "No available ports" lines in place of the head WARNING line.

| Run | Ports | tuned | many | many: peak pids, worker processes, refused forks |
|---|---|---|---|---|
| Core | 24 | PASS, 10 s | HUNG at the guard | 1144, 32 (incl. failed starts), 0 |
| Data | 24 | PASS, 38 s | HUNG | 1161, 32, 0 |
| Tune | 24 | PASS, 74 s | HUNG | 1186, 30, 0 |
| Serve | 24 | PASS, 13 s | node CRASH (2 of 3 runs) or HUNG with 23 refused forks | 1280, 31, 15–23 |
| Serve | 20 | PASS, 12 s | HUNG | 1198, 27, 0 |
| Train | 12 | HUNG (idle workers hold the ports) | HUNG | 813, 19, 0 |
| Train, soft limit 2 | 12 | PASS, 24 s | HUNG | 824, 19, 0 |
| RLlib | 12 | PASS, 13 s | HUNG | 809, 18, 0 |
| RLlib R1, R2, R3 (1 node, CPU) | 12 | R1, R2, R3 PASS (code path only) | | |

Threads for each worker process (from these runs): Core actor or task about 25, Ray Train worker about 20, RLlib env runner about 26, **Serve replica about 35**. A raylet that cannot fork (`worker_pool.cc:731: Failed to start worker with return value system:11`) ends, so the node dies. A port count that uses 25 threads for each process is not safe for Serve replicas.

**With the real head and bootstrap** (`test_c10_local_e2e.py`, 2026-10-16, ray-vast at `pids-tuning` 2ab2adb): the step 11 harness (fake vast, real head server and provider, real bootloader and bootstrap), one worker container with `--pids-limit 1280` and 4 Ray CPUs (the box has 4 cores). The bootstrap gave Ray 14 worker ports and soft limit 4. The jobs ran through `ray job submit --working-dir` in the head container, `C10_SCALE=0.1` (many sizes = max(ports + 4, scaled size) = 18 to 20 processes). Guard 150–180 s. "Peak workers" counts the Ray worker processes in the worker container, with the processes that started and found no port.

| Test | Result | Time | Peak pids | Peak workers | Refused forks | WARNING line for the worker |
|---|---|---|---|---|---|---|
| core.tuned (5 actors, 0.4 CPU) | PASS | 16 s | 450 | 16 | 0 | no |
| core.many (20 actors) | PASS: HUNG | 158 s | 508 | 21 | 0 | yes: ports in use 14 of 14, pids 479 of 1280 |
| data.tuned (2 + 2 + 2 + 2) | PASS | 23 s | 387 | 11 | 0 | no |
| data.many, 1280 | **FAIL (expected, xfail)**: COMPLETED; 4 Ray CPUs give 4 aggregators < 14 ports | 30 s | 474 | 18 | 0 | no |
| data.many, 560 (4 ports) | PASS: HUNG | 159 s | 270 | 12 | 0 | yes: ports in use 4 of 4 |
| tune.tuned (`max_concurrent_trials` 5) | PASS | 36 s | 435 | 15 | 0 | no |
| tune.many (18 trials) | PASS: HUNG | 159 s | 530 | 21 | 0 | yes: ports in use 14 of 14 |
| serve.tuned (5 replicas) | PASS | 22 s | 345 | 8 | 0 | no |
| serve.many (18 replicas) | PASS: HUNG, no crash | 160 s | 628 | 21 | 0 | yes: ports in use 14 of 14, pids 601 of 1280 |
| train.tuned (5 workers) | PASS | 34 s | 359 | 10 | 0 | no |
| train.many (18 workers) | PASS: HUNG | 191 s | 503 | 21 | 0 | yes: ports in use 14 of 14 |
| rllib.tuned (2 runners × 9 envs) | PASS | 21 s | 186 | 4 | 0 | no |
| rllib.many (18 runners) | PASS: HUNG | 188 s | 513 | 20 | 0 | yes: ports in use 14 of 14 |

In every many run, the worker stayed enrolled, `pids.events` stayed at `max 0`, and the worker's `worker-*.err` files had 16–24 "No available ports" lines. The tuned runs had 0–2 such lines and no WARNING line. The first tune.many run used 8 trials (scaled 60 × 0.1) and completed: the many sizes are now max(ports + 4, size).

**Advice (the tuned settings), as a rule on the node's ports** (budget = ports − 4; 14 ports at pids.max 1280 → budget 10; 48 ports at 3328 → budget 44):

| Library | Advice for each node |
|---|---|
| Core | Tasks and actors that run at the same time ≤ budget. An actor or task that waits for another task needs a process for both: an actor pool of budget // 2 when each actor calls tasks. Set `num_cpus` = CPUs / budget so the scheduler keeps the count. |
| Data | `DataContext.max_hash_shuffle_aggregators`, the read `concurrency`, `map_batches(compute=TaskPoolStrategy(size=...))` and `execution_options.resource_limits = ExecutionResources(cpu=...)`: budget // 4 each. |
| Tune | `TuneConfig(max_concurrent_trials = budget // (1 + tasks that a trial runs at the same time))`. |
| Serve | `num_replicas` on one node ≤ budget // 2. |
| Train | `num_workers` on one node ≤ budget // 2. |
| RLlib | Few env runners (here 2) and more `num_envs_per_env_runner`; env runners + learners on one node ≤ budget // 2. |

### C10.6 Teardown and outputs
- Teardown as for all cycles, for each cluster: copy the head logs, the secret scan, `ray-vast down`, `vast_tool.py sweep` until 0, the AWS check.
- Each test has a row in the Results table of `test-results.md`: id, status, key numbers (ports, `pids.max`, processes, threads, pids peak, time), the evidence path.
- The advice table for the user docs (Reference → Limits): the tuned setting of each library, as a rule relative to the node's port count.

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
| L7 | | each C9 sub-test: PASS / FAIL / LIMIT, key numbers | C9 |
| L8 | Done (2026-10-16) | 12 of 12 pair runs as expected except C10.data.tuned (FAIL; C10.data.tuned2 PASS); C9.1.3 and C9.1.4 FAIL without a crash; R1 PASS, R2 PASS (run 3), R3 LIMIT; vast $1.56 | [test-results.md](test-results.md), `archive/results/2026-10-16/` |
