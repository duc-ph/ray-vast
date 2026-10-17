# Test plan

The test levels, the safety rules for paid tests, and the live suite with its claim matrix. Each user-facing claim of the docs has one scenario that proves it, on the cheapest cluster that can prove it. The results are in [test-results.md](test-results.md). The raw data is in `archive/results/`.

The live cycles C0–C10 ran by hand before the suite existed. Their plans are in the git history of this file (for example `git show 6ee09d1:docs/test-plan.md`), and their results are in [test-results.md](test-results.md). The "Origin map" below gives the scenario that replaces each old sub-test.

## Test levels

| Level | What | Where | Cost | When |
|---|---|---|---|---|
| L1 Unit | pure logic, fake clock, `responses`, `moto` | dev box | $0 | each change (fast run) |
| L2 Integration | provider + Ray v2 `NodeProviderAdapter` + fake vast; head server + bootstrap processes; bootloader in Docker | dev box | $0 | full run, once per step |
| L3 Local end-to-end | real Ray, real bootloader and bootstrap in Docker, a fake vast that runs containers (step 11) | dev box | $0 | from step 11 |
| L4 Live suite | `ray-vast-pypi/tests/live/`: the claim matrix below | dev box, AWS, vast | free $0; aws < $0.40; lifecycle about $2.5; libraries about $5 | before a release |

The commands for the fast and the full run are in [CONTRIBUTING.md](../ray-vast-pypi/CONTRIBUTING.md).

## Not tested live (decided with the user, 2026-10-13)

- An IP change (C7): real vast cannot force one. Unit tests and step 11 cover it (limits.md and worker-lifecycle.md say "unit-tested").
- More than about 299 worker IPs (5 full security groups): unit tests only (limits.md says so).
- Images other than `rayproject/ray` (root images): later. Images with an ENTRYPOINT are in scope: `rayproject/ray:*-gpu` has one, and the `aws_only`, `lifecycle` and `libraries` clusters use it.
- Attacks on `/enroll` (wrong token, wrong IP, unknown instance): unit and integration tests cover them (F06).
- The worker's 10 min enrollment deadline: step 11 covers it (F11, F12).

## Safety rules for paid tests

1. **Approval.** The user approved autonomous rentals for testing (2026-10-13). The suite records the offers it rents in each cluster's `vast_timeline.jsonl`.
2. **Credit.** Each run records the vast credit before and after, corrected for the auto top-ups between (`_session/runs/<run>/spend.json`). Each cluster group records an estimate from its own instances (`_session/<group>/spend.json`), because in a parallel run the credit moves for all groups at once. The $8 per UTC day limit is a soft guide. The credit is not a reason to skip a test.
3. **Labels.** The provider labels its instances `ray-vast:<cluster>`. The suite's clusters are `rvtest-<group>-<hex>` (`rvtest-<group>-keep` with `--keep-cluster`). Instances that a test rents without the provider have the label `ray-vast-test-<YYYYMMDD>`.
4. **Teardown** of each cluster: one module, `livekit.end_cluster`, ends every cluster of the run (the group clusters, a scenario's own clusters, the release cluster). It runs `down` (again if something is left), unless the cluster is kept or its last `down` was already clean. Then it does the vast label sweep of that cluster, the independent AWS check of that cluster (as the admin, in its region), and the secret scan of that cluster's records. At the end of the run, once, after every xdist worker ended, only the run-wide guards run: a label sweep of `ray-vast-test-<date>` (and of a cluster that did not end), the account-wide AWS check of the suite names, and the summary. A kept cluster (`--keep-cluster`) gets no `down`, sweep or AWS check; the run prints a `KEPT CLUSTER` line with its `down` command.
5. **Names.** The suite changes or deletes only what it made: names that match `ray-vast-rvtest-*`, `ray-vast-livetest-<date>` or `ray-vast-livetest-<date>-s3-<host>` (`livekit.assert_ours`). It never touches the dev box `ray-vast-dev`, the key pair `ray-vast-devbox`, the two account policies (except A02, which deletes the boundary and makes it again with the documented command), or anything named badminton.
6. **Logs from workers.** After a destroy, vast keeps no container log (C4). A test fetches a worker's log before its destroy.
7. **Secrets.** The run scans all kept data for the cluster tokens, the join tokens, the IAM test users' secret keys, the vast key, `PRIVATE KEY`, `instance_api_key` values, `CONTAINER_API_KEY=` values and `Bearer ` values. The scan reads directories by ownership, never by a name glob: the end of each cluster scans the records of that cluster (its own directories and the record of each test that used it). Each process then scans its records that no cluster owns (`Run.scan_unowned`, `_session/runs/<run>/scan_<worker>.txt`). The scans write only the file names. Never copy `secrets/` from the head, or a packet capture.

## Live suite

Run from `ray-vast-pypi/` on the dev box, with the admin AWS profile `personal` and the vast key:

```bash
.venv/bin/pytest tests/live -m free -n 12                                      # about 6 min (38 min without -n), $0
.venv/bin/pytest tests/live -m aws -n 8                                        # about 12 min, EC2 under $0.40
.venv/bin/pytest tests/live -m lifecycle -n 2                                  # about 2 h, vast about $2.5
.venv/bin/pytest tests/live -m libraries -n 4                                  # about 4.5 h, vast about $5
.venv/bin/pytest tests/live -m "free or aws or lifecycle or libraries" -n 16   # all at once: about 4.5 h
.venv/bin/python tests/live/summarize.py                                        # the table into test-results.md
```

- **Parallel runs.** `-n N` (pytest-xdist) runs the tests in N worker processes; the suite sets `--dist loadgroup` and turns off the xdist reorder by group size, so xdist gives out the groups in the collection order. Each cluster is one xdist group with the tests that share its setup, and each declared need is one group (`livekit.xdist_group`, from markers and fixtures only): `aws_only`, `lifecycle` (with the restricted user), one group for each libraries host (`libraries-h1`, `-h2`, `-h3`, `-gpu2`), `release`, `exclusive_account` and `box_disk`. One worker owns each group, so each cluster has one `up` and one `down`. The tests of a group run in the file order. The other tests go to any worker. Without `-n`, the tests run one after the other in the file order, with the `exclusive_account` tests first.
- **`--keep-cluster`** (for debugging): the clusters are named `<name>-keep`, with their HOME in `~/.cache/ray-vast-live/<group>`. A later run with the option reuses a cluster while its head runs. No `down` runs, the scenarios that end a cluster (A01, L10) skip, and the group teardown checks only the S3 cleanup and the secret scan. A run without the option ends a kept cluster of its groups first, then makes a fresh one: run the final run without it.

- **One test for each scenario.** The name starts with `test_<ID>_`. A parametrized scenario has one run for each host (`LB03[H1]`).
- **Markers** select groups: `free`, `aws`, `lifecycle`, `failure`, `security`, `limits`, `libraries`, `release`. `-m "aws or lifecycle"` starts only the clusters that the selected tests use. Every test in `tests/live` also has the marker `live`, so the default `pytest` run skips them.
- **Declared needs.** A test states a need with a marker (`livekit.NEEDS`). The kit never tests a scenario ID (a unit test fails when an ID of this matrix is in `conftest.py`, `livekit.py`, `cluster.py`, `libraries_setup.py`, `restricted.py` or `summarize.py`). `exclusive_account`: the test needs the account with no ray-vast role and can change an account-wide resource (A02). It runs first, and every cluster `up` of the run (also a scenario's own cluster from `new_cluster`) waits until each such test ended (at most 20 min, then the `up` fails). It cannot use a cluster fixture. `box_disk`: the test builds or installs the package on the dev box (up to about 1.5 GB of disk; F17, F18, F20). These tests run in one process, one after the other, and share one wheel build. A test in two groups is a usage error.
- **Run context.** `livekit.Run` (the `run` fixture) holds what the scenarios of one run share: the run identity, the record root, the secret sink and the clusters of the process. A test gets it as an argument. Each cluster takes it as its first argument and joins the run when it is made (`Run.register`, the only place).
- **Cluster fixtures**: `aws_only`, `lifecycle`, `libraries` (one cluster for each host), `release`. Each cluster runs `up` once, at its first test, and ends at the end of its worker process, also after a failed `up`, a test failure, Ctrl-C or SIGTERM (`conftest.group_lifetime`). The order of the tests in a module keeps the destructive scenarios last on their cluster. A scenario that makes its own clusters (A02, A03) gets them from the `new_cluster` factory; they end at the end of the test, and a problem is a false check of the scenario.
- **Two cluster adapters.** `Cluster` is a suite cluster (a suite name, the CLI of the source tree, a YAML from `tests/live/clusters/`). `ReleaseCluster` is the release cluster: the user's tutorial verbatim, so a foreign name (`gpu-24gb`), the plain CLI of a clean venv with ray-vast from PyPI, and the packaged YAML. Both join the run and end through `livekit.end_cluster`.
- **Group teardown.** After the last test of a cluster group, `livekit.end_cluster` ends its cluster, with the checks of the group (table "Group teardown checks"). The result goes to `_session/<group>/teardown/result.json`. A problem fails the run.
- **Records.** Each test writes `archive/results/<UTC date>/<ID>/result.json` (the outcome, every check with its detail, the numbers) and its evidence files. `summarize.py` builds the results table only from these records: a scenario without a record is NOT RUN, and a record with a false check is FAIL.
- **Group data** in `archive/results/<date>/_session/<group>/`: `cluster.yaml`, `cli.log` (every `ray-vast` command and its output), `vast_timeline.jsonl` (the cluster's instances every 10 s, without env), `spend.json`. **Run data** in `_session/runs/<run>/`: `opened/` (the clusters of the run), `ended/` (the problems of each cluster end), `scan_<worker>.txt`, `sweep.log`, `aws_account_check.log`, `secret_scan.txt`, `spend.json`, `problems.txt`.
- **Consistency.** The unit test `test_the_claim_matrix_and_the_live_suite_have_the_same_scenarios_markers_and_fixtures` (tests/unit/test_live_kit.py) fails when this matrix and the tests differ in an ID, a marker or a fixture.

### Fixtures

| Fixture | Cluster | Profile, region | Cost |
|---|---|---|---|
| `none` | No cluster. Unit, integration and local Docker runs, the public vast offer search, plain AWS calls, or a short own cluster (A02, A03). | admin (A10: the restricted user) | $0; A03 under $0.10 EC2 |
| `aws_only` | `tests/live/clusters/aws_only.yaml`: the head only, `rayproject/ray:2.59.0-py311-gpu` (it has an ENTRYPOINT). Worker type `impossible` (no offer matches) and `poor` (`max_dollars_per_hour` 0.001). | admin, us-west-2, `t3.large` | EC2 about $0.15 |
| `lifecycle` | `tests/live/clusters/lifecycle.yaml`: the C8 shape. 2 GPU types × 2 workers, `candidate_factor` 2, the GPU image, no top-level `max_workers`, `idle_timeout_minutes` 2. | the restricted user `ray-vast-livetest-<date>` (fixture `restricted_user`), **us-east-2**, **`t3.medium`** | vast about $2.5, EC2 $0.10 |
| `libraries` | One cluster for each host, from `tests/live/clusters/libraries.yaml` with only the worker type of that host: H1 (32 or more Ray CPUs, any pids limit: the large-host case; vast sets `pids.max` = 256 × the CPU quota in whole cores, so many CPUs and 1280 do not occur together), H2 (a cgroup v2 host), H3 (2 `gpu` workers), `gpu2` (one 2-GPU host). A test names its host with its `where` parameter or the `host` marker. For each host with S3 jobs, an S3 test bucket and a least-privilege IAM user for `env_vars` (`LibrariesCluster.s3()`; deleted at the group teardown). Hosts pinned by `machine_id`; a host that does not match its probe is ended and blocked, at most 3 rentals (`livekit.find_host`). | admin, us-west-2 | vast about $5, EC2 $1.30 (4 heads) |
| `release` | The packaged `gpu-24gb.yaml` (only the region can change), a clean venv, `pip install ray-vast "ray[default]==2.59.*"` from PyPI. Runs only with `RAYVAST_LIVE_RELEASE=<version>`, after the publish. | admin | vast under $1 |

## Claim matrix

Doc references: `getting-started` = `ray-vast-pypi/docs/getting-started.md`; other names are files in `ray-vast-pypi/docs/` (`architecture.md` is in `explanation/`, `cli.md` in `reference/`, and so on); `README` = `ray-vast-pypi/README.md`; `CHANGELOG` = `ray-vast-pypi/CHANGELOG.md`; `impl-plan` = `docs/implementation-plan.md`. Line numbers are at commit 6ee09d1 plus the doc fixes of the suite commit.

### free

| ID | Name | Markers | Fixture | Claims proved | Pass criterion | Origin | Destructive |
|---|---|---|---|---|---|---|---|
| F01 | Shim rules and mutants | free | none | architecture.md:86-97; shim.md rules, IPv6, errno, log | 14 shim tests pass on the committed `.so` (SHA-256 `79055dea…`); mutation run: only the 2 known equivalent mutants survive | C0.1, step 10c | no |
| F02 | Ray node with a fake public IP and the shim (Docker) | free | none | architecture.md:88-89, :94 | With the shim: node ALIVE, a job runs a task, 0 `Errno 99`. Control without the shim: `Errno 99` or no job | C0.2 | no |
| F03 | Config and CLI checks | free | none | configuration.md:16-115 (old fields :46-54); node-types.md:22-25, :54-58, :108-110; cli.md:7-9, :27-41, :51, :138, :194; security.md:93-94; troubleshoot.md:129; clean-up.md:19 | Config and CLI unit tests pass (with the 0.1.0 `down` tests, the missing-key error, "already exists"). Each removed `provider` field: `up` exits 1 with "is no longer a field (<what to do>)". `--index-url`, `--key-name`, `--key-file`: exit 2, "unrecognized arguments". `--help` of each command works | audit B | no |
| F04 | Provider state logic | free | none | worker-lifecycle.md:26-41, :67; control-cost.md:8-9; enrollment.md:66-75; architecture.md:53-54, :112; gpu-images.md:59; limits.md:7, :11-12; CHANGELOG:31, :33 | Provider, state, SG and vast unit tests pass, with the listing grace (60 s after the last listing), the SG race, the breaker, the blocklist, the budget, the full-groups error, the sweeps, and the heartbeat limit equality (provider = bootstrap = 300 s) | audit E | no |
| F05 | Bootstrap logic | free, limits | none | limits.md:23, :32-38; troubleshoot.md:70-83; security.md:72; CHANGELOG:32, :34 | Bootstrap tests pass, with the cgroup v1 CPU quota (period 0 and -1 = no limit), one call thread for each URL, a thread start failure as a network error | audit C | no |
| F06 | Head server and PKI | free, security | none | enrollment.md:46-47, :54-75, :79-82; security.md:37-55, :173; limits.md:75-76; CHANGELOG:10, :36 | Head server and PKI tests pass; a test asserts the exact `/enroll` answer keys (no `ca_pem`) | audit F | no |
| F07 | Bootloader in Docker | free | none | gpu-images.md:12, :36; worker-lifecycle.md:49-50, :72; CHANGELOG:35 | Worker container tests pass (hash mismatch, no Ray Python, exec ENTRYPOINT, signals, zombies); a missing `CONTAINER_ID` or `CONTAINER_API_KEY` logs and waits | step 10b | no |
| F08 | Local e2e: scale up 3 with k = 2, battery, idle scale-down | free | none | worker-lifecycle.md:5-13; control-cost.md:40 | The local e2e test passes | LE1 | no |
| F09 | Local e2e: two workers on one public IP | free | none | design-decisions.md:22; limits.md:9 | The local e2e test passes | LE2 | no |
| F10 | Local e2e: more cores than declared | free | none | node-types.md:29-31; core.md:41 | The local e2e test passes | LE3 | no |
| F11 | Local e2e: a candidate never enrolls | free | none | worker-lifecycle.md:36-37 | The local e2e test passes | LE4 | no |
| F12 | Local e2e: head silent | free | none | worker-lifecycle.md:50, :57, :65, :82; security.md:88 | The local e2e test passes | LE5 | no |
| F13 | Local e2e: container restart | free | none | worker-lifecycle.md:63, :83 | The local e2e test passes | LE6 | no |
| F14 | Local e2e: the monitor stops | free | none | worker-lifecycle.md:89-90; design-decisions.md:11, :14 | The local e2e test passes | LE7 | no |
| F15 | Local e2e: rent-loop breaker | free | none | troubleshoot.md:44; worker-lifecycle.md:40-41 | The local e2e test passes | LE8, C5a | no |
| F16 | Local C10 pairs with the real head and bootstrap | free, limits | none | troubleshoot.md:60 (WARNING format); the CPUS / B rule (core.md:42, serve.md:71-73) | core tuned PASS, core many HUNG with the WARNING line, data many at `pids.max` 560 (4 ports) HUNG, serve and core with B processes of CPUS / B PASS; the WARNING line has the documented format | C10.5 | no |
| F17 | Wheel and sdist | free, release, box_disk | none | getting-started:64, :101, :137; design-decisions.md:21; impl-plan:144 | `twine check --strict`; the wheel has the shim (same SHA-256), the worker files, the 3 examples and the 2 IAM files; `Requires-Dist: ray[default]>=2.59`; no secret | TestPyPI Build, Scan | no |
| F18 | Python 3.10 install | free, release, box_disk | none | README:29-30; getting-started:8, :10 | In `python:3.10`: the wheel installs, `ray-vast --help` works, the fast unit run passes (in an editable source tree next to `docs/test-plan.md`, the layout that the unit tests need) | audit A | no |
| F19 | Offer queries of the examples and the docs | free | none | the 3 example YAMLs; node-types.md:8-20, :46-52, :67-80, :94-100; troubleshoot.md:104; core.md:76 | Each example type has ≥ 4 offers on ≥ 3 machines, and its first `candidate_factor` offers fit `max_dollars_per_hour`; vast accepts each query of node-types.md | audit A, C | no |
| F20 | Doc snippets in Ray 2.59 | free, box_disk | none | core.md:47-60, :71-72; data.md:51-64; tune.md:49-60; train.md:49-69; serve.md:54-66; rllib.md:47-56, :60-71 | Every Python block of the library pages compiles; their imports work in the Ray 2.59 GPU image (torch 2.7.1 from pip); the core blocks run on a local node | audit C | no |

### aws

| ID | Name | Markers | Fixture | Claims proved | Pass criterion | Origin | Destructive |
|---|---|---|---|---|---|---|---|
| A01 | Head-only life cycle with a saved profile | aws | aws_only | cli.md:49-71, :82-83, :107-140, :148, :160-183; getting-started:16, :41-44, :155-191, :246, :252-265; gpu-images.md:32-36; CHANGELOG 0.1.0 (line-buffered output) | `up --profile` with no `AWS_PROFILE`: the documented output lines (first line: the source). State file: region, profile, head, 5 groups, role, instance profile. `ssh_key.pem` 600 in a 700 directory. Head: `t3.large`, Ubuntu 24.04 amd64, 60 GiB gp3, IMDSv2, no Elastic IP; every resource tagged; the role has the boundary; `/dev/shm` = 0.33 × RAM; the head YAML has the measured `image_size_gb`. `attach` and `dashboard` with the saved profile; the export line arrives while the tunnel runs; token file 600; Ctrl-C prints `tunnel closed`, exit 0; `AWS_PROFILE=bogus` and `--profile bogus` win and fail; the missing-key error. 0 vast rentals. `down` with no flag: the documented lines, `left: nothing`, state files gone; the AWS check after it is the group teardown's | C5r/C6/C6r Shm, C8r Up, test_up_down, audit B | no (`down` is its last step) |
| A02 | Boundary policy missing | aws, security, exclusive_account | none | getting-started:47-60; cli.md:52 | With no ray-vast role: delete `ray-vast-head-boundary`; `up` exits 1 with the exact message and link; nothing is made, no state file. The documented `aws iam create-policy` command makes the policy again (always) | audit B | yes (account policy; runs before every cluster `up`: `exclusive_account`) |
| A03 | `down` recovery | aws, failure | none | clean-up.md:11-19; cli.md:136-139 | (a) Ctrl-C of `up` after the key pair → `down` gives `left: nothing`. (b, c, d) Ctrl-C after the head starts, the head terminated by hand (`attach` then says "no running head"), the state file and the SSH key deleted → `down --profile` gives `left: nothing`. The end of the test checks AWS and vast after each `down` (`new_cluster`) | C5a, audit B | no |
| A05 | Head network exposure | aws, security | aws_only | architecture.md:43, :99-112; security.md:76-78; jobs.md:126; limits.md:95 | From the operator machine: 6379, 7443, 7444, 8265, 10001, 6384, 6385 closed. `ss -ltn` on the head: 8265, 6384, 6385 on 127.0.0.1 only. During a job, 0 packets to the head's own public IP | T9, audit D/F | no |
| A07 | The boundary caps the head role | aws, security | aws_only | getting-started:47-50; iam.md:78, :91; security.md:82-83 | With an extra inline policy on the head role, from the head (IMDS): `s3:ListAllMyBuckets`, `ec2:CreateSecurityGroup` and a rule on an untagged group are denied; a rule on a cluster group works | audit B | yes |
| A08 | No offer and budget texts | aws | aws_only | control-cost.md:16-18; troubleshoot.md:31-44 | Demand for `impossible` and `poor`: `monitor.log` has both documented texts; `ray status` shows `LaunchFailed`; 0 vast rentals | audit E | no |
| A09 | Token on every Ray call | aws, security | aws_only | jobs.md:39, :126; troubleshoot.md:116-118 | `ray job submit` without `RAY_AUTH_*` fails; with the export line, `JobSubmissionClient` submits and the job SUCCEEDED | audit D | no |
| A10 | Restricted user denials | aws, security | none | getting-started:79-128; iam.md:37; security.md:84 | As the restricted user: `CreateRole` without the boundary, with another boundary, with a name not `ray-vast-*`; `PutRolePolicy` on another role; `CreatePolicy`; `DeleteRolePermissionsBoundary`; an untagged `CreateSecurityGroup` → all denied | audit B, live IAM test | no |

### lifecycle

| ID | Name | Markers | Fixture | Claims proved | Pass criterion | Origin | Destructive |
|---|---|---|---|---|---|---|---|
| L01 | `up` as the restricted user, in another region | lifecycle, security | lifecycle | getting-started:75-128 (the user commands); configuration.md:66-70; iam.md:18-35 | The user is made with the documented commands. `up` exits 0. The head runs in us-east-2 as `t3.medium`. The role has the boundary. The state file holds the restricted profile | C5a, C8r Up, C9.0.1, IAM test | no |
| L02 | Scale over 2 types, idle scale-down | lifecycle | lifecycle | control-cost.md:8, :34-40; worker-lifecycle.md:5-29; configuration.md:21; iam.md:78; gpu-images.md:59; README:8 | `c8_job.py`: 4 one-GPU tasks in 4 containers, both types, each 1 GPU and `get_gpu_ids() == [0]`; rentals ≤ 8; peak $/h ≤ 3.0; enroll within 1 min of `running` (design.md:86; −15 to 75 s on the suite's 10 s samples); SG rules added, then all revoked; idle scale-down to 0 within `idle_timeout` + 300 s (safety bound; the time is data); no pull-limit or `missing` ending; no unknown status | C8r L6, C5, C6, T8 | no |
| L03 | Host and task probe on each worker | lifecycle, security, limits | lifecycle | enrollment.md:40-48; security.md:142-147; limits.md:32-38; troubleshoot.md:75-83; core.md:41; architecture.md:62 | On each of 4 workers: uid 1000; Ray 2.59.0; no secret in `os.environ`; PID 1 is the bootstrap (GPU ENTRYPOINT); `/tmp/rayvast` 700, key and token 600; no NET_RAW/NET_ADMIN in CapBnd; Ray CPUs = real cores; worker ports = the formula of `pids.max`; soft limit, gRPC 4, startup 8; 1 GPU device. Container log: the start line and all 4 host checks True. Recorded per host: `pids.max`, cgroup, CPU quota, threads of each worker process | C5 T15, Task, CapBnd; C8r; C2 | no |
| L04 | Network battery, TLS and token | lifecycle, security | lifecycle | README:9-10; architecture.md:66-97; security.md:12, :70-72, :153-160; enrollment.md:48 | C2 battery (actor RTT, pip, 50 MB both ways, worker to worker) SUCCEEDED. Agent and runtime-env ports: 401 without the token, 127.0.0.1 only. No token bytes in a capture on the head's public interface. The metrics port answers without auth | T1–T5 | no |
| L05 | Logs a user reads | lifecycle | lifecycle | troubleshoot.md:3-26; the start line of the library pages | `monitor.log` has the rent, enrolled and ending lines; the legacy-field lines are not errors; the documented `grep "process limit"` runs through `attach` | C5a, audit C | no |
| L07 | Signals to PID 1 | lifecycle, failure, security | lifecycle | worker-lifecycle.md:62, :80-81; security.md:145-146 | KILL, QUIT, USR1, USR2, CONT: same PID 1 after 120 s, 0 zombies. TERM: the head logs the report, gone from vast ≤ 60 s (safety bound), Ray marks the node DEAD (safety bound 300 s), a replacement joins. The times are data | C6 T13, T7 | yes |
| L10 | Head loss, then `down` without a vast key | lifecycle, failure | lifecycle | README:19; index.md:48; worker-lifecycle.md:60, :87-93; clean-up.md:43-51; control-cost.md:72; cli.md:140 | Stop `ray-vast-head` with a worker enrolled and a candidate loading: each worker gone ≤ 360 s (design.md:258, :261), each candidate ≤ 30 min (design.md:144, :260, after its pull). `down` without a vast key: the warning, exit 0; the group teardown checks AWS and vast after it | audit E | yes (last) |

### libraries

| ID | Name | Markers | Fixture | Claims proved | Pass criterion | Origin | Destructive |
|---|---|---|---|---|---|---|---|
| LB00 | Host probe on every libraries host | libraries, limits | libraries | limits.md:29, :34, :36 | One run for each host (`LB00[H1]`, `[H2]`, `[H3]`, `[gpu2]`). Each host recorded: `pids.max`, cgroup, CPU quota, ports, soft limit, threads of each worker process, enroll time. H1: 32 or more Ray CPUs, a pids limit. H2: v2 | audit C | no |
| LB01 | Jobs workflow | libraries | libraries | jobs.md:8-31, :52-69, :103 | `--working-dir` file read on each node; `runtime_env` pip torch with CUDA on 2 GPU nodes; `ray job list`, `logs --follow`, `stop` → STOPPED | C9.0.2–C9.0.4 | no |
| LB02 | Core functions and the core snippet | libraries, limits | libraries | core.md:7-8, :43-75; jobs.md:132 | On H3: PACK and STRICT_SPREAD, actor restarts, 2 GB object between hosts (MB/s recorded); the core snippet with the hosts' CPUS / B completes with no WARNING | C9.5.1–C9.5.3, audit C | no |
| LB03 | Data snippet, verbatim | libraries, limits | libraries | data.md:8, :39-77 | On H1 and on H3: S3 parquet (about 3 GB) with `env_vars` keys of the least-privilege user, the snippet settings at the host's B: SUCCEEDED, output in S3, no refused fork, no crash | audit C | no |
| LB04 | Data shuffles with the tuned settings | libraries, limits | libraries | data.md:7, :75-77, :91-92; README:72 | On H3 with the data.md settings: C9.1.3 and C9.1.4 SUCCEEDED | C9.1.3, C9.1.4, C10 | no |
| LB05 | Data defaults without a shuffle | libraries | libraries | data.md:73, :93; jobs.md:129 | On H3, defaults: C9.1.1, C9.1.2, C9.1.5, C9.1.6 pass | C9.1.x, C10 | no |
| LB06 | Tune snippet with S3 | libraries, limits | libraries | tune.md:7-8, :47-68, :82-84; limits.md:65-71 | The tune.md snippet with S3 and `env_vars`: all trials, 0 errors, trials on both nodes, checkpoints in S3; ASHA stops early; GPU trials 1 GPU each; the default `storage_path` and anonymous S3 give the documented errors | C9.2.1–C9.2.4 | no |
| LB07 | Train snippet with 2 GPUs and S3 | libraries, limits | libraries | train.md:7, :47-77, :91-93; limits.md:63 | On `gpu2`: the train.md snippet (NCCL, STRICT_PACK, 2 workers, S3): checkpoint in S3, both GPUs, no WARNING; the default storage gives the documented error | C9.3.1, C9.3.4 | no |
| LB09 | Serve snippet, HTTP and autoscaling | libraries, limits | libraries | serve.md:7-8, :19-34, :50-74, :81-82, :88 | On H3: `num_replicas` = B × workers with `num_cpus` CPUS / B → at most B on each worker; HTTP through the serve.md tunnel: all 200, also after the job; `curl` on the head :8000 → 200; GPU replicas 1 GPU each; autoscaling up to B × workers | C9.4.1–C9.4.4, audit C | no |
| LB10 | RLlib env runners and learners | libraries, limits | libraries | rllib.md:7-8, :41-82 | `LB10[H3]`: the env runner snippet (2 runners, CPUS / B) runs 2 iterations, no WARNING. `LB10[gpu2]`: the learner snippet → 2 iterations, 0 errors (`num_cpus_per_learner` recorded) | C10 R1, R2, audit C | no |
| LB11 | Process-limit pairs on H1 and H2, diagnostics, refused forks | libraries, limits | libraries | every library "Settings", "What does not work" and "If it fails" table; limits.md:40-47; troubleshoot.md:9, :55, :85-89 | On H1 and on H2, for core, data, tune, serve, train and rllib: tuned (the documented values at that host's B) completes, node alive; many hangs or fails with the head WARNING `ports in use N of N`, node alive. The refused forks of each pair are data, not a pass criterion. During `serve.many` on H1, the 3 diagnostics steps: the dashboard log API opens a `worker-*.err` with `No available ports`; `ray job logs` shows the documented text; the WARNING names the worker. Then a task starts threads until `EAGAIN`: the refused forks, the WARNING and the node's fate are data, not a pass criterion | C10 phase A, T11, audit C, C9 pids finding | yes (many, forks) |

### release

| ID | Name | Markers | Fixture | Claims proved | Pass criterion | Origin | Destructive |
|---|---|---|---|---|---|---|---|
| R01 | Install from PyPI and `up` | release | release | impl-plan:140, :143; README:24; getting-started:20-29, :64, :137, :161 | Clean venv, `pip install ray-vast "ray[default]==2.59.*"`; the `cp` command; the first `up` line is `installing ray-vast==<v> from PyPI`; the head has `<v>` | TestPyPI Upload, Install, Up | no |
| R02 | The getting-started tutorial, verbatim | release | release | getting-started:3-4, :175-265; README:38-54 | `hello.py` prints a host name and `[0]`; `ray status` through `attach`; Ctrl-C prints `tunnel closed`; the `down` output matches (the group teardown then checks AWS and vast, with no second `down`); under 30 min | TestPyPI Job, Down, audit A | no |

### Group teardown checks

These checks are not scenarios. They run at the end of each cluster group, in `livekit.end_cluster` (called by `conftest.group_lifetime`). They replace L11 and LB14 (2026-10-17). The same module ends a scenario's own clusters (A02, A03) and the release cluster (safety rules 4 and 7).

| Group | Claims proved | Checks |
|---|---|---|
| `aws_only`, `lifecycle`, each `libraries-<host>`, `release` | cli.md:107-140; getting-started:252-265; troubleshoot.md:21 | The `down` exits 0 with `left: nothing` (the scenario's `down` for A01, L10 and R02: no second `down`); the label sweep of the cluster finds no instance; the AWS check of the cluster is empty; the scan of the cluster's records finds no secret |
| each `libraries-<host>` | cli.md:125 ("destroyed N vast instance(s)") | One worker runs at the `down`, and `down` prints `destroyed N vast instance(s)` with N ≥ 1; the host's S3 bucket and IAM user are deleted (H2 has none) |

### Scenarios removed on 2026-10-17

Decided with the user (decisions.md, 2026-10-17). Each claim keeps the proof in the last column.

| ID | Name | Claims | Proof now |
|---|---|---|---|
| A04 | Cluster already exists | troubleshoot.md:129; cli.md:51 | F03 (the "already exists" unit tests); live suite 2026-10-16 A04 PASS (`archive/results/2026-10-16/A04/`) |
| A06 | A task on the head can read the head secrets | security.md:20, :23, :137-138 | live suite 2026-10-16 A06 PASS (`archive/results/2026-10-16/A06/`) |
| A11 | `down` of a 0.1.0 cluster | cli.md:7-8, :138; clean-up.md:19; configuration.md:54; CHANGELOG (fixes) | F03 (the 0.1.0 `down` unit tests; the product code stays); live suite 2026-10-16 A11 PASS (`archive/results/2026-10-16/A11/`) |
| A12 | Ray cluster commands do not work | README:62; limits.md:93; jobs.md:127; design.md 10 | live suite 2026-10-16 A12 PASS (`archive/results/2026-10-16/A12/`) |
| A13 | An ARM head type fails | limits.md:106-108 | live suite 2026-10-16 A13 PASS (`archive/results/2026-10-16/A13/`) |
| L06 | `CONTAINER_API_KEY` scope | security.md:111-119, :140-147, :168 | live suite 2026-10-17 L06 PASS (`archive/results/2026-10-17/L06/`); T10 |
| L08 | Heartbeat block | worker-lifecycle.md:57, :65, :82; security.md:88 | F12 (local e2e: head silent); C6 T14 |
| L09 | vast reboot of a worker | worker-lifecycle.md:63, :83 | F13 (local e2e: container restart); C6 Restart |
| LB08 | Multi-node DDP and learners: LIMIT | train.md:8, :91; limits.md:51-61; rllib.md:97; README:73, :76 | C9.3.2, C9.3.3, C9.3.5 and C10 R3 (test-results.md) |
| LB12 | Diagnostics during a many run | "If it fails" of each library page; troubleshoot.md:9 | LB11 (the checks during `serve.many` on H1) |
| LB13 | Refused forks | troubleshoot.md:57-73; limits.md:30, :47; worker-lifecycle.md:61 | F16 (the WARNING format), F05 (a thread start failure); the C9 pids finding; LB11 records the refused forks as data |
| L11, LB14 | Teardown | cli.md:107-140, :125; getting-started:252-265; troubleshoot.md:21 | The group teardown checks (above) and the end-of-run guards |

## Counts

| Marker | Scenarios |
|---|---|
| free | 20 |
| aws | 8 |
| lifecycle | 7 |
| libraries | 11 |
| release | 4 (F17, F18, R01, R02) |
| security | 10 (F06, A02, A05, A07, A09, A10, L01, L03, L04, L07) |
| failure | 3 (A03, L07, L10) |
| limits | 12 (F05, F16, L03, LB00, LB02, LB03, LB04, LB06, LB07, LB09, LB10, LB11) |
| exclusive_account (a need) | 1 (A02) |
| box_disk (a need) | 3 (F17, F18, F20) |

| Fixture | Scenarios |
|---|---|
| none | 23 (F01–F20, A02, A03, A10) |
| aws_only | 5 (A01, A05, A07, A08, A09) |
| lifecycle | 7 |
| libraries | 11 |
| release | 2 |
| **Total** | **48** |

Changes on 2026-10-17 (from 61): A04, A06, A11, A12, A13, L06, L08, L09 and LB08 are removed; L11 and LB14 are group teardown checks; LB12 and LB13 are part of LB11. H1 is the large-host case (32 or more Ray CPUs, any pids limit).

Changes from the draft of 58: added A11 (`down` of a 0.1.0 cluster), A12 (Ray cluster commands), A13 (ARM head type). A09 has the `security` marker now. F03, F04, F05, A01, A03, A04 and L03 have more checks (the audit list). LB02, LB03, LB06, LB07, LB09, LB10 and LB11 use the CPUS / B settings of the docs.

## Origin map: every old sub-test

| Old sub-test | Scenario |
|---|---|
| C0.1, step 10c shim tests and mutation | F01 |
| C0.2 | F02 |
| T1, T2, T3, T4, T5 | L04 |
| T6 (secret push over SSH) | dropped: design removed |
| T7, T13 | L07 |
| T8 | L02 |
| T9 | A05 |
| T10 | L06 (removed 2026-10-17) |
| T11 | LB11 |
| T12 (cert reload) | dropped: deferred, certs live 365 days |
| T14 | L08 (removed 2026-10-17: F12) |
| C2 host checks | L03 |
| C5a: up / rent / enrollment / rent loop / fields / monitor log / down | L01 / L02 / dropped (old design) / F15 / L02 / L05 / group teardown |
| C5 and C5 re-run: T15, CapBnd, Task / Join, Fields, GPU / Shm / Teardown | L03 / L02 / A01 / group teardown |
| C6 and C6 re-run: T13 / T14 / Restart / GPU / Shm / Teardown | L07 / F12 / F13 / L02 / A01 / group teardown |
| C8: L6 (not run) / Teardown | dropped / group teardown |
| C8 re-run: Up / L6 / GPU image / Teardown | L01 / L02 / L03 / group teardown |
| TestPyPI: Build, Scan / Upload, Install, Up / Job, Down | F17 / R01 / R02 |
| Live IAM test (2026-10-16) | A10, L01 |
| C9.0.1 / C9.0.2–0.4 | L01 / LB01 |
| C9.1.1, 1.2, 1.5, 1.6 / C9.1.3, 1.4 | LB05 / LB04 |
| C9.2.1–2.4 | LB06 |
| C9.3.1, 3.4 / C9.3.2, 3.3, 3.5 | LB07 / removed 2026-10-17 (their own results) |
| C9.4.1–4.4 | LB09 |
| C9.5.1–5.3 | LB02 |
| C9 Teardown | group teardown |
| C10 pairs (12 + data.tuned2) | LB11 |
| C10.c9.1.1, c9.1.6 / c9.1.3, c9.1.4 | LB05 / LB04 |
| C10.rllib.R1, R2 / R3 | LB10 / removed 2026-10-17 (its own result) |
| C10.5 local check | F16 |
| Local e2e LE1–LE8 | F08–F15 |
| Step 10b bootloader container test | F07 |
