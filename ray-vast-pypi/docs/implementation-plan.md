# ray-vast implementation plan

Status: draft, 2026-10-12. Source design: [../../docs/design.md](../../docs/design.md). Terms: [../../CONTEXT.md](../../CONTEXT.md). Open tests from the design phase: [../../docs/test-plan.md](../../docs/test-plan.md).

This plan has 4 parts:

1. Facts from the Ray source that change the work.
2. Decisions that you must make before step 1.
3. The steps. Each step has its work, its tests and a checklist ("done when").
4. The test plan: test levels, the paid test cycles and the safety rules.

All new code goes into `ray-vast-pypi/`. The PyPI name is `ray-vast`. The import name is `ray_vast`.

---

## 1. Facts from the Ray source

Source: `ray-project/ray` master, commit `68174559` (2026-10-12). Files: `python/ray/autoscaler/node_provider.py`, `_private/autoscaler.py`, `_private/monitor.py`, `_private/node_launcher.py`, `_private/providers.py`, `_private/spark/node_provider.py`, `_private/fake_multi_node/node_provider.py`, `v2/instance_manager/node_provider.py`, `v2/instance_manager/reconciler.py`, `v2/scheduler.py`. Step 0 checks each fact again on Ray 2.59.0.

| # | Fact | Effect on ray-vast |
|---|---|---|
| F1 | The config `provider: {type: external, module: "ray_vast.provider.VastNodeProvider"}` loads a custom class. | The head container must have `ray-vast` installed. |
| F2 | **Autoscaler v2 is the target (decided 2026-10-12).** `ray up` turns v2 on by default since Ray 2.50 (`commands.py`). For VM clouds, v2 wraps a normal `NodeProvider` in `NodeProviderAdapter` (`v2/instance_manager/node_provider.py`) and calls the same methods. | The head starts Ray with `RAY_enable_autoscaler_v2=1`. We write one `NodeProvider` class. The v1 IP mapping ("assumes ip-address is unique per node") does not apply. |
| F3 | v2 matches a Ray node to a cloud instance by `RAY_CLOUD_INSTANCE_ID` (the Ray node's `instance_id`), not by IP (`reconciler.py`). | Each worker starts Ray with `RAY_CLOUD_INSTANCE_ID=<slot id>` and `RAY_NODE_TYPE_NAME=<node type>`. Two workers with one shared public IP cause no problem. |
| F4 | v2 plans a starting node with the node type's declared resources, and a running node with the resources that the Ray node reports (`scheduler.py`). | Declare the minimum (`CPU: 8`). The bootstrap starts Ray with the host's real core count. GPU stays exact (`num_gpus = GPU`). |
| F5 | v2 still writes the tags `ray-node-status`, `ray-launch-config` and `ray-user-node-type` through the adapter. | The provider keeps the tags in memory (D3). S1 checks how v2 uses them. |
| F6 | `create_node_with_resources_and_labels()` is the call that the autoscaler makes. It can raise `NodeLaunchException`. The launcher runs it in a background thread. | We implement this method directly. It returns at once with the slots. |
| F7 | These methods must be thread-safe: `non_terminated_nodes`, `is_running`, `is_terminated`, `node_tags`, `internal_ip`, `external_ip`, `create_node*`, `set_node_tags`, `terminate_node(s)`. | One `RLock` around the provider state. |
| F8 | `bootstrap_config()` is called only by `ray up`. The monitor calls only `validate_config()`. | `ray-vast up` runs `bootstrap_config()` and writes the final YAML to the head. (The design already says this.) |
| F9 | v1 (`autoscaler.py` `reset()`) makes the provider only once per monitor process. | S1/S2 check the same for v2. |

---

## 2. Decisions before step 1

I recommend the first option in each row. Tell me if you want a different one.

| # | Question | Recommendation | Other options |
|---|---|---|---|
| D1 | Where is the package? | **Decided 2026-10-12:** `projects/ray-vast/` is a separate project, outside `badminton_ai`. The package is in `projects/ray-vast/ray-vast-pypi/`. | — |
| D2 | Where does the enrollment and heartbeat server run? | **Decided 2026-10-12:** a thread in the provider, in the monitor process. The autoscaler makes the provider only once (`if not self.provider` in `reset()`), so there is no second object. If the monitor crashes, autoscaling stops; the heartbeats then stop too, and the workers destroy themselves after 5 min (cost safety over job continuity, as in design.md). | A separate daemon (Spark-style thin provider). Rejected: more parts, and it only helps after a monitor crash, when the cluster cannot scale anyway. |
| D3 | Where is the state? | **Decided 2026-10-12:** memory only, plus a start-up sweep. A provider restart happens only with a head restart (D2). A new head has a new GCS, and old worker raylets exit after about 60 s without a GCS, so old state has no use. On start, the provider destroys every instance with the cluster label and removes every `ray-vast:<cluster>` security group rule (the same code as `down` and the outside check). `/enroll` and `/heartbeat` accept only instances in memory. | SQLite on the head disk. Rejected: more parts, and it cannot save the cluster because the GCS is lost anyway. |
| D4 | Where is the blocklist? | **Decided 2026-10-12:** in memory, one blocklist for each cluster, lost at `down` or a head restart. A bad host costs less than $0.01 and 1–2 min, and the host checks run on every candidate anyway. A cross-cluster file is in `future-work.md`. | The laptop file (design.md); a file on the head disk. Rejected: copy and merge code for a small saving. |
| D5 | What does `down` do with the head? | **Decided 2026-10-12:** terminate it. Then `down` deletes the group and the IAM role, and nothing is left. The CA key is lost, which agrees with "to renew, make a new cluster". A stopped head keeps no useful state after D3. | Stop the head and keep the group. Rejected: disk cost, and a second reuse path in `up` and `down`. |
| D6 | Does the head get an Elastic IP? | **Decided 2026-10-12:** no. A reboot keeps the IP. After a stop and start, the user runs `down` and `up`; the old workers destroy themselves. | An EIP. Rejected: allocate and release code, a leak risk, and a restart script, for a cluster that is new anyway (D3). |
| D7 | Does Ray run as a different user than the image's user? | **Decided 2026-10-12:** no. Ray runs as the image's user. The bootstrap keeps `CONTAINER_API_KEY` in its own memory for the destroy and removes it from Ray's env (stops accidental leaks in logs). A task can still read `/proc/1/environ` when the users are the same (documented risk; a task already has the token and the node cert, which give more). | Change to a non-root user on root images. Rejected: file, `HOME` and device permission failures, for small security gain. |
| D8 | How does a node type write its offer query? | **Decided 2026-10-12:** the vast API JSON form, as a YAML mapping (`gpu_ram: {gte: 24000}`, MB). No parser. `gpu_ram`/`cpu_ram` values below 1000 → error "the unit is MB". | Our own copy of the CLI parser (drift risk); import `parse_query` from `vastai` (internal, unstable). |
| D9 | Which Ray versions? | **Decided 2026-10-12:** the package requires `ray[default]>=2.59` (the image sets the version). `ray-vast up` reads the image's Ray version and warns if it is not in the tested list (now: 2.59). A paid test cycle adds a version to the list. | A hard pin `==2.59.*` (pip would change the image's Ray, so head and workers differ); an error for untested versions. |

---

## 3. Steps

Rules for all steps:

- Write code on the EC2 dev box (50.112.112.27) and run the tests there.
- Follow the CLAUDE.md coding rules: keep simple logic inline, no small helpers that are used only once, linear code.
- Never log `instance_api_key`, `CONTAINER_API_KEY`, the token, a private key or the vast account key. Each step that touches secrets has a redaction test.
- Commit to `main` after each step.
- Before any vast rental: show you the offers (id, GPU, `dph_total`, `inet_down`, `inet_up`, location) and wait for your approval.

### Package layout (target)

```
ray-vast-pypi/
  pyproject.toml            # name = ray-vast, hatchling, entry point `ray-vast`
  README.md  LICENSE  CHANGELOG.md
  src/ray_vast/
    __init__.py
    config.py               # schema, validation, bootstrap_config
    vast_api.py             # vast REST client (retry, redaction)
    pki.py                  # CA, node certs, token, fingerprint
    state.py                # in-memory state (one RLock), blocklist
    aws_sg.py               # security group rules with IP ref counts
    head_server.py          # HTTPS: /ca.pem, /bootstrap, /enroll, /heartbeat, /report
    provider.py             # VastNodeProvider
    cli.py                  # up, down, attach, dashboard
    worker/                 # runs on the worker; stdlib only
      bootloader.sh               # template for the container command
      bootstrap.py          # PID 1: checks, enrollment, Ray start, watchdog
    shim/
      rayvast_shim.c
      librayvast_shim.so    # prebuilt (manylinux2014), sha256 checked
    examples/*.yaml
  tests/
    unit/  integration/  local_e2e/  live/
  scripts/build_shim.sh
  docs/implementation-plan.md (this file)
```

### Step 0 — Spikes on Ray 2.59.0 ($0, no rental)

A spike is a small, short test that answers one question before we build. Write each result into `docs/spikes.md`.

| # | Spike | Where | Cost |
|---|---|---|---|
| S1 | A minimal external provider (2 fake nodes) with autoscaler v2 (`RAY_enable_autoscaler_v2=1`) and `disable_node_updaters: true`. Workers start with `RAY_CLOUD_INSTANCE_ID` and `RAY_NODE_TYPE_NAME`. Check: two workers with the same node IP are both seen, and scale-down removes the correct one; a node declared `CPU: 2` that starts with `--num-cpus 4` gets 4 CPUs of tasks; any v2 timeout for an instance that stays a long time between "allocated" and "Ray running" (our pull can take 15 min); how v2 uses the tags of F5. | EC2 dev box, `ray start` on one machine with 2 raylets | $0 |
| S2 | Facts F1–F9 on Ray 2.59.0 (read the installed source). F9: the provider is made once per monitor process. Kill the monitor on a test head: does Ray start it again? Stop the GCS: does a worker raylet exit after about 60 s (`gcs_rpc_server_reconnect_timeout_s`)? | EC2 dev box | $0 |
| S3 | Dropped 2026-10-12: no scoped key and no separate account. A thief who reaches the head controls the cluster anyway, and vast's fixed key rights (change the account SSH keys) defeat a scope. The head uses the user's vast account key (documented risk). | — | — |
| S4 | Dropped 2026-10-12: the bootloader is short (about 500 bytes) and only downloads the bootstrap and checks its hash. Local tests (step 7) check that it works; C5 shows that vast accepts it. | — | — |
| S5 | Can the bootstrap pin the CA with stdlib Python only? The image has only Python and Ray, and Ray does not depend on `cryptography` (checked in `setup.py`). Plan: download `/ca.pem` without verification, compare its SHA-256 with `RAYVAST_CA_SHA256`, then connect again with `cafile=ca.pem` and full verification (chain + IP SAN). Negative: wrong fingerprint, other CA, missing SAN. mTLS with `load_cert_chain` for the heartbeat. Run in `rayproject/ray:2.59.0-py311-cpu` and in a slim Python 3.9 image. | EC2 dev box | $0 |

Done when:
- [ ] `docs/spikes.md` has a result line for S1, S2 and S5.
- [x] D1 answered (separate project).
- [x] D2 answered (thread in the provider).
- [x] D3 answered (memory only + start-up sweep).
- [x] D4 answered (in-memory blocklist for each cluster).
- [x] D5 answered (terminate the head).
- [x] D6 answered (no Elastic IP).
- [x] D7 answered (Ray runs as the image's user).
- [x] D8 answered (API JSON form, no parser).
- [x] Autoscaler v2 chosen (F2).
- [x] D9 answered (minimum version + warning).
- [ ] If S1 fails, we stop and change the design before step 1.

### Step 1 — Package skeleton

Work:
- `pyproject.toml`: name `ray-vast`, `requires-python >=3.9`, dependencies `ray[default]>=2.59` (D9), `boto3`, `cryptography`, `pyyaml`, `requests`. Optional `[test]`: `pytest`, `moto`, `responses`, `freezegun`.
- Console script `ray-vast = ray_vast.cli:main`.
- The shim `.so` and `worker/*` are package data.
- `scripts/build_shim.sh` builds the shim in `quay.io/pypa/manylinux2014_x86_64` and checks the sha256 and `nm -D` (see `shim/README.md`).
- `ruff` and `pytest` config.

Tests:
- `python -m build` makes an sdist and a wheel. `pip install` the wheel into a clean venv. `ray-vast --help` works. `importlib.resources` finds the shim and `bootstrap.py`.

Done when:
- [ ] Wheel contains `librayvast_shim.so`, `bootstrap.py`, `bootloader.sh`, `examples/`.
- [ ] The shim in the wheel has sha256 `79055dea…17fd` (or a new build with a new recorded hash).
- [ ] `pytest` runs (0 tests is OK).

### Step 2 — Config checks

Work (`config.py`):
- Parse the cluster YAML. Required: `cluster_name`, `provider` (region, image, vast key source, `candidate_factor`, `max_dollars_per_hour`, `cert_max_age`, `allow_non_datacenter`, `order_by`), `available_node_types` with `resources` and `node_config.offer_query`.
- `bootstrap_config()`:
  - Sets `disable_node_updaters: true`, `type: external`, `module: ray_vast.provider.VastNodeProvider`.
  - Errors: the head type has no explicit `resources`; the head has `CPU` other than 0; declared resources contradict the query (`GPU: 2` and `num_gpus=1`).
  - Warnings: `CPU` > number of worker ports (58); `allow_non_datacenter: true`.
- `offer_query` is the vast API JSON form (D8). Each value is a dict with `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in` or `notin`. Error on any other key. Error if `gpu_ram`, `cpu_ram` or `gpu_total_ram` is below 1000 ("the unit is MB").
- Added filters: `num_gpus = GPU`, `cpu_cores_effective >= CPU`, `direct_port_count >= 64`, `datacenter=true` (unless allowed), `verified=true`, `rentable=true`. A user filter that contradicts an added filter is an error.

Tests (unit):
- A query with the added filters gives the expected JSON (one test for each added filter).
- `gpu_ram: {gte: 24}` gives the MB error.
- Each error and warning above has one test.
- The example YAMLs pass validation, also through Ray's own `validate_config()`.

Done when:
- [ ] A live `search_offers` with an example node type returns offers (read only, $0).
- [ ] `bootstrap_config()` output passes Ray's `validate_config()`.
- [ ] Error messages name the node type and the field.

### Step 3 — vast API client

Work (`vast_api.py`):
- Methods: `search_offers(query, order_by)`, `create_instance(offer_id, image, env, args, label)`, `get_instance(id, key)`, `list_instances()`, `destroy_instance(id, key)`.
- Retry with backoff on HTTP 429 and 5xx. Timeouts on every call.
- `create_instance` returns the instance ID only. It drops `instance_api_key` from the response before any log or return.
- Identity ports in `env`: `-p 70000:70000` … `-p 70063:70063`.

Tests:
- Unit, with `responses`: fixtures from `archive/results/*/rent_*.json` and `c4_poll` data, redacted. 429 retry. Destroy of an instance that is already gone is a success.
- Redaction test: a fake response with `instance_api_key: "SECRET123"`; capture all logs and exceptions; `SECRET123` must not appear.

Done when:
- [ ] All methods have a unit test.
- [ ] The redaction test passes.
- [ ] A read-only live smoke test (`search_offers` only, $0) passes.

### Step 4 — PKI and token

Work (`pki.py`), runs only on the head:
- New CA (key never leaves the head), lifetime `cert_max_age`.
- Node cert: SAN = public IP and 127.0.0.1, same lifetime.
- Head server cert for the HTTPS server (SAN = head public IP).
- Token: 32 random bytes, hex.
- CA SHA-256 fingerprint and bootstrap SHA-256 for the worker env.
- Files with mode 600.
- Warning 14 days before expiry (used by `ray-vast up` and the head log).

Tests (unit):
- `openssl verify` accepts the node cert with the CA. SAN has the two IPs.
- File modes are 600.
- The fingerprint matches `openssl x509 -fingerprint -sha256`.
- With a frozen clock, the expiry warning appears at day 351 and not at day 350.

Done when:
- [ ] Ray (2.59) starts with TLS on the dev box with these files (one-node smoke test).

### Step 5 — In-memory state and blocklist

Work (`state.py`):
- Plain Python objects under one `RLock`: slots (id, node type, tags, status, instance id), candidates (instance id, slot, offer, machine id, public IP, status, times, `dph_total`), enrolled instance IDs (never removed while the provider lives), IP ref counts.
- No persistence (D3).
- Blocklist in memory (D4): machine id, reason, kind, expiry (30 days for security failures, 1 day for network or timeout failures; only useful for long-lived clusters).

Tests (unit):
- Concurrent changes from 8 threads: no lost update.
- An enrolled instance ID stays after its slot is removed.
- Blocklist expiry with a frozen clock.

Done when:
- [ ] All tests pass.

### Step 6 — Security group rules

Work (`aws_sg.py`):
- `add_ip(ip)` and `remove_ip(ip)` with ref counts from the state. Rule = TCP all ports from `ip/32`, description `ray-vast:<cluster>`.
- `remove_all()`: remove every rule with this description (used by the start-up sweep and by `down`).
- Error at 299 IPs (quota).

Tests:
- Unit with `moto`: two workers on one IP → one rule; remove one worker → rule stays; remove both → rule is gone. `remove_all()` removes only this cluster's rules. Quota error.
- Live ($0): one add and one remove on a test group in `us-west-2`, from the dev box.

Done when:
- [ ] Ref count tests pass.
- [ ] The live test leaves no rule behind.

### Step 7 — Worker side: bootloader and bootstrap

Work (`worker/bootloader.sh`, `worker/bootstrap.py`), Python stdlib only:
- **Bootloader** (in `args`, no secrets, about 500 bytes): find a Python that can `import ray` (`$PATH`, `/home/ray/anaconda3/bin`, `/opt/conda/bin`, `/usr/local/bin`); download `/bootstrap` without a cert check; compare its SHA-256 with `RAYVAST_BOOTSTRAP_SHA256`, stop if different; `exec` it.
- **Bootstrap, first action:** download `/ca.pem` without verification, compare its SHA-256 with `RAYVAST_CA_SHA256`, then use it as `cafile` for all later calls (S5).
- **Bootstrap, before enrollment:**
  - Local checks: egress IP = `PUBLIC_IPADDR`; no NET_RAW and no NET_ADMIN (CapEff); own netns; empty `/dev/shm`.
  - Open a listener on all 64 identity ports.
  - `POST /enroll` with instance ID, `CONTAINER_API_KEY`, check results, port map.
  - A refusal (403) from the head → destroy at once (for example, after a head restart).
  - Deadline: 10 min, then destroy.
- **Bootstrap, after enrollment:**
  - Close the listeners. Write cert, key, token, shim with mode 600.
  - Keep `CONTAINER_API_KEY` in memory; remove it from the env of the Ray child (D7).
  - Start `ray start --block` as a child, as the image's user, with `--address`, `--node-ip-address`, the 6 fixed ports, `--worker-port-list`, `--num-cpus` (the host's real core count, F4), `--num-gpus` (declared), `RAY_CLOUD_INSTANCE_ID=<slot id>`, `RAY_NODE_TYPE_NAME`, `RAY_num_workers_soft_limit`, TLS and token env, shim env.
  - Token check: both agent ports on 127.0.0.1 answer 401 without a token. `POST /report` with the result.
- **Watchdog:** heartbeat every 30 s with the node cert (mTLS). No success for 5 min → destroy. `TERM`, `INT`, `HUP`, an error, or the end of the Ray child → destroy. Reap child processes. Before each destroy: `POST /report` with the reason, wait up to 10 s. Destroy = `DELETE /instances/<id>` with `CONTAINER_API_KEY`, retried. Never exit without a destroy.
- For tests, the vast API URL and all times come from env values.

Tests:
- Unit: Python finder, CapEff parse, port map, `ray start` argument build, signal → destroy path, heartbeat timer (fake clock).
- Container tests on the dev box (Docker, fake head and fake vast from step 8 test fixtures): the bootloader works on `rayproject/ray:2.59.0-py311-cpu`, on a slim non-root image with Ray, and fails clearly on an image with an ENTRYPOINT (local part of T15). `kill -TERM 1` from a Ray task gives a "destroy" call on the fake vast, not an exit (local part of T13). A task's `os.environ` has no `CONTAINER_API_KEY`. Zombie count stays 0 after 50 short children.
- Redaction: the container stdout never has the token, the key or `CONTAINER_API_KEY`.

Done when:
- [ ] Bootstrap imports only stdlib (test: run it with `python -I -S` plus `ray` absent from the import list).
- [ ] The bootloader is less than 1 KB.
- [ ] A bootstrap with one changed byte is refused by the bootloader.
- [ ] Local T13 and T15 parts pass.

### Step 8 — Head server (enrollment, heartbeat, reports)

Work (`head_server.py`), HTTPS on one port (only worker IPs can reach it, via the security group):
- `GET /ca.pem`, `GET /bootstrap` (no auth; no secrets in them).
- `POST /enroll`:
  1. `GET /instances/<id>` on vast with the sent key. Only 200 is a pass.
  2. The ID is a candidate of this cluster, not in `enrolled`, and the request source IP = the instance public IP.
  3. Outside check: connect to all 64 identity ports.
  4. If a slot is free: make key and cert, answer with cert, key, CA, token, shim, `ray start` parameters, slot ID. Add to `enrolled`. Set slot tag `setting-up`.
  5. If no slot is free, or a check fails: answer 403, destroy the candidate, add to the blocklist with the reason.
- `POST /heartbeat` (mTLS, node cert): record the time.
- `POST /report` (reasons, token check result). A failed token check → destroy the worker.
- Log each enrollment decision with instance ID, machine ID, IP and reason. No secrets.

Tests:
- Integration with a fake vast (a small local HTTP server that answers `GET /instances/<id>` for known key pairs): the refusals of T16 (other instance's key, second enrollment, wrong source IP, not a candidate) all give 403 and a destroy call. A good enrollment gives the files and a cert that `openssl verify` accepts.
- Two candidates for one slot: the first gets the slot, the second is destroyed.
- Heartbeat without a client cert or with a rogue CA: refused.

Done when:
- [ ] All T16 refusals pass on the fake vast.
- [ ] The answer to `/enroll` never appears in a log (redaction test).

### Step 9 — VastNodeProvider

Work (`provider.py`):
- `__init__`: start-up sweep (D3: destroy all instances with the cluster label, `remove_all()` rules), then start the head server thread (D2) and the reconcile thread.
- `create_node_with_resources_and_labels(node_config, tags, count, resources, labels)`: make `count` slots with new integer IDs and status `uninitialized`. Rent min(k·count, matching offers not on the blocklist) candidates, only while the total `dph_total` stays under `max_dollars_per_hour`. Return at once. If no offer matches, raise `NodeLaunchException` (category "no offers").
- `non_terminated_nodes(filters)`: the slots (with or without a candidate), filtered by tags.
- `is_running`: slot has an enrolled worker. `is_terminated`: slot is gone.
- `node_tags`, `set_node_tags`: from the in-memory state (F5).
- `internal_ip(id)` and `external_ip(id)` return the worker's public IP (v2 matches by `RAY_CLOUD_INSTANCE_ID`, F3).
- `terminate_node(s)`: destroy the instance, remove the slot, update the IP ref count.
- Reconcile thread (every 10 s):
  - Candidate timeouts: pull `2 × image_size / inet_down` (5–15 min); enrollment 1 min after `running`; `exited` or an error status → destroy at once, blocklist.
  - A security group rule for each candidate IP when the status becomes `running`.
  - A slot with no candidate left → remove it (the autoscaler asks again).
  - When a slot is filled: destroy the other candidates of the same request.
  - Worker IP change (vast `public_ipaddr` differs from the enrolled IP) → destroy, rule ref count down.
  - Status `exited` on a worker → destroy.
  - Outside check: every instance with the cluster label that is not a slot or a candidate → destroy.

Tests:
- Unit with a fake vast (in memory) and a fake clock: each reconcile rule above; budget limit; blocklist skip; two requests at the same time (thread test).
- **Autoscaler integration ($0):** run Ray's v2 `Autoscaler` with our provider (through `NodeProviderAdapter`), the fake vast and a mocked GCS state (pattern: `ray/autoscaler/v2/tests/test_autoscaler.py`). Scenarios: scale up 3 nodes with k=2 → 6 candidates → 3 slots filled, 3 destroyed; idle scale-down; a candidate that never enrolls → slot removed and requested again; two workers with the same IP → both are seen and the correct one is removed.
- Restart test: stop the provider mid-scale-up, make a new one. The sweep destroys all old instances and removes all old rules; the new provider then scales up from zero.

Done when:
- [ ] All scenarios pass.
- [ ] No vast call happens while the lock is held (calls are made outside the lock; review checklist item).
- [ ] T7 logic (IP change) passes on the fake vast.

### Step 10 — CLI

Work (`cli.py`):
- `ray-vast up <yaml>`:
  1. Validate the YAML (step 2). Show the warnings.
  1b. On the head, read the image's Ray version. Warn if it is not in the tested list (D9).
  2. Read the user's vast key from the laptop (`~/.config/vastai/vast_api_key` or `VAST_API_KEY`). Copy it to the head (mode 600). Never put it in the YAML, the env of a worker, or a log.
  3. Create the IAM role and instance profile (modify own security group, describe EC2), the security group (SSH from the operator IP), the EC2 head (Ubuntu 24.04, IMDSv2 required, Docker installed by user data).
  4. On the head: pull the image, install `ray-vast` into a derived image or at container start, make the CA and token on the head (`pki.py`), write the bootstrapped YAML (F8).
  5. Start `RAY_enable_autoscaler_v2=1 RAY_CLOUD_INSTANCE_ID=head ray start --head --autoscaling-config ... --num-cpus=0` in the container with `--network host`, TLS, token and shim env.
  6. Wait until `ray status` works. Print the dashboard command.
- `ray-vast down <yaml>`: label sweep (list, destroy, list again until 0, retry on 429), remove the rules, terminate the head (D5), delete the group and the IAM role. Print what is left (must be nothing).
- `ray-vast attach`, `ray-vast dashboard` (SSH tunnel to 8265).
- `down` must work when the head is already gone (use only AWS and vast APIs, not the head).

Tests:
- Unit with `moto` and fake vast: `up` creates every resource; `down` removes every resource, also after a partial `up` failure at each step.
- Live ($0.10 for EC2, no vast rental): `up` with `max_workers: 0`, `ray status` on the head, `down`, then an AWS and vast "nothing left" check.

Done when:
- [ ] `down` after a failed `up` at each step leaves nothing.
- [ ] The live no-worker cycle passes.

### Step 11 — Local end-to-end on the dev box ($0)

A fake vast server that runs each "instance" as a local Docker container with the real bootloader and bootstrap, identity ports mapped P→P, and `PUBLIC_IPADDR` = the dev box IP. The head runs in a container with `--network host`. This tests the whole flow without vast cost.

Scenarios:
- Scale up 2 workers, run the C2 battery (`tests/manual/battery.py`, includes `ray job submit`), scale down to 0.
- Two "workers" with the same public IP (F3).
- A host with more cores than declared: Ray uses the real core count (F4).
- Stop the head server for 6 min → both workers destroy themselves; the report endpoint gets the reasons first.
- Container restart → second enrollment refused → destroy.

Done when:
- [ ] All scenarios pass 3 times in a row.

### Step 12 — Live tests on vast (paid)

See part 4. Run T15, T16, T13, T14, T7, then the full scale test L6.

Done when:
- [ ] Each test has a row in `docs/test-results.md` with key numbers and log links.
- [ ] Spend for the step is under the agreed budget.

### Step 13 — Docs and release

Work:
- `README.md`: install, quick start, example YAMLs (VRAM tiers, `dlperf` floors, `static_ip=true` for long jobs), the image rules (sh + Python with Ray, no ENTRYPOINT), the security model and the known gaps (from design.md), the port and actor-error notes, the head key risk (a thief on the head gets the full vast account key).
- `CHANGELOG.md`, version `0.1.0`.
- Publish to TestPyPI, install in a clean venv on the dev box, run the step 10 live no-worker cycle from that install. Then publish to PyPI.

Done when:
- [ ] `pip install ray-vast` from PyPI and `ray-vast up` work on a clean machine.
- [ ] The wheel has no secrets and no test fixtures with real IDs (scan).

---

## 4. Test plan

### Test levels

| Level | What | Where | Cost | When |
|---|---|---|---|---|
| L1 Unit | pure logic, fake clock, `responses`, `moto` | dev box | $0 | each commit |
| L2 Integration | provider + Ray v2 `Autoscaler` + fake vast; head server + fake vast | dev box | $0 | each commit |
| L3 Local end-to-end | real Ray, real bootloader and bootstrap in Docker, fake vast that runs containers | dev box | $0 | each step from 7 |
| L4 AWS only | `up`/`down` with no workers, security group live tests | `us-west-2` | ~$0.10 | steps 6, 10, 13 |
| L5 Live vast | open tests T7, T13–T16 | EC2 head + vast | ~$0.50 | step 12 |
| L6 Live scale | full autoscaling with real jobs | EC2 head + vast | ~$2 | step 12, before release |

### Live test cycles (step 12)

| Cycle | Tests | Rentals | Pass when |
|---|---|---|---|
| C5 | T15 (3 images; vast accepts the bootloader in `args`), T16 (enrollment refusals on real instances; check if the instance list shows other `instance_api_key` values) | 3–4 | the bootloader works on 2 images, fails clearly on the ENTRYPOINT image; all 4 refusals give 403 + destroy |
| C6 | T13 (TERM/INT/HUP to PID 1 from a Ray task → destroy, never an exit; on a root image and on a non-root image), T14 (heartbeat stop 5 min ± 30 s → destroy; 2 min stop → no destroy; no enrollment in 10 min → destroy; reason reaches the head) | 3 | numbers inside the limits; destroy reason in the head log for each destroy |
| C7 | T7 full flow: change of worker IP (simulate: the head's state gets a wrong IP, or rent a host without `static_ip`) → destroy, rule removed only if no other user, new worker arrives | 2 | new worker joins; the rule count is correct |
| C8 | L6: `min_workers: 0`, submit a job that needs 4 GPU workers of 2 node types; scale up with k=2; scale down when idle; `ray-vast down` | 8–10 | job finishes; candidates beyond the slots destroyed in < 1 min; after `down`, 0 instances, 0 rules, 0 IAM objects |

For each cycle, write the setup, steps, expected output, data to capture and teardown into `../../docs/test-plan.md` first (the rule in that file).

### Safety rules for paid tests

- Show you the machines (EC2 type, vast offers) and wait for approval before each cycle.
- Label all instances `ray-vast-test-<date>`. Record the vast credit before and after. Stop if the spend for one day goes above $8.
- Teardown ends with a label sweep until 0, then the AWS cleanup check.
- Record time, CPU, RAM and cost for each step in `resources.csv`.
- Scan all kept logs for the token, `PRIVATE KEY`, the vast key and `instance_api_key` values before a commit.

### Checklist for each code review (all steps)

- [ ] No secret in a log, an exception message, the create request, env, `args` or stdout.
- [ ] No vast or AWS call while the provider lock is held.
- [ ] Every vast call has a timeout and a retry on 429.
- [ ] Every path that ends a candidate or a worker destroys the instance and updates the IP ref count.
- [ ] The worker side imports only stdlib.
- [ ] Simple logic is inline (CLAUDE.md rule).
