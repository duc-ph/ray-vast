# Spike results

## S1 — autoscaler v2 with an external provider (2026-10-13): PASS

Ray 2.59.0, autoscaler v2, `rayproject/ray:2.59.0-py311-cpu` on EC2 `ray-vast-dev` (`t3.xlarge`). A test provider started extra raylets on the same box, so all nodes shared one IP. vast cost $0. Wall time 38 min. Code, config and logs: [`../spikes/s1/`](../spikes/s1/).

| # | Check | Result | Key numbers | Evidence |
|---|---|---|---|---|
| 1 | Matching by `RAY_CLOUD_INSTANCE_ID` with a shared IP | Pass | 2 slots, both raylets on one IP; ALLOCATED → RAY_RUNNING 18 s after submit; no assert | `logs/run1/check1.log`, `monitor/` |
| 2 | Scale-down removes the correct node | Pass | drain after 61.7 s idle, `terminate_nodes([slot])` 5 s later; the correct raylet stopped | `logs/run1/raylet-slot-*.log` |
| 3 | Real cores on a running node | Pass | `CPU: 2` type started with 4 CPUs ran 4 × 1-CPU tasks at once; v2 plans a new node with 2 CPUs | `logs/run1/check3*.log`; `v2/scheduler.py:454` (running node: reported resources), `:571` (new node: declared) |
| 4 | Slow start (15 min) | Pass | ALLOCATED → RAY_RUNNING after 905 s, no timeout. Timeout `RAY_AUTOSCALER_RECONCILE_ALLOCATE_STATUS_TIMEOUT_S`, default 3600 s (`v2/instance_manager/config.py:84-113`, used in `reconciler.py:1036-1050`). With 60 s: terminate and a new slot every 65 s, with no limit. | `logs/run2_timeout60/` |
| 5 | Tags | Info | Create writes `ray-node-name`, `ray-node-type`, `ray-node-status`, `ray-launch-config`, `ray-launch-request`, `ray-user-node-type`. 0 `set_node_tags` calls. `node_tags` reads only `ray-node-type`, `ray-user-node-type`, `ray-launch-request` (`node_provider.py:357-373`). | `logs/run1/provider.log` |
| 6 | One provider object | Pass | `__init__` ran 1 time in 28 min; a config edit was used without a new object (`v2/autoscaler.py:118`, config read again each loop `:211`) | `logs/run1/provider.log` |

### Findings that change the plan

- **A. The provider must list the head.** If `non_terminated_nodes` does not return the head's cloud ID (`RAY_CLOUD_INSTANCE_ID=head`), v2 marks the head TERMINATED after 5 s (`reconciler.py:561`). Then v2 does not scale at all, and it shows no error (`:1143`, `:1264`). The provider lists `head` with the tags `{ray-node-type: head, ray-user-node-type: head}`. A node without a correct `ray-node-type` tag is "unmanaged", and v2 skips it without an error (`node_provider.py:358`).
- **B. A cold start can start too many nodes.** v2 plans a new node with the declared minimum CPU, so before the first node reports its real cores, v2 can start extra nodes. In check 3b, v2 started 2 nodes for 4 tasks; the second node stayed idle until the idle timeout removed it.
- **C. Call rate.** Each 5 s loop calls `non_terminated_nodes` once, and `node_tags` and `is_running` for every node. These must answer from memory, without vast calls. `is_terminated`, `internal_ip` and `external_ip` were never called.
- **D.** Each create call had `count=1`, one launch per 5 s loop.
- **E.** `disable_launch_config_check` is True by default in v2 (`config.py:406`). Keep the default: with False, v2 marks every worker as outdated (`scheduler.py:2031`).
- **F.** The head container needs `docker run --init`. Without it, Ray processes stay as zombies after `ray stop`.
- **G.** Timeouts are read from the env when the monitor starts, so they go on the head's `ray start`. `REQUEST_STATUS` (600 s) and `MAX_NUM_RETRY_REQUEST_TO_ALLOCATE` (3) do not affect us, because the provider returns slots at once.

## S2 — Ray facts on 2.59.0

Not run yet.

## S5 — CA pinning with stdlib Python

Not run yet.
