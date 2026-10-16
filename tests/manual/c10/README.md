# C10: process limits of the Ray libraries (L8)

The scripts for cycle C10 in [docs/test-plan.md](../../../docs/test-plan.md). The test plan is the spec.

- `cluster_a.yaml`: cluster `rvtest-c10a`, phase A. One `big` worker (`CPU: 32` or more, 1 cheap GPU), `max_workers: 1`.
- `cluster_b.yaml`: cluster `rvtest-c10b`, phase B. The C9 layout: 2 `gpu` workers, and `gpu2` (2 GPUs on one host) for R2.
- `jobs/`: the `--working-dir` of every C10 job. `c10common.py` is the shared part. `c10_probe.py` reads the limits of
  each worker node. `c10_cleanup.py` shuts Serve down after each test.
- `run_c10.py`: the driver. It runs on the operator machine.
- `local_check.py`: the local check in Docker (no cost), one plain Ray node.
- `test_c10_local_e2e.py`: the local check with the real ray-vast head and worker bootstrap (the step 11 harness). It checks
  the head's process-limit WARNING line. Run it from `ray-vast-pypi/`:
  `PYTHONPATH=src .venv/bin/python -m pytest -o addopts="" -p no:cacheprovider -s ../tests/manual/c10/test_c10_local_e2e.py`.

A pair job takes the mode as its argument: `python c10_<lib>.py many|tuned`. It prints one last line:
`C10_JOB <id> COMPLETED|HUNG|ERROR <json>`. `run_c10.py` decides and prints `C10_RESULT <id> PASS|FAIL|LIMIT|INCONCLUSIVE <json>`.
The functional jobs (RLlib R1 to R3) print their own `C10_RESULT` line. The C9 jobs print `C9_RESULT`.

## Tests

| ID | Script | Torch | Expected |
|---|---|---|---|
| C10.core.tuned / .many | `c10_core.py` | | tuned PASS; many PASS (hang, warning, node alive) |
| C10.data.tuned / .many | `c10_data.py` | | same |
| C10.tune.tuned / .many | `c10_tune.py` | | same |
| C10.serve.tuned / .many | `c10_serve.py` | | same |
| C10.train.tuned / .many | `c10_train.py` | yes | same |
| C10.rllib.tuned / .many | `c10_rllib.py` | yes | same |
| C10.c9.1.1, .1.3, .1.4, .1.6 | `../c9/jobs/c9_1_*.py`, no `C9_DATA_CPU` | 1.4 | no node crash; record PASS or hang |
| C10.rllib.R1 | `c10_rllib.py r1`: PPO, 4 env runners on 2 nodes | yes | PASS |
| C10.rllib.R3 | `c10_rllib.py r3`: 2 learners on 2 nodes | yes | LIMIT |
| C10.rllib.R2 | `c10_rllib.py r2`: 2 learners on 1 node with 2 GPUs (starts `gpu2`) | yes | PASS |

## Phase commands

Run from the repository root on the operator machine (the dev box). Follow the safety rules and the teardown of the test plan.

```bash
export VAST_API_KEY=$(cat ~/.config/vastai/vast_api_key)   # never echo it
DATE=$(date -u +%F); R=archive/results/$DATE; mkdir -p $R/c10
PY=ray-vast-pypi/.venv/bin/python

# Phase A: setup, check the offers (write them into cluster_a.yaml), record the credit.
$PY tests/manual/offers.py tests/manual/c10/cluster_a.yaml big
$PY tests/manual/vast_tool.py credit
ray-vast up tests/manual/c10/cluster_a.yaml --profile personal 2>&1 | tee $R/c10/up_a.log
ray-vast dashboard tests/manual/c10/cluster_a.yaml --profile personal     # second terminal; keep it open
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=$HOME/.ray-vast/rvtest-c10a/token RAY_ADDRESS=http://127.0.0.1:8265
PATH=$(dirname $PY):$PATH $PY tests/manual/c10/run_c10.py $R a 2>&1 | tee -a $R/c10/run_a.out
# Teardown A (test plan), then phase B:
ray-vast up tests/manual/c10/cluster_b.yaml --profile personal 2>&1 | tee $R/c10/up_b.log
ray-vast dashboard tests/manual/c10/cluster_b.yaml --profile personal
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=$HOME/.ray-vast/rvtest-c10b/token RAY_ADDRESS=http://127.0.0.1:8265
PATH=$(dirname $PY):$PATH $PY tests/manual/c10/run_c10.py $R b 2>&1 | tee -a $R/c10/run_b.out
```

Give ids after the phase to run only some tests, for example `... $R a C10.core.tuned C10.core.many`.
`C10_GUARD_S` changes the guard of every job (default: 600 s for many/tuned, 900 s for Data and Train, 1500 s for R1 to R3).
The head's process-limit WARNING line is matched by `WARNING_PATTERN` and tied to the node by `NODE_NAMES` (`run_c10.py`).
Phase A has a host retry: without a pids limit ≤ 3328, the runner destroys the worker and probes its replacement, up to 3 hosts.

## Data

`run_c10.py <R> a|b` writes `<R>/c10/`:

- `results.jsonl`: one row for each test: `id`, `status`, `job_status`, `submission_id`, `wall_s`, `data` (the reasons, the
  `C10_JOB` line, the head WARNING lines for the node, the probe of the node before and after).
- `<id>/`: the job script, `submit.txt`, `submit.out`, `job.log`, `probe_pre_*.log`, `probe_post_*.log`, `cleanup.log`,
  `head_slice.log` (the head log lines written during the test).

## Local check (no cluster)

```bash
docker build -t c10-local:torch <dir>      # Dockerfile: FROM rayproject/ray:2.59.0-py311-cpu
                                           #             RUN pip install torch==2.7.1 --index-url https://download.pytorch.org/whl/cpu
python3 tests/manual/c10/local_check.py /tmp/c10local c10_core.py:tuned c10_core.py:many c10_data.py:tuned c10_data.py:many
```

One container `c10-<job>-<mode>` for each run: `--pids-limit 1280`, `ray start --head --num-cpus 48`, 24 worker ports
(`C10L_PORTS`), `RAY_num_grpc_internal_threads=4`, `RAY_worker_maximum_startup_concurrency=8`,
`RAY_num_workers_soft_limit` = the port count (`C10L_SOFT`). The local node also runs the head processes and the driver (about 470 pids
when idle, a vast worker has about 300), so it has fewer pids for workers than a vast worker with the same limit. There is
no ray-vast head, so the local verdict uses the "No available ports" lines in the node's `worker-*.err` logs in place of
the head WARNING line. GPU jobs (R2, R3 with GPUs) cannot run here: `C10_GPU=0` runs them on CPU on one node.
The torch jobs use much memory (one torch process for each Train worker or env runner). On the dev box (4 cores, 16 GB) run them on
a smaller node: `C10L_PORTS=12 C10_SCALE=0.1`, and for R1 to R3 also `C10_MIN_NODES=1 C10_GPU=0`. Use `C10_GUARD_S=180` (Tune
tuned needs about 75 s; use 400). The results of the local check are in the test plan (C10.5).
