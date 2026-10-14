# C9: Ray libraries (L7)

The scripts for cycle C9 in [docs/test-plan.md](../../../docs/test-plan.md). The test plan is the spec.

- `cluster.yaml`: cluster `rvtest-c9`. Type `gpu` (`GPU: 1`, `CPU: 4`, `min_workers: 2`) and type `gpu2` (`GPU: 2`, for C9.3.4 only).
- `jobs/`: the `--working-dir` of every job. One script for each sub-test. `c9common.py` and `c9train.py` are shared parts.
- `run_c9.py`: the phase 2 driver. It runs on the operator machine.

Each job prints one last line: `C9_RESULT <id> PASS|FAIL|LIMIT <json>`. A wall-clock guard in each job prints a `FAIL` line with `timeout_s` and ends the job if the job hangs.

## Sub-tests

| ID | Script or step | Torch | Expected |
|---|---|---|---|
| C9.0.1 | `ray-vast up`, `dashboard`, the export line (by hand) | | PASS |
| C9.0.2 | `c9_0_2_hello.py`: the working directory reaches both workers | | PASS |
| C9.0.3 | `c9_0_3_torch_cuda.py`: torch from `runtime_env` pip, CUDA on each worker | yes | PASS |
| C9.0.4 | driver step: `ray job list`, `ray job logs --follow`, `ray job stop` on `c9_0_4_sleeper.py` | | PASS |
| C9.1.1 | `c9_1_1_data_cpu.py`: range → map_batches → filter → count/take | | PASS |
| C9.1.2 | `c9_1_2_data_objstore.py`: 1.5 × the object store of one worker (cap `C9_MAX_GB`, default 3 GB), `iter_batches` in the driver | | PASS (record spilling) |
| C9.1.3 | `c9_1_3_data_shuffle.py`: `random_shuffle`, `groupby().count()` | | PASS |
| C9.1.4 | `c9_1_4_data_gpu.py`: callable class, `concurrency=2`, `num_gpus=1` | yes | PASS (record rows/s) |
| C9.1.5 | `c9_1_5_data_io.py`: `write_parquet` to `/tmp/c9_out`, public `read_parquet`, `read_csv` from the working dir | | PASS (the local read-back on the head fails: files stay on the workers) |
| C9.1.6 | `c9_1_6_data_many.py`: 400 blocks, `num_cpus=0.05` | | PASS (record the processes per node, ≤ 58) |
| C9.2.1 | `c9_2_1_tune_basic.py`: 8 trials | | PASS |
| C9.2.2 | `c9_2_2_tune_asha.py`: ASHA stops trials early | | PASS |
| C9.2.3 | `c9_2_3_tune_gpu.py`: 2 GPU trials on 2 workers | | PASS |
| C9.2.4 | `c9_2_4_tune_ckpt.py`: checkpoints with the default `storage_path` | | LIMIT or PASS: record what Ray does |
| C9.3.1 | `c9_3_1_train_1gpu.py`: 1 worker, 1 GPU, a checkpoint | yes | PASS (a storage error is a LIMIT) |
| C9.3.3 | `c9_3_3_train_gloo.py`: 2 nodes, gloo, CPU | yes | LIMIT (ports that Ray does not manage) |
| C9.3.2 | `c9_3_2_train_nccl.py`: 2 nodes, NCCL | yes | LIMIT (ports that Ray does not manage) |
| C9.4.1 | `c9_4_1_serve_handle.py`: 2 replicas, `DeploymentHandle`. The app stays up for C9.4.2 and C9.4.3. | | PASS |
| C9.4.2 | driver step: `curl http://127.0.0.1:8000/` on the head host, by SSH | | PASS |
| C9.4.3 | driver step: SSH tunnel from local port 18000 to port 8000 of the head, then an HTTP GET | | PASS |
| C9.4.4 | `c9_4_4_serve_gpu.py`: 2 GPU replicas. Then `serve.shutdown()`. | yes | PASS |
| C9.5.1 | `c9_5_1_placement.py`: `PACK` of CPU bundles, `STRICT_SPREAD` of 2 GPU bundles | | PASS |
| C9.5.2 | `c9_5_2_actor_fault.py`: `os._exit` and `ray.kill`, `max_restarts=1` | | PASS |
| C9.5.3 | `c9_5_3_large_object.py`: `ray.put` of 2 GB on worker A, `ray.get` on worker B | | PASS (record MB/s) |
| C9.3.4 | `c9_3_4_train_2gpu_1node.py`: 2 GPUs on one node (`STRICT_PACK`), runs last and starts `gpu2` | yes | PASS |

`run_c9.py` runs them in this order. C9.3.4 is last because it rents the `gpu2` node.

The torch jobs use `runtime_env` `{"pip": {"packages": ["torch==2.7.1"], "pip_check": false}, "config": {"setup_timeout_seconds": 1800}}`.
The image has CUDA 12.1.1. The PyPI wheel of torch 2.7.1 brings its own CUDA 12.6 runtime (the `nvidia-*-cu12` packages). It needs a driver of version 525 or later, and supports sm_50 to sm_90. `compute_cap` in `cluster.yaml` leaves out Blackwell GPUs.
The job's driver runs on the head. So the head also installs torch, at the first torch job.

Ray Train sets `MASTER_PORT` to a free port of rank 0. Ray Train has no setting for it. So C9.3.2 has no second try with other ports. `NCCL_SOCKET_IFNAME` does not help: the TCPStore connection comes first and fails first.

## Data

`run_c9.py <R>` writes `<R>/c9/`:

- `results.jsonl`: one line for each sub-test: `id`, `status`, `job_status`, `submission_id`, `wall_s` and the `data` of the `C9_RESULT` line.
- `<id>/`: the job script, `submit.txt` (the submit command), `submit.out`, `job.log` (`ray job logs`). The driver steps write `job_cli.txt`, `follow.out`, `curl_head.txt` or `tunnel.txt`.
- `ray_status_<epoch>.txt`: `ray status` on the head at the start.

## Phase 2 commands

Run from the repository root on the operator machine (the dev box). Follow the safety rules and the teardown of the test plan.

```bash
export VAST_API_KEY=$(cat ~/.config/vastai/vast_api_key)   # never echo it
DATE=$(date -u +%F); R=archive/results/$DATE; mkdir -p $R/c9
PY=ray-vast-pypi/.venv/bin/python

# Setup: check the offers again (update the comments in cluster.yaml), record the credit.
$PY tests/manual/offers.py tests/manual/c9/cluster.yaml gpu
$PY tests/manual/offers.py tests/manual/c9/cluster.yaml gpu2
$PY tests/manual/vast_tool.py credit

# C9.0.1
ray-vast up tests/manual/c9/cluster.yaml --profile personal 2>&1 | tee $R/c9/up.log
ray-vast dashboard tests/manual/c9/cluster.yaml --profile personal     # second terminal; keep it open
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=$HOME/.ray-vast/rvtest-c9/token RAY_ADDRESS=http://127.0.0.1:8265

# Wait for the 2 `gpu` workers (each job also waits up to C9_WAIT_S = 900 s for its nodes).
ray-vast attach tests/manual/c9/cluster.yaml --profile personal        # then: sudo docker exec ray-vast-head ray status

# C9.0.2 to C9.5.3, then C9.3.4. Give ids to run only some of them, for example: ... $R C9.4.2 C9.4.3
PATH=$(dirname $PY):$PATH $PY tests/manual/c9/run_c9.py $R
```

Optional settings for the jobs (`run_c9.py` passes every `C9_*` variable to the jobs as `env_vars`): `C9_MAX_GB` (C9.1.2 and C9.1.3), `C9_OBJECT_GB` (C9.5.3), `C9_STORAGE` (C9.2.4 and Train, for example an S3 URI). Never put credentials in a `C9_*` variable: `submit.txt` keeps it.

Then run the teardown of the test plan: copy the head logs, scan for secrets, `ray-vast down`, `vast_tool.py sweep` until 0, the AWS check.

## Local check (no cluster)

The scripts run in a small local Ray, on CPU:

```bash
cd tests/manual/c9/jobs
RAY_ADDRESS=local C9_SMALL=1 C9_MIN_NODES=1 C9_WAIT_S=5 C9_LOCAL_CPUS=2 C9_LOCAL_TMP=/tmp/c9r python c9_1_1_data_cpu.py
```

Add `C9_GPU=0` to run a GPU script on CPU. Its CUDA checks then fail. `C9_GUARD_S` changes the guard time.
