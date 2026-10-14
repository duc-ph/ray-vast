# Docs walk-through, ray-vast 0.1.0rc1 from TestPyPI (2026-10-14)

Operator machine: the dev box, a new venv `~/rvtest-venv` (Python 3.12.3), work directory `~/rvtest-l4rc1` (outside the repo).
`AWS_PROFILE=personal` or `--profile personal` on each command. Cluster `rvtest-l4rc1`, no workers.

## README "Quick start" and docs/index.md "Quick start"

| Command (as written) | Result | Notes |
|---|---|---|
| `pip install ray-vast` | Not run | 0.1.0rc1 is on TestPyPI only. The TestPyPI command of `reference/cli.md` was used (see below). |
| Copy an example cluster YAML from `ray_vast/examples/` | Pass | The README gives no command. `getting-started.md` gives one (below). |
| `ray-vast up cluster.yaml --profile my-aws-profile` | Pass | Run with `--index-url https://test.pypi.org/simple/` (needed for an rc on TestPyPI). 550 s, exit 0. |
| `ray-vast dashboard cluster.yaml --profile my-aws-profile` | Pass | Output is the same as the docs. Ctrl-C prints a `KeyboardInterrupt` traceback (finding 2). |
| `export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH=...` | Pass | The line from `dashboard`. |
| `ray job submit --address http://127.0.0.1:8265 --working-dir . -- python train.py` | Pass | Run with `python -c "print(1)"` in place of `train.py`. Job `SUCCEEDED`, 8.7 s. The head has CPU 0; the driver still runs on the head. |
| `ray-vast down cluster.yaml --profile my-aws-profile` | Pass | 35 s, `left: nothing`, exit 0. |

## getting-started.md

| Step | Command (as written) | Result | Notes |
|---|---|---|---|
| 1 | `python -m venv .venv` | Mismatch (small) | Ubuntu 24.04 has no `python` outside a venv (only `python3`). `python3 -m venv` was used. |
| 1 | `source .venv/bin/activate` | Pass | |
| 1 | `pip install ray-vast` | Not run | Replaced by `pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple ray-vast==0.1.0rc1` (`reference/cli.md`). 37 s. See finding 1. |
| 2 | `cp "$(python -c '…')/examples/gpu-24gb.yaml" cluster.yaml` | Pass | |
| 2 | Edit `cluster.yaml` (region, key pair, budget) | Pass | The example file has no `key_name`/`key_file` lines. The docs snippet shows them, so the user adds them. Set: `cluster_name: rvtest-l4rc1`, `max_workers: 0` (top and `gpu24`), `max_dollars_per_hour: 0.5`, key pair `ray-vast-devbox`. |
| 3 | `ray-vast up cluster.yaml --profile my-aws-profile` | Pass | First line `installing ray-vast==0.1.0rc1 from https://test.pypi.org/simple/` (the `--index-url` row of the cli.md table). Other lines match. 9 min 10 s with the GPU image (docs: about 8 min). |
| 4 | `ray-vast dashboard cluster.yaml` | Pass | Output matches the docs, word for word. |
| 5 | `export …` | Pass | |
| 5 | `ray job submit … -- python hello.py` | Not run | `hello.py` needs a GPU worker. This cycle has no workers. |
| 5 | `ray-vast attach cluster.yaml` + `sudo docker exec ray-vast-head ray status` | Pass | Run with the commands on stdin. `ray status`: 1 head, idle, no failures. |
| 7 | Ctrl-C, then `ray-vast down cluster.yaml` | Pass, with finding 2 | `down` output matches. 35 s (docs: about 40 s). |

## reference/cli.md "To test a release on TestPyPI"

| Command | Result | Notes |
|---|---|---|
| `pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple ray-vast==<version>` | Pass, with finding 1 | |
| `ray-vast up cluster.yaml --profile … --index-url https://test.pypi.org/simple/` | Pass | The head ran `pip install --no-cache-dir --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple ray-vast==0.1.0rc1` (image history). In the head container: `0.1.0rc1 /home/ray/anaconda3/lib/python3.11/site-packages/ray_vast/__init__.py`. |
| `ray status` through the tunnel | Fails, as expected | Port 8265 is the dashboard, not the GCS. The docs say `ray status` on the head. No mismatch. |

## Findings

1. `--extra-index-url` mixes the two indexes. On the laptop, pip took `opentelemetry-exporter-prometheus 1.12.0rc1` from TestPyPI, not from PyPI (the newest on PyPI is `0.66b1`, a lower version number). Ray worked in this test. The docs say "The dependencies come from PyPI". That is not always true. The head was not affected: the image already had `0.60b0`.
2. `ray-vast dashboard`: Ctrl-C ends with a Python `KeyboardInterrupt` traceback (`commands.py` line 112, `subprocess.call`). The CLI does not catch `KeyboardInterrupt`. The tunnel closes.
3. `ray-vast --version` does not exist (`error: the following arguments are required: command`). The docs do not claim it.
4. When stdout is not a terminal (a pipe or a file), the `up` progress lines show only at the end (Python block buffering).
