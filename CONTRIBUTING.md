# Contributing to ray-vast

## Set up

Work on a Linux box with Docker. Install the package in editable mode, with the test extras:

```
pip install -e '.[test]'
```

Run all commands in this file from the repository root.

## Tests

The tests are in `tests/`. A test file is named for the part that it tests, for example `tests/test_provider.py`.
The tests that use real AWS or vast.ai are in `tests/live/`.

| Marker | Meaning | Default run |
|---|---|---|
| (none) | Fast. No process, container or real timer. | Runs |
| `slow` | Starts processes, Docker containers or a local Ray cluster, or waits on real timers. No cost. | Skipped |
| `live` | Uses real AWS or vast.ai. It can cost money. | Skipped |

### Fast run

Use the fast run while you change code. It takes about 2 minutes.

```
pytest
```

### Slow run

Use the slow run once before you finish. It needs Docker. It costs nothing. It takes about 30 minutes with `-n 8`.

```
pytest -m slow -n 8
```

The slow run includes the local cluster tests (`tests/test_local_cluster.py`). They run a Ray head and Ray workers as Docker containers on one box. A fake vast server rents the containers.

### Live run

The live tests prove the claims of the docs on real AWS and vast.ai. Read the docstring of each module in `tests/live/` before you run it. The docstring gives the time and the cost.

You need:

- An AWS profile with admin rights (`RAYVAST_LIVE_PROFILE`, default `default`).
- A vast.ai API key (`VAST_API_KEY` or `~/.config/vastai/vast_api_key`).

```
pytest -m live tests/live -n 10                          # all cluster tests, the clusters in parallel
pytest -m live tests/live/test_cluster_on_aws.py -n 4    # AWS only, no vast rental
python tests/live/summarize.py                           # the table of today's run
```

Always give `tests/live` or a file in it, so that pytest loads `tests/live/conftest.py` in every process. A few other `live` tests check single API calls outside a cluster: `pytest -m live tests --ignore tests/live`.

`-n N` runs the tests in N processes (pytest-xdist). All tests of one cluster run in one process, so each cluster has one `up` and one `down`. The clusters run at the same time.

Each test writes `test-results/<UTC date>/<test name>/result.json` and its evidence. `--results-dir DIR` changes the root. The cluster fixtures run `ray-vast up` once and `ray-vast down` at the end, also after a failure or Ctrl-C. Each run ends with a vast label sweep and an AWS check of the test names.

To debug, add `--keep-cluster`. The run keeps its clusters (it prints a `KEPT CLUSTER` line for each), and the next run with the option uses them again. Do the final run without the option: it ends the kept clusters and makes fresh ones.

The release check (`tests/live/test_release.py`) runs the getting-started tutorial with ray-vast from PyPI. It runs only when you set `RAYVAST_LIVE_RELEASE=<version>`.

### Rules for slow tests

- Mark a slow test with `@pytest.mark.slow`.
- A test that runs longer than 300 s fails (`pytest-timeout`). A test that needs more time sets its own `@pytest.mark.timeout`.
- Every test that starts a process starts it in a new session. A finalizer kills the whole process group.
- A session check fails the run if a test leaves a process or a container behind.
- Container names start with `<family>-<pid>-`. The session check removes only the containers of its own run. So runs can overlap.
- A killed run cannot remove its containers. So each run removes the `rvworker-` and `rvlocal-` containers of dead pids at its start. It also removes its own containers at exit, after Ctrl-C and on SIGTERM (`tests/fakes/containers.py`). It never removes the containers of a live pid. A new container family must go into `FAMILIES` there.
- The bootstrap tests set `RAYVAST_DIR` to a temp directory, so runs can overlap.

### Secrets

- Never write a secret in a test, a record or a log: the vast key, `CONTAINER_API_KEY`, the cluster token, the join tokens, AWS keys or private keys.
- The live tests scan their own records for secrets at the end of each cluster and of the run. A hit fails the run.
- Test data uses documentation addresses (`192.0.2.0/24`, `198.51.100.0/24`, `203.0.113.0/24`), never a real host's address.

## Docs

The user docs are a MkDocs Material site. The source is in `docs/`. The configuration is in `mkdocs.yml`.

Install the tool:

```
pip install mkdocs-material==9.7.7
```

Build the site. The build must pass with `--strict`:

```
mkdocs build --strict -d /tmp/ray-vast-site
```

Preview the site at http://127.0.0.1:8000:

```
mkdocs serve
```

Do not commit the `site/` directory.
A GitHub Actions workflow (`.github/workflows/docs.yml`) builds the site and publishes it to GitHub Pages on each push to `main` that changes the docs.

Write the docs in ASD-STE100: short sentences, active voice, one idea per sentence.
Every command and field in the docs must match the code (`src/ray_vast/cli.py`, `src/ray_vast/config.py`).
Use placeholders such as `<instance id>` and `<head ip>`, never real instance IDs, IPs or keys.

The `docs/` directory and `mkdocs.yml` are not in the sdist or the wheel.
