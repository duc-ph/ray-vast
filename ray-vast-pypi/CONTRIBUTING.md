# Contributing to ray-vast

## Set up

Work on a Linux box with Docker. Install the package in editable mode, with the test extras:

```
pip install -e '.[test]'
```

Run all commands in this file from the `ray-vast-pypi/` directory.

## Tests

### Fast run

Use the fast run in the TDD loop. It takes about 30 s.

```
pytest
```

It skips the tests that start processes, containers, TLS servers or wheel builds, or that wait on real timers.

### Full run

Use the full run once before you finish.

```
pytest -o addopts="" -m "not live and not local_e2e"
```

It adds the slow tests (bootstrap subprocesses, wheel build, Docker containers).
It still skips the tests that spend money or need a Ray cluster.

### Local end-to-end

The local end-to-end tests run a Ray head and Ray workers as Docker containers on one box. A fake vast server rents the containers. They cost nothing. They take about 12 minutes.

```
pytest -o addopts="" -m local_e2e tests/local_e2e
```

### Live tests

The live suite in `tests/live/` proves the claims of the docs on real AWS and vast.ai. The claim matrix (one scenario for each claim) is in `docs/test-plan.md` at the repository root. Some groups cost money.

```
pytest tests/live -m free -n 12                     # no cost: unit, Docker, local e2e, the package, the offer search
pytest tests/live -m aws -n 8                       # AWS only, no vast rental
pytest tests/live -m "lifecycle or libraries" -n 8  # vast rentals
python tests/live/summarize.py                      # the results table into docs/test-results.md
```

`-n N` runs the tests in N processes (pytest-xdist). The suite sets `--dist loadgroup`: all tests of one cluster run in one process, so each cluster has one `up` and one `down`. The clusters run at the same time.

Each test writes `archive/results/<UTC date>/<ID>/result.json` and its evidence. The cluster fixtures run `ray-vast up` once and `ray-vast down` at the end, also after a failure or Ctrl-C. Read the docstring of each test module before you run it. It gives the time and the cost.

To debug, add `--keep-cluster`. The run keeps its clusters (it prints a `KEPT CLUSTER` line for each), and the next run with the option uses them again. Do the final run without the option: it ends the kept clusters and makes fresh ones.

The local end-to-end tests also run in parallel: `pytest -o addopts="" -m local_e2e tests/local_e2e -n 8`. Each scenario has its own network namespace.

### Markers

| Marker | Meaning | Default run |
|---|---|---|
| `slow` | Starts processes, containers, TLS servers or wheel builds, or waits on real timers. | Skipped |
| `live` | Uses real AWS or vast.ai (every test in `tests/live`). | Skipped |
| `free`, `aws`, `lifecycle`, `libraries`, `release`, `failure`, `security`, `limits` | The groups of the live suite (`docs/test-plan.md`). | Skipped (with `live`) |
| `exclusive_account` | A live test that needs the AWS account with no ray-vast role. It runs before every cluster `up` of the run. | Skipped (with `live`) |
| `box_disk` | A live test that builds or installs the package on the dev box. These tests run in one process, one after the other. | Skipped (with `live`) |
| `local_e2e` | Runs a Ray head and workers in Docker on the dev box. | Skipped |

### Rules for slow tests

- Mark a slow test with `@pytest.mark.slow`.
- A test that runs longer than 300 s fails (`pytest-timeout`).
- Every test that starts a process starts it in a new session. A finalizer kills the whole process group.
- A session check fails the run if a test leaves a process or a container behind.
- Container names start with `<step>-<pid>-`. The session check removes only the containers of its own run. So runs can overlap.
- A killed run cannot remove its containers. So each run removes the `st10b-`, `st11-` and `c10-` containers of dead pids at its start. It also removes its own containers at exit, after Ctrl-C and on SIGTERM (`tests/fakes/containers.py`). It never removes the containers of a live pid. A new container family must go into `FAMILIES` there.
- The bootstrap tests set `RAYVAST_DIR` to a temp directory, so runs can overlap.

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
A GitHub Actions workflow (`.github/workflows/docs.yml` at the repository root) builds the site and publishes it to GitHub Pages on each push to `main` that changes the docs.

Write the docs in ASD-STE100: short sentences, active voice, one idea per sentence.
Every command and field in the docs must match the code (`src/ray_vast/cli.py`, `src/ray_vast/config.py`).
Use placeholders such as `<instance id>` and `<head ip>`, never real instance IDs, IPs or keys.

The `docs/` directory and `mkdocs.yml` are not in the sdist or the wheel.
