# Archive

Raw data of the test cycles C0–C4 (2026-10-12/13). The important data is extracted to [docs/test-results.md](../docs/test-results.md) and [docs/shim.md](../docs/shim.md). Keep these files for evidence only. The C0–C2 scripts in `tests/manual/` used a top-level `shim/` directory; the shim is now only in `ray-vast-pypi/src/ray_vast/shim/`.

- `results/2026-10-12/test-plan-c0-c4.md`: the plan that ran (old SSH design).
- `results/2026-10-12/report-c0c1.md`: C0/C1 report.
- `results/2026-10-12/progress.md`: C0–C3 step log and the C2/C3 report.
- `results/2026-10-13/progress.md`: C4 step log and report.
- `results/<date>/resources.csv`: duration, CPU, RAM and cost for each long step.
- `results/2026-10-12/shim_builds/`: shim v1, v2 and v3 (top level) sources and builds.
- `results/2026-10-12/remote/`, `c2/`, `c3/`, `results/2026-10-13/c4/`: raw logs, offers, rent requests, polls, Ray logs.
