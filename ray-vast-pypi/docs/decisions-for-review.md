# Decisions for review

Technical decisions made while the user was away. Rule: simplest option that works, no compromise on reliability, no fragility. Each line: date, decision, reason, where.

- 2026-10-13 — The wheel is pure Python (`py3-none-any`) but carries the x86_64 Linux shim `.so` as package data. Reason: the `.so` is loaded only by `LD_PRELOAD` inside Linux containers, never imported; one wheel for all platforms. (Step 1)
- 2026-10-13 — The sdist includes only `src`, `tests`, `scripts` and the top-level files, not `docs/` or `spikes/`. Reason: keep spike logs out of the published package. (Step 1)
- 2026-10-13 — The packaging test installs the wheel with `--no-deps` and builds with `--no-isolation`. Reason: fast and offline; the CLI entry point needs only the stdlib to show `--help`. (Step 1)
- 2026-10-13 — Code review is run per step for steps with logic; step 1 (packaging only) is reviewed together with step 2. Reason: a skeleton has nothing for the Spec/Standards axes to find.
