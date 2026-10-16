# Handoff: continue ray-vast on the dev box (2026-10-15)

You are the lead agent. Read this file first, then do the work in "Next work". The user moved the work from the laptop to the dev box `ray-vast-dev`.

## Read first

1. `CLAUDE.md`: rules, machines, secrets.
2. `CONTEXT.md`: terms. Use them in code, tests and docs.
3. `docs/design.md`: the spec. Section 2 ("Life of a worker") is the full flow.
4. `docs/implementation-plan.md`: steps and status.
5. `docs/decisions.md`: the reasons. Add every new decision here.

## How the user wants you to work

These are the user's rules from the earlier sessions. Follow them.

- **Principle:** the simplest implementation that works, with no compromise on reliability and no fragility. Use the well-known pattern first (for example, `kubeadm`-style join tokens); invent last.
- **Language:** write all docs and chat answers in ASD-STE100 (short sentences, active voice, one idea per sentence).
- **Decisions:** make technical decisions yourself with the principle above. Write each one as a line in `docs/decisions.md`. Ask the user only for decisions that change the system design.
- **Subagents:**
  - Use subagents for implementation, tests and reviews.
  - Give each subagent `docs/agents/implementer.md` (implementation) or `docs/agents/review.md` (review). It must also load the `mattpocock-skills:tdd` skill with the Skill tool, if the skill is installed; `docs/agents/tdd.md` holds the same rules.
  - When a subagent has a large context, do not give it more work. Start a new one.
  - Stop agents at once when the user asks.
- **TDD with proof:** red before green. Each new behavior needs a test that the agent saw fail for the right reason; the report lists the failure line. After a step, run a mutation audit (change the code, check that a test fails) on the new behaviors.
- **Tests:** the fast run in the TDD loop, the full run once at the end of a step (commands in `ray-vast-pypi/CONTRIBUTING.md`).
- **Review:** after each step, run the `mattpocock-skills:code-review` skill (Standards and Spec axes) with subagents, as in `docs/agents/review.md`. Fix the HARD findings.
- **Git:** commit to `main` and push after each step. No branches. Author: Duc Pham <thiel.pham@gmail.com>. End each commit message with the session line that the harness gives you.
- **Money and safety:**
  - Never publish to PyPI without the user.
  - The vast spend limit is $8 per UTC day. Every paid test ends with a label sweep to 0.
  - Never touch the badminton box (50.112.112.27) or anything with the name `badminton`.
  - Never print or commit the vast key, `CONTAINER_API_KEY`, join tokens, the cluster token or private keys.
- **Reports to the user:** short. Lead with the result.

## The dev box

- EC2 `ray-vast-dev`: i-023a693e150d1890f, c6i.4xlarge (16 vCPU, 32 GB) with Elastic IP 52.34.69.95, Ubuntu 24.04, user `ubuntu`, us-west-2. It has Docker and git. It has **no** `gcc` and no `gh`: build C in the `quay.io/pypa/manylinux2014_x86_64` container (`ray-vast-pypi/scripts/build_shim.sh`).
- Clone the repo (`github.com/duc-ph/ray-vast`) and work in the clone. Make a venv in `ray-vast-pypi/.venv` (Python 3.11 or 3.12) and `pip install -e '.[test]'`. Run the tests directly; no rsync is needed on the box.
- `~/ray-vast-pypi/` on the box is an old rsync copy from the laptop, with a venv. Do not edit it. Use the clone.
- `ray-vast up/down` (paid live tests) need AWS credentials (profile `personal`) and the vast key in `~/.config/vastai/vast_api_key` on the box. Check with the user before the first paid run on the box.

## State at handoff (2026-10-14, end of the night run)

- Steps 0–12 are done. Step 13 is partly done.
- Code on `main` is version `0.1.0rc1`. Since step 11:
  - one ending function (`VastNodeProvider._end`) and one label sweep (`vast_api.sweep`);
  - the image ENTRYPOINT is kept, and `up` reads one `RAYVAST_IMAGE_INFO` line;
  - the GPU count comes from `/dev/nvidia<N>`, and the head refuses a mismatch;
  - the head container gets `--shm-size` = 0.33 × RAM;
  - `pip install ray-vast` then `ray-vast up` installs the same version on the head from PyPI. (`--index-url` was removed on 2026-10-16: a release test downloads the TestPyPI wheel and passes it with `--wheel`.)
  - a clean Ctrl-C on `dashboard`, and line-buffered output.
- Tests: the full run has about 405 tests; local_e2e passes 8 of 8 (3 runs in a row after the refactor).
- Live cycles of 2026-10-14 on the final code: C5, C6, C8 (with `rayproject/ray:*-gpu`) and C9 (Ray libraries) pass. Results: `docs/test-results.md`. The vast spend of the day is about $3.82.
- TestPyPI: `0.1.0rc1` is at https://test.pypi.org/project/ray-vast/0.1.0rc1/. A clean install ran `up`/`down` with `--index-url`.
- PyPI: `0.1.0` is at https://pypi.org/project/ray-vast/0.1.0/ (2026-10-15). A clean venv installed it from PyPI. The README and the docs home page say "(work in progress)".
- Docs site: https://duc-ph.github.io/ray-vast/ (MkDocs Material; `.github/workflows/docs.yml` publishes on push).
- The process limit (`pids.max`): design (a) is implemented (decisions.md 2026-10-16, design.md 5.5 and 5.7a). gRPC threads 4, startup concurrency 8, worker ports from `pids.max`, Ray CPUs unchanged, a heartbeat status and a head WARNING line. The branch `pids-limit-review` (shim v4 and a CPU cap) is rejected. Do not merge it.
- **Waits on the user:**
  1. A private contact for security reports (`security.md` has a TODO).
- The box has AWS credentials (`~/.aws`, profile `personal`), the EC2 key pair `ray-vast-devbox` (`~/ray-vast-devbox.pem`), the vast key, and the TestPyPI and PyPI tokens in `~/.pypirc`. The deploy key can push branches and workflows.

## State at 2026-10-16 (before the dev box resize)

- Main is at 06701a6, pushed. AWS and vast are clean (checked: no ray-vast test instance, SG, key pair, role, user or volume; vast 0 instances). Only the policies `ray-vast-head-boundary` and `ray-vast-user` stay in the account.
- Done since 0.1.0: per-library doc pages; a generated SSH key pair per cluster; head `region`/`instance_type` in the head `node_config`; optional top-level `max_workers`; removed options (`port_a/b`, `cert_max_age_days`, `image_size_gb`, `order_by`, `--index-url`); IAM permissions boundary plus `ray-vast-user`; saved AWS profile; `down` of a 0.1.0 cluster; fixes for a bad vast listing, the cgroup v1 CPU quota, the SG rule race, `call()` threads, missing vast env. See decisions.md (2026-10-16).
- The live suite: `ray-vast-pypi/tests/live`, 61 scenarios in the claim matrix in `docs/test-plan.md`. Markers: free, aws, lifecycle, failure, security, limits, libraries, release. Session fixtures `aws_only`, `lifecycle` (restricted user, other region), `libraries`. Records in `archive/results/<date>/<ID>/result.json`; `summarize.py` writes test-results.md.
- Free and aws groups: 31 pass. Test bugs fixed but not re-run: F03, F05, F07, F17, A01 (wait for the head volume to go). Not analysed: F18 (3 unit tests fail on Python 3.10), F19 (`cpu-heavy` has 3 offers, criterion 4). No record: F20. Lifecycle and libraries never ran. Open doc bug: tune.md settings (a task with `num_cpus` = CPUS/B inside a trial needs a `PlacementGroupFactory` bundle).
- The user approved running everything to the end without asking ("Just go ahead without me"), then the 0.2.0 PyPI release and R01/R02.
- The dev box is resized to c6i.4xlarge with an Elastic IP (the user runs it from the laptop).

## Next work

1. Speed up the suite first (user request, iteration is too slow): pytest-xdist `--dist loadgroup`, one group per cluster fixture so the clusters run in parallel; `--keep-cluster` to reuse a live cluster while debugging (the final run does a fresh `up`/`down`); libraries tests pinned per host and run in parallel (a "many" test alone on its host); local e2e scenarios in parallel.
2. Re-run the failed free/aws scenarios, then run the lifecycle, failure, security, limits and libraries groups to the end. Fix product bugs with TDD; list every workaround and failure.
3. Independent Standards and Spec review by the lead (not by the implementer), then fix.
4. Version 0.2.0, publish to PyPI, run R01/R02 (clean PyPI install, getting-started verbatim).
5. Update the Ray library status table from the results; report to the user.
