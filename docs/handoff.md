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

- EC2 `ray-vast-dev`: i-023a693e150d1890f, t3.xlarge, Ubuntu 24.04, user `ubuntu`, us-west-2. It has Docker and git. It has **no** `gcc` and no `gh`: build C in the `quay.io/pypa/manylinux2014_x86_64` container (`ray-vast-pypi/scripts/build_shim.sh`).
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
  - `pip install ray-vast` then `ray-vast up` installs the same version on the head from PyPI; `--index-url` tests a release from TestPyPI;
  - a clean Ctrl-C on `dashboard`, and line-buffered output.
- Tests: the full run has about 405 tests; local_e2e passes 8 of 8 (3 runs in a row after the refactor).
- Live cycles of 2026-10-14 on the final code: C5, C6, C8 (with `rayproject/ray:*-gpu`) and C9 (Ray libraries) pass. Results: `docs/test-results.md`. The vast spend of the day is about $3.82.
- TestPyPI: `0.1.0rc1` is at https://test.pypi.org/project/ray-vast/0.1.0rc1/. A clean install ran `up`/`down` with `--index-url`.
- Docs site: https://duc-ph.github.io/ray-vast/ (MkDocs Material; `.github/workflows/docs.yml` publishes on push).
- **Waits on the user:**
  1. The pids-limit fix on branch `pids-limit-review` (not merged). Part 1 (shim v4) makes `sysconf(_SC_NPROCESSORS_CONF)` return 4 in worker containers, so gRPC pools stay small (user code also sees 4 cores: OpenBLAS uses at most 4 threads). Part 2 (commit marked DESIGN CHANGE) caps Ray CPUs and worker ports by `pids.max`. Live: C9.1.1/1.3/1.6 pass without a CPU limit on a 1280-pid host. Without the fix, `main` documents a Ray Data CPU limit as the workaround.
  2. The PyPI release (version `0.1.0`).
  3. A private contact for security reports (`security.md` has a TODO).
- The box has AWS credentials (`~/.aws`, profile `personal`), the EC2 key pair `ray-vast-devbox` (`~/ray-vast-devbox.pem`), the vast key, and a TestPyPI token in `~/.pypirc`. The deploy key can push branches and workflows.

## Next work

1. The user reviews the branch `pids-limit-review` and chooses: merge both parts, merge part 1 only, or keep the documented workaround. Then run local_e2e and C9.1 again on the result.
2. Step 13: the user reviews the docs site; then version `0.1.0`, the PyPI upload with the user, and the clean-machine check from PyPI.

When a step is done: update the status table in `docs/implementation-plan.md`, add the results to `docs/test-results.md` if it ran a cycle, commit, push, and give the user a short report.
