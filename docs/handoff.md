# Handoff: continue ray-vast on the dev box (2026-10-13)

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
  - Use subagents for implementation, tests and reviews, with model **Sonnet** (`model: "sonnet"`).
  - Give each subagent `docs/agents/implementer.md` (implementation) or `docs/agents/review.md` (review). It must also load the `mattpocock-skills:tdd` skill with the Skill tool, if the skill is installed; `docs/agents/tdd.md` holds the same rules.
  - When a subagent has a large context, do not give it more work. Start a new one.
  - Stop agents at once when the user asks.
- **TDD with proof:** red before green. Each new behavior needs a test that the agent saw fail for the right reason; the report lists the failure line. After a step, run a mutation audit (change the code, check that a test fails) on the new behaviors.
- **Tests:** the fast run in the TDD loop, the full run once at the end of a step (commands in `ray-vast-pypi/README.md`, "Tests").
- **Review:** after each step, run the `mattpocock-skills:code-review` skill (Standards and Spec axes) with Sonnet subagents, as in `docs/agents/review.md`. Fix the HARD findings.
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

## State at handoff

- Steps 0–10 are done and committed. The C5a/C5d live run showed that enrollment fails on real vast (vast answers 401 when the head uses a worker's `CONTAINER_API_KEY`). The design was changed to a one-time join token and other items: this is step 10b. The spec (`docs/design.md`) is already updated; the code is not.
- Step 11 (local end-to-end) work in progress is committed in `ray-vast-pypi/tests/local_e2e/` (deselected by default with the `local_e2e` marker). It uses the OLD enrollment. Before the spec change, it ran 3 times on the box with the old code:
  - Run 1: 7 of 7 passed.
  - Run 2: 7 passed, 1 error: the session leak check found a container left behind (`tests/conftest.py:68`) after `test_k_2_rents_four_candidates_for_two_nodes_and_destroys_the_two_unused`.
  - Run 3: `test_scale_up_two_workers_run_the_battery_then_idle_scale_down_to_zero` failed: `tasks_on_nodes == 1`, expected 2 (`cpus: [0, 4.0, 4.0]`, so both workers were up; the task spread was not even).
  - `test_monitor_crash_stops_the_head_and_the_restarted_head_sweeps_and_scales_again` tests the old Docker restart policy. The spec now has no self-healing (design 7.3), so this test must change.
- The shim has no automated behavior test. It was tested only by hand (C0 `tests/manual/c0_1_shim_unit.sh`) and in the live cycles C0–C4.

## Next work

Steps 10b and 10c have no shared files. Run them in parallel, each with its own subagent. Then do step 11. The details are in `docs/implementation-plan.md`.

1. **Step 10b — spec changes:** join token, 5 security groups, no restart policy, rent-loop breaker, the CA expiry warning in the head log, the log names "port check" and "orphan sweep", `pytest-timeout`, the bootloader container test.
2. **Step 10c — shim tests and mutation evaluation.**
3. **Step 11 — local end-to-end**, after 10b. Bring the 7 scenarios to the new spec, add the breaker scenario, fix the two flaky results above, and pass all scenarios 3 times in a row.
4. Then step 12 (paid live cycles, `docs/test-plan.md`) and step 13 (release). Ask the user before step 12.

When a step is done: update the status table in `docs/implementation-plan.md`, add the results to `docs/test-results.md` if it ran a cycle, commit, push, and give the user a short report.
