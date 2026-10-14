# Handoff: continue ray-vast on the dev box (2026-10-14)

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
- **Tests:** the fast run in the TDD loop, the full run once at the end of a step (commands in `ray-vast-pypi/README.md`, "Tests").
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

## State at handoff

- Steps 0–11 are done and committed (2026-10-14).
  - Step 10b: the one-time join token, 5 security groups, no head restart policy, the rent-loop breaker, the CA expiry warning.
  - Step 10c: 14 shim tests and a mutation script (23 of 25 mutants killed, 2 equivalent).
  - Step 11: 8 local end-to-end scenarios passed 3 runs in a row (about 12 min each). No product bug.
- Test counts: the fast run has about 308 tests; the full run (`-m "not live and not local_e2e"`) has 363.
- The dev box has the clone at `/home/ubuntu/ray-vast` and a venv made with `uv` (`ray-vast-pypi/.venv`, no pip inside; use `VIRTUAL_ENV=.venv uv pip install`). The box has `python3.12-venv` for the packaging test.
- The box has **no** AWS credentials for the profile `personal` and no `aws` CLI. Step 12 needs them.

## Next work

1. Step 12 (paid live cycles C5, C6, C8 in `docs/test-plan.md`). Ask the user before the first paid run. Add AWS credentials on the box first.
2. Step 13 (docs and release). Never publish to PyPI without the user.

When a step is done: update the status table in `docs/implementation-plan.md`, add the results to `docs/test-results.md` if it ran a cycle, commit, push, and give the user a short report.
