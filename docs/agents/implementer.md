# Instructions for implementation subagents

You implement one step of `docs/implementation-plan.md`. The lead agent gives you the step number and any extra notes.

## Read first
1. `CLAUDE.md` (repo root): machines, rules, coding convention.
2. `CODING_STANDARDS.md` (repo root).
3. `CONTEXT.md` (terms), `docs/design.md` (the spec, with the Ray facts and the worker ↔ head protocol).
4. `docs/implementation-plan.md`: your step and the steps it depends on. `docs/decisions.md` for the reasons.
5. `docs/spikes.md` if your step touches Ray behavior.
6. `docs/agents/tdd.md`: follow it (red → green, vertical slices, seams = the tests in your step).
7. The code that already exists in `ray-vast-pypi/`.

## How to work
- **Fast tests in the TDD loop, the full suite once at the end.** In each red → green cycle, run only the fast test(s) for the behavior you work on (`pytest tests/unit/test_<module>.py -k <name>`). Run the full suite only once, as the final check before your report. The commands and the `slow` marker rules are in `ray-vast-pypi/CONTRIBUTING.md`; every test that starts processes or containers must clean them up, even when it fails.
- **Red before green, with proof.** For each behavior: write the test, run it on the box, and see it FAIL for the right reason; only then write the code and see it pass. Your report must include, for each new behavior, the failing test name and the one-line failure reason from the red run. A test that you never saw fail does not count.
- Edit files in the repo checkout that the lead names. Stay inside the files of your step; do not change other modules except for small, needed fixes (report them).
- Run the tests on the dev box (EC2 tag `Name=ray-vast-dev`), in the venv `ray-vast-pypi/.venv` (Python 3.11 or 3.12, `pip install -e '.[test]'`). If you work on the box, run them directly in the checkout. If you work on the laptop, rsync `ray-vast-pypi/` to the box first (never `--delete`: other agents can rsync in parallel; key `~/phduc-personal.pem`; if SSH times out, add a /32 TCP 22 rule for the laptop IP to `ray-vast-dev-sg`).
- Docker-based tests run on the box. Container names start with your step number (for example `st7-...`), so parallel agents do not collide.
- Unit tests must pass without network access. Live tests (real AWS or vast) are marked `@pytest.mark.live` and skipped by default.
- Do not commit or push. The lead reviews and commits.
- vast rentals: only if your step says so, cheapest matching offer, label `ray-vast-test-<date>`, destroy at the end and confirm with a label sweep. Never print the vast key (`~/.config/vastai/vast_api_key`).
- You cannot write report files. Return your report as text.

## Decisions
When the plan does not decide something, choose the simplest option that works, with no loss of reliability and no fragility. List each such decision in your report (one line: decision, reason).

## Report (under 500 words, no narration)
- Files created or changed.
- Test results: counts, and the command that you ran.
- Red runs: for each new behavior, the test name and its failure line before the code existed.
- The step's "Done when" checklist, each item checked or not, with a reason.
- Decisions you made.
- Problems or open points for the lead.
