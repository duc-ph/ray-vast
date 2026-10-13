# Instructions for implementation subagents

You implement one step of `ray-vast-pypi/docs/implementation-plan.md`. The lead agent gives you the step number and any extra notes.

## Read first
1. `CLAUDE.md` (repo root): machines, rules, coding convention.
2. `CODING_STANDARDS.md` (repo root).
3. `CONTEXT.md` (terms), `docs/design.md`.
4. `ray-vast-pypi/docs/implementation-plan.md`: part 1 (facts), part 2 (decisions), your step, and the steps it depends on.
5. `ray-vast-pypi/docs/spikes.md`.
6. `ray-vast-pypi/docs/agents/tdd.md`: follow it (red → green, vertical slices, seams = the tests in your step).
7. The code that already exists in `ray-vast-pypi/`.

## How to work
- **Red before green, with proof.** For each behavior: write the test, run it on the box, and see it FAIL for the right reason; only then write the code and see it pass. Your report must include, for each new behavior, the failing test name and the one-line failure reason from the red run. A test that you never saw fail does not count.
- Edit files in the local repo: `/Users/ducpham/Desktop/workspace/Onedrive/Home/projects/ray-vast/ray-vast-pypi/`. Stay inside the files of your step; do not change other modules except for small, needed fixes (report them).
- Run tests on the dev box, not on the laptop: `rsync -az --delete --exclude .venv --exclude __pycache__ <repo>/ray-vast-pypi/ ubuntu@<box-ip>:~/ray-vast-pypi/`, then run `pytest` there in a venv (`~/ray-vast-pypi/.venv`, Python 3.11 or 3.12, `pip install -e '.[test]'`). The box: EC2 tag `Name=ray-vast-dev`, `--profile personal --region us-west-2`, key `~/phduc-personal.pem`. If SSH times out, add a /32 TCP 22 rule for the laptop's current IP to `ray-vast-dev-sg`.
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
