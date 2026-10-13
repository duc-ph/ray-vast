# ray-vast

A Ray autoscaler NodeProvider for vast.ai workers with an AWS EC2 head. PyPI name `ray-vast`, import `ray_vast`. Package source in `ray-vast-pypi/`.

Read first: `CONTEXT.md` (terms), `docs/design.md` (the spec, with the Ray facts F1–F12 and the worker ↔ head protocol), `docs/implementation-plan.md` (steps and status), `docs/decisions.md` (reasons, D1–D9).

Docs layout: one topic in one file. `docs/` holds the project docs (design, decisions, plan, spikes, shim, tests, future work, agent instructions in `docs/agents/`). `ray-vast-pypi/` holds the user docs (README, CHANGELOG, `docs/security.md`). `archive/` holds raw test data.

## Principle
Simplest implementation that works, with no compromise on reliability and no fragility. Fewer parts, fewer failure modes.

## Machines
- AWS: `--profile personal --region us-west-2`. SSH key `~/phduc-personal.pem`.
- Dev/test box: EC2 tag `Name=ray-vast-dev` (t3.xlarge, Ubuntu 24.04, user `ubuntu`, Docker, image `rayproject/ray:2.59.0-py311-cpu`). Its SG `ray-vast-dev-sg` allows SSH from /32 rules only; the laptop IP changes (carrier NAT), so add a /32 rule when SSH times out.
- NEVER touch the badminton box (50.112.112.27) or anything named badminton.
- vast API key: `~/.config/vastai/vast_api_key` (never print it, never commit it).

## Code
```
Prefer keeping simple, single-use logic inline. Do not extract functions merely to make the parent function shorter.
Avoid trivial single-use helpers that force the reader to jump between
definitions to understand straightforward control flow.
Prefer readable linear code when the operations are tightly coupled and
best understood together.
```
- Worker-side code (`src/ray_vast/worker/`) uses the Python stdlib only.
- Never log `instance_api_key`, `CONTAINER_API_KEY`, join tokens, the cluster token, private keys or the vast key.
- Tests: the fast run in the TDD loop, the full run once at the end. Commands and test rules: README "Tests".
- Edit code on the laptop; run tests on the dev box (rsync `ray-vast-pypi/` to `~/ray-vast-pypi/` on the box).

## Docs
Write docs in ASD-STE100 (short sentences, active voice, one idea per sentence).

## Git
Solo repo `github.com/duc-ph/ray-vast`. Commit to `main`, push. Author Duc Pham <thiel.pham@gmail.com>.

## Paid tests (vast)
Label instances `ray-vast-test-<date>`. Stop if one day's vast spend is above $8. Teardown ends with a label sweep until 0. Scan kept logs for secrets before a commit.
