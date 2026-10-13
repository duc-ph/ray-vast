# Instructions for review subagents

Repo: `/Users/ducpham/Desktop/workspace/Onedrive/Home/projects/ray-vast`. The lead gives you the axis (Standards or Spec), the fixed point and the plan steps under review.

Diff: `git diff <fixed-point>...HEAD -- ray-vast-pypi/` and commits `git log <fixed-point>..HEAD --oneline`. Read only; do not edit files. You may run read-only commands (git, grep, reading files). You may run the tests on the dev box (see `CLAUDE.md`) if you need evidence, but do not change anything there.

## Standards axis
Sources: `CODING_STANDARDS.md`, `CLAUDE.md` ("Code" section), and the smell baseline in `ray-vast-pypi/docs/agents/smell-baseline.md` (a documented repo standard overrides the baseline; baseline smells are always judgement calls). Also check `ray-vast-pypi/docs/agents/tdd.md` test rules (behavior at public seams, no tautological tests, mocks only at system boundaries).

Brief: report, per file/hunk where relevant, (a) every place the diff violates a documented standard: cite the standard (file + rule); (b) any baseline smell: name it and quote the hunk. Mark each finding HARD (documented-standard breach) or JUDGEMENT. Skip anything tooling enforces. Under 400 words.

## Spec axis
Sources: the named steps of `ray-vast-pypi/docs/implementation-plan.md` (Work, Tests, Done when), plus parts 1–2 (facts, decisions), `ray-vast-pypi/docs/spikes.md`, `docs/design.md`, and `ray-vast-pypi/docs/decisions-for-review.md`.

Brief: report (a) requirements the spec asked for that are missing or partial; (b) behaviour in the diff that was not asked for (scope creep); (c) requirements that look implemented but where the implementation looks wrong. Quote the spec line for each finding. Under 400 words.

Return your report as text (you cannot write report files).
