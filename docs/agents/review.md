# Instructions for review subagents

Repo: the checkout that the lead names (on the dev box: `/home/ubuntu/ray-vast`). The lead gives you the axis (Standards or Spec), the fixed point and the plan steps under review.

Diff: `git diff <fixed-point>...HEAD -- ray-vast-pypi/` and commits `git log <fixed-point>..HEAD --oneline`. Read only; do not edit files. You may run read-only commands (git, grep, reading files). You may run the tests on the dev box (see `CLAUDE.md`) if you need evidence, but do not change anything there.

## Standards axis
Sources: `CODING_STANDARDS.md`, `CLAUDE.md` ("Code" section), and the smell baseline in `docs/agents/smell-baseline.md` (a documented repo standard overrides the baseline; baseline smells are always judgement calls). Also check `docs/agents/tdd.md` test rules (behavior at public seams, no tautological tests, mocks only at system boundaries).

Brief: report, per file/hunk where relevant, (a) every place the diff violates a documented standard: cite the standard (file + rule); (b) any baseline smell: name it and quote the hunk. Mark each finding HARD (documented-standard breach) or JUDGEMENT. Skip anything tooling enforces. Under 400 words.

## Spec axis
Sources: `docs/design.md` (the spec), the named steps of `docs/implementation-plan.md` (Work, Done when), `docs/decisions.md`, and `docs/spikes.md`.

Brief: report (a) requirements the spec asked for that are missing or partial; (b) behaviour in the diff that was not asked for (scope creep); (c) requirements that look implemented but where the implementation looks wrong. Quote the spec line for each finding. Under 400 words.

Return your report as text (you cannot write report files).

## Checklist for every review (both axes)
- No secret in a log, an exception message, the container command, stdout, or Ray's env (the join token only in the create env).
- No vast or AWS call while the provider lock is held.
- Every vast call has a timeout; a create retries only on 429.
- Every path that ends a candidate or a worker destroys the instance and updates the IP ref count.
- The worker side imports only the stdlib.
- Simple logic is inline (`CODING_STANDARDS.md`).
