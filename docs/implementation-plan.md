# ray-vast implementation plan

The steps to build the package in `ray-vast-pypi/` (PyPI name `ray-vast`, import `ray_vast`). The spec is [design.md](design.md). The reasons are in [decisions.md](decisions.md). The tests of each level and the paid cycles are in [test-plan.md](test-plan.md).

## Rules for all steps

- Implementation subagents follow [agents/implementer.md](agents/implementer.md) and [agents/tdd.md](agents/tdd.md). Reviews follow [agents/review.md](agents/review.md) (Standards and Spec axes).
- Edit code in the repo; run the tests on the dev box `ray-vast-dev` (see `CLAUDE.md`). Never use the badminton box.
- Follow `CODING_STANDARDS.md`.
- Each step that touches secrets has a redaction test.
- Commit to `main` after each step.
- vast rentals: the cheapest matching offers, label `ray-vast-test-<date>`, $8/day limit (the safety rules in `test-plan.md`).

## Package layout

```
ray-vast-pypi/
  pyproject.toml            # name = ray-vast, hatchling, entry point `ray-vast`
  README.md  LICENSE  CHANGELOG.md  docs/security.md
  src/ray_vast/
    config.py               # schema, validation, bootstrap_config
    vast_api.py             # vast REST client (retry, redaction)
    pki.py                  # CA, node certs, token, fingerprint
    state.py                # in-memory state (one RLock), blocklist
    aws_sg.py               # security group rules with IP ref counts
    head_server.py          # HTTPS: /ca.pem, /bootstrap, /enroll, /heartbeat, /report
    provider.py             # VastNodeProvider
    cli.py, commands.py     # up, down, attach, dashboard
    worker/                 # runs on the worker; stdlib only
      bootloader.sh         # template for the container command
      bootstrap.py          # PID 1: checks, enrollment, Ray start, watchdog
    shim/                   # rayvast_shim.c, librayvast_shim.so (see docs/shim.md)
    examples/*.yaml
  tests/  unit/  integration/  local_e2e/  live/  fakes/
  scripts/build_shim.sh
```

## Status

| Step | What | Status |
|---|---|---|
| 0 | Spikes S1, S2, S5 on Ray 2.59.0 ([spikes.md](spikes.md)) | Done |
| 1 | Package skeleton, wheel with the shim and the worker files | Done |
| 2 | Config checks (`config.py`) | Done |
| 3 | vast API client (`vast_api.py`) | Done |
| 4 | PKI and token (`pki.py`) | Done |
| 5 | In-memory state and blocklist (`state.py`) | Done |
| 6 | Security group rules (`aws_sg.py`) | Done for one group; 5 groups in step 10b |
| 7 | Worker side: bootloader and bootstrap | Done with the old identity proof; join token in step 10b |
| 8 | Head server | Done with the old identity proof; join token in step 10b |
| 9 | `VastNodeProvider` | Done; join token and breaker in step 10b |
| 10 | CLI (`up`, `down`, `attach`, `dashboard`) | Done; live AWS cycle passed; 5 groups and no restart policy in step 10b |
| 10b | Spec changes of 2026-10-13 | To do |
| 11 | Local end-to-end on the dev box | Started, paused |
| 12 | Live tests on vast (paid) | C5a/C5d ran (enrollment failed, the reason for step 10b) |
| 13 | Docs and release | To do |

Work that is done but not committed yet (backup in the session scratchpad `backup-1233/`):
- `tests/unit/test_cli.py`: the step 10 mutation audit (32 of 32 mutants killed).
- `vast_api.py` and `test_vast_api.py`: safe logging of vast errors.
- `provider.py`, `test_provider.py`, `tests/local_e2e/`: step 11 work in progress.

## Step 10b — Spec changes of 2026-10-13

The design changed after the C5a/C5d live run and the design review. Bring the code to the spec. Work in vertical slices, TDD on the fast tests, the full suite once at the end.

1. **Join token** (design 5.1, 5.2, 5.7):
   - Provider: a new `secrets.token_hex(32)` for each candidate at rent time, kept on the `Candidate`, sent as `RAYVAST_JOIN_TOKEN` in the create env. It ends with the candidate.
   - Head server: `/enroll` takes `join_token`, not `container_api_key`. The check order of design 5.7. A missing or wrong token gives 403 without a destroy. No vast call in `/enroll`. 503 only when the head has not seen the instance `running`.
   - Bootstrap: sends the token; removes `RAYVAST_JOIN_TOKEN` from its own env and from Ray's env; never sends `CONTAINER_API_KEY`.
   - Fakes and tests: the fake head and the fake vast follow the new body.
   - Redaction tests: the token never appears in a log, an exception, the container stdout or Ray's env.
2. **5 security groups** (design 7.1, 8.3): `aws_sg.py` (first group with room, next group on `RulesPerSecurityGroupLimitExceeded`, `revoke_ip` from the group that holds the rule, `revoke_all` over all 5), `commands.py` (`up` makes 5 tagged groups on the head's interface; `down` deletes all 5), the IAM policy (all 5 group ARNs).
3. **No restart policy** (design 7.3): remove `--restart unless-stopped` in `commands.py`.
4. **Rent-loop breaker** (design 4) in `provider.py`.
5. **CA expiry warning in the head log** (design 7.2): the provider logs the warning at start, 14 days before expiry.
6. **Log names** (decisions.md, last line): the log lines say "port check" and "orphan sweep", not "outside check".
7. **Test fixes:** add `pytest-timeout` (a `down` test can hang); fix or explain the failing bootloader container integration test.

Done when:
- [ ] The full suite passes.
- [ ] No code path sends `CONTAINER_API_KEY` to the head (grep and a test).
- [ ] A review on both axes finds no HARD item.

## Step 11 — Local end-to-end on the dev box ($0)

A fake vast server runs each "instance" as a local Docker container with the real bootloader and bootstrap, the identity ports mapped P→P, and `PUBLIC_IPADDR` = the dev box IP. The head runs in a container with `--network host`. This tests the whole flow (design 2) without vast cost.

Scenarios:
- Scale up 3 workers with k=2: 6 candidates, 3 slots filled, 3 destroyed. Run the C2 battery (`tests/manual/battery.py`, with `ray job submit`). Idle scale-down to 0.
- Two workers with the same public IP: both are seen, and scale-down removes the correct one (F3).
- A host with more cores than declared: Ray uses the real core count (F4).
- A candidate that never enrolls: the slot is removed and asked for again.
- Stop the head server for 6 min: both workers destroy themselves, and the report endpoint gets the reasons first.
- Container restart: the second enrollment is refused, and the worker destroys itself.
- Three enrollment failures in a row: the breaker stops the node type for 10 min (scaled time).

Done when:
- [ ] All scenarios pass 3 times in a row.

## Step 12 — Live tests on vast (paid)

Run the cycles C5, C6 and C8 of [test-plan.md](test-plan.md).

Done when:
- [ ] Each test has a row in `test-results.md` with key numbers and log links.
- [ ] The spend for the step is under the budget in `test-plan.md`.

## Step 13 — Docs and release

Work:
- Check the README, `security.md` and `CHANGELOG.md` against the code. Version `0.1.0`.
- Publish to TestPyPI, install in a clean venv on the dev box, and run the live no-worker cycle from that install. Then publish to PyPI, only with the user.
- After the release, `up` installs `ray-vast==<the CLI's own version>` from PyPI by default; `--wheel` stays for development.

Done when:
- [ ] `pip install ray-vast` from PyPI and `ray-vast up` work on a clean machine.
- [ ] The wheel has no secrets and no test fixtures with real IDs (scan).
