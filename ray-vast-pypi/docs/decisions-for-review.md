# Decisions for review

Technical decisions made while the user was away. Rule: simplest option that works, no compromise on reliability, no fragility. Each line: date, decision, reason, where.

- 2026-10-13 — The wheel is pure Python (`py3-none-any`) but carries the x86_64 Linux shim `.so` as package data. Reason: the `.so` is loaded only by `LD_PRELOAD` inside Linux containers, never imported; one wheel for all platforms. (Step 1)
- 2026-10-13 — The sdist includes only `src`, `tests`, `scripts` and the top-level files, not `docs/` or `spikes/`. Reason: keep spike logs out of the published package. (Step 1)
- 2026-10-13 — The packaging test installs the wheel with `--no-deps` and builds with `--no-isolation`. Reason: fast and offline; the CLI entry point needs only the stdlib to show `--help`. (Step 1)
- 2026-10-13 — Code review is run per step for steps with logic; step 1 (packaging only) is reviewed together with step 2. Reason: a skeleton has nothing for the Spec/Standards axes to find.
- 2026-10-13 — `create_instance` retries only on HTTP 429, not on 5xx or connection errors. Reason: a lost reply may already have rented the offer; a retry could rent twice. A rental that we did not record is found by the outside check (label sweep) and destroyed. (Step 3)
- 2026-10-13 — vast retries: 5 tries, backoff 1-2-4-8 s with jitter, 30 s timeout per call. A destroy of an instance that is already gone (404) is a success. (Step 3)
- 2026-10-13 — `max_dollars_per_hour` is required (no default). Reason: a missing budget must not mean "no limit". `node_config.disk_gb` defaults to 50. (Step 2)
- 2026-10-13 — A user filter on an added field (`cpu_cores_effective`, `direct_port_count`) is allowed when it is at least as strict, and it is merged; `num_gpus` must equal `GPU`. Reason: lets one node type per CPU size (`{gte: 8, lt: 16}`). (Step 2)
- 2026-10-13 — The vast key is not a config field. The CLI reads it from `VAST_API_KEY` or `~/.config/vastai/vast_api_key` (the vast CLI's file). Reason: keeps the secret out of the cluster YAML. The field is `cert_max_age_days` (plan said `cert_max_age`), for a clear unit. (Step 2 review)
- 2026-10-13 — Provider fields other than `region`, `image` and `max_dollars_per_hour` have defaults (`candidate_factor` 2, `cert_max_age_days` 365, `allow_non_datacenter` false, `order_by` `dph_total`, `head_instance_type` `t3.large`). (Step 2 review)
- 2026-10-13 — The packaging test does not install the full Ray dependency set in a clean venv (it uses `--no-deps`). Reason: Ray is large; the full install is exercised by the head container in step 10 and the end-to-end tests. (Step 1 review)
- 2026-10-13 — PKI: RSA-2048 keys (proven with Ray gRPC in C0–C4); leaf certs have serverAuth + clientAuth; `not_before` is 5 min in the past (worker clock skew); CA `pathlen:0`; `cryptography>=42`. (Step 4)
- 2026-10-13 — Fingerprint = SHA-256 of the CA cert's DER bytes, lowercase hex (S5). (Step 4)
- 2026-10-13 — The head server uses two ports: plain TLS (download, enrollment) and mTLS (heartbeat, reports). Reason: one TLS context cannot require client certs for only some paths (S5).
