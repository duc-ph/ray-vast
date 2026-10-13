# Test plan

Open tests for [design.md](design.md). The cycles C0–C4 ran on 2026-10-12/13. Their setup, results and findings are in [test-results.md](test-results.md). The raw data and the plan that ran are in `archive/results/`. C2–C4 used SSH only as a test tool. The design does not use SSH.

Before a cycle runs, write it here with the same detail as the archived plan: setup, steps, expected output, data to capture, and teardown.

## Proven (C0–C4)

- The shim makes Ray work across NAT, also on hosts without hairpin NAT, on the head and on workers.
- TLS + token: plaintext, a missing cert and an unknown CA are refused; HTTP without a token gives 401.
- Both agent HTTP ports can listen on 127.0.0.1 only. The token never goes over the network.
- `CONTAINER_API_KEY` can destroy its own instance, and cannot read or destroy other instances.
- A lost worker is DEAD on the head after about 41 s.
- Port exhaustion: tasks wait, actors fail after about 6 min, drivers fail at once.
- When PID 1 exits, vast starts the container again (the instance keeps its cost).
- Heartbeat watchdog (C4, test version over SSH): heartbeat age stayed at 40 s or less for 13 min. A pause of 2 min 18 s gave no destroy. After the last heartbeat, the instance destroyed itself after 5 min 11 s.
- After a destroy, vast keeps no container log. The watchdog must send its destroy reason to the head first.
- From inside the container, `kill -9 1` gives no error, but the kernel ignores it.

## Open tests

| # | Item | Needs the provider? |
|---|---|---|
| T15 | **Bootloader on different images.** The bootloader finds the Python with Ray, downloads the bootstrap, checks its SHA-256, and `exec`s it as PID 1. Use 3 images: `rayproject/ray`, a slim image with Ray that runs as non-root, and an image with an ENTRYPOINT (expect a clear failure). This also shows that vast accepts the bootloader in `args`. | No |
| T16 | **Enrollment security.** Refuse: another instance's `CONTAINER_API_KEY`, a second enrollment of the same instance (also after a container restart), a request from a different IP, and an instance that is not a candidate. Also check whether vast's instance list shows the `instance_api_key` of other instances. | No (a test head service) |
| T13 | **Watchdog as PID 1.** Ray runs as the image's user (same user as PID 1). `kill -TERM/-INT/-HUP 1` from a Ray task gives a destroy, never an exit. `kill -9 1` has no effect. A task's `os.environ` has no `CONTAINER_API_KEY`. Test on a root image and on a non-root image. The watchdog collects its finished child processes. | No |
| T14 | **Heartbeat to the head.** Stop the head's heartbeat endpoint: destroy after 5 min (± 30 s). Stop it for 2 min only: no destroy. No enrollment within 10 min: destroy. The head gets the destroy reason before the destroy. (C4 proved the timing with a test watchdog; repeat with the real bootstrap.) | No (a test head service) |
| T7 | **IP change, full flow:** the provider destroys the worker, removes its rule (only if no other worker uses the IP), and the autoscaler gets a new worker. | Yes |
| T12 | Cert reload without a restart. | Deferred (certs are valid for 365 days) |

## Results

| # | Result | Key numbers | Notes and links to logs |
|---|---|---|---|
| T15 | | | |
| T16 | | | |
| T13 | Partly done (C4, old sudo PID 1) | `kill -9` of the watchdog as ray: refused | sudo as PID 1 let ray send TERM/INT; this design was dropped. Repeat with the bootstrap as PID 1 and Ray as the same user. |
| T14 | Timing passed (C4) | max age 40 s; pause 134 s no destroy; destroy at t0+5:11 | `archive/results/2026-10-13/progress.md`; destroy-reason lines lost |
| T7 | | | |
