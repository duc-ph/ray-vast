# rayvast shim

`rayvast_shim.c` is the final shim (v3). It passed all tests in C0–C4 (see [docs/test-results.md](../docs/test-results.md)). `librayvast_shim.so` is the build that the tests used.

- Source sha256 `8b31675b…6611`. It is the same file as `archive/results/2026-10-12/shim_builds/rayvast_shim.c`.
- Build sha256: `79055dea6e913180c362c81e2b0c9903dd7a39bac5e3d5bab0e491bb9b3817fd`.
- Older versions (v1, v2) and their builds are in `archive/results/2026-10-12/shim_builds/`.

## Build

In `quay.io/pypa/manylinux2014_x86_64` (glibc 2.17, gcc 10.2.1):

```
gcc -shared -fPIC -O2 -o librayvast_shim.so rayvast_shim.c -ldl
```

Check: `nm -D librayvast_shim.so` shows `T bind`, `T connect`, `U dlsym`. The highest symbol version is GLIBC_2.17. The build is clean with `-Wall -Wextra`.

## Use

Set these in the environment of every Ray process (head and workers):

| Variable | Value |
|---|---|
| `LD_PRELOAD` | path to `librayvast_shim.so` |
| `RAYVAST_PUBLIC_IP` | the IP that Ray advertises (`--node-ip-address`) |
| `RAYVAST_PRIVATE_IP` | the interface IP |
| `RAYVAST_LOCALHOST_PORTS` | comma list: the agent job port and the runtime-env agent port |
| `RAYVAST_SHIM_LOG` | optional: a file with one line for each rewrite (`time pid comm call old -> new`) |

## Rules

1. `bind(any, port in LOCALHOST_PORTS)` → `bind(127.0.0.1, port)`, with `SO_REUSEPORT`.
2. `bind(PUBLIC_IP, port)` → `bind(PRIVATE_IP, port)`.
3. `connect(PUBLIC_IP, port in LOCALHOST_PORTS)` → `connect(127.0.0.1, port)`.
4. `connect(PUBLIC_IP, port)` → `connect(PRIVATE_IP, port)`. So hairpin NAT is not necessary.
5. All other calls go through without a change.

IPv4 only. `AF_INET6` is handled only for `::ffff:a.b.c.d` (and `::` for rule 1), because gRPC uses dual-stack sockets.

## Version history

- **v1:** rules 1–5.
- **v2:** `SO_REUSEPORT` for rule 1. The dashboard agent binds the job port twice; without it, the second bind fails and `ray job submit` gives 500.
- **v3:** copy the environment values at start. Ray's setproctitle overwrites the environment memory, so v2 lost log lines in renamed processes.
