#!/usr/bin/env python3
"""Mutation run for the shim tests (docs/implementation-plan.md step 10c).

Makes one mutant of src/ray_vast/shim/rayvast_shim.c for each entry in MUTANTS (one small change each), builds the
unchanged source and all mutants in quay.io/pypa/manylinux2014_x86_64 (as scripts/build_shim.sh does), runs
tests/integration/test_shim.py against each .so, and prints killed or survived. The unchanged build must pass.

Run from the package directory with the test venv, where Docker works:
    .venv/bin/python scripts/shim_mutants.py
Exit code 0 when the unchanged build passes; the survivors are printed (explain each one in docs/test-results.md).
"""
import hashlib
import os
import subprocess
import sys
import tempfile
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1]
SOURCE = PACKAGE / "src" / "ray_vast" / "shim" / "rayvast_shim.c"
TEST = PACKAGE / "tests" / "integration" / "test_shim.py"

# name: (text in the source, its replacement). Each text must occur exactly once.
MUTANTS = {
    "rule1-removed": ("if (is_bind && is_localhost_port) {", "if (0) {"),
    "rule1-target-private": (
        "target.s_addr = htonl(INADDR_LOOPBACK);\n        *loopback_bind = 1;",
        "target = private_ip;\n        *loopback_bind = 1;",
    ),
    "rule3-removed": ("} else if (is_public && !is_bind && is_localhost_port) {", "} else if (0) {"),
    "rules2-4-removed": ("} else if (is_public && have_private) {", "} else if (0) {"),
    "rule2-removed": ("} else if (is_public && have_private) {", "} else if (is_public && have_private && !is_bind) {"),
    "rule4-removed": ("} else if (is_public && have_private) {", "} else if (is_public && have_private && is_bind) {"),
    "port-compare-flipped": ("if (localhost_ports[i] == port)", "if (localhost_ports[i] != port)"),
    "public-compare-flipped": (
        "is_public = !v6_any && have_public && ip4->s_addr == public_ip.s_addr;",
        "is_public = !v6_any && have_public && ip4->s_addr != public_ip.s_addr;",
    ),
    "same-target-compare-flipped": (
        "if (!v6_any && ip4->s_addr == target.s_addr)", "if (!v6_any && ip4->s_addr != target.s_addr)"
    ),
    "port-range-flipped": ("p > 0 && p < 65536", "p > 0 && p > 65536"),
    "port-list-first-only": ("s = strchr(end, ',');", "s = NULL;"),
    "inet-length-check-flipped": (
        "addr->sa_family == AF_INET && len >= sizeof(struct sockaddr_in))",
        "addr->sa_family == AF_INET && len < sizeof(struct sockaddr_in))",
    ),
    "inet6-family-check-wrong": (
        "addr->sa_family == AF_INET6 && len >= sizeof(struct sockaddr_in6)",
        "addr->sa_family == AF_INET && len >= sizeof(struct sockaddr_in6)",
    ),
    "inet6-v4mapped-check-removed": (
        "if (!IN6_IS_ADDR_V4MAPPED(&sin6->sin6_addr) && !v6_any)\n            return 0;\n", ""
    ),
    "inet6-any-not-detected": ("v6_any = IN6_IS_ADDR_UNSPECIFIED(&sin6->sin6_addr);", "v6_any = 0;"),
    "inet6-any-no-ffff-prefix": ("sin6->sin6_addr.s6_addr[10] = 0xff;\n", ""),
    "inet6-length-wrong": ("*out_len = sizeof(struct sockaddr_in6);", "*out_len = sizeof(struct sockaddr_in);"),
    "reuseport-removed": ("setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one);", "(void)one;"),
    "connect-uses-old-address": (
        "return real_connect(fd, (const struct sockaddr *)&new_addr, new_len);", "return real_connect(fd, addr, len);"
    ),
    "errno-save-removed-in-log": ("    errno = saved_errno;\n}\n\nint bind", "}\n\nint bind"),
    "errno-save-removed-in-bind": ("        errno = saved_errno;\n    }\n    if (changed)", "    }\n    if (changed)"),
    "errno-save-removed-in-init": ("    errno = saved_errno;\n}\n\n__attribute__", "}\n\n__attribute__"),
    "env-copy-removed": (
        "static char log_path[512];",
        "static const char *log_path = \"\";",
    ),
    "log-bind-removed": ('log_rewrite(loopback_bind ? "bind+reuseport" : "bind", addr, &new_addr);', ""),
    "log-connect-removed": ('log_rewrite("connect", addr, &new_addr);', ""),
}
# The env-copy mutant also needs the copy code replaced by a pointer assignment.
ENV_COPY_CODE = "    log_path[0] = 0;\n    if (s && strlen(s) < sizeof log_path)\n        strcpy(log_path, s);\n"


def main():
    source = SOURCE.read_text()
    work = Path(tempfile.mkdtemp(prefix="shim-mutants-"))
    # Each source keeps the name rayvast_shim.c (the name goes into the .so), so the unchanged build is the same file
    # as the committed .so.
    (work / "original").mkdir()
    (work / "original" / "rayvast_shim.c").write_text(source)
    for name, (old, new) in MUTANTS.items():
        assert source.count(old) == 1, f"{name}: the text occurs {source.count(old)} times"
        mutant = source.replace(old, new)
        if name == "env-copy-removed":
            assert mutant.count(ENV_COPY_CODE) == 1
            mutant = mutant.replace(ENV_COPY_CODE, "    log_path = s ? s : \"\";\n")
        (work / name).mkdir()
        (work / name / "rayvast_shim.c").write_text(mutant)

    build = subprocess.run(
        ["docker", "run", "--rm", "--name", f"st10c-shim-mutants-{os.getpid()}", "-u", f"{os.getuid()}:{os.getgid()}",
         "-v", f"{work}:/work", "quay.io/pypa/manylinux2014_x86_64", "sh", "-eu", "-c",
         'for d in /work/*/; do cd "$d" && gcc -shared -fPIC -O2 -o librayvast_shim.so rayvast_shim.c -ldl; done'],
        capture_output=True, text=True, check=False,
    )
    if build.returncode != 0:
        sys.exit(f"build failed:\n{build.stdout}{build.stderr}")
    print(f"mutants in {work}", flush=True)
    committed = hashlib.sha256((SOURCE.parent / "librayvast_shim.so").read_bytes()).hexdigest()
    rebuilt = hashlib.sha256((work / "original" / "librayvast_shim.so").read_bytes()).hexdigest()
    if rebuilt != committed:
        sys.exit(f"the unchanged source builds to sha256 {rebuilt}, not to the committed .so {committed}")
    print("the unchanged source builds to the committed .so", flush=True)

    def run_tests(so):
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "-o", "addopts=", "-q", "-p", "no:cacheprovider", str(TEST)],
            env={**os.environ, "RAYVAST_TEST_SHIM_SO": str(so)}, cwd=PACKAGE, capture_output=True, text=True, check=False,
        )
        if result.returncode not in (0, 1):  # 1 = some tests failed; anything else is a broken run
            sys.exit(f"pytest error for {so}:\n{result.stdout}{result.stderr}")
        failed = [line.split(" ")[1].split("::")[-1] for line in result.stdout.splitlines() if line.startswith("FAILED")]
        return result.returncode == 0, failed

    passed, failed = run_tests(work / "original" / "librayvast_shim.so")
    if not passed:
        sys.exit(f"the unchanged source fails the tests: {failed}")
    survivors = []
    for name in MUTANTS:
        survived, failed = run_tests(work / name / "librayvast_shim.so")
        print(f"{'SURVIVED' if survived else 'killed  '} {name}" + (f"  ({', '.join(failed)})" if failed else ""), flush=True)
        if survived:
            survivors.append(name)
    print(f"{len(MUTANTS) - len(survivors)} killed, {len(survivors)} survived: {', '.join(survivors) or '-'}")


if __name__ == "__main__":
    main()
