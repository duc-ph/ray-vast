"""The rayvast shim (docs/shim.md) LD_PRELOADed into small Python processes on this Linux host.

RAYVAST_PUBLIC_IP is a TEST-NET address that no host owns. RAYVAST_PRIVATE_IP is 127.0.0.2, which Linux routes on the
loopback interface without setup. The tests use the committed .so, or the .so in RAYVAST_TEST_SHIM_SO (the mutation
run of scripts/shim_mutants.py).
"""
import json
import os
import socket
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(not sys.platform.startswith("linux"), reason="the shim is a Linux LD_PRELOAD library"),
]

SHIM = Path(
    os.environ.get("RAYVAST_TEST_SHIM_SO")
    or Path(__file__).resolve().parents[2] / "src" / "ray_vast" / "shim" / "librayvast_shim.so"
)
PUBLIC = "203.0.113.10"
PRIVATE = "127.0.0.2"


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def shimmed(tmp_path):
    """Run Python code with the shim; the code prints one JSON value. The localhost port list has two ports, and
    `lp` (the second one) is the port that the tests use."""
    assert SHIM.is_file(), f"no shim at {SHIM}"
    lp = free_port()
    log = tmp_path / "shim.log"

    def run(code, **env):
        full_env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "LD_PRELOAD": str(SHIM),
            "RAYVAST_PUBLIC_IP": PUBLIC,
            "RAYVAST_PRIVATE_IP": PRIVATE,
            "RAYVAST_LOCALHOST_PORTS": f"{free_port()},{lp}",
            "RAYVAST_SHIM_LOG": str(log),
            **env,
        }
        result = subprocess.run(
            [sys.executable, "-I", "-c", "import json, socket\n" + textwrap.dedent(code)],
            env=full_env, cwd=tmp_path, capture_output=True, text=True, timeout=30, check=False,
        )
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)

    run.lp = lp
    run.log = log
    return run


def test_bind_any_on_localhost_port_binds_loopback(shimmed):
    name = shimmed(f"""
        s = socket.socket(); s.bind(("0.0.0.0", {shimmed.lp}))
        print(json.dumps(s.getsockname()))
    """)
    assert name == ["127.0.0.1", shimmed.lp]


def test_second_bind_to_localhost_port_in_same_process_works(shimmed):
    # Ray's dashboard agent binds its job port to the node IP, then to 127.0.0.1 (the v2 bug).
    names = shimmed(f"""
        a = socket.socket(); a.bind(("{PUBLIC}", {shimmed.lp})); a.listen()
        b = socket.socket(); b.bind(("127.0.0.1", {shimmed.lp})); b.listen()
        print(json.dumps([a.getsockname(), b.getsockname()]))
    """)
    assert names == [["127.0.0.1", shimmed.lp], ["127.0.0.1", shimmed.lp]]


def test_bind_public_ip_binds_private_ip(shimmed):
    port = free_port()
    name = shimmed(f"""
        s = socket.socket(); s.bind(("{PUBLIC}", {port}))
        print(json.dumps(s.getsockname()))
    """)
    assert name == [PRIVATE, port]


def test_connect_public_ip_on_localhost_port_reaches_loopback_listener(shimmed):
    peer = shimmed(f"""
        server = socket.socket(); server.bind(("127.0.0.1", {shimmed.lp})); server.listen()
        c = socket.socket(); c.settimeout(5); c.connect(("{PUBLIC}", {shimmed.lp}))
        print(json.dumps(c.getpeername()))
    """)
    assert peer == ["127.0.0.1", shimmed.lp]


def test_connect_public_ip_reaches_private_ip_listener(shimmed):
    port = free_port()
    peer = shimmed(f"""
        server = socket.socket(); server.bind(("{PRIVATE}", {port})); server.listen()
        c = socket.socket(); c.settimeout(5); c.connect(("{PUBLIC}", {port}))
        print(json.dumps(c.getpeername()))
    """)
    assert peer == [PRIVATE, port]


def test_other_addresses_and_ports_pass_through(shimmed):
    port, other_port = free_port(), free_port()
    names = shimmed(f"""
        a = socket.socket(); a.bind(("127.0.0.3", {port}))
        b = socket.socket(); b.bind(("0.0.0.0", {other_port})); b.listen()
        c = socket.socket(); c.settimeout(5); c.connect(("127.0.0.1", {other_port}))
        print(json.dumps([a.getsockname(), b.getsockname(), c.getpeername()]))
    """)
    assert names == [["127.0.0.3", port], ["0.0.0.0", other_port], ["127.0.0.1", other_port]]


def test_plain_ipv6_address_passes_through_also_on_localhost_port(shimmed):
    name = shimmed(f"""
        s = socket.socket(socket.AF_INET6); s.bind(("::1", {shimmed.lp}))
        print(json.dumps(s.getsockname()[:2]))
    """)
    assert name == ["::1", shimmed.lp]


# gRPC makes dual-stack AF_INET6 sockets and passes IPv4 addresses as ::ffff:a.b.c.d.
def test_ipv6_any_on_localhost_port_binds_loopback(shimmed):
    name = shimmed(f"""
        s = socket.socket(socket.AF_INET6); s.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        s.bind(("::", {shimmed.lp}))
        print(json.dumps(s.getsockname()[:2]))
    """)
    assert name == ["::ffff:127.0.0.1", shimmed.lp]


def test_ipv4_mapped_public_ip_binds_and_connects_to_private_ip(shimmed):
    port = free_port()
    names = shimmed(f"""
        server = socket.socket(socket.AF_INET6); server.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        server.bind(("::ffff:{PUBLIC}", {port})); server.listen()
        c = socket.socket(socket.AF_INET6); c.settimeout(5); c.connect(("::ffff:{PUBLIC}", {port}))
        print(json.dumps([server.getsockname()[:2], c.getpeername()[:2]]))
    """)
    assert names == [[f"::ffff:{PRIVATE}", port], [f"::ffff:{PRIVATE}", port]]


def test_ipv4_mapped_public_ip_on_localhost_port_connects_to_loopback(shimmed):
    peer = shimmed(f"""
        server = socket.socket(); server.bind(("127.0.0.1", {shimmed.lp})); server.listen()
        c = socket.socket(socket.AF_INET6); c.settimeout(5); c.connect(("::ffff:{PUBLIC}", {shimmed.lp}))
        print(json.dumps(c.getpeername()[:2]))
    """)
    assert peer == ["::ffff:127.0.0.1", shimmed.lp]


def test_failed_rewritten_connect_keeps_errno(shimmed):
    port = free_port()  # no listener on the private IP
    errno = shimmed(f"""
        c = socket.socket(); c.settimeout(5)
        try:
            c.connect(("{PUBLIC}", {port}))
        except OSError as e:
            print(json.dumps(e.errno))
    """)
    assert errno == 111  # ECONNREFUSED from the private IP


def test_successful_rewritten_bind_keeps_errno_when_log_write_fails(shimmed):
    # A C caller can read errno after a call that worked. The log open fails here (no such directory) and sets errno.
    port = free_port()
    errno = shimmed(f"""
        import ctypes, struct
        libc = ctypes.CDLL(None, use_errno=True)
        s = socket.socket()
        addr = struct.pack("=H", socket.AF_INET) + struct.pack(">H", {port}) + socket.inet_aton("{PUBLIC}") + bytes(8)
        ctypes.set_errno(0)
        assert libc.bind(s.fileno(), addr, len(addr)) == 0
        print(json.dumps(ctypes.get_errno()))
    """, RAYVAST_SHIM_LOG="/nonexistent-dir/shim.log")
    assert errno == 0


def test_log_has_one_line_for_each_rewrite(shimmed):
    port, other_port = free_port(), free_port()
    pid, comm = shimmed(f"""
        import os
        server = socket.socket(); server.bind(("{PUBLIC}", {port})); server.listen()
        c = socket.socket(); c.settimeout(5); c.connect(("{PUBLIC}", {port}))
        other = socket.socket(); other.bind(("127.0.0.3", {other_port}))  # not a rewrite
        print(json.dumps([os.getpid(), open("/proc/self/comm").read().strip()]))
    """)
    # Format: time pid comm call old -> new af
    lines = [line.split()[1:] for line in shimmed.log.read_text().splitlines()]
    assert lines == [
        [str(pid), comm, "bind", f"{PUBLIC}:{port}", "->", f"{PRIVATE}:{port}", "af=inet"],
        [str(pid), comm, "connect", f"{PUBLIC}:{port}", "->", f"{PRIVATE}:{port}", "af=inet"],
    ]


def test_log_survives_overwritten_environment_memory(shimmed):
    # Ray's setproctitle overwrites the environment strings of the process (the v3 bug). This clears the
    # RAYVAST_SHIM_LOG value in place, then makes one rewrite.
    port = free_port()
    shimmed(f"""
        import ctypes
        libc = ctypes.CDLL(None)
        environ = ctypes.POINTER(ctypes.c_char_p).in_dll(libc, "environ")
        entries = ctypes.cast(environ, ctypes.POINTER(ctypes.c_void_p))
        i = 0
        while entries[i]:
            entry = ctypes.string_at(entries[i])
            if entry.startswith(b"RAYVAST_SHIM_LOG="):
                ctypes.memset(entries[i] + len(b"RAYVAST_SHIM_LOG="), 0, len(entry) - len(b"RAYVAST_SHIM_LOG="))
            i += 1
        s = socket.socket(); s.bind(("{PUBLIC}", {port}))
        print(json.dumps(None))
    """)
    assert shimmed.log.exists() and len(shimmed.log.read_text().splitlines()) == 1
