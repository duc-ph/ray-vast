"""The rayvast shim LD_PRELOADed into small Python processes on this Linux host, and into a Ray head in Docker.

RAYVAST_PUBLIC_IP is a TEST-NET address that no host owns. RAYVAST_PRIVATE_IP is 127.0.0.2, which Linux routes on the
loopback interface without setup. The tests use the committed .so, or the .so in RAYVAST_TEST_SHIM_SO (for example a
build of a changed shim source).
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(not sys.platform.startswith("linux"), reason="the shim is a Linux LD_PRELOAD library"),
]

SHIM = Path(
    os.environ.get("RAYVAST_TEST_SHIM_SO")
    or Path(__file__).resolve().parents[1] / "src" / "ray_vast" / "shim" / "librayvast_shim.so"
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


# A worker sets RAYVAST_OWN_PORTS (its identity ports). Another worker can share its public IP: two containers on one
# host, or two hosts behind one NAT IP. Each has its own network namespace, so a connect to the public IP goes to this
# container only for an own port, or for a port that a socket in this namespace listens on.
def test_with_own_ports_a_connect_to_another_port_of_the_public_ip_is_not_rewritten(shimmed):
    port, own = free_port(), free_port()  # no listener on `port` in this namespace
    errno = shimmed(f"""
        c = socket.socket(); c.settimeout(1)
        try:
            c.connect(("{PUBLIC}", {port}))
        except OSError as e:
            print(json.dumps(e.errno))
    """, RAYVAST_OWN_PORTS=str(own))
    assert errno != 111  # not ECONNREFUSED from the private IP: the connect went to the public IP
    assert not shimmed.log.exists() or shimmed.log.read_text() == ""


def test_with_own_ports_a_connect_to_an_own_port_of_the_public_ip_goes_to_the_private_ip(shimmed):
    own = free_port()  # no listener: the rewrite does not depend on one
    errno = shimmed(f"""
        c = socket.socket(); c.settimeout(5)
        try:
            c.connect(("{PUBLIC}", {own}))
        except OSError as e:
            print(json.dumps(e.errno))
    """, RAYVAST_OWN_PORTS=f"{free_port()},{own}")
    assert errno == 111  # ECONNREFUSED from the private IP
    assert f"connect {PUBLIC}:{own} -> {PRIVATE}:{own}" in shimmed.log.read_text()


@pytest.mark.parametrize("family", ["ipv4", "dual-stack ipv6"])
def test_with_own_ports_a_connect_to_a_port_that_this_namespace_listens_on_goes_to_the_private_ip(shimmed, family):
    # Ray Serve's HTTP port and a torch MASTER_PORT are not identity ports. gRPC listens on dual-stack IPv6 sockets.
    port = free_port()
    server = ('socket.socket(); server.bind(("0.0.0.0", %d))' % port if family == "ipv4" else
              'socket.socket(socket.AF_INET6); server.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0); '
              'server.bind(("::", %d))' % port)
    peer = shimmed(f"""
        server = {server}; server.listen()
        c = socket.socket(); c.settimeout(5); c.connect(("{PUBLIC}", {port}))
        print(json.dumps(c.getpeername()))
    """, RAYVAST_OWN_PORTS=str(free_port()))
    assert peer == [PRIVATE, port]


RAY_IMAGE = "rayproject/ray:2.59.0-py311-cpu"
# Answer every connection on each port in argv with the container's name, until killed.
NAME_SERVER = """
import socket, sys, threading
def serve(port):
    s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(("0.0.0.0", port)); s.listen()
    while True:
        c, _ = s.accept(); c.sendall(sys.argv[1].encode()); c.close()
for p in sys.argv[2:]:
    threading.Thread(target=serve, args=(int(p),)).start()
print("ready", flush=True)
"""
NAME_CLIENT = """
import socket, sys, time
out = []
for p in sys.argv[2:]:
    try:
        c = socket.create_connection((sys.argv[1], int(p)), timeout=5); out.append(c.recv(10).decode())
    except OSError as e:
        out.append(type(e).__name__)
print("ANSWERS", " ".join(out))
"""


@pytest.mark.skipif(shutil.which("docker") is None, reason="needs Docker")
@pytest.mark.timeout(300)
def test_a_worker_reaches_a_neighbor_container_on_its_own_public_ip_and_keeps_its_own_ports():
    """Two containers, each in its own network namespace. A advertises B's IP as its public IP, as two workers do on one
    vast host (there, hairpin NAT takes the connection from the public IP to B). A connects to: B's own port, which goes
    to B; its own port and a port that it listens on, which go to A. Each port answers with its container's name."""
    from fakes.worker_world import CONTAINER_PREFIX
    a, b = CONTAINER_PREFIX + "shim-a", CONTAINER_PREFIX + "shim-b"
    own_a, own_b, other = 41001, 41002, 41003
    try:
        for name, ports in ((a, (own_a, other)), (b, (own_a, own_b, other))):
            subprocess.run(["docker", "run", "-d", "--name", name, "-v", f"{SHIM}:/shim/librayvast_shim.so:ro", RAY_IMAGE,
                            "python", "-c", NAME_SERVER, name[-1], *map(str, ports)], check=True, capture_output=True)
        for name in (a, b):
            deadline = time.monotonic() + 60
            while "ready" not in subprocess.run(["docker", "logs", name], capture_output=True, text=True).stdout:
                assert time.monotonic() < deadline, f"{name} did not start"
                time.sleep(0.5)
        b_ip = subprocess.run(["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", b],
                              check=True, capture_output=True, text=True).stdout.strip()
        client = subprocess.run(
            ["docker", "exec", "-e", "LD_PRELOAD=/shim/librayvast_shim.so", "-e", f"RAYVAST_PUBLIC_IP={b_ip}",
             "-e", "RAYVAST_PRIVATE_IP=127.0.0.1", "-e", "RAYVAST_LOCALHOST_PORTS=1", "-e", f"RAYVAST_OWN_PORTS={own_a}",
             a, "python", "-c", NAME_CLIENT, b_ip, str(own_b), str(own_a), str(other)],
            capture_output=True, text=True, timeout=60)
    finally:
        subprocess.run(["docker", "rm", "-f", a, b], capture_output=True)
    assert "ANSWERS b a a" in client.stdout, client.stdout + client.stderr


RAY_PORTS = ("--port=6379 --dashboard-host=127.0.0.1 --node-manager-port=6380 --object-manager-port=6381 --metrics-export-port=6382 "
             "--dashboard-agent-grpc-port=6383 --dashboard-agent-listen-port=6384 --runtime-env-agent-port=6385 "
             "--min-worker-port=10002 --max-worker-port=10100")
RAY_SCRIPT = f"""
set -u
PRIV=$(hostname -i | awk '{{print $1}}')
export RAY_enable_autoscaler_v2=1
if [ "$MODE" = shim ]; then
  export LD_PRELOAD=/shim/librayvast_shim.so RAYVAST_PUBLIC_IP=203.0.113.10 RAYVAST_PRIVATE_IP=$PRIV RAYVAST_LOCALHOST_PORTS=6384,6385
fi
timeout 120 ray start --head --node-ip-address=203.0.113.10 --num-cpus=1 {RAY_PORTS} > /tmp/start.log 2>&1; echo "ray start exit $?"
sleep 30
ray list nodes --format json 2>/dev/null | python -c 'import json,sys; print("NODES", json.dumps([n["state"] for n in json.load(sys.stdin)]))' || echo "NODES none"
timeout 180 ray job submit --address http://127.0.0.1:8265 -- python -c 'import ray; ray.init(); print("RESULT", ray.get(ray.remote(lambda: 7).remote()))' 2>&1 | grep -E "RESULT|succeeded|failed"
echo "ERRNO99 $(grep -l 'Errno 99' /tmp/ray/session_latest/logs/* 2>/dev/null | wc -l)"
"""


@pytest.mark.skipif(shutil.which("docker") is None, reason="needs Docker")
@pytest.mark.timeout(1800)
def test_a_ray_head_that_advertises_a_public_ip_it_does_not_own_works_only_with_the_shim():
    """A Ray head in a container advertises 203.0.113.10, an IP that it does not own. With the shim: no Errno 99, the node
    is ALIVE, and a job through the shimmed job port succeeds. Without it: Errno 99 or no job."""
    out = {}
    for mode in ("shim", "control"):
        done = subprocess.run(["docker", "run", "--rm", "-e", f"MODE={mode}", "-v", f"{SHIM.parent}:/shim:ro", RAY_IMAGE, "bash", "-c",
                               RAY_SCRIPT], capture_output=True, text=True, timeout=600)
        out[mode] = done.stdout + done.stderr
    assert 'NODES ["ALIVE"]' in out["shim"], out["shim"][-2000:]
    assert "RESULT 7" in out["shim"] and "ERRNO99 0" in out["shim"], out["shim"][-2000:]
    assert "ERRNO99 0" not in out["control"] or "RESULT 7" not in out["control"], out["control"][-2000:]
