"""Worker bootstrap: parsers, and the whole bootstrap run as a subprocess against the fake head and fake vast.

The bootstrap runs under `python -I -S` (stdlib only: no site-packages, so no ray). Its Ray child is the fake
`ray.scripts.scripts` of tests/fakes/fake_ray_pkg, found through PYTHONPATH.
"""
import ast
import importlib.util
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fakes.worker_world import BOOTLOADER_PATH, BOOTSTRAP_PATH, FAKE_RAY_DIR, World, die_with_parent  # noqa: E402

spec = importlib.util.spec_from_file_location("rayvast_bootstrap", BOOTSTRAP_PATH)
bootstrap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bootstrap)

# Fast clock: every bootstrap timer is divided by 30 (heartbeat every 1 s, heartbeat timeout 10 s).
FAST = {"RAYVAST_TIME_SCALE": "30", "RAYVAST_SKIP_HOST_CHECKS": "1"}


def wait_for(condition, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.1)
    return False


class Run:
    """The bootstrap in a subprocess, with its stdout in a file."""

    def __init__(self, world, tmp_path, argv=None, **env_overrides):
        self.world = world
        self.ray_out = tmp_path / "ray_out.json"
        self.dir = tmp_path / "rayvast"  # RAYVAST_DIR: not the fixed /tmp/rayvast, so test runs can overlap
        self.stdout_path = tmp_path / "stdout.txt"
        # RAYVAST_CGROUP_DIR: the host's own cgroup must not decide the test. Default: cgroup v1, 2 cores, no process limit.
        no_pids_limit = tmp_path / "cgroup-none"
        (no_pids_limit / "cpu").mkdir(parents=True, exist_ok=True)
        (no_pids_limit / "cpu" / "cpu.cfs_quota_us").write_text("200000\n")
        (no_pids_limit / "cpu" / "cpu.cfs_period_us").write_text("100000\n")
        env = world.env()
        env.update(
            PATH=os.environ["PATH"], PYTHONPATH=str(FAKE_RAY_DIR), FAKE_RAY_OUT=str(self.ray_out),
            RAYVAST_DIR=str(self.dir), RAYVAST_CGROUP_DIR=str(no_pids_limit), **FAST
        )
        env.update(env_overrides)
        env = {k: v for k, v in env.items() if v is not None}  # an override of None removes the variable
        self.proc = subprocess.Popen(
            argv or [sys.executable, "-I", "-S", str(BOOTSTRAP_PATH)], env=env,
            stdout=open(self.stdout_path, "w"), stderr=subprocess.STDOUT, start_new_session=True,
            preexec_fn=die_with_parent,
        )

    @property
    def stdout(self):
        return self.stdout_path.read_text()

    @property
    def ray(self):
        return json.loads(self.ray_out.read_text())


@pytest.fixture
def new_world(tmp_path):
    """Return a function that makes a World (not started: change the fake head first, then `start()` it)."""
    worlds = []

    def make(**kwargs):
        w = World(tmp_path / f"world{len(worlds)}", **kwargs)
        runs = []
        w.run = lambda **overrides: runs.append(Run(w, tmp_path, **overrides)) or runs[-1]
        w.runs = runs
        worlds.append(w)
        return w

    yield make
    for w in worlds:
        for r in w.runs:
            try:
                os.killpg(r.proc.pid, signal.SIGKILL)  # the bootstrap and its (fake) ray child
            except ProcessLookupError:
                pass
            r.proc.wait()
        w.stop()


@pytest.fixture
def world(new_world):
    w = new_world()
    w.start()
    return w


def test_bootstrap_imports_only_stdlib():
    tree = ast.parse(BOOTSTRAP_PATH.read_text())
    imported = {
        alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
    } | {node.module.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    assert imported <= set(sys.stdlib_module_names)


def test_bootloader_is_small_posix_sh_and_found_as_package_data():
    import importlib.resources

    text = importlib.resources.files("ray_vast").joinpath("worker/bootloader.sh").read_text()
    assert text == BOOTLOADER_PATH.read_text()
    assert len(text.encode()) < 1536
    assert subprocess.run(["sh", "-n", str(BOOTLOADER_PATH)]).returncode == 0
    assert "'" not in text.replace("-c '", "").rstrip().rstrip("'")  # one single-quoted block, so `sh -c` can carry it


@pytest.mark.parametrize("text, cores", [("max 100000\n", None), ("200000 100000\n", 2.0), ("150000 100000", 1.5)])
def test_cgroup_cpu_max_parse(text, cores):
    assert bootstrap.parse_cpu_max(text) == cores


@pytest.mark.slow
def test_enrolls_and_starts_ray_with_the_right_flags_and_env(world, tmp_path):
    world.head.num_cpus = 5  # the node type declares CPU 5; the container has a quota of 2 cores
    run = world.run()
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat") and run.ray_out.exists()), run.stdout
    enroll = world.head.bodies("A", "/enroll")[0]
    assert enroll["instance_id"] == 424242
    assert enroll["join_token"] == world.join_token
    assert "container_api_key" not in enroll and world.api_key not in json.dumps(enroll)
    assert enroll["ports"] == {str(70000 + i): world.port_base + i for i in range(64)}
    assert set(enroll["checks"]) == {"egress_ip_ok", "no_net_raw_admin", "own_netns", "shm_empty", "details"}
    assert enroll["cpu_count"] == len(os.sched_getaffinity(0)) and enroll["cpu_quota"] == 2.0
    assert enroll["gpu_count"] == 0

    p = world.port_base
    argv = run.ray["argv"]
    assert argv[:2] == ["start", "--block"]
    assert set(argv[2:]) == {
        f"--address={world.head_ip}:6379", "--node-ip-address=127.0.0.1",
        f"--node-manager-port={p}", f"--object-manager-port={p + 1}", f"--metrics-export-port={p + 2}",
        f"--dashboard-agent-grpc-port={p + 3}", f"--dashboard-agent-listen-port={p + 4}",
        f"--runtime-env-agent-port={p + 5}",
        "--worker-port-list=" + ",".join(str(p + i) for i in range(6, 64)),
        "--num-cpus=5", "--num-gpus=0",  # Ray gets the declared CPU; the head checked the quota
    }
    env = run.ray["env"]
    assert env["RAY_CLOUD_INSTANCE_ID"] == "slot-1" and env["RAY_NODE_TYPE_NAME"] == "worker"
    assert env["RAY_USE_TLS"] == "1" and env["RAY_AUTH_MODE"] == "token"
    assert env["RAY_TLS_SERVER_CERT"] == f"{run.dir}/node.pem" and env["RAY_TLS_SERVER_KEY"] == f"{run.dir}/node.key"
    assert env["RAY_TLS_CA_CERT"] == f"{run.dir}/ca.pem" and env["RAY_AUTH_TOKEN_PATH"] == f"{run.dir}/token"
    assert env["LD_PRELOAD"] == f"{run.dir}/librayvast_shim.so"
    assert env["RAYVAST_PUBLIC_IP"] == "127.0.0.1" and env["RAYVAST_PRIVATE_IP"] == "127.0.0.1"
    assert env["RAYVAST_LOCALHOST_PORTS"] == f"{p + 4},{p + 5}"
    assert env["RAY_num_workers_soft_limit"] == "5"  # min(the declared CPU 5, 58 ports // 2)
    assert env["RAY_num_grpc_internal_threads"] == "4" and env["RAY_worker_maximum_startup_concurrency"] == "8"
    assert "CONTAINER_API_KEY" not in env and "RAYVAST_JOIN_TOKEN" not in env
    assert world.api_key not in env.values() and world.join_token not in env.values()

    assert oct(os.stat(run.dir).st_mode & 0o777) == "0o700"
    for name in ("ca.pem", "node.pem", "node.key", "token", "librayvast_shim.so"):
        assert oct(os.stat(f"{run.dir}/{name}").st_mode & 0o777) == "0o600", name
    assert Path(run.dir / "token").read_text() == world.token


@pytest.mark.parametrize("cpus, pids_max, ports, soft_limit", [
    # The soft limit is at most half the ports: idle workers must leave ports free for a new kind of worker.
    (48, None, 58, 29), (4, None, 58, 4), (48, 3328, 48, 24),
    (48, 1280, 14, 7), (4, 1280, 14, 4), (48, 1299, 14, 7), (48, 1300, 15, 7),
    (48, 900, 8, 4), (48, 500, 4, 2), (48, 100, 4, 2),
    # A node type with CPU 0: Ray's own soft limit for `--num-cpus=0` is 0 too (the default is the CPU count). Checked
    # on Ray 2.59: tasks and actors with num_cpus=0 run the same with RAY_num_workers_soft_limit=0 and without it.
    (0, None, 58, 0),
])
def test_worker_ports_fit_the_pids_limit_and_the_soft_limit_leaves_free_ports(cpus, pids_max, ports, soft_limit):
    assert bootstrap.worker_ports_and_soft_limit(cpus, pids_max, 64) == (ports, soft_limit)


@pytest.mark.parametrize("pids_max, identity_ports, ports", [
    (None, 20, 14),   # 20 identity ports: 6 fixed, 14 worker ports
    (1280, 30, 14),   # the pids limit (14) is below the mapped ports (24)
    (3328, 30, 24),   # the mapped ports (24) are below the pids limit (48)
    (500, 20, 4),     # the minimum of 4 worker ports
    (None, 10, 4),    # the fewest identity ports that an offer can have (direct_port_count >= 10)
])
def test_worker_ports_are_at_most_the_mapped_identity_ports_minus_the_6_fixed(pids_max, identity_ports, ports):
    assert bootstrap.worker_ports_and_soft_limit(48, pids_max, identity_ports)[0] == ports


@pytest.mark.slow
def test_a_worker_with_20_identity_ports_enrolls_them_and_gives_ray_14_worker_ports(new_world):
    world = new_world(identity_ports=20)
    world.start()
    run = world.run()
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat") and run.ray_out.exists()), run.stdout
    p = world.port_base
    assert world.head.bodies("A", "/enroll")[0]["ports"] == {str(70000 + i): p + i for i in range(20)}
    assert "--worker-port-list=" + ",".join(str(p + i) for i in range(6, 20)) in run.ray["argv"]
    assert world.head.bodies("B", "/heartbeat")[-1]["ports_total"] == 14


def cgroup_dir(tmp_path, layout, pids_max, events="max 17\n"):
    """A fake cgroup directory with 2 cores and the pids files in the place of cgroup v1 or v2."""
    cgroup = tmp_path / "cgroup"
    pids = cgroup if layout == "v2" else cgroup / "pids"
    pids.mkdir(parents=True)
    if layout == "v2":
        (cgroup / "cpu.max").write_text("200000 100000\n")
        (cgroup / "cgroup.controllers").write_text("cpu pids\n")
    else:
        (cgroup / "cpu").mkdir()
        (cgroup / "cpu" / "cpu.cfs_quota_us").write_text("200000\n")
        (cgroup / "cpu" / "cpu.cfs_period_us").write_text("100000\n")
    (pids / "pids.max").write_text(pids_max)
    (pids / "pids.current").write_text("1203\n")
    (pids / "pids.events").write_text(events)
    return cgroup


@pytest.mark.slow
def test_a_worker_port_that_an_outbound_connection_uses_or_left_in_time_wait_counts_as_in_use(world, tmp_path):
    # Seen in a live run: the identity ports are in the kernel's ephemeral range. An outbound connection had a worker
    # port as its local port (ESTABLISHED, no listener), so the heartbeat said 57 of 58 while Ray had no free port, and
    # the head wrote no WARNING. The TIME_WAIT of an outbound connection also blocks a new listener on that port, and
    # Ray's free-port check (a plain bind) fails on it.
    cgroup = cgroup_dir(tmp_path, "v2", "1280\n")
    run = world.run(RAYVAST_CGROUP_DIR=str(cgroup), FAKE_RAY_LISTEN_WORKER_PORTS="3", FAKE_RAY_CONNECT_FROM_WORKER_PORTS="1",
                    FAKE_RAY_TIME_WAIT_WORKER_PORTS="1")
    assert wait_for(lambda: len(world.head.bodies("B", "/heartbeat")) >= 2), run.stdout
    assert world.head.bodies("B", "/heartbeat")[-1]["ports_in_use"] == 5


@pytest.mark.slow
@pytest.mark.parametrize("layout", ["v2", "v1"])
def test_a_pids_limit_gives_fewer_worker_ports_and_the_heartbeat_reports_the_limit(world, tmp_path, layout):
    # cgroup v2 can also write a `max.imposed` line; only the `max` line counts the refusals.
    cgroup = cgroup_dir(tmp_path, layout, "1280\n", events="max.imposed 5\nmax 17\n")
    run = world.run(RAYVAST_CGROUP_DIR=str(cgroup), FAKE_RAY_LISTEN_WORKER_PORTS="3")  # 1 on IPv6, 2 on IPv4
    assert wait_for(lambda: len(world.head.bodies("B", "/heartbeat")) >= 2), run.stdout
    p = world.port_base
    assert "--worker-port-list=" + ",".join(str(p + i) for i in range(6, 6 + 14)) in run.ray["argv"]
    assert run.ray["env"]["RAY_num_workers_soft_limit"] == "2"
    assert world.head.bodies("A", "/enroll")[0]["cpu_quota"] == 2.0  # cpu.max (v2) or cpu.cfs_quota_us (v1)
    beat =world.head.bodies("B", "/heartbeat")[-1]
    assert beat == {"instance_id": 424242, "pids_current": 1203, "pids_max": 1280, "pids_refused": 17,
                    "ports_in_use": 3, "ports_total": 14}
    start_line = next(line for line in run.stdout.splitlines() if "pids.max" in line)
    assert start_line == (f"bootstrap: limits: pids.max 1280 (cgroup {layout}), cpus 2, processes at once 10, "
                          "worker ports 14, soft limit 2, grpc threads 4, startup concurrency 8")


@pytest.mark.slow
@pytest.mark.parametrize("quota, period, cores", [("150000\n", "100000\n", 1.5), ("-1\n", "100000\n", None),
                                                 ("150000\n", "0\n", None)])
def test_cgroup_v1_cpu_quota_is_reported_in_enroll_and_minus_1_is_no_limit(world, tmp_path, quota, period, cores):
    # cgroup v1 (no cgroup.controllers): the CPU controller is in cpu/, with the quota and the period in microseconds.
    cgroup = cgroup_dir(tmp_path, "v1", "max\n")
    (cgroup / "cpu" / "cpu.cfs_quota_us").write_text(quota)
    (cgroup / "cpu" / "cpu.cfs_period_us").write_text(period)
    run = world.run(RAYVAST_CGROUP_DIR=str(cgroup))
    assert wait_for(lambda: world.head.bodies("A", "/enroll")), run.stdout
    enroll = world.head.bodies("A", "/enroll")[0]
    assert enroll["cpu_quota"] == cores and enroll["cpu_count"] == len(os.sched_getaffinity(0))


@pytest.mark.slow
def test_a_pids_max_of_max_is_no_limit(world, tmp_path):
    run = world.run(RAYVAST_CGROUP_DIR=str(cgroup_dir(tmp_path, "v2", "max\n")))
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat")), run.stdout
    p = world.port_base
    assert "--worker-port-list=" + ",".join(str(p + i) for i in range(6, 64)) in run.ray["argv"]
    beat = world.head.bodies("B", "/heartbeat")[-1]
    assert beat["pids_max"] is None and beat["pids_refused"] is None and beat["ports_total"] == 58
    assert "pids.max none (cgroup v2)" in run.stdout


@pytest.mark.slow
def test_without_a_pids_limit_the_heartbeat_says_so(world, tmp_path):
    run = world.run()
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat")), run.stdout
    beat = world.head.bodies("B", "/heartbeat")[-1]
    assert beat["pids_max"] is None and beat["pids_refused"] is None and beat["ports_total"] == 58
    assert "pids.max none (cgroup v1)" in run.stdout


@pytest.mark.slow
@pytest.mark.parametrize("missing", ["CONTAINER_ID", "CONTAINER_API_KEY"])
def test_without_the_vast_instance_variables_pid_1_logs_and_waits_instead_of_exiting(world, tmp_path, missing):
    # PID 1 must not exit: vast would start the container again in a loop. The provider's time limits end the instance.
    run = world.run(**{missing: None})
    assert wait_for(lambda: "is not set" in run.stdout, timeout=10), run.stdout
    time.sleep(2)
    assert run.proc.poll() is None and world.head.events == [] and world.vast.destroys == []
    assert missing in run.stdout and world.api_key not in run.stdout


@pytest.mark.slow
def test_token_check_is_reported_ok_when_agent_ports_answer_401(world, tmp_path):
    run = world.run()
    assert wait_for(lambda: world.head.bodies("B", "/report")), run.stdout
    report = world.head.bodies("B", "/report")[0]
    assert (report["kind"], report["ok"]) == ("token_check", True)


@pytest.mark.slow
def test_token_check_is_reported_failed_when_an_agent_port_lets_a_tokenless_request_in(world, tmp_path):
    world.run(FAKE_RAY_AGENT_STATUS="200")
    assert wait_for(lambda: world.head.bodies("B", "/report"), timeout=40)
    report = world.head.bodies("B", "/report")[0]
    assert (report["kind"], report["ok"]) == ("token_check", False)


@pytest.mark.slow
def test_head_refusing_enrollment_destroys_at_once_without_starting_ray(world, tmp_path):
    world.head.refuse_enroll = True
    run = world.run()
    assert wait_for(lambda: world.vast.destroys)
    assert world.vast.bad_auth == 0
    report = world.head.bodies("A", "/report")[0]
    assert report["kind"] == "destroy" and "refused" in report["reason"]
    assert "container_api_key" not in report and world.api_key not in json.dumps(report)
    assert not run.ray_out.exists()


@pytest.mark.slow
def test_wrong_ca_fingerprint_destroys_before_any_enrollment(world, tmp_path):
    world.run(RAYVAST_CA_SHA256="0" * 64)
    assert wait_for(lambda: world.vast.destroys)
    assert world.head.bodies("A", "/enroll") == []


@pytest.mark.slow
def test_sigterm_reports_then_destroys_and_the_process_stays_alive(world, tmp_path):
    run = world.run()
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat"))
    run.proc.send_signal(signal.SIGTERM)
    assert wait_for(lambda: world.vast.destroys)
    destroy_report = [e for e in world.head.events if e[1] == "B" and e[3] and e[3].get("kind") == "destroy"][0]
    assert "SIGTERM" in destroy_report[3]["reason"]
    assert destroy_report[0] < world.vast.destroys[0][0]
    time.sleep(1)
    assert run.proc.poll() is None  # never exits: vast stops the container after the delete


@pytest.mark.slow
@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGHUP])
def test_sigint_and_sighup_destroy_too(world, tmp_path, sig):
    run = world.run()
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat"))
    run.proc.send_signal(sig)
    assert wait_for(lambda: world.vast.destroys)


@pytest.mark.slow
def test_ray_exit_destroys_with_its_exit_code(world, tmp_path):
    world.run(FAKE_RAY_EXIT_AFTER="2", FAKE_RAY_EXIT_CODE="3")
    assert wait_for(lambda: world.vast.destroys)
    reports = [e[3] for e in world.head.events if e[1] == "B" and e[3] and e[3].get("kind") == "destroy"]
    assert "code 3" in reports[0]["reason"]


@pytest.mark.slow
def test_heartbeats_stop_then_destroy_after_the_timeout_and_not_before(world, tmp_path):
    run = world.run(RAYVAST_TIME_SCALE="100")  # the timeout is 300 s / 100 = 3 s
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat"))
    world.head.answer_heartbeats = False
    stopped = time.monotonic()
    time.sleep(1.5)
    assert world.vast.destroys == []
    assert wait_for(lambda: world.vast.destroys, timeout=10)
    assert 2.9 <= world.vast.destroys[0][0] - stopped <= 6
    reports = [e[3] for e in world.head.events if e[1] == "B" and e[3] and e[3].get("kind") == "destroy"]
    assert "heartbeat" in reports[0]["reason"]


def test_heartbeat_403_destroys_at_once(world, tmp_path):
    world.run()
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat"))
    world.head.enrolled.clear()  # the head forgot this worker (for example, after a restart)
    assert wait_for(lambda: world.vast.destroys, timeout=5)


@pytest.mark.slow
def test_enroll_503_is_retried_until_the_head_accepts(world, tmp_path):
    world.head.enroll_503s = 2
    run = world.run()
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat")), run.stdout  # retry period is 10 s / 30
    assert len(world.head.bodies("A", "/enroll")) == 3
    assert world.vast.destroys == []


@pytest.mark.slow
def test_destroy_retries_until_vast_accepts(world, tmp_path):
    world.vast.delete_statuses = [500, 429]
    world.head.refuse_enroll = True
    world.run()
    assert wait_for(lambda: world.vast.destroys)  # retry period is 5 s / 30
    assert len(world.vast.destroys) == 1


@pytest.mark.slow
def test_stdout_has_no_secret(world, tmp_path):
    run = world.run()
    assert wait_for(lambda: world.head.bodies("B", "/report"), timeout=40)
    run.proc.send_signal(signal.SIGTERM)
    assert wait_for(lambda: world.vast.destroys)
    node_key = Path(run.dir / "node.key").read_text()
    for secret in (world.api_key, world.join_token, world.token, node_key.splitlines()[1]):
        assert secret not in run.stdout
    assert "destroying this instance" in run.stdout


def serve_once(handler_body, tmp_port_holder=None):
    """A plain HTTP server on a free port; `handler_body(handler)` answers GET and DELETE. Return (server, url)."""
    import http.server
    import threading

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            handler_body(self)

        do_DELETE = do_GET

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


@pytest.mark.slow
def test_a_ca_pem_with_a_second_cert_behind_the_real_one_is_not_trusted(new_world):
    """`PEM_cert_to_DER_cert` decodes only up to the first base64 padding, so real+rogue PEM has the real fingerprint."""
    import hashlib
    import ssl
    from datetime import datetime, timedelta, timezone

    from cryptography import x509
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ed25519
    from cryptography.x509.oid import NameOID
    from ray_vast import pki

    # The forgery needs a real CA whose base64 ends in padding. An ECDSA signature has a random DER length, so an ECDSA
    # CA is unpadded by chance (the test then failed about 3 runs in 100). An Ed25519 signature is always 64 bytes, so
    # this CA has the same DER length (223 bytes, 2 padding chars) in every run.
    key = ed25519.Ed25519PrivateKey.generate()
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "c")])
    now = datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(1).not_valid_before(now - timedelta(days=1)).not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True).sign(key, None)
    )
    padded_ca = cert.public_bytes(serialization.Encoding.PEM)
    assert len(cert.public_bytes(serialization.Encoding.DER)) % 3 != 0
    world = new_world()
    world.ca_pem = padded_ca
    rogue_dir = world.pki_dir.parent / "rogue"
    rogue_ca, _ = pki.write_head_files(rogue_dir, "127.0.0.1")
    forged = padded_ca + rogue_ca
    pinned = hashlib.sha256(ssl.PEM_cert_to_DER_cert(forged.decode())).hexdigest()
    assert pinned == pki.ca_fingerprint(padded_ca)  # the forgery passes a hash of the decoded PEM
    world.head.served_ca = forged
    world.head.tls_dir = rogue_dir  # the head cert is signed by the rogue CA only
    world.start()
    world.run()
    assert wait_for(lambda: world.vast.destroys)
    assert world.head.bodies("A", "/enroll") == []


@pytest.mark.slow
def test_a_slow_trickle_answer_hits_the_total_time_limit():
    import threading

    def trickle(handler):
        handler.send_response(200)
        handler.send_header("Content-Length", "1000")
        handler.end_headers()
        for _ in range(1000):
            handler.wfile.write(b"x")
            handler.wfile.flush()
            time.sleep(0.5)

    server, url = serve_once(trickle)
    outcome = []

    def attempt():
        try:
            bootstrap.call(url, None, timeout=2)
            outcome.append("returned")
        except OSError:
            outcome.append("network error")

    thread = threading.Thread(target=attempt, daemon=True)
    started = time.monotonic()
    thread.start()
    thread.join(timeout=8)
    server.shutdown()
    assert outcome == ["network error"] and time.monotonic() - started < 6


def test_the_signal_handler_ignores_all_three_signals_before_it_raises():
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)}
    try:
        with pytest.raises(bootstrap.Stop):
            bootstrap.on_signal(signal.SIGTERM, None)
        assert [signal.getsignal(sig) for sig in previous] == [signal.SIG_IGN] * 3
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def test_destroy_keeps_retrying_through_any_exception(monkeypatch):
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)}
    answers = [RuntimeError("odd"), (500, b""), (200, b"{}")]
    calls = []

    class Done(BaseException):
        pass

    def fake_call(*args, **kwargs):
        calls.append(args[0])
        answer = answers[len(calls) - 1]
        if isinstance(answer, Exception):
            raise answer
        return answer

    def fake_sleep(seconds):
        if seconds >= 3600:
            raise Done

    monkeypatch.setattr(bootstrap, "call", fake_call)
    monkeypatch.setattr(bootstrap.time, "sleep", fake_sleep)
    state = bootstrap.SimpleNamespace(
        instance_id=7, api_key="k", vast_api="http://vast.invalid", scale=1.0, report_url=None, report_ctx=None,
    )
    try:
        with pytest.raises(Done):
            bootstrap.destroy(state, "test")
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    assert len(calls) == 3


def run_destroy(monkeypatch, state, answers):
    """Run `destroy` with `call` and `sleep` replaced (the injected clock). Return the calls and the sleeps."""
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)}
    calls, sleeps = [], []

    class Done(BaseException):
        pass

    def fake_call(url, ctx, body=None, **kwargs):
        calls.append((url, body, kwargs.get("method")))
        return answers[len(calls) - 1]

    def fake_sleep(seconds):
        sleeps.append(seconds)
        if seconds >= 3600:
            raise Done

    monkeypatch.setattr(bootstrap, "call", fake_call)
    monkeypatch.setattr(bootstrap.time, "sleep", fake_sleep)
    try:
        with pytest.raises(Done):
            bootstrap.destroy(state, "the reason")
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    return calls, sleeps


def test_destroy_before_enrollment_reports_without_the_container_key_then_deletes(monkeypatch):
    state = bootstrap.SimpleNamespace(
        instance_id=7, api_key="k", vast_api="http://vast.invalid", scale=30.0, report_url="https://head/report",
        report_ctx=object(),
    )
    calls, _ = run_destroy(monkeypatch, state, [(200, b""), (200, b"{}")])
    assert calls[0] == ("https://head/report", {"instance_id": 7, "kind": "destroy", "reason": "the reason"}, None)
    assert calls[1][0] == "http://vast.invalid/instances/7/" and calls[1][2] == "DELETE"


def test_destroy_after_enrollment_reports_without_the_key_and_retries_on_the_scaled_period(monkeypatch):
    ctx = object()
    state = bootstrap.SimpleNamespace(
        instance_id=7, api_key="k", vast_api="http://vast.invalid", scale=30.0, report_url="https://head/report",
        report_ctx=ctx,
    )
    calls, sleeps = run_destroy(monkeypatch, state, [(200, b""), (500, b""), (429, b""), (404, b"")])
    assert calls[0][1] == {"instance_id": 7, "kind": "destroy", "reason": "the reason"}
    assert [c[2] for c in calls[1:]] == ["DELETE"] * 3  # 500 and 429 are retried, 404 counts as done
    assert sleeps == [bootstrap.DESTROY_RETRY_S / 30.0] * 2 + [3600]


def test_destroy_without_a_report_channel_still_deletes(monkeypatch):
    state = bootstrap.SimpleNamespace(
        instance_id=7, api_key="k", vast_api="http://vast.invalid", scale=1.0, report_url=None, report_ctx=None,
    )
    calls, _ = run_destroy(monkeypatch, state, [(200, b"{}")])
    assert [c[2] for c in calls] == ["DELETE"]


@pytest.mark.slow
def test_token_check_retries_a_non_401_answer_until_401(world, tmp_path):
    run = world.run(FAKE_RAY_AGENT_STATUS="503,401")
    assert wait_for(lambda: world.head.bodies("B", "/report")), run.stdout
    report = world.head.bodies("B", "/report")[0]
    assert (report["kind"], report["ok"]) == ("token_check", True)


def test_gpu_count_is_the_nvidia_device_nodes_of_the_container_only():
    # A 1-GPU rental on a 4-GPU host had these /dev entries (and 4 entries in /proc/driver/nvidia/gpus)
    c5 = ["nvidia-modeset", "nvidia-uvm", "nvidia-uvm-tools", "nvidia3", "nvidiactl", "null", "shm"]
    assert bootstrap.count_gpus(c5) == 1
    assert bootstrap.count_gpus(["nvidia0", "nvidia1", "nvidia-caps", "nvidiactl", "nvidia10"]) == 3
    assert bootstrap.count_gpus(["null", "zero", "shm"]) == 0


def test_capbnd_parse_reads_the_bounding_set():
    text = "CapInh:\t0\nCapEff:\t0000000000000000\nCapBnd:\t00000000a80425fb\n"
    assert bootstrap.parse_capbnd(text) == 0xA80425FB  # CapEff is 0 here (non-root); the bounding set is not
    assert bootstrap.parse_capbnd(text) & (3 << 12) != 0  # docker default has NET_RAW (bit 13)
    assert bootstrap.parse_capbnd("CapBnd:\t00000000a80405fb\n") & (3 << 12) == 0  # after --cap-drop NET_RAW
    with pytest.raises(ValueError):
        bootstrap.parse_capbnd("CapEff:\t0\n")


def test_egress_ip_tries_the_next_echo_url_after_a_non_200():
    def bad(handler):
        handler.send_response(500)
        handler.send_header("Content-Length", "11")
        handler.end_headers()
        handler.wfile.write(b"server down")

    def good(handler):
        handler.send_response(200)
        handler.send_header("Content-Length", "10")
        handler.end_headers()
        handler.wfile.write(b"192.0.2.4\n")

    bad_server, bad_url = serve_once(bad)
    good_server, good_url = serve_once(good)
    try:
        assert bootstrap.egress_ip([bad_url, good_url]) == "192.0.2.4"
        assert bootstrap.egress_ip([bad_url]) is None
    finally:
        bad_server.shutdown()
        good_server.shutdown()


def test_calls_do_not_follow_redirects_so_the_vast_key_stays_on_its_host():
    seen = []

    def target(handler):
        seen.append(handler.headers.get("Authorization"))
        handler.send_response(200)
        handler.send_header("Content-Length", "0")
        handler.end_headers()

    target_server, target_url = serve_once(target)

    def redirect(handler):
        handler.send_response(302)
        handler.send_header("Location", target_url)
        handler.send_header("Content-Length", "0")
        handler.end_headers()

    redirect_server, redirect_url = serve_once(redirect)
    try:
        status, _ = bootstrap.call(redirect_url, None, headers={"Authorization": "Bearer secret"})  # urllib follows GET redirects
    finally:
        redirect_server.shutdown()
        target_server.shutdown()
    assert status == 302 and seen == []


BOOTLOADER = ["sh", "-c", BOOTLOADER_PATH.read_text()]  # as the provider's `args` run it; `import ray` works through the fake


@pytest.mark.slow
def test_bootloader_with_a_wrong_bootstrap_hash_destroys_its_instance_and_does_not_exit(world):
    run = world.run(argv=BOOTLOADER, RAYVAST_BOOTSTRAP_SHA256="0" * 64)
    assert wait_for(lambda: world.vast.destroys)
    assert world.vast.destroys[0][1] == world.instance_id
    time.sleep(1)
    assert run.proc.poll() is None
    assert world.head.bodies("A", "/ca.pem") == []  # the downloaded bootstrap never ran


@pytest.mark.slow
def test_bootloader_retries_the_download_until_the_head_is_up(new_world):
    world = new_world()  # not started: the first download is refused
    run = world.run(argv=BOOTLOADER)
    time.sleep(3)
    assert run.proc.poll() is None and world.vast.destroys == []
    world.start()
    assert wait_for(lambda: world.head.bodies("A", "/enroll"), timeout=30), run.stdout
    assert world.vast.destroys == []


@pytest.mark.slow
def test_bootloader_gives_up_a_trickling_download_after_its_total_time_limit(new_world):
    world = new_world()
    world.head.trickle_bootstrap = True
    world.start()
    run = world.run(argv=BOOTLOADER)
    downloads = lambda: [e for e in world.head.events if e[2] == "/bootstrap"]
    assert wait_for(lambda: len(downloads()) >= 2, timeout=70), run.stdout  # 30 s limit + 10 s pause, then a retry
    assert run.proc.poll() is None and world.vast.destroys == []


@pytest.mark.slow
def test_a_trickling_server_holds_at_most_one_thread_for_each_url():
    # Each call thread counts against pids.max. A call that timed out leaves its thread reading the trickle; the next
    # call to the same URL (the next heartbeat) must fail at once and not start a second thread.
    import threading

    def trickle(handler):
        handler.send_response(200)
        handler.send_header("Content-Length", "1000")
        handler.end_headers()
        for _ in range(1000):
            handler.wfile.write(b"x")
            handler.wfile.flush()
            time.sleep(0.5)

    server, url = serve_once(trickle)
    try:
        with pytest.raises(TimeoutError):
            bootstrap.call(url, None, timeout=1)
        threads = threading.active_count()
        for _ in range(3):
            with pytest.raises(bootstrap.NET_ERRORS):
                bootstrap.call(url, None, timeout=1)
        assert threading.active_count() == threads
    finally:
        server.shutdown()


def test_a_call_that_cannot_start_its_thread_is_a_transient_network_error(monkeypatch):
    # Under the container's pids limit, `threading.Thread.start` raises RuntimeError("can't start new thread").
    # Only the heartbeat time limit may end a worker, so the watchdog must see a failed call (NET_ERRORS).
    def no_thread(self):
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(bootstrap.threading.Thread, "start", no_thread)
    with pytest.raises(bootstrap.NET_ERRORS):
        bootstrap.call("http://127.0.0.1:9/", None, timeout=1)
