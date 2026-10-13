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

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fakes.worker_world import BOOTLOADER_PATH, BOOTSTRAP_PATH, FAKE_RAY_DIR, World  # noqa: E402

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

    def __init__(self, world, tmp_path, **env_overrides):
        self.world = world
        self.ray_out = tmp_path / "ray_out.json"
        self.stdout_path = tmp_path / "stdout.txt"
        env = world.env()
        env.update(
            PATH=os.environ["PATH"], PYTHONPATH=str(FAKE_RAY_DIR), FAKE_RAY_OUT=str(self.ray_out), **FAST
        )
        env.update(env_overrides)
        self.proc = subprocess.Popen(
            [sys.executable, "-I", "-S", str(BOOTSTRAP_PATH)], env=env,
            stdout=open(self.stdout_path, "w"), stderr=subprocess.STDOUT, start_new_session=True,
        )

    @property
    def stdout(self):
        return self.stdout_path.read_text()

    @property
    def ray(self):
        return json.loads(self.ray_out.read_text())


@pytest.fixture
def world(tmp_path):
    w = World(tmp_path)
    w.start()
    runs = []
    w.run = lambda **overrides: runs.append(Run(w, tmp_path, **overrides)) or runs[-1]
    yield w
    for r in runs:
        os.killpg(r.proc.pid, signal.SIGKILL)  # the bootstrap and its (fake) ray child
        r.proc.wait()
    w.stop()


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
    assert len(text.encode()) < 1024
    assert subprocess.run(["sh", "-n", str(BOOTLOADER_PATH)]).returncode == 0
    assert "'" not in text.replace("-c '", "").rstrip().rstrip("'")  # one single-quoted block, so `sh -c` can carry it


def test_capeff_parse_reads_the_effective_set():
    docker_default = "Name:\tsh\nCapInh:\t0000000000000000\nCapEff:\t00000000a80425fb\nCapBnd:\t00000000a80425fb\n"
    assert bootstrap.parse_capeff(docker_default) == 0xA80425FB
    assert 0xA80425FB & (3 << 12) != 0  # docker default has NET_RAW (bit 13)
    assert bootstrap.parse_capeff("CapEff:\t00000000a80405fb\n") & (3 << 12) == 0  # after --cap-drop NET_RAW
    with pytest.raises(ValueError):
        bootstrap.parse_capeff("Name:\tsh\n")


@pytest.mark.parametrize("text, cores", [("max 100000\n", None), ("200000 100000\n", 2.0), ("150000 100000", 1.5)])
def test_cgroup_cpu_max_parse(text, cores):
    assert bootstrap.parse_cpu_max(text) == cores


def test_enrolls_and_starts_ray_with_the_right_flags_and_env(world, tmp_path):
    shutil.rmtree("/tmp/rayvast", ignore_errors=True)  # modes apply only to new files
    run = world.run()
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat") and run.ray_out.exists()), run.stdout
    enroll = world.head.bodies("A", "/enroll")[0]
    assert enroll["instance_id"] == 424242
    assert enroll["container_api_key"] == world.api_key
    assert enroll["ports"] == {str(70000 + i): world.port_base + i for i in range(64)}
    assert set(enroll["checks"]) == {"egress_ip_ok", "no_net_raw_admin", "own_netns", "shm_empty", "details"}
    assert enroll["cpu_count"] >= 1 and enroll["gpu_count"] == 0

    p = world.port_base
    argv = run.ray["argv"]
    assert argv[:2] == ["start", "--block"]
    assert set(argv[2:]) == {
        f"--address={world.head_ip}:6379", "--node-ip-address=127.0.0.1",
        f"--node-manager-port={p}", f"--object-manager-port={p + 1}", f"--metrics-export-port={p + 2}",
        f"--dashboard-agent-grpc-port={p + 3}", f"--dashboard-agent-listen-port={p + 4}",
        f"--runtime-env-agent-port={p + 5}",
        "--worker-port-list=" + ",".join(str(p + i) for i in range(6, 64)),
        f"--num-cpus={enroll['cpu_count']}", "--num-gpus=0",
    }
    env = run.ray["env"]
    assert env["RAY_CLOUD_INSTANCE_ID"] == "slot-1" and env["RAY_NODE_TYPE_NAME"] == "worker"
    assert env["RAY_USE_TLS"] == "1" and env["RAY_AUTH_MODE"] == "token"
    assert env["RAY_TLS_SERVER_CERT"] == "/tmp/rayvast/node.pem" and env["RAY_TLS_SERVER_KEY"] == "/tmp/rayvast/node.key"
    assert env["RAY_TLS_CA_CERT"] == "/tmp/rayvast/ca.pem" and env["RAY_AUTH_TOKEN_PATH"] == "/tmp/rayvast/token"
    assert env["LD_PRELOAD"] == "/tmp/rayvast/librayvast_shim.so"
    assert env["RAYVAST_PUBLIC_IP"] == "127.0.0.1" and env["RAYVAST_PRIVATE_IP"] == "127.0.0.1"
    assert env["RAYVAST_LOCALHOST_PORTS"] == f"{p + 4},{p + 5}"
    assert env["RAY_num_workers_soft_limit"] == "10"
    assert "CONTAINER_API_KEY" not in env

    assert oct(os.stat("/tmp/rayvast").st_mode & 0o777) == "0o700"
    for name in ("ca.pem", "node.pem", "node.key", "token", "librayvast_shim.so"):
        assert oct(os.stat(f"/tmp/rayvast/{name}").st_mode & 0o777) == "0o600", name
    assert Path("/tmp/rayvast/token").read_text() == world.token


def test_token_check_is_reported_ok_when_agent_ports_answer_401(world, tmp_path):
    run = world.run()
    assert wait_for(lambda: world.head.bodies("B", "/report")), run.stdout
    report = world.head.bodies("B", "/report")[0]
    assert (report["kind"], report["ok"]) == ("token_check", True)


def test_token_check_is_reported_failed_when_an_agent_port_lets_a_tokenless_request_in(world, tmp_path):
    world.run(FAKE_RAY_AGENT_STATUS="200")
    assert wait_for(lambda: world.head.bodies("B", "/report"))
    report = world.head.bodies("B", "/report")[0]
    assert (report["kind"], report["ok"]) == ("token_check", False)


def test_head_refusing_enrollment_destroys_at_once_without_starting_ray(world, tmp_path):
    world.head.refuse_enroll = True
    run = world.run()
    assert wait_for(lambda: world.vast.destroys)
    assert world.vast.bad_auth == 0
    report = world.head.bodies("A", "/report")[0]
    assert report["kind"] == "destroy" and "refused" in report["reason"]
    assert report["container_api_key"] == world.api_key
    assert not run.ray_out.exists()


def test_wrong_ca_fingerprint_destroys_before_any_enrollment(world, tmp_path):
    world.run(RAYVAST_CA_SHA256="0" * 64)
    assert wait_for(lambda: world.vast.destroys)
    assert world.head.bodies("A", "/enroll") == []


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


@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGHUP])
def test_sigint_and_sighup_destroy_too(world, tmp_path, sig):
    run = world.run()
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat"))
    run.proc.send_signal(sig)
    assert wait_for(lambda: world.vast.destroys)


def test_ray_exit_destroys_with_its_exit_code(world, tmp_path):
    world.run(FAKE_RAY_EXIT_AFTER="2", FAKE_RAY_EXIT_CODE="3")
    assert wait_for(lambda: world.vast.destroys)
    reports = [e[3] for e in world.head.events if e[1] == "B" and e[3] and e[3].get("kind") == "destroy"]
    assert "code 3" in reports[0]["reason"]


def test_heartbeats_stop_then_destroy_after_the_timeout_and_not_before(world, tmp_path):
    run = world.run()
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat"))
    world.head.answer_heartbeats = False
    stopped = time.monotonic()
    time.sleep(7)  # the timeout is 300 s / 30 = 10 s
    assert world.vast.destroys == []
    assert wait_for(lambda: world.vast.destroys, timeout=15)
    assert 9 <= world.vast.destroys[0][0] - stopped <= 14
    reports = [e[3] for e in world.head.events if e[1] == "B" and e[3] and e[3].get("kind") == "destroy"]
    assert "heartbeat" in reports[0]["reason"]


def test_heartbeat_403_destroys_at_once(world, tmp_path):
    world.run()
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat"))
    world.head.enrolled.clear()  # the head forgot this worker (for example, after a restart)
    assert wait_for(lambda: world.vast.destroys, timeout=5)


def test_enroll_503_is_retried_until_the_head_accepts(world, tmp_path):
    world.head.enroll_503s = 2
    run = world.run()
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat")), run.stdout  # retry period is 10 s / 30
    assert len(world.head.bodies("A", "/enroll")) == 3
    assert world.vast.destroys == []


def test_destroy_retries_until_vast_accepts(world, tmp_path):
    world.vast.delete_statuses = [500, 429]
    world.head.refuse_enroll = True
    world.run()
    assert wait_for(lambda: world.vast.destroys)  # retry period is 5 s / 30
    assert len(world.vast.destroys) == 1


def test_stdout_has_no_secret(world, tmp_path):
    run = world.run()
    assert wait_for(lambda: world.head.bodies("B", "/report"))
    run.proc.send_signal(signal.SIGTERM)
    assert wait_for(lambda: world.vast.destroys)
    node_key = Path("/tmp/rayvast/node.key").read_text()
    for secret in (world.api_key, world.token, node_key.splitlines()[1]):
        assert secret not in run.stdout
    assert "destroying this instance" in run.stdout
