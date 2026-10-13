"""Worker bootloader and bootstrap in real containers on the dev box (Docker), against the fake head and fake vast.

Fake parts: the head, vast, and (except in the full-stack test) the `ray` CLI, which is tests/fakes/fake_ray_pkg
mounted on PYTHONPATH so `import ray` works and `ray start` is a stand-in. Container names start with `st7b-`.
"""
import json
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fakes.worker_world import BOOTLOADER_PATH, FAKE_RAY_DIR, World  # noqa: E402

pytestmark = [pytest.mark.slow, pytest.mark.skipif(shutil.which("docker") is None, reason="needs Docker")]

RAY_IMAGE = "rayproject/ray:2.59.0-py311-cpu"
BOOTLOADER = BOOTLOADER_PATH.read_text()
FAST = {"RAYVAST_TIME_SCALE": "30"}


def docker(*args, check=True):
    result = subprocess.run(["docker", *args], capture_output=True, text=True)
    if check and result.returncode != 0:
        raise AssertionError(f"docker {args[0]} failed: {result.stdout}{result.stderr}")
    return result


def wait_for(condition, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.2)
    return False


def local_ip_to(address):
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    probe.connect((address, 9))
    ip = probe.getsockname()[0]
    probe.close()
    return ip


@pytest.fixture(scope="module")
def box_public_ip():
    try:
        return urllib.request.urlopen("https://api.ipify.org", timeout=5).read().decode().strip()
    except OSError:
        pytest.skip("no public IP echo reachable")


@pytest.fixture(scope="module")
def gateway():
    return docker("network", "inspect", "bridge", "-f", "{{(index .IPAM.Config 0).Gateway}}").stdout.strip()


@pytest.fixture(autouse=True)
def clean_containers():
    yield
    names = docker("ps", "-aq", "--filter", "name=^st7b-", check=False).stdout.split()
    if names:
        docker("rm", "-f", *names, check=False)


@pytest.fixture
def bridge_world(tmp_path, gateway, box_public_ip):
    """Fakes on the docker gateway address; the container has its own netns and sees the box's public IP as egress."""
    world = World(tmp_path, head_ip=gateway, public_ip=box_public_ip)
    world.start()
    yield world
    world.stop()


def start_worker(name, world, tmp_path, *, image=RAY_IMAGE, script=BOOTLOADER, network="bridge",
                 cap_args=("--cap-drop", "NET_RAW"), fake_ray=True, env=None):
    """`docker run` the bootloader as the container command, as the provider's `args` do. Return the name."""
    env_file = tmp_path / f"{name}.env"
    values = world.env(**FAST)
    values.update(env or {})
    if fake_ray:
        values.update(PYTHONPATH="/fakeray", FAKE_RAY_OUT="/tmp/fakeray.json")
    env_file.write_text("".join(f"{k}={v}\n" for k, v in values.items()))
    args = ["run", "-d", "--name", name, "--env-file", str(env_file), "--network", network]
    args += cap_args
    if fake_ray:
        args += ["-v", f"{FAKE_RAY_DIR}:/fakeray:ro"]
    docker(*args, image, "sh", "-c", script)
    return name


def logs(name):
    result = docker("logs", name)
    return result.stdout + result.stderr


def destroy_reports(world):
    return [e[3] for e in world.head.events if e[3] and e[3].get("kind") == "destroy"]


@pytest.mark.parametrize("image", [RAY_IMAGE, "python:3.9-slim"])
def test_bootloader_downloads_checks_and_execs_bootstrap_which_enrolls_and_heartbeats(
    bridge_world, tmp_path, image
):
    """On the Ray image (Python 3.11, user ray) and on a plain python:3.9-slim (root). All four host checks are real."""
    world = bridge_world
    start_worker("st7b-w", world, tmp_path, image=image)
    assert wait_for(lambda: len(world.head.bodies("B", "/heartbeat")) >= 2), logs("st7b-w")
    enroll = world.head.bodies("A", "/enroll")[0]
    assert enroll["checks"]["details"].count("SKIPPED") == 0
    expected = {"egress_ip_ok": True, "no_net_raw_admin": True, "own_netns": True, "shm_empty": True}
    assert {k: enroll["checks"][k] for k in expected} == expected, enroll["checks"]["details"]
    assert wait_for(lambda: world.head.bodies("B", "/report")), logs("st7b-w")
    assert world.head.bodies("B", "/report")[0]["kind"] == "token_check" and world.head.bodies("B", "/report")[0]["ok"]
    assert docker("exec", "st7b-w", "cat", "/proc/1/cmdline").stdout.split("\0")[1:2] == ["/tmp/rayvast-bootstrap.py"]


def test_net_raw_cap_and_host_network_fail_the_matching_host_checks(bridge_world, tmp_path):
    """The cap check reads the bounding set, so it works for the non-root user `ray` (its CapEff is always empty)."""
    world = bridge_world
    start_worker("st7b-w1", world, tmp_path, cap_args=("--cap-add", "NET_RAW"))
    assert wait_for(lambda: world.head.bodies("A", "/enroll"))
    checks = world.head.bodies("A", "/enroll")[0]["checks"]
    assert (checks["no_net_raw_admin"], checks["own_netns"]) == (False, True)
    docker("rm", "-f", "st7b-w1")
    world.head.events.clear()
    start_worker("st7b-w2", world, tmp_path, network="host")
    assert wait_for(lambda: world.head.bodies("A", "/enroll"))
    checks = world.head.bodies("A", "/enroll")[0]["checks"]
    assert (checks["no_net_raw_admin"], checks["own_netns"]) == (True, False)


def test_bootstrap_with_one_changed_byte_is_refused_by_the_bootloader_which_destroys_its_instance(
    bridge_world, tmp_path
):
    world = bridge_world
    good = world.bootstrap_bytes
    world.head.bootstrap_bytes = good[:-1] + bytes([good[-1] ^ 1])
    start_worker("st7b-w", world, tmp_path)
    assert wait_for(lambda: world.vast.destroys), logs("st7b-w")
    assert docker("inspect", "-f", "{{.State.Running}}", "st7b-w").stdout.strip() == "true"  # it waits for vast
    assert [e for e in world.head.events if e[2] == "/bootstrap"]  # it was downloaded
    assert [e for e in world.head.events if e[2] == "/ca.pem"] == []  # the changed bootstrap never ran


def test_image_without_ray_python_prints_a_clear_error_and_waits(bridge_world, tmp_path):
    start_worker("st7b-w", bridge_world, tmp_path, image="alpine:3.19", fake_ray=False)
    assert wait_for(lambda: "no Python with ray" in logs("st7b-w"), timeout=30)
    time.sleep(1)
    assert docker("inspect", "-f", "{{.State.Running}}", "st7b-w").stdout.strip() == "true"
    assert bridge_world.head.events == []


def test_image_with_an_entrypoint_fails_clearly(bridge_world, tmp_path):
    """vast `args` become the arguments of the ENTRYPOINT, so `sh -c <script>` is not run as a command."""
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(f'FROM {RAY_IMAGE}\nENTRYPOINT ["python"]\n')
    docker("build", "-q", "-t", "st7b-entrypoint-image", "-f", str(dockerfile), str(tmp_path))
    try:
        start_worker("st7b-w", bridge_world, tmp_path, image="st7b-entrypoint-image")
        assert wait_for(lambda: docker("inspect", "-f", "{{.State.Running}}", "st7b-w").stdout.strip() == "false")
        output = logs("st7b-w")
        print("ENTRYPOINT image output:", output.strip())
        assert docker("inspect", "-f", "{{.State.ExitCode}}", "st7b-w").stdout.strip() != "0"
        assert "can't open file" in output and "/sh'" in output
        assert bridge_world.head.events == []
    finally:
        docker("rmi", "-f", "st7b-entrypoint-image", check=False)


@pytest.mark.parametrize("sig", ["TERM", "INT", "HUP"])
def test_signal_to_pid_1_from_the_ray_user_reports_then_destroys_and_the_container_stays_up(
    bridge_world, tmp_path, sig
):
    world = bridge_world
    start_worker("st7b-w", world, tmp_path)
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat")), logs("st7b-w")
    assert docker("exec", "st7b-w", "id", "-u").stdout.strip() == "1000"
    docker("exec", "st7b-w", "kill", f"-{sig}", "1")
    assert wait_for(lambda: world.vast.destroys), logs("st7b-w")
    report = [r for r in destroy_reports(world)][0]
    assert f"SIG{sig}" in report["reason"]
    report_time = [e[0] for e in world.head.events if e[3] and e[3].get("kind") == "destroy"][0]
    assert report_time < world.vast.destroys[0][0]
    assert world.vast.bad_auth == 0 and world.vast.destroys[0][1] == world.instance_id
    time.sleep(2)
    assert docker("inspect", "-f", "{{.State.Running}}", "st7b-w").stdout.strip() == "true"


def test_heartbeats_stop_then_destroy_after_heartbeat_timeout(bridge_world, tmp_path):
    world = bridge_world
    start_worker("st7b-w", world, tmp_path)  # time scale 30: timeout 300 s -> 10 s
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat")), logs("st7b-w")
    world.head.answer_heartbeats = False
    stopped = time.monotonic()
    assert wait_for(lambda: world.vast.destroys, timeout=30)
    assert 9 <= world.vast.destroys[0][0] - stopped <= 15
    assert "heartbeat" in destroy_reports(world)[0]["reason"]


def test_head_refusing_enrollment_destroys_at_once(bridge_world, tmp_path):
    world = bridge_world
    world.head.refuse_enroll = True
    started = time.monotonic()
    start_worker("st7b-w", world, tmp_path)
    assert wait_for(lambda: world.vast.destroys, timeout=30), logs("st7b-w")
    assert world.vast.destroys[0][0] - started < 15
    assert "refused" in destroy_reports(world)[0]["reason"]
    assert world.head.bodies("B", "/heartbeat") == []


def test_no_zombies_after_50_short_orphan_children(bridge_world, tmp_path):
    spawn = "i=0; while [ $i -lt 50 ]; do (sleep 0.2 &); i=$((i+1)); done"
    count_zombies = "cat /proc/[0-9]*/status 2>/dev/null | grep -c '^State:.*zombie'"
    world = bridge_world
    start_worker("st7b-w", world, tmp_path)
    assert wait_for(lambda: world.head.bodies("B", "/heartbeat")), logs("st7b-w")
    docker("exec", "st7b-w", "sh", "-c", spawn)
    time.sleep(4)
    assert docker("exec", "st7b-w", "sh", "-c", count_zombies, check=False).stdout.strip() == "0"
    # Control: with a PID 1 that does not reap, the same commands leave zombies, so the count above means something.
    docker("run", "-d", "--name", "st7b-control", RAY_IMAGE, "sleep", "3600")
    docker("exec", "st7b-control", "sh", "-c", spawn)
    time.sleep(2)
    assert int(docker("exec", "st7b-control", "sh", "-c", count_zombies, check=False).stdout.strip()) >= 40


def test_container_output_and_ray_env_hold_no_secret(bridge_world, tmp_path):
    world = bridge_world
    start_worker("st7b-w", world, tmp_path)
    assert wait_for(lambda: world.head.bodies("B", "/report")), logs("st7b-w")
    docker("exec", "st7b-w", "kill", "-TERM", "1")
    assert wait_for(lambda: world.vast.destroys)
    output = logs("st7b-w")
    node_key = docker("exec", "st7b-w", "cat", "/tmp/rayvast/node.key").stdout
    for secret in (world.api_key, world.token, node_key.splitlines()[1]):
        assert secret not in output
    ray = json.loads(docker("exec", "st7b-w", "cat", "/tmp/fakeray.json").stdout)
    assert "CONTAINER_API_KEY" not in ray["env"] and world.api_key not in ray["env"].values()


def test_full_stack_real_ray_worker_joins_real_tls_token_head_and_a_task_runs_without_the_key(tmp_path):
    """Host networking on both containers (one box): worker public IP = private IP = the box's private IP.

    Skipped host checks in this test: egress_ip_ok (egress is the box's public IP, PUBLIC_IPADDR is the private IP)
    and own_netns (host network). The fake Ray stand-in is NOT used here: the worker starts a real raylet.
    """
    ip = local_ip_to("8.8.8.8")
    world = World(tmp_path, head_ip=ip, public_ip=ip, gcs_address=f"{ip}:19990")
    world.start()
    try:
        tls = {
            "RAY_USE_TLS": "1", "RAY_TLS_SERVER_CERT": "/pki/head.pem", "RAY_TLS_SERVER_KEY": "/pki/head.key",
            "RAY_TLS_CA_CERT": "/pki/ca.pem", "RAY_AUTH_MODE": "token", "RAY_AUTH_TOKEN_PATH": "/pki/token",
        }
        env_args = [arg for k, v in tls.items() for arg in ("-e", f"{k}={v}")]
        docker(
            "run", "-d", "--name", "st7b-head", "--network", "host", "-v", f"{world.pki_dir}:/pki:ro", *env_args,
            RAY_IMAGE, "ray", "start", "--head", "--block", f"--node-ip-address={ip}", "--port=19990",
            "--num-cpus=0", "--dashboard-host=127.0.0.1", "--dashboard-port=19991", "--node-manager-port=19992",
            "--object-manager-port=19993", "--metrics-export-port=19994", "--dashboard-agent-grpc-port=19995",
            "--dashboard-agent-listen-port=19996", "--runtime-env-agent-port=19997", "--ray-client-server-port=19998",
            "--min-worker-port=19900", "--max-worker-port=19949",
        )
        ray_status = lambda: docker(  # noqa: E731
            "exec", "st7b-head", "ray", "status", "--address", f"{ip}:19990", check=False
        ).stdout
        assert wait_for(lambda: "Active:" in ray_status(), timeout=90), logs("st7b-head")
        start_worker(
            "st7b-w", world, tmp_path, network="host", fake_ray=False,
            env={"RAYVAST_SKIP_HOST_CHECKS": "1", "RAYVAST_TIME_SCALE": "10"},
        )
        nodes = lambda: ray_status().count(" node_")  # noqa: E731
        assert wait_for(lambda: nodes() == 2, timeout=120), logs("st7b-w") + ray_status()
        assert wait_for(lambda: world.head.bodies("B", "/report"), timeout=60), logs("st7b-w")
        assert world.head.bodies("B", "/report")[0]["ok"] is True
        assert len(world.head.bodies("B", "/heartbeat")) >= 1
        task = (
            "import os, ray; ray.init(address='%s:19990'); "
            "f = ray.remote(num_cpus=1)(lambda: (os.environ.get('CONTAINER_API_KEY'), os.getpid())); "
            "print('TASK', ray.get(f.remote()))" % ip
        )
        output = docker("exec", "st7b-head", "python", "-c", task).stdout
        assert "TASK (None," in output, output
        assert world.api_key not in logs("st7b-w")
    finally:
        world.stop()
