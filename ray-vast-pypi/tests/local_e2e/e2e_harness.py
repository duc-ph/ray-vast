"""Step 11 harness and fixtures (not a conftest.py: tests/unit has one and imports it by name): a fake vast (containers on the box), moto as the AWS endpoint, and a Ray head in a container.

Run on the dev box: `pytest -m local_e2e tests/local_e2e`. Container names start with `st11-`.
"""
import json
import shutil
import socket
import ssl
import subprocess
import time
from pathlib import Path

import boto3
import pytest
import yaml
from moto.server import ThreadedMotoServer

from ray_vast import config as ray_vast_config
from ray_vast import pki
from fake_vast_server import FakeVast, make_offers

PYPI_DIR = Path(__file__).resolve().parents[2]
RAY_IMAGE = "rayproject/ray:2.59.0-py311-cpu"
HEAD_IMAGE = "st11-head"
HEAD = "st11-head"
REGION = "us-west-2"

def docker(*args, check=True):
    result = subprocess.run(["docker", *args], capture_output=True, text=True)
    if check and result.returncode != 0:
        raise AssertionError(f"docker {args[0]} failed: {result.stdout}{result.stderr}")
    return result


@pytest.fixture(scope="session")
def box_ip():
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    probe.connect(("10.255.255.255", 9))
    ip = probe.getsockname()[0]
    probe.close()
    return ip


@pytest.fixture(scope="session")
def head_image(tmp_path_factory):
    """The Ray image with ray-vast installed from the local source."""
    context = tmp_path_factory.mktemp("head-image")
    for name in ("pyproject.toml", "README.md", "LICENSE"):
        shutil.copy(PYPI_DIR / name, context / name)
    shutil.copytree(PYPI_DIR / "src", context / "src", ignore=shutil.ignore_patterns("__pycache__"))
    (context / "Dockerfile").write_text(
        f"FROM {RAY_IMAGE}\nCOPY --chown=ray:users . /src\nRUN pip install --no-cache-dir /src\n")
    docker("build", "-q", "-t", HEAD_IMAGE, str(context))
    return HEAD_IMAGE


@pytest.fixture(scope="session")
def moto_url(box_ip):
    with socket.socket() as s:
        s.bind(("", 0))
        port = s.getsockname()[1]
    server = ThreadedMotoServer(ip_address="0.0.0.0", port=port, verbose=False)
    server.start()
    yield f"http://{box_ip}:{port}"
    server.stop()


@pytest.fixture(autouse=True)
def no_leftover_containers():
    yield
    names = docker("ps", "-aq", "--filter", "name=^st11-", check=False).stdout.split()
    if names:
        docker("rm", "-f", *names, check=False)


class Cluster:
    def __init__(self, tmp_path, box_ip, moto_url, fake):
        self.box_ip, self.fake, self.moto_url, self.work = box_ip, fake, moto_url, tmp_path / "head"
        self.ec2 = boto3.client("ec2", region_name=REGION, endpoint_url=moto_url, aws_access_key_id="x",
                                aws_secret_access_key="x")
        self.sg_id = self.ec2.create_security_group(GroupName=f"st11-{tmp_path.name}", Description="st11")["GroupId"]
        (self.work / "secrets").mkdir(parents=True)
        pki.write_head_files(self.work / "secrets", box_ip)
        (self.work / "secrets" / "vast_api_key").write_text(fake.account_key + "\n")
        shutil.copy(PYPI_DIR / "src/ray_vast/shim/librayvast_shim.so", self.work / "librayvast_shim.so")
        self.config = None

    def write_config(self, cpu=2, max_workers=4, idle_timeout_minutes=1, **provider):
        config, _ = ray_vast_config.bootstrap_config({
            "cluster_name": "st11",
            "max_workers": max_workers,
            "idle_timeout_minutes": idle_timeout_minutes,
            "provider": {
                "region": REGION, "image": RAY_IMAGE, "max_dollars_per_hour": 10, "head_ip": self.box_ip,
                "secrets_dir": "/work/secrets", "security_group_id": self.sg_id, "vast_api_url": self.fake.url,
                "candidate_factor": 1, **provider,
            },
            "head_node_type": "head",
            "available_node_types": {
                "head": {"resources": {"CPU": 0}, "max_workers": 0, "node_config": {}},
                "cpu": {"resources": {"CPU": cpu, "GPU": 0}, "max_workers": max_workers,
                        "node_config": {"offer_query": {"inet_down": {"gte": 100}}}},
            },
        })
        (self.work / "autoscaling.yaml").write_text(yaml.safe_dump(config))

    def start_head(self):
        """`docker run` the head like step 10 does: ray start --head --block, v2, TLS, token, shim."""
        ip = self.box_ip
        env = {
            "RAY_enable_autoscaler_v2": "1", "RAY_CLOUD_INSTANCE_ID": "head", "RAY_NODE_TYPE_NAME": "head",
            "RAY_USE_TLS": "1", "RAY_TLS_SERVER_CERT": "/work/secrets/head.pem",
            "RAY_TLS_SERVER_KEY": "/work/secrets/head.key", "RAY_TLS_CA_CERT": "/work/secrets/ca.pem",
            "RAY_AUTH_MODE": "token", "RAY_AUTH_TOKEN_PATH": "/work/secrets/token",
            "LD_PRELOAD": "/work/librayvast_shim.so", "RAYVAST_PUBLIC_IP": ip, "RAYVAST_PRIVATE_IP": ip,
            "RAYVAST_LOCALHOST_PORTS": "6384,6385",
            "AWS_ENDPOINT_URL": self.moto_url, "AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "x",
            "AWS_DEFAULT_REGION": REGION,
        }
        docker(
            "run", "-d", "--init", "--network", "host", "--name", HEAD,
            *[a for k, v in env.items() for a in ("-e", f"{k}={v}")],
            "-v", f"{self.work}:/work", "-v", f"{Path(__file__).parent}:/e2e:ro", HEAD_IMAGE,
            "ray", "start", "--head", "--block", f"--node-ip-address={ip}", "--port=6379", "--num-cpus=0",
            "--autoscaling-config=/work/autoscaling.yaml", "--dashboard-host=127.0.0.1",
            "--node-manager-port=6380", "--object-manager-port=6381", "--metrics-export-port=6382",
            "--dashboard-agent-grpc-port=6383", "--dashboard-agent-listen-port=6384", "--runtime-env-agent-port=6385",
            "--min-worker-port=10002", "--max-worker-port=10100",
        )
        self.wait_for(self.head_server_answers, 90, "the head server answers on port A")
        self.wait_for(lambda: docker("exec", HEAD, "ray", "status", check=False).returncode == 0, 90, "ray status works")

    def sg_cidrs(self):
        """The ingress rules that ray-vast made (the security group only has those)."""
        group = self.ec2.describe_security_groups(GroupIds=[self.sg_id])["SecurityGroups"][0]
        return sorted(r["CidrIp"] for p in group["IpPermissions"] for r in p.get("IpRanges", []))

    def head_server_answers(self):
        ctx = ssl.create_default_context()
        ctx.check_hostname, ctx.verify_mode = False, ssl.CERT_NONE
        try:
            socket.create_connection((self.box_ip, 7443), timeout=2).close()
            with ctx.wrap_socket(socket.create_connection((self.box_ip, 7443), timeout=5)) as tls:
                tls.sendall(b"GET /ca.pem HTTP/1.0\r\n\r\n")
                return b"200" in tls.recv(100)
        except OSError:
            return False

    def run_driver(self, script, *args, timeout=600):
        """Run a script from tests/local_e2e inside the head container; return its stdout."""
        result = subprocess.run(["docker", "exec", HEAD, "python", f"/e2e/{script}", *map(str, args)],
                                capture_output=True, text=True, timeout=timeout)
        assert result.returncode == 0, f"{script} failed:\n{result.stdout[-2000:]}\n{result.stderr[-3000:]}"
        return result.stdout

    def wait_for(self, condition, timeout, what):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return
            time.sleep(1)
        raise AssertionError(f"timeout after {timeout} s: {what}\n{self.diagnostics()}")

    def diagnostics(self):
        head_log = docker("logs", "--tail", "40", HEAD, check=False)
        state = docker("ps", "-a", "--filter", "name=^st11-", "--format", "{{.Names}} {{.Status}}", check=False).stdout
        monitor = docker("exec", HEAD, "sh", "-c", "tail -n 80 /tmp/ray/session_latest/logs/monitor.log", check=False)
        return (f"fake vast: live={self.fake.live_ids()} rentals={self.fake.rentals} "
                f"destroys={[d[:2] for d in self.fake.destroys]}\ncontainers: {state}--- head container log\n"
                f"{head_log.stdout[:1200]}{head_log.stderr[:1200]}\n--- monitor.log\n{monitor.stdout}")


@pytest.fixture
def make_cluster(tmp_path, box_ip, moto_url, head_image):
    """`make_cluster(offers=6, docker_args=(), worker_env={})` returns a Cluster whose head has not started yet."""
    fakes = []

    def make(offers=6, docker_args=(), worker_env=None):
        fake = FakeVast(box_ip, make_offers(offers), docker_args=docker_args,
                        extra_env={"RAYVAST_SKIP_HOST_CHECKS": "1", **(worker_env or {})})
        fake.start()
        fakes.append(fake)
        return Cluster(tmp_path, box_ip, moto_url, fake)

    yield make
    for fake in fakes:
        fake.stop()
