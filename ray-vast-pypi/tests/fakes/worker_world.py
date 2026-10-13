"""One test world for the worker: PKI, fake head, fake vast, and the container env that vast would set."""
import hashlib
import random
import socket
from pathlib import Path

import ray_vast
from ray_vast import pki

from .fake_head import FakeHead
from .fake_vast import FakeVast

PACKAGE = Path(ray_vast.__file__).parent
BOOTSTRAP_PATH = PACKAGE / "worker" / "bootstrap.py"
BOOTLOADER_PATH = PACKAGE / "worker" / "bootloader.sh"
SHIM_PATH = PACKAGE / "shim" / "librayvast_shim.so"
FAKE_RAY_DIR = Path(__file__).parent / "fake_ray_pkg"


def free_ports(count):
    """Return `count` consecutive ports in 19000-19999 that nobody listens on now (other test runs may share the box)."""
    while True:
        base = random.randrange(19000, 20000 - count)
        sockets = []
        try:
            for port in range(base, base + count):
                probe = socket.socket()
                sockets.append(probe)
                probe.bind(("0.0.0.0", port))
        except OSError:
            continue
        finally:
            for probe in sockets:
                probe.close()
        return base


class World:
    """Ports (all in 19000-19999, chosen free): head A and B, vast, and the 64 worker host ports in one block."""

    def __init__(self, tmp_path, *, head_ip="127.0.0.1", public_ip="127.0.0.1", gcs_address=None,
                 api_key="FAKEKEY-vast-container-api-key", instance_id=424242, bootstrap_bytes=None):
        self.pki_dir = Path(tmp_path) / "pki"
        self.ca_pem, self.token = pki.write_head_files(self.pki_dir, head_ip)
        self.api_key = api_key
        self.instance_id = instance_id
        self.head_ip = head_ip
        self.public_ip = public_ip
        block = free_ports(67)  # one block, so the head ports can never fall inside the worker ports
        self.port_base = block
        head_ports = block + 64
        self.vast_port = head_ports + 2
        self.bootstrap_bytes = bootstrap_bytes if bootstrap_bytes is not None else BOOTSTRAP_PATH.read_bytes()
        self.head = FakeHead(
            self.pki_dir, head_ip, head_ports, head_ports + 1, self.bootstrap_bytes, SHIM_PATH,
            gcs_address=gcs_address or f"{head_ip}:6379", expected_api_key=api_key,
        )
        self.vast = FakeVast(self.vast_port, api_key)

    def env(self, **overrides):
        """The environment of the container (public values and the fake secrets), as vast and the provider set it."""
        env = {
            "RAYVAST_HEAD_IP": self.head_ip,
            "RAYVAST_PORT_A": str(self.head.ports["A"]),
            "RAYVAST_PORT_B": str(self.head.ports["B"]),
            "RAYVAST_BOOTSTRAP_SHA256": hashlib.sha256(self.bootstrap_bytes).hexdigest(),
            "RAYVAST_CA_SHA256": pki.ca_fingerprint(self.ca_pem),
            "RAYVAST_CLUSTER": "st7",
            "CONTAINER_ID": str(self.instance_id),
            "CONTAINER_API_KEY": self.api_key,
            "PUBLIC_IPADDR": self.public_ip,
            "RAYVAST_VAST_API": f"http://{self.head_ip}:{self.vast_port}",
        }
        for i in range(64):
            env[f"VAST_TCP_PORT_{70000 + i}"] = str(self.port_base + i)
        env.update(overrides)
        return env

    def start(self):
        self.head.start()
        self.vast.start()

    def stop(self):
        self.head.stop()
        self.vast.stop()
