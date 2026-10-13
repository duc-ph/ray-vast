"""A fake vast.ai HTTP server that runs each rented "instance" as a local Docker container (plan step 11).

It answers what `vast_api.VastClient` calls: POST /bundles/, PUT /asks/<offer>/, GET /instances/, GET and DELETE
/instances/<id>/. A rental runs `docker run --network host` with the given image and args. Host networking gives
every instance the box IP as `PUBLIC_IPADDR`, so two instances always share one public IP, and a connection from an
instance to the head comes from that same IP (the head server checks it). Each instance gets its own block of 64
host ports, mapped P to P (the bootstrap listens on them directly). Containers are named `st11-w-<id>`.
"""
import http.server
import json
import re
import secrets
import subprocess
import tempfile
import threading
import time
from pathlib import Path

PREFIX = "/api/v0"
PORT_BLOCK_START = 20000


class FakeVast:
    def __init__(self, box_ip, offers, account_key="st11-account-key", docker_args=(), extra_env=None):
        self.box_ip, self.account_key = box_ip, account_key
        self.docker_args = list(docker_args)  # added to every `docker run` (for example --cpus 2)
        self.extra_env = dict(extra_env or {})  # added to the env of every instance
        self.lock = threading.Lock()
        self.pool = {o["id"]: dict(o) for o in offers}  # offers that can be rented
        self.instances = {}  # id -> dict(key, offer, name, block, label)
        self.rentals = []  # instance ids in the order of creation
        self.destroys = []  # (instance id, "account" | "instance", docker logs of the container at that time)
        self.calls = []  # (method, path)
        self._next_id = 1000
        self._next_block = PORT_BLOCK_START
        self._tmp = tempfile.TemporaryDirectory()
        fake = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _handle(self, method):
                path = self.path.split("?")[0]
                assert path.startswith(PREFIX), path
                path = path[len(PREFIX):]
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length)) if length else None
                token = (self.headers.get("Authorization") or "").removeprefix("Bearer ")
                status, answer = fake.handle(method, path, token, body)
                data = json.dumps(answer).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = lambda self: self._handle("GET")
            do_POST = lambda self: self._handle("POST")
            do_PUT = lambda self: self._handle("PUT")
            do_DELETE = lambda self: self._handle("DELETE")

        self._server = http.server.ThreadingHTTPServer(("0.0.0.0", 0), Handler)
        self._server.daemon_threads = True
        self.url = f"http://{box_ip}:{self._server.server_address[1]}{PREFIX}"

    def start(self):
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def stop(self):
        self._server.shutdown()
        self._server.server_close()
        names = subprocess.run(["docker", "ps", "-aq", "--filter", "name=^st11-w-"], capture_output=True, text=True)
        if names.stdout.split():
            subprocess.run(["docker", "rm", "-f", *names.stdout.split()], capture_output=True)
        self._tmp.cleanup()

    def name_of(self, instance_id):
        return self.instances[instance_id]["name"]

    def live_ids(self):
        with self.lock:
            return sorted(self.instances)

    def _status(self, name):
        state = subprocess.run(["docker", "inspect", "-f", "{{.State.Status}}", name], capture_output=True, text=True)
        return {"running": "running", "restarting": "running", "created": "loading"}.get(state.stdout.strip(), "exited")

    def _record(self, instance_id):
        inst = self.instances[instance_id]
        return {
            "id": instance_id, "label": inst["label"], "actual_status": self._status(inst["name"]),
            "public_ipaddr": self.box_ip, "machine_id": inst["offer"]["machine_id"], "status_msg": "success",
            "dph_total": inst["offer"]["dph_total"],
        }

    def handle(self, method, path, token, body):
        with self.lock:
            self.calls.append((method, path))
        if method == "POST" and path == "/bundles/":
            if token != self.account_key:
                return 401, {"error": "bad key"}
            with self.lock:
                return 200, {"offers": sorted(self.pool.values(), key=lambda o: o["dph_total"])}
        if method == "PUT" and (m := re.fullmatch(r"/asks/(\d+)/", path)):
            if token != self.account_key:
                return 401, {"error": "bad key"}
            return self._rent(int(m.group(1)), body)
        if method == "GET" and path == "/instances/":
            if token != self.account_key:
                return 401, {"error": "bad key"}
            with self.lock:
                return 200, {"instances": [self._record(i) for i in self.instances]}
        if m := re.fullmatch(r"/instances/(\d+)/", path):
            instance_id = int(m.group(1))
            with self.lock:
                inst = self.instances.get(instance_id)
                own_key = inst is not None and token == inst["key"]
            if token != self.account_key and not own_key:
                return 401, {"error": "bad key"}  # an instance key works for its own instance only
            if inst is None:
                return 404, {"error": "no such instance"}
            if method == "GET":
                return 200, {"instances": self._record(instance_id)}
            if method == "DELETE":
                logs = subprocess.run(["docker", "logs", inst["name"]], capture_output=True, text=True)
                subprocess.run(["docker", "rm", "-f", inst["name"]], capture_output=True)
                with self.lock:
                    del self.instances[instance_id]
                    self.pool[inst["offer"]["id"]] = inst["offer"]
                    self.destroys.append((instance_id, "instance" if own_key else "account", logs.stdout + logs.stderr))
                return 200, {"success": True}
        return 404, {"error": f"unknown call {method} {path}"}

    def _rent(self, offer_id, body):
        with self.lock:
            offer = self.pool.pop(offer_id, None)
            if offer is None:
                return 400, {"success": False, "error": "offer is gone"}
            instance_id, block = self._next_id, self._next_block
            self._next_id += 1
            self._next_block += 64
            key = secrets.token_hex(16)
        name = f"st11-w-{instance_id}"
        env = {k: v for k, v in body["env"].items() if not k.startswith("-p ")}  # "-p P:P" keys are docker flags
        env.update(
            CONTAINER_ID=str(instance_id), CONTAINER_API_KEY=key, PUBLIC_IPADDR=self.box_ip,
            RAYVAST_VAST_API=self.url, **self.extra_env,
        )
        env.update({f"VAST_TCP_PORT_{70000 + i}": str(block + i) for i in range(64)})
        env_file = Path(self._tmp.name, f"{name}.env")
        env_file.write_text("".join(f"{k}={v}\n" for k, v in env.items()))
        run = subprocess.run(
            ["docker", "run", "-d", "--name", name, "--network", "host", "--env-file", str(env_file),
             *self.docker_args, body["image"], *body["args"]],
            capture_output=True, text=True,
        )
        env_file.unlink()
        if run.returncode != 0:
            with self.lock:
                self.pool[offer_id] = offer
            return 500, {"success": False, "error": run.stderr[-300:]}
        with self.lock:
            self.instances[instance_id] = dict(key=key, offer=offer, name=name, block=block, label=body["label"])
            self.rentals.append(instance_id)
        return 200, {"success": True, "new_contract": instance_id, "instance_api_key": key}


def make_offers(count):
    """Cheapest first. Every offer is its own machine."""
    return [{"id": 500 + i, "machine_id": 900 + i, "dph_total": round(0.10 + 0.01 * i, 2), "inet_down": 1000.0}
            for i in range(count)]
