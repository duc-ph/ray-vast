"""A fake head server for the worker tests: the endpoints of docs/design.md section 5 on ports A and B, in threads.

Behavior switches (plain attributes, change them while it runs): `refuse_enroll`, `answer_heartbeats`.
Everything it receives is recorded in `events` as (monotonic time, port name, path, body).
"""
import base64
import http.server
import json
import ssl
import threading
import time
from pathlib import Path

from ray_vast import pki


class FakeHead:
    def __init__(self, pki_dir, head_ip, port_a, port_b, bootstrap_bytes, shim_path, *, gcs_address,
                 expected_join_token=None, listen_host="0.0.0.0"):
        """`pki_dir` holds ca.pem, ca.key, head.pem, head.key and token (from `pki.write_head_files`)."""
        self.pki_dir = Path(pki_dir)
        self.head_ip = head_ip
        self.ports = {"A": port_a, "B": port_b}
        self.bootstrap_bytes = bootstrap_bytes
        self.shim_b64 = base64.b64encode(Path(shim_path).read_bytes()).decode()
        self.gcs_address = gcs_address
        self.expected_join_token = expected_join_token
        self.listen_host = listen_host
        self.tls_dir = self.pki_dir  # where the TLS cert and key of the two ports come from
        self.served_ca = None  # bytes to serve at /ca.pem instead of the real CA cert
        self.trickle_bootstrap = False  # send /bootstrap at one byte per second
        self.refuse_enroll = False
        self.enroll_503s = 0  # answer 503 to this many /enroll calls first
        self.answer_heartbeats = True
        self.events = []
        self.enrolled = set()
        self._servers = []

    def bodies(self, port, path):
        return [body for _, p, pa, body in self.events if p == port and pa == path]

    def start(self):
        for name, require_client_cert in (("A", False), ("B", True)):
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(self.tls_dir / "head.pem", self.tls_dir / "head.key")
            if require_client_cert:
                ctx.verify_mode = ssl.CERT_REQUIRED
                ctx.load_verify_locations(self.pki_dir / "ca.pem")
            server = http.server.ThreadingHTTPServer((self.listen_host, self.ports[name]), self._handler_class(name))
            server.socket = ctx.wrap_socket(server.socket, server_side=True, do_handshake_on_connect=False)
            server.handle_error = lambda request, client_address: None  # failed client handshakes are expected
            server.daemon_threads = True
            threading.Thread(target=server.serve_forever, daemon=True).start()
            self._servers.append(server)

    def stop(self):
        for server in self._servers:
            server.shutdown()
            server.server_close()

    def _handler_class(self, port_name):
        head = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def setup(self):
                # The TLS handshake runs in the request thread with a timeout, so one stuck client cannot block the server.
                self.request.settimeout(10)
                self.request.do_handshake()
                super().setup()

            def _send(self, status, body, content_type="application/json"):
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                head.events.append((time.monotonic(), port_name, self.path, None))
                if port_name == "A" and self.path == "/ca.pem":
                    self._send(200, head.served_ca or (head.pki_dir / "ca.pem").read_bytes(), "application/x-pem-file")
                elif port_name == "A" and self.path == "/bootstrap" and head.trickle_bootstrap:
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(head.bootstrap_bytes)))
                    self.end_headers()
                    for i in range(len(head.bootstrap_bytes)):
                        self.wfile.write(head.bootstrap_bytes[i:i + 1])
                        self.wfile.flush()
                        time.sleep(1)
                elif port_name == "A" and self.path == "/bootstrap":
                    self._send(200, head.bootstrap_bytes, "text/x-python")
                else:
                    self._send(404, b"{}")

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
                head.events.append((time.monotonic(), port_name, self.path, body))
                if port_name == "A" and self.path == "/enroll":
                    if head.enroll_503s > 0:
                        head.enroll_503s -= 1
                        return self._send(503, json.dumps({"reason": "vast unreachable"}).encode())
                    if head.refuse_enroll or (
                        head.expected_join_token is not None and body.get("join_token") != head.expected_join_token
                    ):
                        return self._send(403, json.dumps({"reason": "refused by the fake head"}).encode())
                    worker_ip = self.client_address[0]
                    ca_pem = (head.pki_dir / "ca.pem").read_bytes()
                    cert_pem, key_pem = pki.make_leaf(ca_pem, (head.pki_dir / "ca.key").read_bytes(), worker_ip,
                                                instance_id=body["instance_id"])
                    head.enrolled.add(body["instance_id"])
                    answer = {
                        "slot_id": "slot-1", "node_type": "worker", "cert_pem": cert_pem.decode(),
                        "key_pem": key_pem.decode(),
                        "token": (head.pki_dir / "token").read_text().strip(), "shim_b64": head.shim_b64,
                        "gcs_address": head.gcs_address,
                    }
                    self._send(200, json.dumps(answer).encode())
                elif self.path == "/report":
                    self._send(200, b"{}")
                elif port_name == "B" and self.path == "/heartbeat":
                    if body.get("instance_id") not in head.enrolled:
                        self._send(403, json.dumps({"reason": "unknown"}).encode())
                    elif head.answer_heartbeats:
                        self._send(200, b"{}")
                    # else: say nothing; the worker's call times out
                else:
                    self._send(404, b"{}")

        return Handler
