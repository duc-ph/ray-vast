"""Head server: enrollment, heartbeat and reports (protocol: docs/protocol.md). Runs as threads inside the provider (D2).

Port A is plain TLS (no client cert); port B is mTLS with a client cert from the cluster CA (S5). The TLS handshake runs
in the request thread with a timeout, so one stalled client cannot block the accept loop.
Nothing secret is logged: not the container key, the token, the node key or the `/enroll` answer.
"""
import base64
import hashlib
import http.server
import json
import logging
import socket
import ssl
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from . import pki
from .vast_api import IDENTITY_PORTS, VastError

log = logging.getLogger(__name__)

HANDSHAKE_TIMEOUT_S = 10
REQUEST_TIMEOUT_S = 30
MAX_BODY_BYTES = 1 << 20
OUTSIDE_CHECK_TIMEOUT_S = 5
HEARTBEAT_INTERVAL_S = 30
HEARTBEAT_TIMEOUT_S = 300
SECURITY_CHECKS = ("no_net_raw_admin", "own_netns", "shm_empty")


def _clip(text):
    return repr(str(text)[:200])


class _TlsServer(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, handler, ctx, head, role):
        self.ctx, self.head, self.role = ctx, head, role
        super().__init__(address, handler)

    def finish_request(self, request, client_address):
        try:
            request.settimeout(HANDSHAKE_TIMEOUT_S)
            tls = self.ctx.wrap_socket(request, server_side=True)
        except (ssl.SSLError, OSError) as e:
            log.info("TLS handshake failed on port %s from %s: %s", self.role, client_address[0], type(e).__name__)
            return
        try:
            self.RequestHandlerClass(tls, client_address, self)
        finally:
            tls.close()


class _Handler(http.server.BaseHTTPRequestHandler):
    timeout = REQUEST_TIMEOUT_S

    def log_message(self, *args):
        pass

    def _send(self, status, body, content_type="application/json"):
        if not isinstance(body, bytes):
            body = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        head = self.server.head
        if self.server.role == "A" and self.path == "/ca.pem":
            self._send(200, head.ca_cert_pem, "application/x-pem-file")
        elif self.server.role == "A" and self.path == "/bootstrap":
            self._send(200, head.bootstrap_bytes, "text/x-python")
        else:
            self._send(404, {"reason": "not found"})

    def do_POST(self):
        head = self.server.head
        route = (self.server.role, self.path)
        if route not in {("A", "/enroll"), ("A", "/report"), ("B", "/heartbeat"), ("B", "/report")}:
            return self._send(404, {"reason": "not found"})
        try:
            length = int(self.headers.get("Content-Length", 0))
            if not 0 <= length <= MAX_BODY_BYTES:
                raise ValueError("body size")
            body = json.loads(self.rfile.read(length))
            if not isinstance(body, dict):
                raise ValueError("body type")
        except (ValueError, OSError):
            return self._send(400, {"reason": "bad request"})
        try:
            if route == ("A", "/enroll"):
                status, answer = head.enroll(body, self.client_address[0])
            elif route == ("A", "/report"):
                status, answer = head.report_before_enrollment(body, self.client_address[0])
            else:
                status, answer = head.worker_request(self.path, body, self.connection.getpeercert())
        except Exception as e:  # the answer and the log carry only the type: a message may hold a secret
            log.error("request %s failed: %s", self.path, type(e).__name__)
            status, answer = 500, {"reason": "internal error"}
        self._send(status, answer)


class HeadServer:
    """`on_destroy(public_ip, remaining_refs)` is called after the head destroys a candidate or worker.

    The head already lowered the IP ref count in `state`; the provider removes the security group rule when it hits 0.
    """

    def __init__(self, state, vast, ca_cert_pem, ca_key_pem, head_cert_pem, head_key_pem, token, shim_bytes,
                 bootstrap_bytes, head_ip, port_a, port_b, cluster, clock=time.time, on_destroy=None):
        self.state, self.vast, self.clock, self.on_destroy = state, vast, clock, on_destroy
        self.ca_cert_pem, self.ca_key_pem, self.token = ca_cert_pem, ca_key_pem, token
        self.shim_bytes, self.bootstrap_bytes = shim_bytes, bootstrap_bytes
        self.head_ip, self.cluster = head_ip, cluster
        self.bootstrap_sha256 = hashlib.sha256(bootstrap_bytes).hexdigest()
        self.port_a, self.port_b = port_a, port_b
        self._head_cert_pem, self._head_key_pem = head_cert_pem, head_key_pem
        self._servers = []

    def start(self):
        with tempfile.TemporaryDirectory() as tmp:  # load_cert_chain needs files; the key file is 600 and short-lived
            cert_path, key_path = Path(tmp, "head.pem"), Path(tmp, "head.key")
            cert_path.write_bytes(self._head_cert_pem)
            key_path.touch(mode=0o600)
            key_path.write_bytes(self._head_key_pem)
            ctx_a = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx_a.load_cert_chain(cert_path, key_path)
            ctx_b = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx_b.load_cert_chain(cert_path, key_path)
        ctx_b.verify_mode = ssl.CERT_REQUIRED
        ctx_b.load_verify_locations(cadata=self.ca_cert_pem.decode())
        for role, port, ctx in (("A", self.port_a, ctx_a), ("B", self.port_b, ctx_b)):
            server = _TlsServer(("0.0.0.0", port), _Handler, ctx, self, role)
            threading.Thread(target=server.serve_forever, name=f"head-server-{role}", daemon=True).start()
            self._servers.append(server)

    def stop(self):
        for server in self._servers:
            server.shutdown()
            server.server_close()
        self._servers = []

    def _destroy(self, cand, reason):
        """End a candidate or worker: destroy the vast instance, drop it from state, lower the IP ref count.

        Idempotent: only the caller that removes the candidate from state does the work.
        """
        if self.state.remove_candidate(cand.instance_id) is None:
            return
        log.warning("destroying instance %s (machine %s, ip %s): %s", cand.instance_id, cand.machine_id,
                    cand.public_ip, _clip(reason))
        try:
            self.vast.destroy_instance(cand.instance_id)
        except Exception as e:  # the provider sweep destroys what is left
            log.error("destroy of instance %s failed (the provider sweep will retry): %s", cand.instance_id,
                      type(e).__name__)
        finally:
            if cand.public_ip is not None:
                remaining = self.state.ip_ref_remove(cand.public_ip)
                if self.on_destroy:
                    self.on_destroy(cand.public_ip, remaining)

    def enroll(self, body, source_ip):
        """Return (status, answer) for POST /enroll. The checks run in the order of the plan, cheapest first."""
        state = self.state
        try:
            instance_id = int(body["instance_id"])
            container_key = body["container_api_key"]
            checks = body["checks"]
            ports = {int(k): int(v) for k, v in body["ports"].items()}
            cpu_count, gpu_count = int(body["cpu_count"]), int(body["gpu_count"])
            if not isinstance(container_key, str) or not isinstance(checks, dict):
                raise ValueError("types")
            if sorted(ports) != list(IDENTITY_PORTS) or not all(0 < p < 65536 for p in ports.values()):
                raise ValueError("ports")
            if len(set(ports.values())) != len(ports) or not 1 <= cpu_count <= 4096 or not 0 <= gpu_count <= 64:
                raise ValueError("values")
        except (KeyError, TypeError, ValueError, AttributeError):
            return 400, {"reason": "bad request"}

        with state.lock:
            cand = state.candidates.get(instance_id)
            reason = None
            if not container_key.strip():
                reason = "empty container key"  # vast must never see an empty key (it could fall back to the account key)
            elif cand is None:
                reason = "not a candidate"
            elif instance_id in state.enrolled:
                reason = "already enrolled"
            elif cand.public_ip != source_ip:
                reason = "source IP does not match the instance public IP"
        if reason:
            # No destroy here: this request may come from someone who only knows the ID.
            log.warning("enroll refused for instance %s from %s: %s", instance_id, source_ip, reason)
            return 403, {"reason": reason}
        log.info("enroll request: instance %s, machine %s, ip %s", instance_id, cand.machine_id, source_ip)

        # A wrong key gets no destroy: a co-tenant behind the same IP must not be able to kill our candidate.
        try:
            instance = self.vast.get_instance(instance_id, key=container_key)
        except VastError as e:
            if e.status not in (401, 403, 404):
                log.warning("enroll of instance %s: vast error %s, asking the worker to retry", instance_id, e.status)
                return 503, {"reason": "vast is unavailable, retry"}
            instance = None
        if instance is None:
            log.warning("enroll refused for instance %s: container key is not valid", instance_id)
            return 403, {"reason": "container key is not valid for this instance"}

        failed = [name for name in (*SECURITY_CHECKS, "egress_ip_ok") if checks.get(name) is not True]
        if failed:
            reason = "worker checks failed: %s (%s)" % (", ".join(failed), _clip(checks.get("details", "")))
            kind = "security" if set(failed) & set(SECURITY_CHECKS) else "network"
            state.block(cand.machine_id, reason, kind, self.clock())
            self._destroy(cand, reason)
            return 403, {"reason": reason}

        with ThreadPoolExecutor(max_workers=len(ports)) as pool:
            def reachable(port):
                try:
                    socket.create_connection((cand.public_ip, port), timeout=OUTSIDE_CHECK_TIMEOUT_S).close()
                    return True
                except OSError:
                    return False
            unreachable = sorted(p for p, ok in zip(ports.values(), pool.map(reachable, ports.values())) if not ok)
        if unreachable:
            reason = "outside check failed: %d of %d ports unreachable" % (len(unreachable), len(ports))
            state.block(cand.machine_id, reason, "network", self.clock())
            self._destroy(cand, reason)
            return 403, {"reason": reason}

        # The cert comes before the slot is taken, so a failure here leaves nothing half-enrolled.
        cert_pem, key_pem = pki.make_leaf(self.ca_cert_pem, self.ca_key_pem, cand.public_ip, instance_id=instance_id)
        with state.lock:
            if instance_id in state.enrolled or instance_id not in state.candidates:
                return 403, {"reason": "already enrolled"}  # a parallel request won; keep the instance
            wanted = state.slots.get(cand.slot_id)
            node_type = cand.node_type or (wanted.node_type if wanted else None)
            slot = None
            if wanted is not None and wanted.instance_id is None:
                slot = wanted
            elif node_type is not None:
                slot = next((s for s in state.slots.values() if s.node_type == node_type and s.instance_id is None), None)
            if slot is not None:
                state.mark_enrolled(instance_id)
                slot.instance_id, slot.status = instance_id, "setting-up"
        if slot is None:
            self._destroy(cand, "no free slot")
            return 403, {"reason": "no free slot"}

        log.info("enrolled instance %s (machine %s, ip %s) into %s", instance_id, cand.machine_id, cand.public_ip,
                 slot.id)
        return 200, {
            "slot_id": slot.id,
            "node_type": slot.node_type,
            "cert_pem": cert_pem.decode(),
            "key_pem": key_pem.decode(),
            "ca_pem": self.ca_cert_pem.decode(),
            "token": self.token,
            "shim_b64": base64.b64encode(self.shim_bytes).decode(),
            "gcs_address": f"{self.head_ip}:6379",
            "num_gpus": gpu_count,
            "heartbeat_interval_s": HEARTBEAT_INTERVAL_S,
            "heartbeat_timeout_s": HEARTBEAT_TIMEOUT_S,
            "num_workers_soft_limit": min(cpu_count, len(IDENTITY_PORTS) - 6),
        }

    def report_before_enrollment(self, body, source_ip):
        """Port A report. It only gets logged: a report causes no action, so the container key is not checked (and
        never read). The text is clipped and quoted, and marked as unverified."""
        log.warning("unverified report from %s: instance %s kind %s reason %s", source_ip, _clip(body.get("instance_id")),
                    _clip(body.get("kind")), _clip(body.get("reason")))
        return 200, {}

    def worker_request(self, path, body, peer_cert):
        """Port B: /heartbeat and /report. The client cert must carry the enrolled public IP of the claimed worker."""
        try:
            instance_id = int(body["instance_id"])
        except (KeyError, TypeError, ValueError):
            return 400, {"reason": "bad request"}
        with self.state.lock:
            cand = self.state.candidates.get(instance_id)
            enrolled = instance_id in self.state.enrolled
        san = (peer_cert or {}).get("subjectAltName", ())
        if (cand is None or not enrolled or ("IP Address", cand.public_ip) not in san
                or ("URI", f"rayvast:instance/{instance_id}") not in san):
            log.warning("port B request %s refused for instance %s", path, instance_id)
            return 403, {"reason": "unknown"}
        if path == "/heartbeat":
            with self.state.lock:
                cand.last_heartbeat = self.clock()
        elif body.get("kind") == "token_check" and body.get("ok") is not True:
            self._destroy(cand, "token check failed: " + str(body.get("reason", ""))[:200])
        elif body.get("kind") == "destroy":
            log.warning("worker %s asks to be destroyed: %s", instance_id, _clip(body.get("reason")))
        return 200, {}
