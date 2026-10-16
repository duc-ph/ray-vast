"""Head server: enrollment, heartbeat and reports (protocol: docs/design.md section 5). Runs as threads inside the provider (D2).

Port A is plain TLS (no client cert); port B is mTLS with a client cert from the cluster CA (S5). The TLS handshake runs
in the request thread with a timeout, so one stalled client cannot block the accept loop.
Nothing secret is logged: not the join token, the cluster token, the node key or the `/enroll` answer.
"""
import base64
import hashlib
import hmac
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
from .vast_api import IDENTITY_PORTS

log = logging.getLogger(__name__)

HANDSHAKE_TIMEOUT_S = 10
REQUEST_TIMEOUT_S = 30
MAX_BODY_BYTES = 1 << 20
PORT_CHECK_TIMEOUT_S = 5
HEARTBEAT_INTERVAL_S = 30
HEARTBEAT_TIMEOUT_S = 300
SECURITY_CHECKS = ("no_net_raw_admin", "own_netns", "shm_empty")
# The process-limit warning of a worker: at most once a minute while its refused forks rise, else at most once in 5 min.
PROCESS_LIMIT_REFUSALS_WARNING_S = 60
PROCESS_LIMIT_WARNING_S = 300
PROCESS_LIMIT_DOCS_URL = "https://duc-ph.github.io/ray-vast/how-to/troubleshoot/#a-worker-is-at-its-process-limit"


def _clip(text):
    """At most about 200 characters: the start and the end (the error line of a traceback is the last one)."""
    text = str(text)
    return repr(text if len(text) <= 200 else text[:80] + " ... " + text[-115:])


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
    """`end(cand, reason, block_kind=None)` is the provider's one ending path (rule 9): the head server ends a
    candidate or worker only through it, and makes no vast or AWS call of its own. Call it without `state.lock`."""

    def __init__(self, state, end, ca_cert_pem, ca_key_pem, head_cert_pem, head_key_pem, token, shim_bytes,
                 bootstrap_bytes, head_ip, port_a, port_b, cluster, clock=time.time):
        self.state, self.end, self.clock = state, end, clock
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

    def enroll(self, body, source_ip):
        """Return (status, answer) for POST /enroll. The checks run in the order of design 5.7. No vast call."""
        state = self.state
        try:
            instance_id = int(body["instance_id"])
            checks = body["checks"]
            ports = {int(k): int(v) for k, v in body["ports"].items()}
            cpu_count, gpu_count = int(body["cpu_count"]), int(body["gpu_count"])
            if not isinstance(checks, dict):
                raise ValueError("types")
            if sorted(ports) != list(IDENTITY_PORTS) or not all(0 < p < 65536 for p in ports.values()):
                raise ValueError("ports")
            if len(set(ports.values())) != len(ports) or not 1 <= cpu_count <= 4096 or not 0 <= gpu_count <= 64:
                raise ValueError("values")
        except (KeyError, TypeError, ValueError, AttributeError):
            return 400, {"reason": "bad request"}
        join_token = body.get("join_token")

        with state.lock:
            cand = state.candidates.get(instance_id)
            reason = None
            if cand is None:
                reason = "not a candidate"
            elif instance_id in state.enrolled:
                reason = "already enrolled"
            elif cand.public_ip is None:
                log.info("enroll of instance %s from %s: not seen running yet, asking it to retry", instance_id,
                         source_ip)
                return 503, {"reason": "the head has not seen this instance running yet, retry"}
            elif cand.public_ip != source_ip:
                reason = "source IP does not match the instance public IP"
            elif not (isinstance(join_token, str) and join_token and cand.join_token
                      and hmac.compare_digest(join_token.encode(), cand.join_token.encode())):
                reason = "join token is not valid for this instance"
        if reason:
            # No destroy here: this request may come from someone who only knows the ID (and shares the IP).
            log.warning("enroll refused for instance %s from %s: %s", instance_id, source_ip, reason)
            return 403, {"reason": reason}
        log.info("enroll request: instance %s, machine %s, ip %s", instance_id, cand.machine_id, source_ip)

        failed = [name for name in (*SECURITY_CHECKS, "egress_ip_ok") if checks.get(name) is not True]
        if failed:
            reason = "worker checks failed: %s (%s)" % (", ".join(failed), _clip(checks.get("details", "")))
            self.end(cand, reason, "security" if set(failed) & set(SECURITY_CHECKS) else "network")
            return 403, {"reason": reason}

        # GPU is exact (design F4). Fewer GPUs is a broken host; more can be another tenant's GPUs.
        if gpu_count != cand.gpus:
            reason = "GPU count: the container has %d, the node type declares %d" % (gpu_count, cand.gpus)
            self.end(cand, reason, "security")
            return 403, {"reason": reason}

        with ThreadPoolExecutor(max_workers=len(ports)) as pool:
            def reachable(port):
                try:
                    socket.create_connection((cand.public_ip, port), timeout=PORT_CHECK_TIMEOUT_S).close()
                    return True
                except OSError:
                    return False
            unreachable = sorted(p for p, ok in zip(ports.values(), pool.map(reachable, ports.values())) if not ok)
        if unreachable:
            reason = "port check failed: %d of %d ports unreachable" % (len(unreachable), len(ports))
            self.end(cand, reason, "network")
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
                cand.join_token = None  # one enrollment only (design 5.2)
                state.slot_enrolled(slot.node_type)
        if slot is None:
            self.end(cand, "no free slot")
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
            "num_gpus": cand.gpus,
            "heartbeat_interval_s": HEARTBEAT_INTERVAL_S,
            "heartbeat_timeout_s": HEARTBEAT_TIMEOUT_S,
        }

    def report_before_enrollment(self, body, source_ip):
        """Port A report. It only gets logged: a report causes no action, so it carries no proof. The text is clipped
        and quoted, and marked as unverified."""
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
            now = self.clock()
            # The status fields are optional (a heartbeat without them, or with bad values, is still a heartbeat).
            status = {k: body.get(k) for k in ("pids_current", "pids_max", "pids_refused", "ports_in_use", "ports_total")}
            status = {k: v if type(v) is int else None for k, v in status.items()}  # type(): a bool is not a count
            with self.state.lock:
                cand.last_heartbeat = now
                refused = status["pids_refused"]
                refused_before = cand.limit_warned_refused
                since_warning = None if cand.limit_warned_at is None else now - cand.limit_warned_at
                refusals_rose = refused is not None and refused > refused_before
                ports_all_used = (status["ports_total"] is not None and status["ports_in_use"] is not None
                                  and status["ports_in_use"] >= status["ports_total"])
                pids_near_limit = (status["pids_max"] is not None and status["pids_current"] is not None
                                   and status["pids_current"] > 0.9 * status["pids_max"])
                log_warning = (
                    (refusals_rose and (since_warning is None or since_warning >= PROCESS_LIMIT_REFUSALS_WARNING_S))
                    or ((ports_all_used or pids_near_limit)
                        and (since_warning is None or since_warning >= PROCESS_LIMIT_WARNING_S)))
                if log_warning:
                    cand.limit_warned_at = now
                    if refused is not None:
                        cand.limit_warned_refused = refused
                slot_id = next((s.id for s in self.state.slots.values() if s.instance_id == instance_id), cand.slot_id)
            if log_warning:
                log.warning(
                    "worker %s (instance %s, ip %s) is at its process limit: %s refused forks/threads (+%s), "
                    "pids %s of %s, worker ports in use %s of %s. Ray work on this worker can queue, hang or fail. "
                    "See %s", slot_id, instance_id, cand.public_ip, refused,
                    refused - refused_before if refusals_rose else 0, status["pids_current"], status["pids_max"],
                    status["ports_in_use"], status["ports_total"], PROCESS_LIMIT_DOCS_URL)
        elif body.get("kind") == "token_check" and body.get("ok") is not True:
            self.end(cand, "token check failed: " + str(body.get("reason", ""))[:200])
        elif body.get("kind") == "destroy":
            log.warning("worker %s asks to be destroyed: %s", instance_id, _clip(body.get("reason")))
        return 200, {}
