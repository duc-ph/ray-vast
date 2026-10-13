"""PID 1 on a worker: host checks, enrollment, Ray start, watchdog. Python stdlib only (see docs/protocol.md).

The bootloader downloads this file from the head and runs it. Rules for this file:
- It never ends without a destroy of its vast instance (`destroy`).
- Secrets (CONTAINER_API_KEY, token, node key) are never printed and never put in an error message.
"""
import base64
import glob
import hashlib
import http.client
import json
import math
import os
import signal
import socket
import ssl
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request
from types import SimpleNamespace

NET_ERRORS = (OSError, http.client.HTTPException)  # OSError covers URLError, SSLError and timeouts
RAYVAST_DIR = "/tmp/rayvast"
# Inode of the initial network namespace (new kernels, old kernels). A container with its own netns has another one.
HOST_NETNS_INODES = (0xEFFFFFF9, 0xF0000000)
ENROLL_DEADLINE_S = 600
DESTROY_REPORT_TIMEOUT_S = 10
DESTROY_RETRY_S = 5
ENROLL_RETRY_S = 10
TOKEN_CHECK_TIMEOUT_S = 180


class Stop(Exception):
    """Raised by the signal handler; the message is the destroy reason."""


def log(message):
    print(f"bootstrap: {message}", flush=True)


def parse_capeff(status_text):
    """Return the effective capability set (int) from the text of /proc/<pid>/status."""
    for line in status_text.splitlines():
        if line.startswith("CapEff:"):
            return int(line.split()[1], 16)
    raise ValueError("no CapEff line")


def parse_cpu_max(text):
    """Return the CPU quota in cores from the text of a cgroup v2 `cpu.max` file, or None if there is no limit."""
    quota, period = text.split()
    return None if quota == "max" else int(quota) / int(period)


def call(url, ctx, body=None, method=None, headers=None, timeout=10):
    """One HTTP call. Return (status, body bytes). HTTP error statuses are returned, network errors are raised."""
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url, data=data, method=method or ("POST" if data is not None else "GET"), headers=headers or {}
    )
    # No proxy: an image with http_proxy set must not send the head calls or the 127.0.0.1 token check through it.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=ctx))
    try:
        with opener.open(request, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def destroy(s, reason):
    """Report the reason to the head (best effort, at most 10 s), delete this vast instance, and wait. Never returns."""
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, signal.SIG_IGN)
    log(f"destroying this instance: {reason}")
    if s.report_ctx is not None:
        body = {"instance_id": s.instance_id, "kind": "destroy", "reason": reason[:2000]}
        if s.node_ctx is None:
            body["container_api_key"] = s.api_key
        else:
            body["ok"] = False
        try:
            call(s.report_url, s.report_ctx, body, timeout=DESTROY_REPORT_TIMEOUT_S)
        except NET_ERRORS as e:
            log(f"destroy report failed: {e}")
    while True:
        try:
            status, _ = call(
                f"{s.vast_api}/instances/{s.instance_id}/", None, method="DELETE",
                headers={"Authorization": f"Bearer {s.api_key}"}, timeout=30,
            )
            if status in (200, 404):
                break
            log(f"vast answered HTTP {status} to the delete; retrying")
        except NET_ERRORS as e:
            log(f"vast delete failed: {e}; retrying")
        time.sleep(DESTROY_RETRY_S / s.scale)
    log("vast accepted the delete; waiting for it to stop this container")
    while True:
        time.sleep(3600)


def main():
    env = os.environ
    s = SimpleNamespace(
        instance_id=int(env["CONTAINER_ID"]),
        api_key=env.pop("CONTAINER_API_KEY"),  # kept in memory only; the Ray child must not inherit it (D7)
        vast_api=env.get("RAYVAST_VAST_API", "https://console.vast.ai/api/v0").rstrip("/"),
        scale=float(env.get("RAYVAST_TIME_SCALE", "1")),
        report_url=None, report_ctx=None, node_ctx=None,
    )
    start = time.monotonic()

    def on_signal(signum, frame):
        raise Stop(f"received {signal.Signals(signum).name}")

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, on_signal)
    try:
        head_ip = env["RAYVAST_HEAD_IP"]
        base_a = f"https://{head_ip}:{env['RAYVAST_PORT_A']}"
        base_b = f"https://{head_ip}:{env['RAYVAST_PORT_B']}"
        public_ip = env["PUBLIC_IPADDR"]

        # 1. Pin the CA (S5): the unverified download is trusted only if its fingerprint matches the one in the env.
        unverified = ssl.create_default_context()
        unverified.check_hostname = False
        unverified.verify_mode = ssl.CERT_NONE
        while True:
            try:
                status, ca_pem = call(f"{base_a}/ca.pem", unverified)
                if status == 200:
                    break
                log(f"head answered HTTP {status} for /ca.pem")
            except NET_ERRORS as e:
                log(f"cannot fetch /ca.pem: {e}")
            if time.monotonic() - start > ENROLL_DEADLINE_S / s.scale:
                destroy(s, "no CA cert from the head before the enrollment deadline")
            time.sleep(DESTROY_RETRY_S / s.scale)
        if hashlib.sha256(ssl.PEM_cert_to_DER_cert(ca_pem.decode())).hexdigest() != env["RAYVAST_CA_SHA256"]:
            destroy(s, "CA cert fingerprint does not match RAYVAST_CA_SHA256")
        ctx = ssl.create_default_context(cadata=ca_pem.decode())
        s.report_url, s.report_ctx = f"{base_a}/report", ctx

        # 2. Host checks. The head decides what a failed check means.
        egress_ip = None
        for echo_url in ("https://api.ipify.org", "https://ifconfig.me/ip"):
            try:
                egress_ip = call(echo_url, None, timeout=5)[1].decode().strip()
                break
            except NET_ERRORS:
                pass
        with open("/proc/self/status") as f:
            cap_eff = parse_capeff(f.read())
        netns_inode = os.stat("/proc/self/ns/net").st_ino
        shm_entries = os.listdir("/dev/shm")
        checks = {
            "egress_ip_ok": egress_ip == public_ip,
            "no_net_raw_admin": cap_eff & (3 << 12) == 0,  # NET_ADMIN is bit 12, NET_RAW is bit 13
            "own_netns": netns_inode not in HOST_NETNS_INODES,
            "shm_empty": not shm_entries,
        }
        checks["details"] = (
            f"egress_ip={egress_ip} public_ip={public_ip} cap_eff={cap_eff:x} netns_inode={netns_inode} "
            f"shm_entries={len(shm_entries)}"
        )
        if env.get("RAYVAST_SKIP_HOST_CHECKS") == "1":
            failed = sorted(k for k, v in checks.items() if v is False)
            checks.update(egress_ip_ok=True, no_net_raw_admin=True, own_netns=True, shm_empty=True)
            checks["details"] += f" SKIPPED (these failed: {failed})"
        log(f"host checks: {checks}")

        # 3. Open a listener on every host port, so the head can connect to each one from outside.
        ports = {str(70000 + i): int(env[f"VAST_TCP_PORT_{70000 + i}"]) for i in range(64)}
        listeners = []
        for port in ports.values():
            listener = socket.socket()
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("0.0.0.0", port))
            listener.listen(16)
            listeners.append(listener)

        # 4. Enroll. The real core count is the smaller of the affinity and the cgroup quota.
        cpu_count = len(os.sched_getaffinity(0))
        try:
            with open("/sys/fs/cgroup/cpu.max") as f:
                quota_cores = parse_cpu_max(f.read())
        except OSError:
            quota_cores = None
        if quota_cores is not None:
            cpu_count = min(cpu_count, max(1, math.floor(quota_cores)))
        gpu_count = len(glob.glob("/proc/driver/nvidia/gpus/*"))
        enroll_body = {
            "instance_id": s.instance_id, "container_api_key": s.api_key, "checks": checks, "ports": ports,
            "cpu_count": cpu_count, "gpu_count": gpu_count,
        }
        while True:
            try:
                status, answer = call(f"{base_a}/enroll", ctx, enroll_body, timeout=120)
                if status != 503:
                    break
                log("head answered 503 to /enroll; retrying")
            except NET_ERRORS as e:
                log(f"enroll call failed: {e}; retrying")
            if time.monotonic() - start > ENROLL_DEADLINE_S / s.scale:
                destroy(s, "not enrolled before the enrollment deadline")
            time.sleep(ENROLL_RETRY_S / s.scale)
        if status == 403:
            destroy(s, f"head refused enrollment: {json.loads(answer).get('reason')}")
        if status != 200:
            destroy(s, f"enrollment failed with HTTP {status}")
        answer = json.loads(answer)

        # 5. Write the files, close the listeners, start Ray.
        os.makedirs(RAYVAST_DIR, mode=0o700, exist_ok=True)
        os.chmod(RAYVAST_DIR, 0o700)
        files = {
            "ca.pem": ca_pem,  # the pinned bytes; the head sends the same
            "node.pem": answer["cert_pem"].encode(),
            "node.key": answer["key_pem"].encode(),
            "token": answer["token"].encode(),
            "librayvast_shim.so": base64.b64decode(answer["shim_b64"]),
        }
        for name, content in files.items():
            fd = os.open(f"{RAYVAST_DIR}/{name}", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "wb") as f:
                f.write(content)
        for listener in listeners:
            listener.close()
        node_ctx = ssl.create_default_context(cadata=ca_pem.decode())
        node_ctx.load_cert_chain(f"{RAYVAST_DIR}/node.pem", f"{RAYVAST_DIR}/node.key")
        s.report_url, s.report_ctx, s.node_ctx = f"{base_b}/report", node_ctx, node_ctx

        # The private IP is the interface address that routes to the head.
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.connect((head_ip, int(env["RAYVAST_PORT_A"])))
        private_ip = probe.getsockname()[0]
        probe.close()
        agent_ports = [ports["70004"], ports["70005"]]
        worker_ports = [ports[str(p)] for p in range(70006, 70064)]
        ray_cmd = [
            sys.executable, "-c", "from ray.scripts.scripts import main; main()", "start", "--block",
            f"--address={answer['gcs_address']}", f"--node-ip-address={public_ip}",
            f"--node-manager-port={ports['70000']}", f"--object-manager-port={ports['70001']}",
            f"--metrics-export-port={ports['70002']}", f"--dashboard-agent-grpc-port={ports['70003']}",
            f"--dashboard-agent-listen-port={ports['70004']}", f"--runtime-env-agent-port={ports['70005']}",
            f"--worker-port-list={','.join(map(str, worker_ports))}",
            f"--num-cpus={cpu_count}", f"--num-gpus={answer['num_gpus']}",
        ]
        ray_env = dict(
            env,
            RAY_CLOUD_INSTANCE_ID=str(answer["slot_id"]), RAY_NODE_TYPE_NAME=answer["node_type"],
            RAY_USE_TLS="1", RAY_TLS_SERVER_CERT=f"{RAYVAST_DIR}/node.pem",
            RAY_TLS_SERVER_KEY=f"{RAYVAST_DIR}/node.key", RAY_TLS_CA_CERT=f"{RAYVAST_DIR}/ca.pem",
            RAY_AUTH_MODE="token", RAY_AUTH_TOKEN_PATH=f"{RAYVAST_DIR}/token",
            LD_PRELOAD=f"{RAYVAST_DIR}/librayvast_shim.so",
            RAYVAST_PUBLIC_IP=public_ip, RAYVAST_PRIVATE_IP=private_ip,
            RAYVAST_LOCALHOST_PORTS=",".join(map(str, agent_ports)),
            RAY_num_workers_soft_limit=str(answer["num_workers_soft_limit"]),
        )
        ray = subprocess.Popen(ray_cmd, env=ray_env, stdin=subprocess.DEVNULL)
        log(f"enrolled as slot {answer['slot_id']}; started ray (pid {ray.pid}) with {cpu_count} cpus")

        # 6. Watchdog: heartbeat, token check, reap children, destroy on any exit.
        heartbeat_interval = answer["heartbeat_interval_s"] / s.scale
        heartbeat_timeout = answer["heartbeat_timeout_s"] / s.scale
        last_ok = next_heartbeat = time.monotonic()
        token_deadline = last_ok + TOKEN_CHECK_TIMEOUT_S / s.scale
        token_result = None  # (ok, reason) once decided
        token_reported = False
        while True:
            while True:
                try:
                    pid, wait_status = os.waitpid(-1, os.WNOHANG)
                except ChildProcessError:
                    break
                if pid == 0:
                    break
                if pid == ray.pid:
                    destroy(s, f"ray start exited with code {os.waitstatus_to_exitcode(wait_status)}")
            now = time.monotonic()

            if not token_reported:
                if token_result is None:
                    statuses = []
                    for port in agent_ports:
                        try:
                            statuses.append(call(f"http://127.0.0.1:{port}/", None, timeout=2)[0])
                        except NET_ERRORS:
                            statuses.append(None)
                    if None not in statuses:
                        token_result = (statuses == [401, 401], f"agent ports answered {statuses} without a token")
                    elif now > token_deadline:
                        token_result = (False, f"agent ports answered {statuses} before the deadline")
                if token_result is not None:
                    try:
                        status, _ = call(
                            f"{base_b}/report", node_ctx,
                            {"instance_id": s.instance_id, "kind": "token_check", "ok": token_result[0],
                             "reason": token_result[1]},
                        )
                        token_reported = status == 200
                    except NET_ERRORS as e:
                        log(f"token check report failed: {e}")

            if now >= next_heartbeat:
                next_heartbeat = now + heartbeat_interval
                try:
                    status, _ = call(f"{base_b}/heartbeat", node_ctx, {"instance_id": s.instance_id})
                    if status == 200:
                        last_ok = now
                    elif status == 403:
                        destroy(s, "head refused the heartbeat (403)")
                    else:
                        log(f"heartbeat answered HTTP {status}")
                except NET_ERRORS as e:
                    log(f"heartbeat failed: {e}")
            if now - last_ok > heartbeat_timeout:
                destroy(s, f"no successful heartbeat for {heartbeat_timeout * s.scale:.0f} s")
            time.sleep(1)
    except Stop as e:
        destroy(s, str(e))
    except Exception:
        destroy(s, f"unexpected error:\n{traceback.format_exc()}")


if __name__ == "__main__":
    main()
