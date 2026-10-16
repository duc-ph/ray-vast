"""PID 1 on a worker: host checks, enrollment, Ray start, watchdog. Python stdlib only (see docs/design.md section 5).

The bootloader downloads this file from the head and runs it. Rules for this file:
- It never ends without a destroy of its vast instance (`destroy`).
- Secrets (CONTAINER_API_KEY, the join token, the cluster token, the node key) are never printed and never put in an
  error message. CONTAINER_API_KEY never leaves this instance: only vast sees it, in our own DELETE.
"""
import base64
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
import threading
import time
import traceback
import urllib.error
import urllib.request
from types import SimpleNamespace

NET_ERRORS = (OSError, http.client.HTTPException)  # OSError covers URLError, SSLError and timeouts
RAYVAST_DIR = os.environ.get("RAYVAST_DIR", "/tmp/rayvast")  # the override is for tests only
CGROUP_DIR = os.environ.get("RAYVAST_CGROUP_DIR", "/sys/fs/cgroup")  # the override is for tests only
# The container's process limit (pids.max, design 5.5): Ray's gRPC pools are sized by these threads, not by the host cores, and the
# raylet starts at most this many worker processes at the same time. The worker port count follows from pids.max.
GRPC_THREADS = 4
STARTUP_CONCURRENCY = 8
# PIDS_RESERVE: threads of the fixed Ray processes and the bootstrap (about 160), plus STARTUP_CONCURRENCY starting worker
# processes (8 x 25; a process that finds no free port also starts before it exits), plus 40 of headroom.
PIDS_RESERVE = 400
# PIDS_PER_WORKER: threads of the heaviest common Ray worker process (a Serve replica: 50), plus 10 for the user's threads.
PIDS_PER_WORKER = 60
MIN_WORKER_PORTS = 4
WORKER_PORT_IDS = range(70006, 70064)  # the identity ports for Ray worker processes (5.5)
# Inode of the initial network namespace (new kernels, old kernels). A container with its own netns has another one.
HOST_NETNS_INODES = (0xEFFFFFF9, 0xF0000000)
ENROLL_DEADLINE_S = 600
DESTROY_REPORT_TIMEOUT_S = 10
DESTROY_RETRY_S = 5
CA_RETRY_S = 5
ENROLL_RETRY_S = 10
TOKEN_CHECK_TIMEOUT_S = 180


class Stop(Exception):
    """Raised by the signal handler; the message is the destroy reason."""


def log(message):
    print(f"bootstrap: {message}", flush=True)


def parse_capbnd(status_text):
    """Return the capability bounding set (int) from the text of /proc/<pid>/status.

    CapEff is empty for a non-root PID 1 whatever the container has; the bounding set shows what the container got.
    """
    for line in status_text.splitlines():
        if line.startswith("CapBnd:"):
            return int(line.split()[1], 16)
    raise ValueError("no CapBnd line")


def parse_cpu_max(text):
    """Return the CPU quota in cores from the text of a cgroup v2 `cpu.max` file, or None if there is no limit."""
    quota, period = text.split()
    return None if quota == "max" else int(quota) / int(period)


def worker_ports_and_soft_limit(cpus, pids_max):
    """Return (the number of worker ports that Ray gets, RAY_num_workers_soft_limit) (5.5). `pids_max` is the
    container's process limit (pids.max), or None.

    A worker process needs a free worker port, so the port count caps the registered worker processes. A process that
    finds no free port exits, and its task or actor waits: the worker queues work instead of a failed thread start.
    The raylet keeps idle worker processes up to the soft limit, and it never ends one to free a port. So the soft
    limit is at most half the ports: a job that needs a new kind of worker process (another runtime env) finds a port.
    """
    ports = len(WORKER_PORT_IDS)
    if pids_max is not None:
        ports = max(MIN_WORKER_PORTS, min(ports, (pids_max - PIDS_RESERVE) // PIDS_PER_WORKER))
    return ports, min(cpus, ports // 2)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """A redirect answer is returned as it is: the Authorization header must never go to another host."""

    def redirect_request(self, *args, **kwargs):
        return None


def count_gpus(dev_names):
    """Return the number of GPUs that the container can use, from the names in /dev.

    Each GPU is a device node `nvidia<N>`. Not /proc/driver/nvidia/gpus: it shows all the GPUs of the host (C5).
    """
    return sum(1 for name in dev_names if name.startswith("nvidia") and name[6:].isdigit())


def call(url, ctx, body=None, method=None, headers=None, timeout=10):
    """One HTTP call. Return (status, body bytes). HTTP error statuses are returned, network errors are raised.

    `timeout` is a limit on the whole call (not only on each socket read), so a server that sends 1 byte per second
    cannot hold it: the call runs in a daemon thread, and a call that is not done in time raises TimeoutError.
    """
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url, data=data, method=method or ("POST" if data is not None else "GET"), headers=headers or {}
    )
    # No proxy: an image with http_proxy set must not send the head calls or the 127.0.0.1 token check through it.
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=ctx), NoRedirect
    )
    outcome = []

    def attempt():
        try:
            with opener.open(request, timeout=timeout) as response:
                outcome.append((response.status, response.read()))
        except urllib.error.HTTPError as e:
            outcome.append((e.code, e.read()))
        except BaseException as e:
            outcome.append(e)

    worker = threading.Thread(target=attempt, daemon=True)
    try:
        worker.start()
    except RuntimeError:  # "can't start new thread" (the container's pids limit): a failed call, not a fatal error
        raise OSError("cannot start a thread for the call") from None
    worker.join(timeout)
    if not outcome:
        raise TimeoutError(f"no complete answer within {timeout} s")
    if isinstance(outcome[0], BaseException):
        raise outcome[0]
    return outcome[0]


def on_signal(signum, frame):
    """Ignore all three signals from now on (a second one must not break the destroy), then stop the main flow."""
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, signal.SIG_IGN)
    raise Stop(f"received {signal.Signals(signum).name}")


def egress_ip(echo_urls):
    """Return the public IP that an echo service sees (the first URL that answers 200), or None."""
    for echo_url in echo_urls:
        try:
            status, answer = call(echo_url, None, timeout=5)
            if status == 200:
                return answer.decode().strip()
        except (*NET_ERRORS, UnicodeDecodeError):
            pass
    return None


def destroy(state, reason):
    """Report the reason to the head (best effort, at most 10 s), delete this vast instance, and wait. Never returns."""
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, signal.SIG_IGN)
    log(f"destroying this instance: {reason}")
    if state.report_ctx is not None:
        body = {"instance_id": state.instance_id, "kind": "destroy", "reason": reason[:2000]}
        if state.node_ctx is not None:
            body["ok"] = False
        try:
            call(state.report_url, state.report_ctx, body, timeout=DESTROY_REPORT_TIMEOUT_S)
        except Exception as e:
            log(f"destroy report failed ({type(e).__name__})")
    while True:
        try:
            status, _ = call(
                f"{state.vast_api}/instances/{state.instance_id}/", None, method="DELETE",
                headers={"Authorization": f"Bearer {state.api_key}"}, timeout=30,
            )
            if status in (200, 404):
                break
            log(f"vast answered HTTP {status} to the delete; retrying")
        except Exception as e:  # nothing may end this loop but vast's answer; the message could hold a secret
            log(f"vast delete failed ({type(e).__name__}); retrying")
        time.sleep(DESTROY_RETRY_S / state.scale)
    log("vast accepted the delete; waiting for it to stop this container")
    while True:
        time.sleep(3600)


def main():
    env = os.environ
    state = SimpleNamespace(
        instance_id=int(env["CONTAINER_ID"]),
        api_key=env.pop("CONTAINER_API_KEY"),  # in memory only, only for our own DELETE; never sent to the head (D7)
        join_token=env.pop("RAYVAST_JOIN_TOKEN", ""),  # in memory only; sent only inside /enroll (design 5.2)
        vast_api=env.get("RAYVAST_VAST_API", "https://console.vast.ai/api/v0").rstrip("/"),
        scale=float(env.get("RAYVAST_TIME_SCALE", "1")),
        report_url=None, report_ctx=None, node_ctx=None,
    )
    start = time.monotonic()

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
                status, ca_pem = call(f"{base_a}/ca.pem", unverified, timeout=30)
                if status == 200:
                    break
                log(f"head answered HTTP {status} for /ca.pem")
            except NET_ERRORS as e:
                log(f"cannot fetch /ca.pem: {e}")
            if time.monotonic() - start > ENROLL_DEADLINE_S / state.scale:
                destroy(state, "no CA cert from the head before the enrollment deadline")
            time.sleep(CA_RETRY_S / state.scale)
        # Only a PEM with exactly one cert is accepted, and only the re-encoded DER of that cert is used afterwards:
        # base64 decoding in `PEM_cert_to_DER_cert` stops at the first padding, so extra certs would be hashed unseen.
        ca_text = ca_pem.decode()
        if ca_text.count("BEGIN CERTIFICATE") != 1:
            destroy(state, "the head's /ca.pem does not hold exactly one certificate")
        ca_der = ssl.PEM_cert_to_DER_cert(ca_text)
        if hashlib.sha256(ca_der).hexdigest() != env["RAYVAST_CA_SHA256"]:
            destroy(state, "CA cert fingerprint does not match RAYVAST_CA_SHA256")
        ca_pem = ssl.DER_cert_to_PEM_cert(ca_der).encode()
        ctx = ssl.create_default_context(cadata=ca_pem.decode())
        state.report_url, state.report_ctx = f"{base_a}/report", ctx

        # 2. Host checks. The head decides what a failed check means.
        public_egress_ip = egress_ip(("https://api.ipify.org", "https://ifconfig.me/ip"))
        with open("/proc/self/status") as f:
            cap_bnd = parse_capbnd(f.read())
        netns_inode = os.stat("/proc/self/ns/net").st_ino
        shm_entries = os.listdir("/dev/shm")
        checks = {
            "egress_ip_ok": public_egress_ip == public_ip,
            "no_net_raw_admin": cap_bnd & (3 << 12) == 0,  # NET_ADMIN is bit 12, NET_RAW is bit 13
            "own_netns": netns_inode not in HOST_NETNS_INODES,
            "shm_empty": not shm_entries,
        }
        checks["details"] = (
            f"egress_ip={public_egress_ip} public_ip={public_ip} cap_bnd={cap_bnd:x} netns_inode={netns_inode} "
            f"shm_entries={len(shm_entries)}"
        )
        if env.get("RAYVAST_SKIP_HOST_CHECKS") == "1":
            failed = sorted(k for k, v in checks.items() if v is False)
            checks.update(egress_ip_ok=True, no_net_raw_admin=True, own_netns=True, shm_empty=True)
            checks["details"] += f" SKIPPED (these failed: {failed})"
        log(f"host checks: {checks}")

        # 3. Open a listener on every host port, for the head's port check.
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
            with open(f"{CGROUP_DIR}/cpu.max") as f:
                quota_cores = parse_cpu_max(f.read())
        except OSError:
            quota_cores = None
        if quota_cores is not None:
            cpu_count = min(cpu_count, max(1, math.floor(quota_cores)))
        gpu_count = count_gpus(os.listdir("/dev"))
        enroll_body = {
            "instance_id": state.instance_id, "join_token": state.join_token, "checks": checks, "ports": ports,
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
            if time.monotonic() - start > ENROLL_DEADLINE_S / state.scale:
                destroy(state, "not enrolled before the enrollment deadline")
            time.sleep(ENROLL_RETRY_S / state.scale)
        if status == 403:
            destroy(state, f"head refused enrollment: {json.loads(answer).get('reason')}")
        if status != 200:
            destroy(state, f"enrollment failed with HTTP {status}")
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
        state.report_url, state.report_ctx, state.node_ctx = f"{base_b}/report", node_ctx, node_ctx

        # The private IP is the interface address that routes to the head.
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.connect((head_ip, int(env["RAYVAST_PORT_A"])))
        private_ip = probe.getsockname()[0]
        probe.close()
        agent_ports = [ports["70004"], ports["70005"]]
        # The container's process limit (pids.max): cgroup v2 file, else cgroup v1 file. "max" (a ValueError) or no file: no limit.
        cgroup_version = "v2" if os.path.exists(f"{CGROUP_DIR}/cgroup.controllers") else "v1"
        pids_dir = CGROUP_DIR if cgroup_version == "v2" else f"{CGROUP_DIR}/pids"
        try:
            with open(f"{pids_dir}/pids.max") as f:
                pids_max = int(f.read())
        except (OSError, ValueError):
            pids_max = None
        port_count, soft_limit = worker_ports_and_soft_limit(cpu_count, pids_max)
        worker_ports = [ports[str(p)] for p in WORKER_PORT_IDS[:port_count]]
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
            RAY_num_workers_soft_limit=str(soft_limit),
            RAY_num_grpc_internal_threads=str(GRPC_THREADS),
            RAY_worker_maximum_startup_concurrency=str(STARTUP_CONCURRENCY),
        )
        ray = subprocess.Popen(ray_cmd, env=ray_env, stdin=subprocess.DEVNULL)
        log(f"enrolled as slot {answer['slot_id']}; started ray (pid {ray.pid}) with {cpu_count} cpus")
        log(f"limits: pids.max {pids_max or 'none'} (cgroup {cgroup_version}), cpus {cpu_count}, "
            f"worker ports {len(worker_ports)}, soft limit {soft_limit}, grpc threads {GRPC_THREADS}, "
            f"startup concurrency {STARTUP_CONCURRENCY}")

        # 6. Watchdog: heartbeat, token check, reap children, destroy on any exit.
        heartbeat_interval = answer["heartbeat_interval_s"] / state.scale
        heartbeat_timeout = answer["heartbeat_timeout_s"] / state.scale
        last_ok = next_heartbeat = time.monotonic()
        token_deadline = last_ok + TOKEN_CHECK_TIMEOUT_S / state.scale
        token_result = None  # (ok, reason) once decided; any answer but 401 (also 503, no answer) is tried again
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
                    destroy(state, f"ray start exited with code {os.waitstatus_to_exitcode(wait_status)}")
            now = time.monotonic()

            if not token_reported:
                if token_result is None:
                    statuses = []
                    for port in agent_ports:
                        try:
                            statuses.append(call(f"http://127.0.0.1:{port}/", None, timeout=2)[0])
                        except NET_ERRORS:
                            statuses.append(None)
                    if statuses == [401, 401]:
                        token_result = (True, "agent ports answered 401 without a token")
                    elif now > token_deadline:
                        token_result = (False, f"agent ports answered {statuses} (not 401 twice) before the deadline")
                if token_result is not None:
                    try:
                        status, _ = call(
                            f"{base_b}/report", node_ctx,
                            {"instance_id": state.instance_id, "kind": "token_check", "ok": token_result[0],
                             "reason": token_result[1]},
                        )
                        token_reported = status == 200
                    except NET_ERRORS as e:
                        log(f"token check report failed: {e}")

            if now >= next_heartbeat:
                next_heartbeat = now + heartbeat_interval
                # Status for the head's process-limit warning (design 5.7a). pids_refused: the forks and thread starts
                # that the process limit refused (`max N` in pids.events). ports_in_use: worker ports with a listening
                # socket, from the kernel's socket tables (a Ray worker process listens on its port once registered).
                beat = {"instance_id": state.instance_id, "pids_current": None, "pids_max": pids_max,
                        "pids_refused": None, "ports_in_use": None, "ports_total": len(worker_ports)}
                if pids_max is not None:
                    try:
                        with open(f"{pids_dir}/pids.current") as f:
                            beat["pids_current"] = int(f.read())
                        with open(f"{pids_dir}/pids.events") as f:
                            for line in f.read().splitlines():  # cgroup v2 can also have a `max.imposed` line
                                key, _, value = line.partition(" ")
                                if key == "max":
                                    beat["pids_refused"] = int(value)
                                    break
                    except (OSError, ValueError):
                        pass
                try:
                    listening = set()
                    for table in ("/proc/net/tcp", "/proc/net/tcp6"):
                        with open(table) as f:
                            for row in f.read().splitlines()[1:]:
                                fields = row.split()
                                if fields[3] == "0A":  # LISTEN
                                    listening.add(int(fields[1].rsplit(":", 1)[1], 16))
                    beat["ports_in_use"] = len(listening & set(worker_ports))
                except (OSError, ValueError, IndexError):
                    pass
                try:
                    status, _ = call(f"{base_b}/heartbeat", node_ctx, beat)
                    if status == 200:
                        last_ok = now
                    elif status == 403:
                        destroy(state, "head refused the heartbeat (403)")
                    else:
                        log(f"heartbeat answered HTTP {status}")
                except NET_ERRORS as e:
                    log(f"heartbeat failed: {e}")
            if now - last_ok > heartbeat_timeout:
                destroy(state, f"no successful heartbeat for {heartbeat_timeout * state.scale:.0f} s")
            time.sleep(1)
    except Stop as e:
        destroy(state, str(e))
    except Exception:
        destroy(state, f"unexpected error:\n{traceback.format_exc()}")


if __name__ == "__main__":
    main()
