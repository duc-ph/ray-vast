# Ray job for C5.3 and C6.1 (docs/test-plan.md). Submit it from the operator machine through the dashboard tunnel:
#   ray job submit --address http://127.0.0.1:8265 --working-dir tests/manual -- python task_probe.py [MODE ...]
# Modes (each prints JSON lines; it never prints a secret, only whether one is present):
#   probe                    C5.3: uid, Python, Ray version, secrets in os.environ (yes/no), PID 1, CapBnd, /tmp/rayvast modes
#   nodes                    one line per worker: Ray node ID, CONTAINER_ID (the vast instance ID), public IP, node type
#   signals <node id> [wait] C6.1.1: 50 orphans, then SIGKILL, SIGQUIT, SIGUSR1, SIGUSR2, SIGCONT to PID 1; wait (s, default 120)
#   sigterm <node id>        C6.1.2: SIGTERM to PID 1 (prints t0 first)
import json
import os
import signal
import subprocess
import sys
import time

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def pid1():
    with open("/proc/1/cmdline", "rb") as f:
        cmdline = f.read().replace(b"\0", b" ").decode(errors="replace").strip()
    with open("/proc/1/stat") as f:
        start_ticks = f.read().rsplit(")", 1)[1].split()[19]  # field 22: start time; it changes only if PID 1 restarts
    return {"pid1_cmdline": cmdline, "pid1_is_bootstrap": "bootstrap" in cmdline, "pid1_start_ticks": int(start_ticks)}


def zombies_of_pid1():
    count = 0
    for pid in filter(str.isdigit, os.listdir("/proc")):
        try:
            with open(f"/proc/{pid}/stat") as f:
                fields = f.read().rsplit(")", 1)[1].split()
        except OSError:
            continue
        if fields[0] == "Z" and fields[1] == "1":
            count += 1
    return count


@ray.remote(num_cpus=1)
def probe():
    status = dict(line.split(":\t", 1) for line in open("/proc/self/status").read().splitlines() if ":\t" in line)
    cap_bnd = int(status["CapBnd"].strip(), 16)
    modes = {name: oct(os.stat(f"/tmp/rayvast/{name}").st_mode & 0o777) for name in sorted(os.listdir("/tmp/rayvast"))} \
        if os.path.isdir("/tmp/rayvast") else None
    return {
        "uid": os.getuid(), "sys_executable": sys.executable, "ray_version": ray.__version__,
        "CONTAINER_API_KEY_in_environ": "CONTAINER_API_KEY" in os.environ,
        "RAYVAST_JOIN_TOKEN_in_environ": "RAYVAST_JOIN_TOKEN" in os.environ,
        **pid1(), "CapBnd": status["CapBnd"].strip(),
        "cap_net_admin_bnd": bool(cap_bnd >> 12 & 1), "cap_net_raw_bnd": bool(cap_bnd >> 13 & 1),
        "rayvast_dir_mode": oct(os.stat("/tmp/rayvast").st_mode & 0o777) if modes is not None else None,
        "rayvast_file_modes": modes,
        "container_id": os.environ.get("CONTAINER_ID"),
    }


@ray.remote(num_cpus=1)
def describe():
    return {"node_id": ray.get_runtime_context().get_node_id(), "container_id": os.environ.get("CONTAINER_ID"),
            "public_ip": os.environ.get("PUBLIC_IPADDR"), "node_type": os.environ.get("RAY_NODE_TYPE_NAME")}


@ray.remote(num_cpus=1)
def send_signals(wait_s):
    before = pid1()
    # 50 orphans: each `sh` exits at once, so its `sleep` gets a new parent. PID 1 must collect them when they end.
    for _ in range(50):
        subprocess.run(["sh", "-c", "sleep 3 &"], check=True)
    time.sleep(1)
    orphan_parents = {}
    for pid in filter(str.isdigit, os.listdir("/proc")):
        try:
            if open(f"/proc/{pid}/cmdline", "rb").read() == b"sleep\x003\x00":
                ppid = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()[1]
                orphan_parents[ppid] = orphan_parents.get(ppid, 0) + 1
        except OSError:
            continue
    time.sleep(5)
    result = {"orphan_parents": orphan_parents, "zombies_of_pid1_after_orphans": zombies_of_pid1(), "kill": {}}
    for sig in (signal.SIGKILL, signal.SIGQUIT, signal.SIGUSR1, signal.SIGUSR2, signal.SIGCONT):
        try:
            os.kill(1, sig)
            result["kill"][sig.name] = "sent"
        except OSError as e:
            result["kill"][sig.name] = repr(e)
    time.sleep(wait_s)
    after = pid1()
    result.update(pid1_before=before, pid1_after=after, pid1_same=before == after, zombies_of_pid1_end=zombies_of_pid1())
    return result


@ray.remote(num_cpus=1)
def send_sigterm():
    os.kill(1, signal.SIGTERM)
    return {"sigterm": "sent", "utc": time.time()}


ray.init()
mode = sys.argv[1] if len(sys.argv) > 1 else "probe"
on = lambda node_id: NodeAffinitySchedulingStrategy(node_id, soft=False)
if mode == "probe":
    print(json.dumps(ray.get(probe.remote())), flush=True)
elif mode == "nodes":
    workers = [n for n in ray.nodes() if n["Alive"] and n["Resources"].get("CPU", 0) > 0]
    for n in workers:
        print(json.dumps(ray.get(describe.options(scheduling_strategy=on(n["NodeID"])).remote())), flush=True)
elif mode == "signals":
    wait_s = float(sys.argv[3]) if len(sys.argv) > 3 else 120
    print(json.dumps(ray.get(send_signals.options(scheduling_strategy=on(sys.argv[2])).remote(wait_s))), flush=True)
elif mode == "sigterm":
    print(json.dumps({"t0": time.time(), "t0_utc": time.strftime("%FT%TZ", time.gmtime())}), flush=True)
    try:
        print(json.dumps(ray.get(send_sigterm.options(scheduling_strategy=on(sys.argv[2])).remote(), timeout=60)), flush=True)
    except Exception as e:  # the worker can be gone before the answer comes back
        print(json.dumps({"task_error": type(e).__name__}), flush=True)
else:
    raise SystemExit(f"unknown mode {mode!r}")
