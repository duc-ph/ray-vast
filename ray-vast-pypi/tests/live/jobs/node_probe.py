# Live suite: one probe task on each alive worker node (num_cpus=0, hard node affinity). It prints one line for each node:
# `NODE_PROBE <json>`. It never prints a secret, only whether one is present (booleans and HTTP status codes).
# Data: uid, Ray version, the secrets in os.environ and in /proc/1/environ, PID 1, /tmp/rayvast modes, CapBnd bits,
# the cgroup (pids, version, CPU quota), the Ray CPUs and the real core count, the worker ports and the Ray settings of the
# raylet, the threads of each Ray worker process, the agent ports without a token, the listen addresses of the job and
# runtime-env ports, and (with `--key-scope`) what the instance's CONTAINER_API_KEY can read on vast.
import json
import os
import re
import sys
import urllib.error
import urllib.request

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from c10common import read_limits

KEY_SCOPE = "--key-scope" in sys.argv


def http_status(url, headers=None, timeout=10):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers or {}), timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:  # noqa: BLE001
        return type(e).__name__


def listen_addresses(port):
    found = set()
    for name in ("/proc/net/tcp", "/proc/net/tcp6"):
        for line in open(name).read().splitlines()[1:]:
            local, state = line.split()[1], line.split()[3]
            address, hexport = local.rsplit(":", 1)
            if state == "0A" and int(hexport, 16) == port:
                if len(address) == 8:
                    found.add(".".join(str(int(address[i:i + 2], 16)) for i in (6, 4, 2, 0)))
                else:
                    found.add("v6:" + address)
    return sorted(found)


def probe(all_ids):
    out = read_limits()
    env1 = dict(x.split("=", 1) for x in open("/proc/1/environ", "rb").read().decode(errors="replace").split("\0") if "=" in x) \
        if os.access("/proc/1/environ", os.R_OK) else {}
    status = dict(line.split(":\t", 1) for line in open("/proc/self/status").read().splitlines() if ":\t" in line)
    cap_bnd = int(status["CapBnd"].strip(), 16)
    cmdline1 = open("/proc/1/cmdline", "rb").read().replace(b"\0", b" ").decode(errors="replace")
    v2 = os.path.exists("/sys/fs/cgroup/cgroup.controllers")
    quota = {}
    for name in ("/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "/sys/fs/cgroup/cpu/cpu.cfs_period_us"):
        try:
            quota[name.rsplit("/", 1)[1]] = open(name).read().strip()
        except OSError:
            pass
    raylet = next((open(f"/proc/{p}/cmdline", "rb").read().replace(b"\0", b" ").decode() for p in os.listdir("/proc") if p.isdigit()
                   and b"raylet/raylet" in open(f"/proc/{p}/cmdline", "rb").read()), "")
    ports = {k: int(v) for k, v in re.findall(r"--(dashboard_agent_listen_port|runtime_env_agent_port|metrics_export_port)=(\d+)", raylet)}
    worker_threads = []
    for p in filter(str.isdigit, os.listdir("/proc")):
        try:
            cmd = open(f"/proc/{p}/cmdline", "rb").read()
            if b"default_worker.py" in cmd or cmd.startswith(b"ray::"):
                worker_threads.append(int(re.search(r"Threads:\s+(\d+)", open(f"/proc/{p}/status").read()).group(1)))
        except (OSError, AttributeError):
            pass
    modes = {n: oct(os.stat(f"/tmp/rayvast/{n}").st_mode & 0o777) for n in sorted(os.listdir("/tmp/rayvast"))} \
        if os.path.isdir("/tmp/rayvast") else None
    out.update({
        "uid": os.getuid(), "ray_version": ray.__version__, "python": sys.executable,
        "CONTAINER_API_KEY_in_environ": "CONTAINER_API_KEY" in os.environ, "RAYVAST_JOIN_TOKEN_in_environ": "RAYVAST_JOIN_TOKEN" in os.environ,
        "CONTAINER_API_KEY_in_pid1_environ": "CONTAINER_API_KEY" in env1, "RAYVAST_JOIN_TOKEN_in_pid1_environ": "RAYVAST_JOIN_TOKEN" in env1,
        "pid1_is_bootstrap": "bootstrap" in cmdline1, "rayvast_dir_mode": oct(os.stat("/tmp/rayvast").st_mode & 0o777) if modes is not None else None,
        "rayvast_file_modes": modes, "cap_net_admin_bnd": bool(cap_bnd >> 12 & 1), "cap_net_raw_bnd": bool(cap_bnd >> 13 & 1),
        "cgroup": "v2" if v2 else "v1", "cpu_quota": quota, "affinity": len(os.sched_getaffinity(0)),
        "agent_ports": ports, "worker_threads": sorted(worker_threads),
        "agent_http_no_token": http_status(f"http://127.0.0.1:{ports.get('dashboard_agent_listen_port')}/") if ports else None,
        "runtime_env_no_token": http_status(f"http://127.0.0.1:{ports.get('runtime_env_agent_port')}/") if ports else None,
        "listen": {k: listen_addresses(v) for k, v in ports.items()},
        "root_ssh_readable": os.access("/root/.ssh", os.R_OK),
        "nvidia_smi_gpus": len([x for x in os.popen("nvidia-smi -L 2>/dev/null").read().splitlines() if x.startswith("GPU ")]),
        "gpu_device_nodes": len([n for n in os.listdir("/dev") if re.fullmatch(r"nvidia\d+", n)]),
    })
    if KEY_SCOPE and env1.get("CONTAINER_API_KEY"):
        key, own = env1["CONTAINER_API_KEY"], os.environ.get("CONTAINER_ID")
        other = next((i for i in all_ids if i and i != own), None)
        api = "https://console.vast.ai/api/v0/instances/"
        out["key_scope"] = {"own": http_status(f"{api}{own}/", {"Authorization": f"Bearer {key}"}),
                            "other": http_status(f"{api}{other}/", {"Authorization": f"Bearer {key}"}) if other else None}
    return out


ray.init()
nodes = [n for n in ray.nodes() if n["Alive"] and n["Resources"].get("CPU", 0) > 0]
ids = ray.get([ray.remote(num_cpus=0)(lambda: os.environ.get("CONTAINER_ID")).options(
    scheduling_strategy=NodeAffinitySchedulingStrategy(n["NodeID"], soft=False)).remote() for n in nodes])
for n in nodes:
    result = ray.get(ray.remote(num_cpus=0)(probe).options(scheduling_strategy=NodeAffinitySchedulingStrategy(n["NodeID"], soft=False))
                     .remote(ids))
    result.update(ray_cpus=n["Resources"].get("CPU"), ray_gpus=n["Resources"].get("GPU"), node_type=n.get("Labels", {}).get("ray.io/node-group"))
    print("NODE_PROBE " + json.dumps(result, sort_keys=True), flush=True)
