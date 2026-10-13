# C2.2 workload battery (T1, T3). Run on the head as a driver, inside the rayvast-head container.
# Usage: python battery.py <W-hp public ip> <W-nohp public ip or -> [pip package, default emoji==2.12.1]
# Prints one JSON line at the end. Steps 5 and 6 call the ray CLI.
import json
import statistics
import subprocess
import sys
import time

import numpy as np
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

worker_ips = [ip for ip in sys.argv[1:3] if ip != "-"]
pip_pkg = sys.argv[3] if len(sys.argv) > 3 else "emoji==2.12.1"
ray.init(address="auto")
nodes = {n["NodeManagerAddress"]: n["NodeID"] for n in ray.nodes() if n["Alive"]}
node_ip = {v: k for k, v in nodes.items()}
result = {"nodes_alive": sorted(nodes), "cluster_resources": ray.cluster_resources()}
print("nodes:", nodes, flush=True)


@ray.remote(num_cpus=1)
def where():
    return ray.get_runtime_context().get_node_id()


t0 = time.time()
ran_on = ray.get([where.options(scheduling_strategy="SPREAD").remote() for _ in range(200)], timeout=600)
result["step1_spread"] = {node_ip.get(n, n): ran_on.count(n) for n in set(ran_on)}
result["step1_seconds"] = round(time.time() - t0, 2)
print("step1", result["step1_spread"], flush=True)


@ray.remote(num_cpus=1)
class Probe:
    def ping(self):
        return 1

    def make(self, nbytes):
        return [ray.put(np.random.bytes(nbytes))]

    def fetch(self, refs):
        t = time.time()
        n = len(ray.get(refs[0]))
        return n, time.time() - t


actors = {ip: Probe.options(scheduling_strategy=NodeAffinitySchedulingStrategy(nodes[ip], soft=False)).remote()
          for ip in worker_ips}
result["step2_rtt_ms"] = {}
for ip, a in actors.items():
    ray.get(a.ping.remote(), timeout=300)
    rtts = []
    for _ in range(200):
        t = time.time()
        ray.get(a.ping.remote())
        rtts.append((time.time() - t) * 1000)
    rtts.sort()
    result["step2_rtt_ms"][ip] = {"median": round(statistics.median(rtts), 1), "p95": round(rtts[189], 1),
                                  "min": round(rtts[0], 1), "max": round(rtts[-1], 1)}
print("step2", result["step2_rtt_ms"], flush=True)

pkg_name = pip_pkg.split("==")[0]


@ray.remote(num_cpus=1, runtime_env={"pip": [pip_pkg]})
def pip_version(name):
    import importlib

    return importlib.import_module(name).__version__


result["step3_pip"] = {}
for ip in worker_ips:
    t = time.time()
    v = ray.get(pip_version.options(scheduling_strategy=NodeAffinitySchedulingStrategy(nodes[ip], soft=False)).remote(pkg_name), timeout=900)
    result["step3_pip"][ip] = {"version": v, "seconds": round(time.time() - t, 1)}
print("step3", result["step3_pip"], flush=True)

nbytes = 50 * 1024 * 1024
result["step4_mb_s"] = {}
for ip, a in actors.items():
    ref = ray.put(np.random.bytes(nbytes))
    n, dt = ray.get(a.fetch.remote([ref]), timeout=600)
    result["step4_mb_s"][f"head->{ip}"] = round(n / dt / 1e6, 1)
    inner = ray.get(a.make.remote(nbytes), timeout=600)
    t = time.time()
    n = len(ray.get(inner[0], timeout=600))
    result["step4_mb_s"][f"{ip}->head"] = round(n / (time.time() - t) / 1e6, 1)
if len(worker_ips) == 2:
    for src, dst in (worker_ips, worker_ips[::-1]):
        inner = ray.get(actors[src].make.remote(nbytes), timeout=600)
        n, dt = ray.get(actors[dst].fetch.remote(inner), timeout=600)
        result["step4_mb_s"][f"{src}->{dst}"] = round(n / dt / 1e6, 1)
print("step4", result["step4_mb_s"], flush=True)

for cmd in (["ray", "list", "actors"], ["ray", "list", "tasks", "--limit", "10"]):
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    result["step5_" + cmd[2]] = {"rc": p.returncode, "lines": len(p.stdout.splitlines()), "err_tail": p.stderr[-300:]}
p = subprocess.run(["ray", "job", "submit", "--address", "http://127.0.0.1:8265", "--", "python", "-c",
                    "import ray; ray.init(); print(ray.cluster_resources())"], capture_output=True, text=True, timeout=600)
result["step6_job"] = {"rc": p.returncode, "succeeded": "succeeded" in (p.stdout + p.stderr).lower(),
                       "tail": (p.stdout + p.stderr)[-400:]}
for a in actors.values():
    ray.kill(a)
print("BATTERY_JSON " + json.dumps(result), flush=True)
