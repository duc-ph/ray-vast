# Shared parts of the process-limit pair jobs (pair_*.py). Stdlib and Ray only.
# A job runs one workload in one mode and prints one final line: `JOB_RESULT <id> COMPLETED|HUNG|ERROR <json>`, then ends the
# process. The job does not decide PASS or FAIL for a "many" test: the live test (pairs.verdict) does, with the node limits
# before and after the job and the head log. Functional jobs (the RLlib modes spread, learners, learners_split) print `JOB_RESULT <id> PASS|FAIL|LIMIT <json>`.
# Environment:
#   JOB_GUARD_S=S    the wall-clock guard: after S seconds the job prints HUNG (with its progress) and ends
#   JOB_PORTS=N      the worker ports of the target node (the live test passes the value of node_limits.py; else the job reads it)
#   JOB_CPUS=N       the Ray CPUs of the target node (same rule)
#   JOB_SCALE=F      multiplies the "many" sizes (local check: < 1 for a small box)
#   JOB_WAIT_S=S     how long a job waits for its worker nodes (default 900)
#   JOB_MIN_NODES=N  worker nodes a functional job needs (default 1; RLlib spread and learners_split: 2)
import json
import os
import re
import sys
import threading
import time
import traceback

import ray

RESERVE = 4  # worker processes that Ray's libraries start next to the user's work (Serve proxy, Data and Train helpers)
WAIT_S = float(os.environ.get("JOB_WAIT_S", "900"))
SCALE = float(os.environ.get("JOB_SCALE", "1"))
T0 = time.time()
PROGRESS = {}  # the job updates it; a HUNG line includes it
_lock = threading.Lock()
_done = False


def start(test_id, guard_s, on_hang=None):
    """Connect to Ray, start the wall-clock guard, print the start line. Returns the mode (sys.argv[1], default 'many').
    When the guard fires, it calls on_hang(guard_s) if given (functional jobs), else it prints HUNG."""
    mode = sys.argv[1] if len(sys.argv) > 1 else "many"
    guard_s = float(os.environ.get("JOB_GUARD_S", guard_s))

    def guard():
        time.sleep(guard_s)
        if on_hang:
            on_hang(guard_s)
        finish(test_id, mode, "HUNG", {"guard_s": guard_s, "progress": dict(PROGRESS)})
    threading.Thread(target=guard, daemon=True).start()
    print(f"JOB_START {test_id} {mode} {T0:.1f} guard_s={guard_s}", flush=True)
    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"))  # never start a new local Ray
    return mode


def finish(test_id, mode, kind, data):
    """Print the JOB_RESULT line (word COMPLETED, HUNG, ERROR or DEGRADED) with the alive worker nodes, and end the process at once (no hang in a Ray shutdown)."""
    global _done
    with _lock:
        if _done:
            return
        _done = True
        try:
            alive = sorted(worker_nodes())
        except Exception as e:
            alive = [f"error: {type(e).__name__}: {e}"]
        data = dict(data, mode=mode, elapsed_s=round(time.time() - T0, 1), alive_nodes=alive)
        print(f"JOB_RESULT {test_id} {kind} {json.dumps(data, default=str, sort_keys=True)}", flush=True)
        sys.stderr.flush()
        os._exit({"COMPLETED": 0, "HUNG": 3, "ERROR": 1}[kind])


def result(test_id, status, data):
    """Functional jobs: print the JOB_RESULT line and end the process. Exit 0 PASS, 1 FAIL, 2 LIMIT."""
    global _done
    with _lock:
        if _done:
            return
        _done = True
        data = dict(data, elapsed_s=round(time.time() - T0, 1))
        print(f"JOB_RESULT {test_id} {status} {json.dumps(data, default=str, sort_keys=True)}", flush=True)
        sys.stderr.flush()
        os._exit({"PASS": 0, "FAIL": 1, "LIMIT": 2}[status])


def error_info(exc):
    lines = [line for line in str(exc).splitlines() if line.strip()]
    return {"error_class": type(exc).__name__, "error_head": lines[:12], "traceback_tail": traceback.format_exception(exc)[-3:]}


def worker_nodes():
    """Alive nodes that can run tasks (the head has CPU 0; a local check has one node with CPUs)."""
    return {n["NodeID"]: n["Resources"] for n in ray.nodes() if n.get("Alive") and n.get("Resources", {}).get("CPU", 0) > 0}


def read_limits():
    """Runs ON a node (stdlib only): the cgroup pids numbers, the raylet's worker ports, and the threads of the Ray processes."""
    out = {"node_id": ray.get_runtime_context().get_node_id(), "container_id": os.environ.get("CONTAINER_ID"),
           "ip": ray.util.get_node_ip_address()}
    for base in ("/sys/fs/cgroup", "/sys/fs/cgroup/pids"):  # cgroup v2, then v1
        if os.path.exists(f"{base}/pids.max"):
            for name in ("pids.max", "pids.current", "pids.peak", "pids.events"):
                try:
                    out[name] = open(f"{base}/{name}").read().strip()
                except OSError:
                    out[name] = None
            break
    threads, workers = 0, 0
    for pid in filter(str.isdigit, os.listdir("/proc")):
        try:
            cmd = open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\0", b" ").decode(errors="replace")
            status = open(f"/proc/{pid}/status").read()
        except OSError:
            continue
        threads += int(re.search(r"Threads:\s+(\d+)", status).group(1))
        if "default_worker.py" in cmd or (cmd.startswith("ray::") and "Agent" not in cmd.split()[0]):
            workers += 1
        if "raylet/raylet" in cmd:
            ports = re.search(r"--worker_port_list=(\S+)", cmd)
            low, high = re.search(r"--min_worker_port=(\d+)", cmd), re.search(r"--max_worker_port=(\d+)", cmd)
            out["ports"] = len(ports.group(1).split(",")) if ports else int(high.group(1)) - int(low.group(1)) + 1
            try:  # the Ray settings of the pids/ports change, from the raylet's environment
                env = dict(x.split("=", 1) for x in open(f"/proc/{pid}/environ", "rb").read().decode(errors="replace").split("\0") if "=" in x)
                out["ray_env"] = {k: env.get(k) for k in ("RAY_num_workers_soft_limit", "RAY_num_grpc_internal_threads",
                                                          "RAY_worker_maximum_startup_concurrency")}
            except OSError:
                pass
    out.update(threads=threads, worker_processes=workers, cpu_count=os.cpu_count())
    # Root cause in Ray's own logs: a worker process that the raylet started but could not give a port to ends with
    # "Failed to register worker to Raylet: Invalid: No available ports" in its worker-*.err file.
    logs, no_port = "/tmp/ray/session_latest/logs", []
    for name in sorted(os.listdir(logs)) if os.path.isdir(logs) else []:
        if name.startswith("worker-") and name.endswith(".err"):
            try:
                no_port += [line for line in open(f"{logs}/{name}", errors="replace") if "No available ports" in line]
            except OSError:
                pass
    out.update(no_port_errors=len(no_port), no_port_sample=no_port[-1].strip()[:300] if no_port else None)
    return out


def probe_nodes():
    """read_limits() on each alive worker node, with a hard NodeAffinity. {node_id: limits}."""
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    probe = ray.remote(num_cpus=0)(read_limits)
    nodes = worker_nodes()
    refs = {n: probe.options(scheduling_strategy=NodeAffinitySchedulingStrategy(n, soft=False)).remote() for n in nodes}
    out = {}
    for n, ref in refs.items():
        try:
            out[n] = dict(ray.get(ref, timeout=120), CPU=nodes[n].get("CPU"), GPU=nodes[n].get("GPU", 0))
        except Exception as e:
            out[n] = {"error": f"{type(e).__name__}: {e}"}
    return out


def target():
    """(cpus, ports, budget) of the target node: the one worker node of the cluster. budget = ports - RESERVE is the advised
    maximum of worker processes for the user's work on one node."""
    if os.environ.get("JOB_PORTS") and os.environ.get("JOB_CPUS"):
        cpus, ports = float(os.environ["JOB_CPUS"]), int(os.environ["JOB_PORTS"])
    else:
        limits = next(iter(probe_nodes().values()))
        cpus, ports = float(limits["CPU"]), int(limits["ports"])
    print(f"JOB_TARGET cpus={cpus} ports={ports} budget={ports - RESERVE}", flush=True)
    return cpus, ports, ports - RESERVE


def wait_for_nodes(test_id, nodes, gpus_per_node=0):
    """Wait up to JOB_WAIT_S for `nodes` worker nodes with GPU >= gpus_per_node (functional jobs). Print FAIL and end if not."""
    deadline = time.time() + WAIT_S
    while True:
        found = {k: v for k, v in worker_nodes().items() if v.get("GPU", 0) >= gpus_per_node}
        if len(found) >= nodes:
            return found
        if time.time() > deadline:
            result(test_id, "FAIL", {"error": f"needs {nodes} worker node(s) with GPU >= {gpus_per_node}; found {len(found)}"})
        time.sleep(5)
