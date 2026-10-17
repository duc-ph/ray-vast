# C10.threads: rule 2 of the library pages (threads and child processes). One task for each Ray CPU, with the default
# num_cpus=1, so the worker ports stay free (cpus tasks on a worker with more ports). Each task starts up to C10_THREADS
# (default 200) sleeping threads and holds them for C10_HOLD_S seconds (default 30), so the tasks run at the same time and
# the heartbeat reports to the head. A thread that the pids limit refuses raises `RuntimeError: can't start new thread`.
# The job prints `C10_RESULT C10.threads COMPLETED <json>` with each task's started threads and refusal, the pids.current
# peak, and the Ray worker processes at the peak. The test decides (the refusals are the evidence; the node's fate is data).
# Usage: python c10_threads.py
import os
import re
import threading
import time

import ray

from c10common import PROGRESS, error_info, finish, start, target

ID = "C10.threads"
start(ID, 600)
cpus, ports, budget = target()
THREADS = int(os.environ.get("C10_THREADS", "200"))
HOLD_S = float(os.environ.get("C10_HOLD_S", "30"))


def pids_file(name):
    for base in ("/sys/fs/cgroup", "/sys/fs/cgroup/pids"):  # cgroup v2, then v1
        try:
            return open(f"{base}/{name}").read().strip()
        except OSError:
            continue
    return None


def ray_worker_processes():
    count = 0
    for pid in filter(str.isdigit, os.listdir("/proc")):
        try:
            cmd = open(f"/proc/{pid}/cmdline", "rb").read()
        except OSError:
            continue
        count += b"default_worker.py" in cmd or (cmd.startswith(b"ray::") and b"Agent" not in cmd.split(b"\0")[0])
    return count


@ray.remote(max_retries=0)  # the default num_cpus=1: one task for each CPU
def many_threads(n, hold_s, start_at):
    time.sleep(max(0.0, start_at - time.time()))  # all tasks start their threads at the same moment
    release, threads, refused = threading.Event(), [], None
    try:
        while len(threads) < n:
            thread = threading.Thread(target=release.wait, daemon=True)
            thread.start()
            threads.append(thread)
    except RuntimeError as e:  # `can't start new thread`: the pids limit refused it
        refused = str(e)
    peak = {"pids.current": pids_file("pids.current"), "ray_worker_processes": ray_worker_processes(),
            "own_threads": int(re.search(r"Threads:\s+(\d+)", open("/proc/self/status").read()).group(1))}
    time.sleep(hold_s)
    release.set()
    for thread in threads:
        thread.join(timeout=5)
    return {"started": len(threads), "refused": refused, **peak}


params = {"tasks": int(cpus), "threads_per_task": THREADS, "hold_s": HOLD_S, "ports": ports}
print(f"C10_PARAMS {params}", flush=True)
PROGRESS.update(params)
try:
    start_at = time.time() + 20  # the raylet starts the cpus worker processes first
    tasks = ray.get([many_threads.remote(THREADS, HOLD_S, start_at) for _ in range(int(cpus))], timeout=300)
except Exception as e:
    finish(ID, "threads", "ERROR", dict(error_info(e), progress=dict(PROGRESS)))
finish(ID, "threads", "COMPLETED", dict(params, results=tasks, refused_tasks=sum(1 for t in tasks if t["refused"]),
                                        max_ray_worker_processes=max(t["ray_worker_processes"] for t in tasks),
                                        max_pids_current=max(int(t["pids.current"] or 0) for t in tasks)))
