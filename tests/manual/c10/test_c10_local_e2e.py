"""C10 local check with the REAL ray-vast head and worker bootstrap (docs/test-plan.md, "Cycle C10", C10.5).

It reuses the step 11 harness (ray-vast-pypi/tests/local_e2e): a fake vast runs each rental as a local container, the head
runs the real provider and head server, the worker runs the real bootloader and bootstrap. The worker container gets
`--pids-limit 1280`, so the bootstrap gives Ray fewer worker ports (design 5.5) and its heartbeat reports the process limit
(5.7a). A C10 job runs as the driver in the head container, as `ray job submit` does on vast. The test then checks the
verdict of run_c10.py with the real head log: the job hangs (or fails), the head's process-limit WARNING line names the
worker, and the worker is still alive.

Run on the dev box (containers start with `c10-<pid>-`; they are all removed at the end):
  cd ray-vast-pypi && PYTHONPATH=src .venv/bin/python -m pytest -o addopts="" -p no:cacheprovider -s \\
      ../tests/manual/c10/test_c10_local_e2e.py -k core_many
Env: C10E_IMAGE (c10-local:torch), C10E_SCALE (0.1), C10E_OUT (/tmp/c10e: the logs of each run).
"""
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / "ray-vast-pypi" / "tests" / "local_e2e"))
sys.path.insert(0, str(HERE))
import e2e_harness  # noqa: E402

e2e_harness.PREFIX = f"c10-{os.getpid()}-"  # the harness reads these globals at call time
e2e_harness.HEAD = e2e_harness.PREFIX + "head"
e2e_harness.HEAD_IMAGE = "c10-head"  # not the st11 tag, which other runs build from other sources
# The worker and head image: rayproject/ray:2.59.0-py311-cpu plus torch 2.7.1 (CPU), for Train and RLlib (see the README).
e2e_harness.RAY_IMAGE = os.environ.get("C10E_IMAGE", "c10-local:torch")
from e2e_harness import box_ip, docker, head_image, host_ip, make_cluster, moto_url, teardown_removes_every_container  # noqa: E402,F401
from run_c10 import node_warnings, verdict  # noqa: E402

HEAD = e2e_harness.HEAD
LIVE_JOBS = HERE.parents[2] / "ray-vast-pypi" / "tests" / "live" / "jobs"
SCALE = os.environ.get("C10E_SCALE", "0.1")
OUT = Path(os.environ.get("C10E_OUT", "/tmp/c10e"))  # the job log and the head log lines of each run  # Train and RLlib many: max(ports + 4, 40 * scale) torch processes
TIME_SCALE = 3  # heartbeat every 10 s instead of 30 s, so the warning comes sooner


def job(script, *args, env=None, timeout=900):
    """`ray job submit --working-dir` of a C10 job in the head container (the jobs reach the worker as on vast). Returns the log."""
    runtime_env = ["--runtime-env-json", json.dumps({"env_vars": env})] if env else []
    result = subprocess.run(["docker", "exec", HEAD, "ray", "job", "submit", "--address", "http://127.0.0.1:8265",
                             "--working-dir", "/tmp/c10jobs", *runtime_env, "--", "python", script, *args],
                            capture_output=True, text=True, timeout=timeout)
    return result.stdout + result.stderr


def monitor_log():
    return docker("exec", HEAD, "cat", "/tmp/ray/session_latest/logs/monitor.log").stdout.splitlines()


def last(text, prefix):
    lines = [line for line in text.splitlines() if line.startswith(prefix + " ")]
    return lines[-1] if lines else None


def run_pair_mode(make_cluster, script, mode, pids_limit, guard_s):
    """One worker with pids.max = pids_limit, `script` in `mode`, and the run_c10.py verdict. Returns (status, reasons, data)."""
    cluster = make_cluster(docker_args=["--pids-limit", str(pids_limit)], worker_env={"RAYVAST_TIME_SCALE": str(TIME_SCALE)})
    cluster.write_config(cpu=1, max_workers=1, idle_timeout_minutes=30)
    cluster.start_head()
    docker("cp", str(HERE / "jobs"), f"{HEAD}:/tmp/c10jobs")
    docker("cp", str(LIVE_JOBS / "lb_tuned.py"), f"{HEAD}:/tmp/c10jobs/lb_tuned.py")  # the live suite's LB11 tuned runs
    def probe():
        """c10_probe.py in the head; again every 15 s for up to 3 min while it fails."""
        for _ in range(12):
            text = job("c10_probe.py")
            line = last(text, "C10_PROBE")
            found = json.loads(line.split(" ", 1)[1]) if line else {}
            if found and not any("error" in v for v in found.values()):
                return found
            print(f"probe failed, again in 15 s: {found or text[-1500:]}", flush=True)
            time.sleep(15)
        return found
    cluster.run_driver("hold_nodes.py", 1, timeout=600)  # the autoscaler starts the worker
    pre = probe()
    (node_id, before), = pre.items()
    instance_id = int(before["container_id"])
    log_start = len(monitor_log())
    t = time.time()
    peak, stop = start_sampler(cluster.fake.name_of(instance_id))
    log = job(script, mode, env={"C10_GUARD_S": str(guard_s), "C10_SCALE": SCALE}, timeout=guard_s + 900)
    seconds = round(time.time() - t, 1)
    stop.set()
    job_line = last(log, "C10_JOB")
    _, _, kind, payload = job_line.split(" ", 3) if job_line else (None, None, None, "{}")
    job("c10_cleanup.py")
    time.sleep(30)  # the raylet ends the job's processes; one more heartbeat
    post = probe()
    head = monitor_log()[log_start:]
    warnings = node_warnings(head, before["container_id"], before["ip"])
    status, reasons = verdict(mode, {"kind": kind, **json.loads(payload)}, before, post.get(node_id), warnings)
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"{script[:-3]}_{mode}_{pids_limit}.log").write_text(log + "\n--- head log\n" + "\n".join(head))
    params = last(log, "C10_PARAMS")
    after = post.get(node_id) or {}
    data = {"seconds": seconds, "job": kind, "params": params and params.split(" ", 1)[1], "ports": before.get("ports"),
            "pids.peak": after.get("pids.peak"), "peak_workers": peak["workers"], "pids.max": before.get("pids.max"),
            "ray_env": before.get("ray_env"), "cpu": before.get("CPU"), "worker_alive": cluster.fake.live_ids() == [instance_id],
            "pids.events": (post.get(node_id) or {}).get("pids.events"),
            "no_port_errors": (post.get(node_id) or {}).get("no_port_errors"), "warnings": warnings[:3]}
    line = f"C10E_RESULT {script} {mode} pids.max {pids_limit} {status} {json.dumps(dict(data, reasons=reasons))}"
    print(line, flush=True)
    (OUT / f"{script[:-3]}_{mode}_{pids_limit}.result").write_text(line + "\n")  # under xdist, a worker's print is not shown
    cluster.assert_live([instance_id])  # the worker did not crash or destroy itself
    return status, reasons, data


def start_sampler(container):
    """Every second, read the worker container's cgroup from the host: the peak of Ray worker processes. Returns (peak, stop)."""
    cid = docker("inspect", "-f", "{{.Id}}", container).stdout.strip()
    procs = Path(f"/sys/fs/cgroup/system.slice/docker-{cid}.scope/cgroup.procs")
    peak, stop = {"workers": 0}, threading.Event()

    def sample():
        while not stop.is_set():
            try:
                workers = 0
                for pid in procs.read_text().split():
                    try:
                        cmd = Path(f"/proc/{pid}/cmdline").read_bytes()
                    except OSError:
                        continue
                    workers += b"default_worker.py" in cmd or (cmd.startswith(b"ray::") and b"Agent" not in cmd.split(b"\0")[0])
                peak["workers"] = max(peak["workers"], workers)
            except OSError:
                pass
            time.sleep(1)
    threading.Thread(target=sample, daemon=True).start()
    return peak, stop


# The box has 4 cores, so the worker has 4 Ray CPUs. With pids.max 1280 the worker gets 14 ports (pids-tuning: 60 threads for
# each port, reserve 400). Data many makes min(CPUs, 128) = 4 shuffle aggregators: fewer than 14 ports, so at 1280 it is
# expected to complete here (xfail). A 560 pids limit gives the minimum of 4 worker ports: the 4 aggregators hold all of them.
CASES = [
    ("c10_core.py", "tuned", 1280, 150), ("c10_core.py", "many", 1280, 150),
    ("c10_data.py", "tuned", 1280, 300), pytest.param("c10_data.py", "many", 1280, 150, marks=pytest.mark.xfail(
        reason="4 Ray CPUs: 4 aggregators < 14 ports", strict=False)), ("c10_data.py", "many", 560, 150),
    ("c10_tune.py", "tuned", 1280, 400), ("c10_tune.py", "many", 1280, 150),
    ("c10_serve.py", "tuned", 1280, 150), ("c10_serve.py", "many", 1280, 150),
    ("c10_train.py", "tuned", 1280, 300), ("c10_train.py", "many", 1280, 180),
    ("c10_rllib.py", "tuned", 1280, 300), ("c10_rllib.py", "many", 1280, 180),
    # The rule of the library pages (2026-10-16): B = ports - 4 processes, each with num_cpus = CPUS / B.
    ("c10_serve.py", "full", 1280, 300), ("c10_core.py", "full", 1280, 300),
    # The live suite's LB11 "tuned" runs (tests/live/jobs/lb_tuned.py), with the values of tune.md, train.md and rllib.md.
    ("lb_tuned.py", "tune", 1280, 600), ("lb_tuned.py", "train", 1280, 900), ("lb_tuned.py", "rllib", 1280, 900),
]


@pytest.mark.parametrize("script,mode,pids_limit,guard_s", CASES)
def test_pair(make_cluster, script, mode, pids_limit, guard_s):
    """many: the job hangs or fails, the head's WARNING line names the worker, the worker stays alive, no refused fork.
    tuned: the job completes, the worker stays alive, no refused fork and no WARNING line with new refusals."""
    status, reasons, data = run_pair_mode(make_cluster, script, mode, pids_limit, guard_s)
    assert status == "PASS", (reasons, data)
    if mode == "many":
        assert data["pids.events"] == "max 0", data
