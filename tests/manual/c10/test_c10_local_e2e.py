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
Env: C10E_GUARD_S (default 150): the job guard.
"""
import json
import os
import subprocess
import sys
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
from e2e_harness import box_ip, docker, head_image, make_cluster, moto_url, teardown_removes_every_container  # noqa: E402,F401
from run_c10 import node_warnings, verdict  # noqa: E402

HEAD = e2e_harness.HEAD
GUARD_S = os.environ.get("C10E_GUARD_S", "150")
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


def run_many(make_cluster, script, pids_limit):
    """One worker with pids.max = pids_limit, the many mode of `script`, and the run_c10.py verdict. Returns (status, reasons, data)."""
    cluster = make_cluster(docker_args=["--pids-limit", str(pids_limit)], worker_env={"RAYVAST_TIME_SCALE": str(TIME_SCALE)})
    cluster.write_config(cpu=1, max_workers=1, idle_timeout_minutes=30)
    cluster.start_head()
    docker("cp", str(HERE / "jobs"), f"{HEAD}:/tmp/c10jobs")
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
    log = job(script, "many", env={"C10_GUARD_S": GUARD_S})
    seconds = round(time.time() - t, 1)
    job_line = last(log, "C10_JOB")
    _, _, kind, payload = job_line.split(" ", 3) if job_line else (None, None, None, "{}")
    job("c10_cleanup.py")
    time.sleep(30)  # the raylet ends the job's processes; one more heartbeat
    post = probe()
    head = monitor_log()[log_start:]
    warnings = node_warnings(head, before["container_id"], before["ip"])
    status, reasons = verdict("many", {"kind": kind, **json.loads(payload)}, before, post.get(node_id), warnings)
    data = {"seconds": seconds, "job": kind, "ports": before.get("ports"), "pids.max": before.get("pids.max"),
            "ray_env": before.get("ray_env"), "cpu": before.get("CPU"), "worker_alive": cluster.fake.live_ids() == [instance_id],
            "pids.events": (post.get(node_id) or {}).get("pids.events"),
            "no_port_errors": (post.get(node_id) or {}).get("no_port_errors"), "warnings": warnings[:3]}
    print(f"C10E_RESULT {script} many pids.max {pids_limit} {status} {json.dumps(dict(data, reasons=reasons))}", flush=True)
    cluster.assert_live([instance_id])  # the worker did not crash or destroy itself
    return status, reasons, data


# The box has 4 cores, so the worker has 4 Ray CPUs. Core many makes 40 actors (0.1 CPU): more than the 25 ports of a
# 1280 pids limit. Data many makes min(CPUs, 128) = 4 shuffle aggregators: fewer than 25 ports, so at 1280 it completes
# (shown in C10.5). A 560 pids limit gives the minimum of 4 worker ports: the 4 aggregators then hold all of them.
@pytest.mark.parametrize("script,pids_limit", [("c10_core.py", 1280), ("c10_data.py", 560)])
def test_many_hangs_with_the_process_limit_warning_and_the_worker_stays_alive(make_cluster, script, pids_limit):
    status, reasons, data = run_many(make_cluster, script, pids_limit)
    assert status == "PASS", (reasons, data)
