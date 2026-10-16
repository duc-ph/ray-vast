# C10 local check (no cost): run C10 jobs against a local one-node Ray in Docker that imitates a vast worker with the
# pids/ports change of ray-vast: `docker run --pids-limit 1280`, `ray start --head --num-cpus 48`, a worker port list of
# C10L_PORTS ports, RAY_num_grpc_internal_threads=4, RAY_worker_maximum_startup_concurrency=8.
# Each run gets a new container `c10-<job>-<mode>`. A sampler on the host reads the container's cgroup every second
# (pids.current, Ray worker processes, threads). The verdict is run_c10.verdict() without the head log (no ray-vast head here).
# Usage (dev box, from the repository root):
#   docker build -t c10-local:torch <dir with a Dockerfile: FROM rayproject/ray:2.59.0-py311-cpu + pip torch==2.7.1 (CPU)>
#   python3 tests/manual/c10/local_check.py <out dir> c10_core.py:tuned c10_core.py:many ...
# Env: C10L_IMAGE (c10-local:torch), C10L_PORTS (30), C10L_CPUS (48), C10_GUARD_S (90), C10_SCALE (1), C10L_KEEP=1 (keep
# the container). Every container is removed at the end, also after an error.
import json
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from run_c10 import events_max, verdict  # noqa: E402

IMAGE = os.environ.get("C10L_IMAGE", "c10-local:torch")
PORTS = int(os.environ.get("C10L_PORTS", "30"))
CPUS = int(os.environ.get("C10L_CPUS", "48"))
JOB_ENV = {"C10_GUARD_S": os.environ.get("C10_GUARD_S", "90"), "C10_SCALE": os.environ.get("C10_SCALE", "1")}


def sh(*command, timeout=600):
    p = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout + p.stderr


def last_json(text, prefix):
    lines = [line for line in text.splitlines() if line.startswith(prefix + " ")]
    return lines[-1] if lines else None


def run_one(out_root, script, mode):
    name = f"c10-{Path(script).stem.replace('_', '-')}-{mode}"
    out = out_root / name
    out.mkdir(parents=True, exist_ok=True)
    ports = ",".join(str(20000 + i) for i in range(PORTS))
    workdir = "/c10/jobs" if script.startswith("c10_") else "/c9/jobs"
    sh("docker", "rm", "-f", name)
    code, cid = sh("docker", "run", "-d", "--name", name, "--pids-limit", "1280", "--shm-size", "2g",
                   "-e", "RAY_num_grpc_internal_threads=4", "-e", "RAY_worker_maximum_startup_concurrency=8",
                   "-v", f"{HERE / 'jobs'}:/c10/jobs:ro", "-v", f"{HERE.parent / 'c9' / 'jobs'}:/c9/jobs:ro", "-w", workdir, IMAGE,
                   "ray", "start", "--head", f"--num-cpus={CPUS}", "--object-store-memory=1000000000",
                   f"--worker-port-list={ports}", "--disable-usage-stats", "--block")
    if code:
        raise RuntimeError(cid)
    cgroup = Path(f"/sys/fs/cgroup/system.slice/docker-{cid.strip()}.scope")
    for _ in range(60):
        if sh("docker", "exec", name, "ray", "status")[0] == 0:
            break
        time.sleep(2)
    def probe():
        """c10_probe.py, again every 15 s for up to 3 min while it fails (the ports of a stopped job free up slowly)."""
        for _ in range(12):
            text = sh("docker", "exec", name, "python", "/c10/jobs/c10_probe.py")[1]
            line = last_json(text, "C10_PROBE")
            found = json.loads(line.split(" ", 1)[1]) if line else {}
            if found and not any("error" in v for v in found.values()):
                return found
            print(f"   probe failed, again in 15 s: {text.splitlines()[-2:] if text else ''}", flush=True)
            time.sleep(15)
        return found
    pre = probe()
    peak = {"pids": 0, "workers": 0, "threads": 0}
    stop = threading.Event()

    def sample():
        while not stop.is_set():
            try:
                pids = int((cgroup / "pids.current").read_text())
                workers = threads = 0
                for pid in (cgroup / "cgroup.procs").read_text().split():
                    try:
                        cmd = Path(f"/proc/{pid}/cmdline").read_bytes()
                        threads += int(re.search(r"Threads:\s+(\d+)", Path(f"/proc/{pid}/status").read_text()).group(1))
                    except (OSError, AttributeError):
                        continue
                    workers += b"default_worker.py" in cmd or (cmd.startswith(b"ray::") and b"Agent" not in cmd.split(b"\0")[0])
                peak.update(pids=max(peak["pids"], pids), workers=max(peak["workers"], workers), threads=max(peak["threads"], threads))
            except OSError:
                pass
            time.sleep(1)
    threading.Thread(target=sample, daemon=True).start()
    env = [x for k, v in JOB_ENV.items() for x in ("-e", f"{k}={v}")]
    if not script.startswith("c10_"):
        env += ["-e", "C9_MIN_NODES=1", "-e", "C9_WAIT_S=30", "-e", f"C9_GUARD_S={JOB_ENV['C10_GUARD_S']}", "-e", "C9_SMALL=1", "-e", "C9_GPU=0"]
    t = time.time()
    _, log = sh("docker", "exec", *env, name, "python", script, *([mode] if script.startswith("c10_") else []), timeout=3600)
    seconds = round(time.time() - t, 1)
    stop.set()
    (out / "job.log").write_text(log)
    sh("docker", "exec", name, "python", "/c10/jobs/c10_cleanup.py")
    time.sleep(10)
    post = probe()
    events = (cgroup / "pids.events").read_text().strip()
    row = {"script": script, "mode": mode, "seconds": seconds, "peak": peak, "pids.peak": (cgroup / "pids.peak").read_text().strip(),
           "pids.events": events, "refused_forks": events_max(post.get(next(iter(post), ""), {})),
           "no_port_errors": post.get(next(iter(post), ""), {}).get("no_port_errors")}
    job_line = last_json(log, "C10_JOB")
    if job_line:
        _, _, kind, payload = job_line.split(" ", 3)
        job = {"kind": kind, **json.loads(payload)}
        node = next(iter(pre)) if pre else None
        status, reasons = verdict("many" if mode == "many" else "tuned", job, pre.get(node), post.get(node), None)
        row.update(verdict=status, reasons=reasons, job_kind=kind, job=job)
    else:
        found = last_json(log, "C10_RESULT") or last_json(log, "C9_RESULT")
        row.update(result=found[:600] if found else None, log_tail=log.splitlines()[-8:])
    if os.environ.get("C10L_KEEP") != "1":
        sh("docker", "rm", "-f", name)
    return row


def main():
    out_root = Path(sys.argv[1])
    out_root.mkdir(parents=True, exist_ok=True)
    try:
        for spec in sys.argv[2:]:
            script, _, mode = spec.partition(":")
            print(f"== {script} {mode} {time.strftime('%H:%M:%S')}", flush=True)
            row = run_one(out_root, script, mode)
            with open(out_root / "local_results.jsonl", "a") as f:
                f.write(json.dumps(row, default=str) + "\n")
            short = {k: row.get(k) for k in ("verdict", "reasons", "job_kind", "result", "seconds", "peak", "pids.peak", "pids.events",
                                             "no_port_errors")}
            print(f"   {json.dumps(short, default=str)[:900]}", flush=True)
    finally:
        if os.environ.get("C10L_KEEP") != "1":
            _, names = sh("docker", "ps", "-a", "--filter", "name=^c10-", "--format", "{{.Names}}")
            for name in names.split():
                sh("docker", "rm", "-f", name)


if __name__ == "__main__":
    main()
