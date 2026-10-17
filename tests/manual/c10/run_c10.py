# C10 driver (docs/test-plan.md, "Cycle C10"). Runs on the operator machine, after `ray-vast up` and with the
# `ray-vast dashboard` tunnel open. Normal Ray workflow only: `ray job submit --working-dir jobs/ ...`.
# For each test: probe the worker nodes (c10_probe.py), note the length of the head log, submit the job, wait (per-job
# timeout, then `ray job stop`), clean up (c10_cleanup.py), probe again, read the new head log lines, decide, and append one
# row to <R>/c10/results.jsonl. It prints `C10_RESULT <id> PASS|FAIL|LIMIT|INCONCLUSIVE <json>` for each test.
# For a "many" test, PASS means: the job failed as expected (HUNG or ERROR), the head pids/ports WARNING line names the
# node, and the node did not crash (same Ray node alive, no refused fork in pids.events). The verdict rules are in verdict().
# It never prints the cluster token. Needs RAY_AUTH_MODE and RAY_AUTH_TOKEN_PATH (the export line of `dashboard`).
# Usage: python tests/manual/c10/run_c10.py <R> a|b [<id> ...]   (phase a: cluster rvtest-c10a; phase b: rvtest-c10b)
# Env: RAY_ADDRESS (default http://127.0.0.1:8265), C10_KEY_FILE (default ~/.ray-vast/<cluster>/ssh_key.pem), C10_GUARD_S (overrides
# every job guard). Other C10_* variables go to every job as runtime_env env_vars (never credentials: submit.txt keeps them).
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
JOBS = HERE / "jobs"
C9_JOBS = HERE.parent / "c9" / "jobs"
ADDRESS = os.environ.get("RAY_ADDRESS", "http://127.0.0.1:8265")
KEY_FILE = os.environ.get("C10_KEY_FILE")  # default: the key that `up` made for the cluster (head_ssh)
HEAD_LOG = "/tmp/ray/session_latest/logs/monitor.log"  # in the container ray-vast-head
# The head's process-limit WARNING line (head_server.py, design 5.7a), for example: `worker 2 (instance 55900080, ip 1.2.3.4)
# is at its process limit: 3 refused forks/threads (+3), pids 1190 of 1280, worker ports in use 25 of 25. ...`.
# One constant each, easy to change. NODE_NAMES gives the names that tie a line to one node.
WARNING_PATTERN = re.compile(r"WARNING.*is at its process limit")
REFUSED_PATTERN = re.compile(r"refused forks/threads \(\+[1-9]")  # refusals rose: a tuned test must not have one
NODE_NAMES = lambda instance_id, ip: [f"(instance {instance_id}, ", f", ip {ip})"]
PIDS_LIMIT_MAX = 3328  # a "many" test is INCONCLUSIVE on a node with no pids limit or a higher one
HOST_RENTALS_MAX = 3  # phase A: rent a new big worker up to this many times in all to find a pids limit
TORCH_ENV = {"pip": {"packages": ["torch==2.7.1"], "pip_check": False}, "config": {"setup_timeout_seconds": 1800}}
JOB_ENV = {k: v for k, v in os.environ.items() if k.startswith("C10_") and k != "C10_KEY_FILE"}
TERMINAL = ("SUCCEEDED", "FAILED", "STOPPED")

# (id, script, args, kind, needs torch, job timeout in s). kind: tuned / many (decided here), functional (the job's own
# C10_RESULT line), c9 (a C9 job from ../c9/jobs, its C9_RESULT line). Tuned runs before many for each library: a many test
# can leave the node in a bad state. The job guards (c10common C10_GUARD_S) are shorter than these timeouts.
PLAN = {
    "a": [
        ("C10.core.tuned", "c10_core.py", ["tuned"], "tuned", False, 900),
        ("C10.core.many", "c10_core.py", ["many"], "many", False, 900),
        ("C10.data.tuned", "c10_data.py", ["tuned"], "tuned", False, 1200),
        ("C10.data.many", "c10_data.py", ["many"], "many", False, 1200),
        ("C10.data.tuned2", "c10_data.py", ["tuned2"], "tuned", False, 1200),  # added in the live run (see c10_data.py)
        ("C10.tune.tuned", "c10_tune.py", ["tuned"], "tuned", False, 900),
        ("C10.tune.many", "c10_tune.py", ["many"], "many", False, 900),
        ("C10.serve.tuned", "c10_serve.py", ["tuned"], "tuned", False, 900),
        ("C10.serve.many", "c10_serve.py", ["many"], "many", False, 900),
        ("C10.train.tuned", "c10_train.py", ["tuned"], "tuned", True, 2400),  # the first torch job installs torch
        ("C10.train.many", "c10_train.py", ["many"], "many", True, 1200),
        ("C10.rllib.tuned", "c10_rllib.py", ["tuned"], "tuned", True, 1200),
        ("C10.rllib.many", "c10_rllib.py", ["many"], "many", True, 1200),
    ],
    "b": [
        ("C10.c9.1.1", "c9_1_1_data_cpu.py", [], "c9", False, 1500),
        ("C10.c9.1.3", "c9_1_3_data_shuffle.py", [], "c9", False, 2700),
        ("C10.c9.1.4", "c9_1_4_data_gpu.py", [], "c9", True, 2700),
        ("C10.c9.1.6", "c9_1_6_data_many.py", [], "c9", False, 2100),
        ("C10.rllib.R1", "c10_rllib.py", ["r1"], "functional", True, 2400),
        ("C10.rllib.R3", "c10_rllib.py", ["r3"], "functional", True, 2400),  # longer than its guard (1500 s)
        ("C10.rllib.R2", "c10_rllib.py", ["r2"], "functional", True, 3600),  # last: it starts the gpu2 node
    ],
}
CLUSTER = {"a": "rvtest-c10a", "b": "rvtest-c10b"}


def events_max(limits):
    """The 'max N' counter of pids.events (forks that the pids limit refused), or None."""
    found = re.search(r"max (\d+)", (limits or {}).get("pids.events") or "")
    return int(found.group(1)) if found else None


def pids_max(limits):
    value = (limits or {}).get("pids.max")
    return int(value) if value and value.isdigit() else None


def verdict(kind, job, pre, post, warnings, limit_max=PIDS_LIMIT_MAX, refused_fails=True):
    """Decide one test. kind: tuned or many. job: {"kind": COMPLETED|HUNG|ERROR|None, ...}. pre and post: the probe of the
    target node before and after (None if it is gone). warnings: the head WARNING lines for the node (None = not checked,
    for the local check). limit_max: a many test on a node with a higher pids.max is INCONCLUSIVE. refused_fails=False (the
    live LB11): refused forks of a tuned test are data, not a reason to fail. Returns (status, reasons)."""
    alive = post is not None and "error" not in post and post.get("node_id") == (pre or {}).get("node_id")
    before, after = events_max(pre), events_max(post)
    refused = None if before is None or after is None else after - before
    limit = pids_max(pre)
    if kind == "many":
        if limit is None or limit > limit_max:
            return "INCONCLUSIVE", [f"pids.max of the node is {limit or 'max'}, not <= {limit_max}"]
        reasons = []
        if job.get("kind") not in ("HUNG", "ERROR"):
            reasons.append(f"the job did not fail ({job.get('kind')}): no advice is needed at this size")
        if not alive:
            reasons.append("the node crashed or left the cluster")
        if warnings is not None and not any(WARNING_PATTERN.search(w) for w in warnings):
            reasons.append("no head pids/ports WARNING line for the node")
        no_port = (post or {}).get("no_port_errors", 0) - (pre or {}).get("no_port_errors", 0)
        if warnings is None and no_port <= 0:  # local check: no head; Ray's own "No available ports" lines are the evidence
            reasons.append("no 'No available ports' line in the worker-*.err logs of the node")
        return ("FAIL" if reasons else "PASS"), reasons or [
            f"{job.get('kind')}, warning {'seen' if warnings else 'not checked'}, node alive, refused forks {refused}, "
            f"'No available ports' lines {no_port}"]
    reasons = []
    if job.get("kind") != "COMPLETED":
        reasons.append(f"the job did not complete ({job.get('kind')})")
    if not alive:
        reasons.append("the node crashed or left the cluster")
    if refused_fails and refused:
        reasons.append(f"pids.events: {refused} refused forks")
    if refused_fails and warnings and any(REFUSED_PATTERN.search(w) for w in warnings):
        reasons.append("a head WARNING line with new refused forks or thread starts")
    if not refused_fails:
        return ("FAIL" if reasons else "PASS"), reasons or [f"completed, node alive, refused forks {refused} (data, not a pass criterion)"]
    return ("FAIL" if reasons else "PASS"), reasons or ["completed, node alive, no refused fork"]


def node_warnings(lines, instance_id, ip):
    """The head process-limit WARNING lines of one node (by its vast instance ID or its public IP)."""
    names = NODE_NAMES(instance_id, ip) if instance_id or ip else []
    return [line for line in lines if WARNING_PATTERN.search(line) and any(n in line for n in names)]


def ray_cli(command, *args, timeout=120):
    p = subprocess.run(["ray", "job", command, "--address", ADDRESS, *args], capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout + p.stderr


def job_status(sid):
    from ray.job_submission import JobSubmissionClient
    try:
        return str(JobSubmissionClient(ADDRESS).get_job_status(sid).value)
    except Exception as e:
        print(f"   job status failed: {type(e).__name__}: {str(e)[:200]}", flush=True)
        return "UNKNOWN"


def head_ssh(cluster, command):
    head_ip = json.loads((Path.home() / ".ray-vast" / f"{cluster}.json").read_text())["head_ip"]
    known_hosts = Path.home() / ".ray-vast" / f"{cluster}.known_hosts"
    return ["ssh", "-i", KEY_FILE or str(Path.home() / ".ray-vast" / cluster / "ssh_key.pem"), "-o", f"UserKnownHostsFile={known_hosts}", "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=15", "-o", "BatchMode=yes", f"ubuntu@{head_ip}", command]


def head_log_lines(cluster, start_line=1):
    """The head log from line start_line on (a list of lines)."""
    p = subprocess.run(head_ssh(cluster, f"sudo docker exec ray-vast-head tail -n +{start_line} {HEAD_LOG}"), capture_output=True,
                       text=True, timeout=120)
    return p.stdout.splitlines()


def run_job(test_id, script, args, torch, timeout, out_dir, env=None, jobs=JOBS, name="job"):
    """Submit one job, wait for its end (or stop it at the timeout), keep its log. Returns (job state, log text, submission id)."""
    sid = f"{test_id.replace('.', '-').lower()}-{int(time.time())}"
    runtime_env = dict(TORCH_ENV) if torch else {}
    job_env = dict(JOB_ENV, **(env or {}))
    if job_env:
        runtime_env["env_vars"] = job_env
    command = ["submit", "--submission-id", sid, "--working-dir", str(jobs), "--no-wait"]
    if runtime_env:
        command += ["--runtime-env-json", json.dumps(runtime_env)]
    command += ["--", "python", script, *args]
    (out_dir / f"{name}_submit.txt").write_text(f"ray job submit --address {ADDRESS} " + " ".join(command[1:]) + "\n")
    shutil.copy(jobs / script, out_dir / script)
    t0 = time.time()
    code, out = ray_cli(*command, timeout=600)
    (out_dir / f"{name}_submit.out").write_text(out)
    if code != 0:
        return "SUBMIT_FAILED", out, sid
    status = job_status(sid)
    while status not in TERMINAL and time.time() - t0 < timeout:
        time.sleep(10)
        status = job_status(sid)
    if status not in TERMINAL:
        ray_cli("stop", sid, timeout=120)
        status = f"TIMEOUT({job_status(sid)})"
    _, log = ray_cli("logs", sid, timeout=300)
    (out_dir / f"{name}.log").write_text(log)
    return status, log, sid


def last_line(log, prefix):
    """The (id, word, json) of the last line that starts with prefix, or None."""
    lines = [line for line in log.splitlines() if line.startswith(prefix + " ")]
    if not lines:
        return None
    _, test_id, word, payload = lines[-1].split(" ", 3)
    return test_id, word, json.loads(payload)


def probe(out_dir, name):
    """Run c10_probe.py; save and return {node_id: limits}. Again every 30 s for up to 5 min while a node gives an error (the
    ports of a stopped job free up slowly)."""
    for attempt in range(10):
        _, log, _ = run_job("C10.probe", "c10_probe.py", [], False, 300, out_dir, name=f"probe_{name}_{attempt}")
        lines = [line for line in log.splitlines() if line.startswith("C10_PROBE ")]
        found = json.loads(lines[-1].split(" ", 1)[1]) if lines else {}
        if found and not any("error" in v for v in found.values()):
            return found
        time.sleep(30)
    return found


def main():
    if len(sys.argv) < 3 or sys.argv[2] not in PLAN:
        sys.exit("usage: run_c10.py <R> a|b [<id> ...]")
    if not os.environ.get("RAY_AUTH_TOKEN_PATH"):
        sys.exit("run the export line of `ray-vast dashboard` first (RAY_AUTH_MODE, RAY_AUTH_TOKEN_PATH)")
    phase, wanted = sys.argv[2], sys.argv[3:]
    cluster = CLUSTER[phase]
    root = Path(sys.argv[1]) / "c10"
    root.mkdir(parents=True, exist_ok=True)
    first = probe(root, f"start_{phase}_{int(time.time())}")
    print(f"== probe {json.dumps(first)[:600]}", flush=True)
    limited = [n for n, v in first.items() if pids_max(v) is not None and pids_max(v) <= PIDS_LIMIT_MAX]
    rentals = [first]
    while phase == "a" and not limited and len(rentals) < HOST_RENTALS_MAX:
        # Host retry: destroy the worker; the autoscaler rents a replacement (min_workers 1). vast_tool.py has the vast call.
        sys.path.insert(0, str(HERE.parent))
        import vast_tool
        old = {v.get("container_id") for v in first.values()}
        machines = {i["id"]: i.get("machine_id") for i in vast_tool.instances() if i.get("label") == f"ray-vast:{cluster}"}
        print(f"!! no pids.max <= {PIDS_LIMIT_MAX} (machines {machines}); destroying the worker for rental {len(rentals) + 1}", flush=True)
        for instance_id in old:
            vast_tool.call("DELETE", f"/instances/{instance_id}/")
        deadline = time.time() + 1800
        while time.time() < deadline:
            time.sleep(60)
            first = probe(root, f"rental{len(rentals) + 1}_{int(time.time())}")
            if first and not {v.get("container_id") for v in first.values()} & old:
                break
        rentals.append(first)
        print(f"== probe {json.dumps(first)[:600]}", flush=True)
        limited = [n for n, v in first.items() if pids_max(v) is not None and pids_max(v) <= PIDS_LIMIT_MAX]
    (root / f"rentals_{phase}.json").write_text(json.dumps([{n[:8]: {k: v.get(k) for k in ("container_id", "pids.max", "ports", "CPU")}
                                                           for n, v in r.items()} for r in rentals], indent=1))
    if phase == "a" and not limited:
        print(f"!! no worker has pids.max <= {PIDS_LIMIT_MAX}: the many tests are INCONCLUSIVE and do not run", flush=True)
    for test_id, script, args, kind, torch, timeout in PLAN[phase]:
        if wanted and test_id not in wanted:
            continue
        out_dir = root / test_id
        out_dir.mkdir(exist_ok=True)
        if kind == "many" and not limited:  # no node with a pids limit: the test cannot show the limit; do not run it
            row = {"id": test_id, "status": "INCONCLUSIVE", "data": {"reasons": [f"no worker has pids.max <= {PIDS_LIMIT_MAX}"],
                                                                      "probe": first}}
            with open(root / "results.jsonl", "a") as f:
                f.write(json.dumps(row, default=str) + "\n")
            print(f"C10_RESULT {test_id} INCONCLUSIVE {json.dumps(row['data'], default=str)[:600]}", flush=True)
            continue
        print(f"== {test_id} {script} {' '.join(args)} (timeout {timeout} s) {time.strftime('%H:%M:%S')}", flush=True)
        t0 = time.time()
        pre = probe(out_dir, "pre")
        target = min(pre, key=lambda n: pids_max(pre[n]) or 10**9) if pre else None  # phase a has one worker node
        env = {"C10_PORTS": str(pre[target]["ports"]), "C10_CPUS": str(pre[target]["CPU"])} if kind in ("tuned", "many") and target else {}
        log_start = len(head_log_lines(cluster)) + 1
        state, log, sid = run_job(test_id, script, args, torch, timeout, out_dir, env, C9_JOBS if kind == "c9" else JOBS)
        run_job("C10.cleanup", "c10_cleanup.py", [], False, 300, out_dir, name="cleanup")
        time.sleep(30)  # the raylet ends the job's worker processes
        post = probe(out_dir, "post")
        head = head_log_lines(cluster, log_start)
        (out_dir / "head_slice.log").write_text("\n".join(head) + "\n")
        node = pre.get(target) or {}
        warnings = node_warnings(head, node.get("container_id"), node.get("ip"))
        if kind in ("tuned", "many"):
            job_line = last_line(log, "C10_RESULT")
            job = {"kind": job_line[1], **job_line[2]} if job_line else {"kind": None, "log_tail": log.splitlines()[-15:]}
            status, reasons = verdict(kind, job, pre.get(target), post.get(target), warnings)
            data = {"reasons": reasons, "job": job, "warnings": warnings[:20], "pre": pre.get(target), "post": post.get(target)}
        else:
            found = last_line(log, "C9_RESULT" if kind == "c9" else "C10_RESULT")
            status, data = (found[1], found[2]) if found else ("FAIL", {"error": "no result line", "log_tail": log.splitlines()[-15:]})
            crashed = [n for n in pre if n not in post]
            refused = {n[:8]: events_max(post[n]) - events_max(pre[n]) for n in pre if n in post and events_max(pre[n]) is not None
                       and events_max(post.get(n)) is not None}
            data = dict(data, nodes_gone=crashed, refused_forks=refused, warnings=warnings[:20])
        row = {"id": test_id, "status": status, "job_status": state, "submission_id": sid, "start": round(t0, 1),
               "wall_s": round(time.time() - t0, 1), "data": data}
        with open(root / "results.jsonl", "a") as f:
            f.write(json.dumps(row, default=str) + "\n")
        print(f"C10_RESULT {test_id} {status} {json.dumps(data, default=str)[:600]}", flush=True)


if __name__ == "__main__":
    main()
