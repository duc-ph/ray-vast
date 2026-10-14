# C9 phase 2 driver (docs/test-plan.md, "Cycle C9"). Runs on the operator machine, after `ray-vast up` and with the
# `ray-vast dashboard` tunnel open. It uses only the normal Ray workflow: `ray job submit --working-dir jobs/ ...`.
# For each sub-test: submit, wait (per-job timeout, then `ray job stop`), save `ray job logs` to <R>/c9/<id>/job.log,
# append the `C9_RESULT` line to <R>/c9/results.jsonl, and go on after a failure. C9.0.4 (job CLI) and the Serve HTTP
# checks C9.4.2 (curl on the head, by SSH) and C9.4.3 (SSH tunnel to port 8000 of the head) run here, not as jobs.
# It never prints the cluster token. Needs RAY_AUTH_MODE and RAY_AUTH_TOKEN_PATH (the export line of `dashboard`).
# Usage: python tests/manual/c9/run_c9.py <R> [<id> ...]      (R = archive/results/<date>; ids default to all, in order)
# Env: RAY_ADDRESS (default http://127.0.0.1:8265), C9_KEY_FILE (default ~/ray-vast-devbox.pem), C9_TUNNEL_PORT (18000).
# Other C9_* variables (for example C9_MAX_GB, C9_OBJECT_GB, C9_STORAGE; see jobs/c9common.py) go to every job as
# runtime_env env_vars. Do not put credentials there: submit.txt keeps the runtime_env.
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
JOBS = HERE / "jobs"
CLUSTER = "rvtest-c9"
ADDRESS = os.environ.get("RAY_ADDRESS", "http://127.0.0.1:8265")
KEY_FILE = os.path.expanduser(os.environ.get("C9_KEY_FILE", "~/ray-vast-devbox.pem"))
TUNNEL_PORT = int(os.environ.get("C9_TUNNEL_PORT", "18000"))
TORCH_ENV = {"pip": {"packages": ["torch==2.7.1"], "pip_check": False}, "config": {"setup_timeout_seconds": 1800}}
JOB_ENV = {k: v for k, v in os.environ.items() if k.startswith("C9_") and k not in ("C9_KEY_FILE", "C9_TUNNEL_PORT")}
TERMINAL = ("SUCCEEDED", "FAILED", "STOPPED")

# (id, script or a special step, needs torch, timeout of the whole job in s). Torch jobs include the first pip install.
PLAN = [
    ("C9.0.2", "c9_0_2_hello.py", False, 900),
    ("C9.0.3", "c9_0_3_torch_cuda.py", True, 4200),
    ("C9.0.4", "job-cli", False, 600),
    ("C9.1.1", "c9_1_1_data_cpu.py", False, 1500),
    ("C9.1.2", "c9_1_2_data_objstore.py", False, 2700),
    ("C9.1.3", "c9_1_3_data_shuffle.py", False, 2700),
    ("C9.1.4", "c9_1_4_data_gpu.py", True, 2700),
    ("C9.1.5", "c9_1_5_data_io.py", False, 1500),
    ("C9.1.6", "c9_1_6_data_many.py", False, 2100),
    ("C9.2.1", "c9_2_1_tune_basic.py", False, 1500),
    ("C9.2.2", "c9_2_2_tune_asha.py", False, 1500),
    ("C9.2.3", "c9_2_3_tune_gpu.py", False, 1500),
    ("C9.2.4", "c9_2_4_tune_ckpt.py", False, 1500),
    ("C9.3.1", "c9_3_1_train_1gpu.py", True, 2700),
    ("C9.3.3", "c9_3_3_train_gloo.py", True, 1800),
    ("C9.3.2", "c9_3_2_train_nccl.py", True, 1800),
    ("C9.3.5", "c9_3_5_store_port.py", True, 1500),  # extra: a TCPStore on an identity port, then gloo, by hand
    ("C9.4.1", "c9_4_1_serve_handle.py", False, 1200),
    ("C9.4.2", "serve-http-head", False, 120),
    ("C9.4.3", "serve-http-tunnel", False, 120),
    ("C9.4.4", "c9_4_4_serve_gpu.py", True, 2700),
    ("C9.5.1", "c9_5_1_placement.py", False, 1200),
    ("C9.5.2", "c9_5_2_actor_fault.py", False, 1200),
    ("C9.5.3", "c9_5_3_large_object.py", False, 2100),
    ("C9.3.4", "c9_3_4_train_2gpu_1node.py", True, 3600),  # last: it starts the gpu2 node
]


def ray_cli(command, *args, timeout=120):
    """`ray job <command> --address ADDRESS <args>`; the address goes before the args (they can end with `-- entrypoint`)."""
    p = subprocess.run(["ray", "job", command, "--address", ADDRESS, *args], capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout + p.stderr


def job_status(sid):
    from ray.job_submission import JobSubmissionClient
    try:
        return str(JobSubmissionClient(ADDRESS).get_job_status(sid).value)
    except Exception as e:
        print(f"   job status failed: {type(e).__name__}: {str(e)[:200]}", flush=True)
        return "UNKNOWN"


def head_ssh(*command, options=()):
    head_ip = json.loads((Path.home() / ".ray-vast" / f"{CLUSTER}.json").read_text())["head_ip"]
    known_hosts = Path.home() / ".ray-vast" / f"{CLUSTER}.known_hosts"
    return ["ssh", "-i", KEY_FILE, "-o", f"UserKnownHostsFile={known_hosts}", "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=15", "-o", "BatchMode=yes", *options, f"ubuntu@{head_ip}", *command]


def submit(test_id, script, torch, timeout, out_dir):
    """Submit one job, wait for its end, keep its log. Returns (status, data) from its C9_RESULT line."""
    sid = f"{test_id.replace('.', '-').lower()}-{int(time.time())}"
    command = ["submit", "--submission-id", sid, "--working-dir", str(JOBS), "--no-wait"]
    runtime_env = dict(TORCH_ENV if torch else {}, **({"env_vars": JOB_ENV} if JOB_ENV else {}))
    extra_pip = os.environ.get("C9_EXTRA_PIP", "").split()  # phase 2: for example s3fs for C9_STORAGE=s3anon://...
    if extra_pip:
        runtime_env["pip"] = {"packages": (TORCH_ENV["pip"]["packages"] if torch else []) + extra_pip, "pip_check": False}
        runtime_env["config"] = TORCH_ENV["config"]
    if runtime_env:
        command += ["--runtime-env-json", json.dumps(runtime_env)]
    command += ["--", "python", script]
    (out_dir / "submit.txt").write_text(f"ray job submit --address {ADDRESS} " + " ".join(command[1:]) + "\n")
    shutil.copy(JOBS / script, out_dir / script)
    t0 = time.time()
    code, out = ray_cli(*command, timeout=600)
    (out_dir / "submit.out").write_text(out)
    if code != 0:
        return "FAIL", {"error": "ray job submit failed", "exit_code": code, "output_tail": out.splitlines()[-5:]}, sid, None, t0
    status = job_status(sid)
    while status not in TERMINAL and time.time() - t0 < timeout:
        time.sleep(10)
        status = job_status(sid)
    if status not in TERMINAL:
        ray_cli("stop", sid, timeout=120)
        status = f"TIMEOUT({job_status(sid)})"
    _, log = ray_cli("logs", sid, timeout=300)
    (out_dir / "job.log").write_text(log)
    lines = [line for line in log.splitlines() if line.startswith("C9_RESULT ")]
    if not lines:
        return "FAIL", {"error": "no C9_RESULT line", "log_tail": log.splitlines()[-15:]}, sid, status, t0
    _, _, result_status, payload = lines[-1].split(" ", 3)
    return result_status, json.loads(payload), sid, status, t0


def job_cli(out_dir):
    """C9.0.4: ray job list, ray job logs --follow and ray job stop through the tunnel, on a long sleeper job."""
    sid = f"c9-0-4-{int(time.time())}"
    data, record = {}, []
    code, out = ray_cli("submit", "--submission-id", sid, "--working-dir", str(JOBS), "--no-wait", "--", "python", "c9_0_4_sleeper.py",
                        timeout=600)
    record.append(("submit", code, out))
    deadline = time.time() + 300
    while job_status(sid) != "RUNNING" and time.time() < deadline:
        time.sleep(5)
    code, out = ray_cli("list", timeout=60)
    record.append(("list", code, out))
    data["list_has_job"] = sid in out
    follow_file = out_dir / "follow.out"
    with open(follow_file, "w") as f:  # --follow streams until the job ends: read it for 30 s, then end it
        follow = subprocess.Popen(["ray", "job", "logs", "--address", ADDRESS, "--follow", sid], stdout=f, stderr=subprocess.STDOUT,
                                  env=dict(os.environ, PYTHONUNBUFFERED="1"))
        try:
            data["follow_exit"] = follow.wait(timeout=30)
        except subprocess.TimeoutExpired:
            follow.terminate()
            follow.wait(timeout=10)
            data["follow_exit"] = "still streaming after 30 s (expected)"
    follow_out = follow_file.read_text()
    record.append(("logs --follow (30 s)", data["follow_exit"], follow_out))
    data["follow_ticks"] = follow_out.count("C9_SLEEPER tick")
    code, out = ray_cli("stop", sid, timeout=120)
    record.append(("stop", code, out))
    data["stop_exit"] = code
    deadline = time.time() + 120
    while (status := job_status(sid)) not in TERMINAL and time.time() < deadline:
        time.sleep(5)
    data["status_after_stop"] = status
    (out_dir / "job_cli.txt").write_text("".join(f"== ray job {name} (exit {c})\n{o}\n" for name, c, o in record))
    ok = data["list_has_job"] and data["follow_ticks"] >= 3 and data["stop_exit"] == 0 and status == "STOPPED"
    return ("PASS" if ok else "FAIL"), data, sid, status


def serve_http_head(out_dir):
    """C9.4.2: curl http://127.0.0.1:8000/ on the head host (the head container uses the host network)."""
    p = subprocess.run(head_ssh("curl -s -m 10 -w '\\nHTTP_CODE %{http_code}' http://127.0.0.1:8000/"), capture_output=True,
                       text=True, timeout=60)
    (out_dir / "curl_head.txt").write_text(f"exit {p.returncode}\n{p.stdout}\n{p.stderr}")
    code = re.findall(r"HTTP_CODE (\d+)", p.stdout)
    data = {"ssh_exit": p.returncode, "http_code": code[-1] if code else None, "body": p.stdout.split("\nHTTP_CODE")[0][:300]}
    return ("PASS" if data["http_code"] == "200" and '"hello"' in data["body"] else "FAIL"), data, None, None


def serve_http_tunnel(out_dir):
    """C9.4.3: an SSH tunnel from the operator machine to port 8000 of the head (no new security group rule)."""
    tunnel = subprocess.Popen(head_ssh(options=("-N", "-o", "ExitOnForwardFailure=yes", "-L", f"{TUNNEL_PORT}:127.0.0.1:8000")), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    data = {"command": f"ssh -i <key> -N -L {TUNNEL_PORT}:127.0.0.1:8000 ubuntu@<head ip>"}
    try:
        time.sleep(5)
        t = time.time()
        with urllib.request.urlopen(f"http://127.0.0.1:{TUNNEL_PORT}/", timeout=15) as response:
            data.update(http_code=response.status, body=response.read().decode()[:300], latency_s=round(time.time() - t, 2))
    except Exception as e:
        data.update(error=f"{type(e).__name__}: {e}")
    finally:
        tunnel.terminate()
        data["tunnel_stderr"] = tunnel.communicate(timeout=10)[1].decode()[-300:]
    (out_dir / "tunnel.txt").write_text(json.dumps(data, indent=1))
    return ("PASS" if data.get("http_code") == 200 and '"hello"' in data.get("body", "") else "FAIL"), data, None, None


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: run_c9.py <R> [<id> ...]")
    if not os.environ.get("RAY_AUTH_TOKEN_PATH"):
        sys.exit("run the export line of `ray-vast dashboard` first (RAY_AUTH_MODE, RAY_AUTH_TOKEN_PATH)")
    root = Path(sys.argv[1]) / "c9"
    wanted = sys.argv[2:]
    root.mkdir(parents=True, exist_ok=True)
    try:
        status_out = subprocess.run(head_ssh("sudo docker exec ray-vast-head ray status"), capture_output=True, text=True, timeout=120)
        (root / f"ray_status_{int(time.time())}.txt").write_text(status_out.stdout + status_out.stderr)
    except Exception as e:
        print(f"ray status on the head failed: {e}", flush=True)
    for test_id, step, torch, timeout in PLAN:
        if wanted and test_id not in wanted:
            continue
        out_dir = root / test_id
        out_dir.mkdir(exist_ok=True)
        print(f"== {test_id} {step} (timeout {timeout} s) {time.strftime('%H:%M:%S')}", flush=True)
        t0 = time.time()
        try:
            if step == "job-cli":
                status, data, sid, job_state = job_cli(out_dir)
            elif step == "serve-http-head":
                status, data, sid, job_state = serve_http_head(out_dir)
            elif step == "serve-http-tunnel":
                status, data, sid, job_state = serve_http_tunnel(out_dir)
            else:
                status, data, sid, job_state, t0 = submit(test_id, step, torch, timeout, out_dir)
        except Exception as e:
            status, data, sid, job_state = "FAIL", {"error": f"driver error {type(e).__name__}: {e}"}, None, None
        row = {"id": test_id, "status": status, "job_status": job_state, "submission_id": sid, "start": round(t0, 1),
               "wall_s": round(time.time() - t0, 1), "data": data}
        with open(root / "results.jsonl", "a") as f:
            f.write(json.dumps(row, default=str) + "\n")
        print(f"   {status} job={job_state} wall={row['wall_s']} s {json.dumps(data, default=str)[:400]}", flush=True)


if __name__ == "__main__":
    main()
