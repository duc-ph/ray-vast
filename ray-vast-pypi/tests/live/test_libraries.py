"""Group `libraries` (docs/test-plan.md, claim matrix LB00-LB11): the Ray libraries on vast, with the snippets of the user
docs (docs/libraries/*.md) taken from the docs at run time (livekit.doc_snippet), the C9 jobs and the C10 pairs.

One cluster for each host (clusters/libraries.yaml with one worker type, admin profile, us-west-2), so the hosts run in
parallel with `pytest -n` (each host is one xdist group): H1 = 1 `h1` worker (32 or more Ray CPUs, any pids limit),
H2 = 1 `h2` worker (cgroup v2), H3 = 2 `gpu` workers, gpu2 = 1 `gpu2` worker (2 GPUs). A test names its host with the
`where` parameter or the `host` marker. Each test sets the workers that it needs with `libraries.ensure_shape()` (only
these workers run), so the library code needs no node pinning. H1 and H2 come from `libraries.find()` (probe, at most 3
rentals). The tests of one host run one after the other, in the file order (destructive scenarios last), so a "many" run
of LB11 is alone on its host:
  H3:   LB00, LB01, LB02, LB04, LB05, LB06, LB09, LB10, LB03
  gpu2: LB00, LB10, LB07
  H1:   LB00, LB03, LB11 (with the diagnostics during serve.many)
  H2:   LB00, LB11
The teardown of each host's cluster (conftest.group_lifetime) checks the `down` and the S3 cleanup.
S3: for each host that needs it (H3, gpu2, H1), one bucket `ray-vast-rvtest-<date>-<hex>` and an IAM user
`ray-vast-livetest-<date>-s3-<host>` that can use only that bucket (`libraries.s3()`). Its keys go to the jobs in
runtime_env env_vars; they are never written (the `*_submit.txt` files are redacted).
In parallel: about 4.5 h (H1 with LB11 is the longest); vast about $5 ($4 to $7), EC2 about $1.3 (4 heads)."""
import json
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

import livekit
from cluster import BIN, MANUAL, free_port, wait_port
from libraries_setup import HOST_TYPE, JOBS, PINS, alive_workers, b_and_cpus, probe, workdir
from livekit import check

sys.path.insert(0, str(MANUAL / "c10"))
from run_c10 import REFUSED_PATTERN, TORCH_ENV, WARNING_PATTERN, events_max, node_warnings, verdict  # noqa: E402  (the C10 runner)

pytestmark = [pytest.mark.libraries, pytest.mark.timeout(0)]  # hours-long tests: no pytest-timeout (each job has its own)
C9_JOBS = MANUAL / "c9" / "jobs"
C10_JOBS = MANUAL / "c10" / "jobs"
DOCS = Path(__file__).resolve().parents[2] / "docs"
PARQUET_ROWS = 3_000_000  # about 3 GB: 1000 random bytes in each row


# --- fixtures ---------------------------------------------------------------------------------------------------------
@pytest.fixture
def env(libraries):
    with libraries.tunnel() as tunnel_env:
        yield tunnel_env


@pytest.fixture
def s3(libraries):
    """The S3 access of the test's host (made at its first use; the cluster deletes it at its teardown)."""
    return libraries.s3()


@pytest.fixture
def parquet_in(libraries, env, s3):
    """The LB03 input of the host (LB03 runs once on each host): about 3 GB of parquet in s3://<bucket>/lb03/in, written
    by a job (lb_make_parquet.py)."""
    path = f"s3://{s3.bucket}/lb03/in"
    status, log = s3_job(libraries, env, s3, workdir(JOBS / "lb_make_parquet.py"), ["python", "lb_make_parquet.py", path, str(PARQUET_ROWS)],
                         libraries.dir, "make_parquet", timeout=3600)
    found = result_line(log, "LB_RESULT")
    size = sum(o["Size"] for o in s3.objects("lb03/in/"))
    (libraries.dir / "parquet_in.json").write_text(json.dumps({"status": status, "result": found, "bytes": size, "rows": PARQUET_ROWS}))
    assert found and found[1] == "PASS", f"the LB03 input was not written: {status} {found}"
    return {"path": path, "bytes": size, "rows": PARQUET_ROWS}


# --- helpers ------------------------------------------------------------------------------------------------------------
def result_line(log, prefix):
    """(id, word, data) of the last `<prefix> <id> <word> <json>` line, or None."""
    lines = [x for x in log.splitlines() if x.startswith(prefix + " ")]
    if not lines:
        return None
    _, name, word, payload = lines[-1].split(" ", 3)
    return name, word, json.loads(payload)


def job(cluster, env, record, wd, entrypoint, name, runtime_env=None, timeout=1800, during=None):
    """Submit, run `during(sid)` while it runs (optional), wait for the end (stopped at `timeout`), keep the log."""
    sid, _ = cluster.job(env, wd, entrypoint, runtime_env=runtime_env, out_dir=record.dir, name=name, no_wait=True)
    if sid == "SUBMIT_FAILED":
        return sid, (record.dir / f"{name}.log").read_text()
    t0 = time.time()
    if during:
        during(sid)
    status = cluster.job_wait(env, sid, max(60, timeout - (time.time() - t0)))
    log = cluster.job_logs(env, sid)
    (record.dir / f"{name}.log").write_text(log)
    return status, log


def s3_job(cluster, env, s3, wd, entrypoint, out_dir, name, runtime_env=None, timeout=1800, extra_env=None):
    """A job with the S3 keys in env_vars. `<name>_submit.txt` keeps the runtime_env: the secret is redacted there."""
    runtime_env = dict(runtime_env or {}, env_vars={**s3.env_vars, **(extra_env or {})})
    try:
        return cluster.job(env, wd, entrypoint, runtime_env=runtime_env, out_dir=out_dir, name=name, timeout=timeout)
    finally:
        submit = Path(out_dir) / f"{name}_submit.txt"
        if submit.exists():
            submit.write_text(submit.read_text().replace(s3.secret, "<redacted>"))


def c9(cluster, env, record, script, torch=False, timeout=1800, env_vars=None):
    """One C9 job (working dir tests/manual/c9/jobs, as run_c9.py). Returns (word, data) of its C9_RESULT line."""
    runtime_env = dict(TORCH_ENV) if torch else {}
    if env_vars:
        runtime_env["env_vars"] = env_vars
    status, log = cluster.job(env, C9_JOBS, ["python", script], runtime_env=runtime_env or None, out_dir=record.dir,
                              name=script.removesuffix(".py"), timeout=timeout)
    found = result_line(log, "C9_RESULT")
    return (found[1], found[2]) if found else ("NO_RESULT", {"job_status": status, "log_tail": log.splitlines()[-10:]})


def snippet(doc, index, subs, record, name):
    """The doc snippet with its substitutions; both go into the evidence."""
    code = livekit.doc_snippet((DOCS / "libraries" / doc).read_text(), "## Settings", index, subs)
    record.evidence(f"{name}_snippet.py", code)
    record.data(f"{name}_substitutions", [{"doc": f"libraries/{doc}", "old": old, "new": new} for old, new in subs])
    return code


def snippet_job(cluster, env, record, lib, code, name, runtime_env=None, timeout=1800, s3=None, extra_env=None):
    """Run a doc snippet with jobs/lb_snippet.py. Returns (job status, word, data, log)."""
    wd = workdir(JOBS / "lb_snippet.py")
    (wd / "snippet.py").write_text(code)
    extra = {"LB_GUARD_S": str(timeout - 120), **(extra_env or {})}
    if s3:
        status, log = s3_job(cluster, env, s3, wd, ["python", "lb_snippet.py", lib], record.dir, name, runtime_env, timeout, extra)
    else:
        status, log = cluster.job(env, wd, ["python", "lb_snippet.py", lib], runtime_env=dict(runtime_env or {}, env_vars=extra),
                                  out_dir=record.dir, name=name, timeout=timeout)
    found = result_line(log, "LB_RESULT")
    return status, *((found[1], found[2]) if found else ("NO_RESULT", {"log_tail": log.splitlines()[-10:]})), log


def log_mark(cluster):
    """The line number where the next monitor.log line starts."""
    return int(cluster.head_exec("wc -l < /tmp/ray/session_latest/logs/monitor.log", timeout=120).strip()) + 1


def warnings_for(lines, rows):
    """The head process-limit WARNING lines of these worker nodes (NODE_PROBE rows)."""
    return [w for r in rows for w in node_warnings(lines, r["container_id"], r["ip"])]


def doc_line(doc, start):
    """The first line of a user doc that starts with `start` (a command to run as written)."""
    return next(x for x in (DOCS / doc).read_text().splitlines() if x.startswith(start))


def h3(cluster, env, record, name="probe_h3"):
    """The H3 shape (2 `gpu` workers): (rows, B, CPUS)."""
    record.data("shape_wait_s", cluster.ensure_shape({"gpu": 2}))
    rows = probe(cluster, env, record.dir, name)["gpu"]
    b, cpus = b_and_cpus(rows)
    record.data("H3", {"B": b, "CPUS": cpus, "hosts": [{k: r.get(k) for k in ("container_id", "machine_id", "ports", "ray_cpus", "pids.max")}
                                                        for r in rows]})
    return rows, b, cpus


def host(cluster, env, record, name):
    """H1 or H2 alone: reuse the matched host when it still runs, else find one. Returns its NODE_PROBE row."""
    kind = {"H1": "h1", "H2": "h2"}[name]
    if cluster.shape == {kind: 1} and kind in cluster.matched:
        cluster.ensure_shape({kind: 1})
        rows = probe(cluster, env, record.dir, f"probe_{kind}")[kind]
        if rows[0]["machine_id"] == cluster.matched[kind]["machine_id"]:
            return rows[0]
    facts, tries = cluster.find(env, kind, PINS[kind]["want"], {}, record.dir)
    record.data(f"{name}_rentals", [{k: t.get(k) for k in ("machine_id", "pids_max", "cgroup", "cpus", "match", "query")} for t in tries])
    return facts["probe"]


# --- LB00 to LB10: not destructive ------------------------------------------------------------------------------------
@pytest.mark.limits
@pytest.mark.parametrize("where", ["H3", "gpu2", "H1", "H2"])
def test_LB00_host_probe_on_every_libraries_host(record, libraries, env, where):
    t_start = time.time()
    if where in ("H1", "H2"):
        kind = where.lower()
        facts, tries = libraries.find(env, kind, PINS[kind]["want"], {}, record.dir)
        record.data(f"{where}_rentals", [{k: t.get(k) for k in ("machine_id", "pids_max", "cgroup", "cpus", "match", "query")} for t in tries])
        hosts = {where: [facts["probe"]]}
    else:
        kind = HOST_TYPE[where]
        libraries.ensure_shape({kind: 2 if where == "H3" else 1})
        hosts = {where: probe(libraries, env, record.dir, f"probe_{kind}")[kind]}
    record.evidence("node_probe.json", json.dumps(hosts, indent=1))
    if where == "H1":
        check(record, "H1: 32 or more Ray CPUs, a pids limit", hosts["H1"][0]["ray_cpus"] >= 32 and hosts["H1"][0].get("pids.max") not in (None, "max"),
              {k: hosts["H1"][0].get(k) for k in ("machine_id", "ray_cpus", "pids.max", "cgroup")})
    if where == "H2":
        check(record, "H2: cgroup v2", hosts["H2"][0]["cgroup"] == "v2", {k: hosts["H2"][0].get(k) for k in ("machine_id", "pids.max", "cgroup")})
    lines = libraries.monitor_log()
    enrolled = {int(m.group(1)): libraries.log_epoch(x) for x in lines for m in [re.search(r"enrolled instance (\d+)", x)] if m}
    first_running = {}
    for row in libraries.timeline:
        for i in row.get("instances", []):
            if i.get("actual_status") == "running" and row["t"] >= t_start - 5:
                first_running.setdefault(i["id"], row["t"])
    for name, rows in hosts.items():
        for n in rows:
            cid = n["container_id"]
            limit = n.get("pids.max")
            ports = 58 if limit in (None, "max") else max(4, min(58, (int(limit) - 400) // 60))
            env_ray = n.get("ray_env") or {}
            enroll_s = round(enrolled[int(cid)] - first_running[int(cid)], 1) if int(cid) in enrolled and int(cid) in first_running else None
            record.data(f"{name}_{cid}", {**{k: n.get(k) for k in ("machine_id", "pids.max", "cgroup", "cpu_quota", "affinity", "ray_cpus",
                                                                   "ports", "ray_env", "worker_threads", "cpu_count")},
                                          "enroll_s_from_first_running_sample": enroll_s})
            check(record, f"{name} {cid}: worker ports = (pids.max - 400) // 60 in 4..58 (pids.max {limit})", n.get("ports") == ports,
                  (n.get("ports"), ports))
            check(record, f"{name} {cid}: soft limit min(CPUs, ports // 2), grpc threads 4, startup concurrency 8",
                  env_ray.get("RAY_num_workers_soft_limit") == str(min(int(n["ray_cpus"]), ports // 2)) and
                  env_ray.get("RAY_num_grpc_internal_threads") == "4" and env_ray.get("RAY_worker_maximum_startup_concurrency") == "8", env_ray)
            check(record, f"{name} {cid}: Ray CPUs = the real core count (affinity and quota)", 1 <= n["ray_cpus"] <= n["affinity"],
                  (n["ray_cpus"], n["affinity"], n["cpu_quota"]))
            # design.md:56, :86: enrollment within 1 min of `running`; the suite's 10 s samples allow -15 to 75 s.
            check(record, f"{name} {cid}: enrolled within 1 min of `running` (design.md:86; -15 to 75 s on the suite's samples)",
                  enroll_s is not None and -15 <= enroll_s <= 75, enroll_s)


@pytest.mark.host("H3")
def test_LB01_jobs_workflow(record, libraries, env):
    h3(libraries, env, record)
    word, data = c9(libraries, env, record, "c9_0_2_hello.py", timeout=900)
    record.data("hello", data)
    check(record, "--working-dir: data/hello.txt read by tasks on both workers", word == "PASS", data)
    word, data = c9(libraries, env, record, "c9_0_3_torch_cuda.py", torch=True, timeout=4200)
    record.data("torch_cuda", data)
    check(record, "runtime_env pip torch==2.7.1: CUDA on both GPU workers", word == "PASS", data)
    sid, _ = libraries.job(env, C9_JOBS, ["python", "c9_0_4_sleeper.py"], out_dir=record.dir, name="sleeper", no_wait=True)
    deadline = time.time() + 300
    while libraries.job_status(env, sid) != "RUNNING" and time.time() < deadline:
        time.sleep(5)
    ray = str(BIN / "ray")
    listed = subprocess.run([ray, "job", "list", "--address", env["RAY_ADDRESS"]], env=env, capture_output=True, text=True, timeout=120)
    with open(record.dir / "follow.out", "w") as f:  # --follow streams until the job ends: read it for 30 s
        follow = subprocess.Popen([ray, "job", "logs", "--address", env["RAY_ADDRESS"], "--follow", sid], env=env, stdout=f,
                                  stderr=subprocess.STDOUT)
        try:
            follow.wait(timeout=30)
        except subprocess.TimeoutExpired:
            follow.terminate()
            follow.wait(timeout=10)
    stopped = subprocess.run([ray, "job", "stop", "--address", env["RAY_ADDRESS"], sid], env=env, capture_output=True, text=True, timeout=120)
    status = libraries.job_wait(env, sid, 120)
    record.evidence("job_cli.txt", f"== ray job list (exit {listed.returncode})\n{listed.stdout}{listed.stderr}\n"
                                   f"== ray job stop (exit {stopped.returncode})\n{stopped.stdout}{stopped.stderr}\n")
    ticks = (record.dir / "follow.out").read_text().count("C9_SLEEPER tick")
    check(record, "ray job list shows the job", sid in listed.stdout, listed.returncode)
    check(record, "ray job logs --follow streams (>= 3 ticks in 30 s)", ticks >= 3, ticks)
    check(record, "ray job stop: exit 0, then STOPPED", stopped.returncode == 0 and status == "STOPPED", (stopped.returncode, status))


@pytest.mark.limits
@pytest.mark.host("H3")
def test_LB02_core_functions_and_the_core_snippet(record, libraries, env):
    rows, b, cpus = h3(libraries, env, record)
    for script, claim in (("c9_5_1_placement.py", "PACK on one worker, STRICT_SPREAD on both"),
                          ("c9_5_2_actor_fault.py", "actors restart after os._exit and ray.kill"),
                          ("c9_5_3_large_object.py", "a 2 GB object between the hosts")):
        word, data = c9(libraries, env, record, script, timeout=2100)
        record.data(script.removesuffix(".py"), data)
        check(record, claim, word == "PASS", data)
    share = round(cpus / b, 3)
    code = (snippet("core.md", 1, [], record, "cpus") +
            snippet("core.md", 0, [("@ray.remote(num_cpus=2.4)          # CPUS / B\ndef child",
                                    f"@ray.remote(num_cpus={share})          # CPUS / B\ndef child"),
                                   ("@ray.remote(num_cpus=2.4)          # CPUS / B\nclass Runner",
                                    f"@ray.remote(num_cpus={share})          # CPUS / B\nclass Runner"),
                                   ("range(5)", f"range({b // 2})")], record, "core"))
    start = log_mark(libraries)
    status, word, data, _ = snippet_job(libraries, env, record, "core", code, "core_snippet", timeout=1500)
    lines = libraries.monitor_log(start)
    record.evidence("monitor_slice.log", "\n".join(lines))
    record.data("core_snippet", data)
    check(record, f"the core snippet (num_cpus {share}, pool {b // 2}) completes", status == "SUCCEEDED" and word == "PASS", (status, data))
    check(record, "no process-limit WARNING for the H3 workers", not warnings_for(lines, rows), warnings_for(lines, rows)[:2])


@pytest.mark.limits
@pytest.mark.host("H3")
def test_LB04_data_shuffles_with_the_tuned_settings(record, libraries, env):
    rows, b, cpus = h3(libraries, env, record)
    wd = workdir(JOBS / "lb_data_tuned.py", C9_JOBS / "c9common.py")
    for mode, torch in (("shuffle", False), ("gpu", True)):
        status, log = libraries.job(env, wd, ["python", "lb_data_tuned.py", mode], runtime_env=dict(TORCH_ENV if torch else {},
                                    env_vars={"LB_B": str(b)}), out_dir=record.dir, name=f"data_{mode}", timeout=2700)
        found = result_line(log, "C9_RESULT")
        record.data(mode, found[2] if found else log.splitlines()[-10:])
        check(record, f"C9.1.{'3' if mode == 'shuffle' else '4'} with the data.md settings at B {b}: SUCCEEDED",
              status == "SUCCEEDED" and found and found[1] == "PASS", (status, found[2] if found else None))


@pytest.mark.host("H3")
def test_LB05_data_defaults_without_a_shuffle(record, libraries, env):
    h3(libraries, env, record)
    for script in ("c9_1_1_data_cpu.py", "c9_1_2_data_objstore.py", "c9_1_5_data_io.py", "c9_1_6_data_many.py"):
        word, data = c9(libraries, env, record, script, timeout=2700)
        record.data(script.removesuffix(".py"), data)
        check(record, f"{script} with the defaults", word == "PASS", data)
    io = record.result["data"]["c9_1_5_data_io"]
    check(record, "a local output path stays on the workers: the driver reads none of it",
          sum(v["files"] for v in io["files_per_node"].values()) > 0 and ("readback_error" in io or io.get("readback_rows") == 0), io)


@pytest.mark.limits
@pytest.mark.host("H3")
def test_LB06_tune_snippet_with_s3(record, libraries, env, s3):
    rows, b, cpus = h3(libraries, env, record)
    share = round(cpus / b, 3)
    code = snippet("tune.md", 0, [('{"cpu": 2.4}', f'{{"cpu": {share}}}'), ("max_concurrent_trials=5", f"max_concurrent_trials={b // 2}"),
                                  ('"s3://my-bucket/experiments"', f'"s3://{s3.bucket}/lb06/experiments"')], record, "tune")
    start = log_mark(libraries)
    status, word, data, _ = snippet_job(libraries, env, record, "tune", code, "tune_snippet", timeout=2400, s3=s3)
    lines = libraries.monitor_log(start)
    record.data("tune_snippet", data)
    checkpoints = {o["Key"].split("/checkpoint_")[0] for o in s3.objects("lb06/experiments/") if "/checkpoint_" in o["Key"]}
    record.data("s3_trials_with_checkpoints", len(checkpoints))
    check(record, "the tune.md snippet: 60 trials, 0 errors", word == "PASS" and data.get("results") == 60 and data.get("errors") == 0, data)
    check(record, "trials on both workers", len([n for n in data.get("trial_nodes", []) if n]) >= 2, data.get("trial_nodes"))
    check(record, "checkpoints in S3 for every trial", len(checkpoints) == 60, len(checkpoints))
    check(record, "no refused fork in a WARNING for the H3 workers", not [w for w in warnings_for(lines, rows) if REFUSED_PATTERN.search(w)], "")
    for script, claim, torch in (("c9_2_2_tune_asha.py", "ASHA stops trials early", False),
                                 ("c9_2_3_tune_gpu.py", "GPU trials get 1 GPU each, on both workers", False)):
        word, data = c9(libraries, env, record, script, torch=torch, timeout=1500)
        record.data(script.removesuffix(".py"), data)
        check(record, claim, word == "PASS", data)
    word, data = c9(libraries, env, record, "c9_2_4_tune_ckpt.py", timeout=1500)
    record.data("default_storage", data)
    # The job cuts each trial error to 300 characters in its result line; the full error is in the job log.
    log = (record.dir / "c9_2_4_tune_ckpt.log").read_text()
    shutil.copy(record.dir / "c9_2_4_tune_ckpt.log", record.dir / "c9_2_4_tune_ckpt_default.log")
    check(record, "the default storage_path: `Unable to set up cluster storage ... not accessible by worker nodes` (tune.md:83)",
          word == "LIMIT" and "Unable to set up cluster storage" in log and "not accessible by worker nodes" in log,
          (word, data.get("errors"), data.get("experiment_path")))
    word, data = c9(libraries, env, record, "c9_2_4_tune_ckpt.py", timeout=1500, env_vars={"C9_STORAGE": f"s3://anonymous@{s3.bucket}/lb06/anon"})
    record.data("anonymous_s3", data)
    check(record, "anonymous S3 through pyarrow: S3 refuses the uploads (tune.md:84)",
          word == "LIMIT" and re.search(r"(?i)access_?denied|access denied|forbidden|\b403\b",
                                        json.dumps(data) + (record.dir / "c9_2_4_tune_ckpt.log").read_text()), data)


@pytest.mark.limits
@pytest.mark.host("H3")
def test_LB09_serve_snippet_http_and_autoscaling(record, libraries, env):
    rows, b, cpus = h3(libraries, env, record)
    share, replicas = round(cpus / b, 3), b * len(rows)
    code = snippet("serve.md", 0, [("num_replicas=10,", f"num_replicas={replicas},"), ('{"num_cpus": 2.4}', f'{{"num_cpus": {share}}}')],
                   record, "serve")
    start = log_mark(libraries)
    status, word, data, _ = snippet_job(libraries, env, record, "serve", code, "serve_snippet", timeout=1500)
    record.data("serve_snippet", data)
    per_node = data.get("replica_nodes", {})
    check(record, f"the serve.md snippet: {replicas} replicas, at most B = {b} on each worker", status == "SUCCEEDED" and word == "PASS" and
          sum(per_node.values()) == replicas and max(per_node.values(), default=b + 1) <= b, (status, per_node))
    threads = probe(libraries, env, record.dir, "probe_with_replicas")["gpu"]
    record.data("worker_threads_with_replicas", {r["container_id"]: r["worker_threads"] for r in threads})
    # HTTP through the tunnel of serve.md, as written. Changed: the cluster name, the head IP, `~` (ssh expands `~` in
    # UserKnownHostsFile from the passwd home, not $HOME, and the suite keeps its state in a private HOME), and the local
    # port if 18000 is in use.
    line = doc_line("libraries/serve.md", "ssh -i ~/.ray-vast/<cluster_name>/ssh_key.pem")
    port = 18000
    with socket.socket() as s:
        if s.connect_ex(("127.0.0.1", 18000)) == 0:
            port = free_port()
    command = (line.replace("<cluster_name>", libraries.name).replace("<head ip>", libraries.head_ip).replace("18000:", f"{port}:")
               .replace("~/", f"{libraries.home}/"))
    record.data("tunnel_command", command.replace(libraries.head_ip, "<head ip>"))
    tunnel = subprocess.Popen(["bash", "-c", command], env={**libraries.env, "HOME": str(libraries.home)}, stdin=subprocess.DEVNULL,
                              stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True)
    codes = []
    try:
        wait_port(port, 60)
        for _ in range(100):
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=30) as response:
                    codes.append(response.status)
            except Exception as e:  # noqa: BLE001
                codes.append(getattr(e, "code", type(e).__name__))
    finally:
        tunnel.terminate()
        tunnel.wait(timeout=30)
    record.data("http_codes", {str(c): codes.count(c) for c in set(codes)})
    check(record, "100 HTTP calls through the serve.md tunnel after the job ended: all 200", codes == [200] * 100,
          {str(c): codes.count(c) for c in set(codes)})
    head = libraries.ssh("curl -s -o /dev/null -m 15 -w '%{http_code}' http://127.0.0.1:8000/", check=False).strip()
    check(record, "curl on the head, port 8000: 200", head == "200", head)
    libraries.job(env, C10_JOBS, ["python", "c10_cleanup.py"], out_dir=record.dir, name="serve_cleanup", timeout=300)
    lines = libraries.monitor_log(start)
    check(record, "no refused fork in a WARNING for the H3 workers", not [w for w in warnings_for(lines, rows) if REFUSED_PATTERN.search(w)], "")
    word, data = c9(libraries, env, record, "c9_4_4_serve_gpu.py", torch=True, timeout=2700)
    record.data("serve_gpu", data)
    check(record, "GPU replicas: 1 GPU id each, on both workers", word == "PASS", data)
    status, log = libraries.job(env, workdir(JOBS / "lb_serve_autoscale.py"), ["python", "lb_serve_autoscale.py"],
                                runtime_env={"env_vars": {"LB_B": str(b), "LB_CPUS": str(cpus), "LB_WORKERS": str(len(rows))}},
                                out_dir=record.dir, name="serve_autoscale", timeout=1200)
    found = result_line(log, "LB_RESULT")
    record.data("serve_autoscale", found[2] if found else log.splitlines()[-10:])
    check(record, f"autoscaling: scales out, up to max_replicas {replicas}, at most B on each worker", found and found[1] == "PASS",
          {k: (found[2] if found else {}).get(k) for k in ("peak_replicas", "per_node_max", "max_replicas")})
    libraries.job(env, C10_JOBS, ["python", "c10_cleanup.py"], out_dir=record.dir, name="serve_cleanup_2", timeout=300)


@pytest.mark.limits
@pytest.mark.parametrize("where", ["H3", "gpu2"])
def test_LB10_rllib_env_runners_and_learners(record, libraries, env, where):
    if where == "H3":
        rows, b, cpus = h3(libraries, env, record)
        share = round(cpus / b, 3)
        code = snippet("rllib.md", 0, [("num_cpus_per_env_runner=2.4", f"num_cpus_per_env_runner={share}")], record, "rllib_runners")
        start = log_mark(libraries)
        status, word, data, _ = snippet_job(libraries, env, record, "rllib_runners", code, "rllib_runners", runtime_env=TORCH_ENV,
                                            timeout=2700)
        lines = libraries.monitor_log(start)
        record.data("rllib_runners", data)
        check(record, f"the env runner snippet (2 runners, num_cpus {share}): 2 iterations", word == "PASS", data)
        check(record, "no process-limit WARNING for the H3 workers", not warnings_for(lines, rows), warnings_for(lines, rows)[:2])
        return
    record.data("shape_wait_gpu2_s", libraries.ensure_shape({"gpu2": 1}))
    gpu2 = probe(libraries, env, record.dir, "probe_gpu2")["gpu2"]
    b2, cpus2 = b_and_cpus(gpu2)
    share2 = round(cpus2 / b2, 3)
    record.data("num_cpus_per_learner", share2)
    code = snippet("rllib.md", 1, [("num_cpus_per_learner=2.4", f"num_cpus_per_learner={share2}"),
                                   ("tune.Tuner(config.algo_class", "grid = tune.Tuner(config.algo_class")], record, "rllib_learners")
    status, word, data, _ = snippet_job(libraries, env, record, "rllib_learners", code, "rllib_learners", runtime_env=TORCH_ENV, timeout=2700)
    record.data("rllib_learners", data)
    check(record, f"the learner snippet on gpu2 (num_cpus_per_learner {share2}): 2 iterations, 0 errors", word == "PASS", data)


@pytest.mark.limits
@pytest.mark.host("gpu2")
def test_LB07_train_snippet_with_2_gpus_and_s3(record, libraries, env, s3):
    libraries.ensure_shape({"gpu2": 1})
    gpu2 = probe(libraries, env, record.dir, "probe_gpu2")["gpu2"]
    b, cpus = b_and_cpus(gpu2)
    share = round(cpus / b, 3)
    code = snippet("train.md", 0, [('{"CPU": 2.4, "GPU": 1}', f'{{"CPU": {share}, "GPU": 1}}'),
                                   ('"s3://my-bucket/experiments"', f'"s3://{s3.bucket}/lb07/experiments"'),
                                   ("    ...\n", "    train_body(loader)\n")], record, "train")
    start = log_mark(libraries)
    status, word, data, log = snippet_job(libraries, env, record, "train", code, "train_snippet", runtime_env=TORCH_ENV, timeout=3600, s3=s3)
    lines = libraries.monitor_log(start)
    ranks = [json.loads(x.split("LB_RANK ", 1)[1]) for x in log.splitlines() if "LB_RANK " in x]
    last = [r for r in ranks if r["epoch"] == 1]
    record.data("train_ranks", ranks)
    gpu2_node = gpu2[0]["node_id"][:8]
    check(record, "the train.md snippet: fit returns", status == "SUCCEEDED" and word == "PASS", (status, data))
    check(record, "2 epochs on 2 ranks on the gpu2 worker, cuda, one GPU id each, 2 different GPUs",
          len(last) == 2 and {r["node"] for r in last} == {gpu2_node} and all(r["device"].startswith("cuda") and len(r["gpu_ids"]) == 1 for r in last)
          and len({r["gpu_ids"][0] for r in last}) == 2, last)
    checkpoints = [o["Key"] for o in s3.objects("lb07/experiments/") if "/checkpoint_" in o["Key"]]
    check(record, "checkpoints in S3", len(checkpoints) > 0, len(checkpoints))
    check(record, "no process-limit WARNING for the gpu2 worker", not warnings_for(lines, gpu2), warnings_for(lines, gpu2)[:2])
    word, data = c9(libraries, env, record, "c9_3_1_train_1gpu.py", torch=True, timeout=2700)
    record.data("default_storage", data)
    check(record, "1 GPU with the default storage_path: `A local path on the head node is not accessible by worker nodes`",
          word == "LIMIT" and "not accessible by worker nodes" in json.dumps(data) + (record.dir / "c9_3_1_train_1gpu.log").read_text(),
          data)


@pytest.mark.limits
@pytest.mark.parametrize("where", ["H3", "H1"])
def test_LB03_data_snippet_verbatim(record, libraries, env, s3, parquet_in, where):
    record.data("input", parquet_in)
    if where == "H3":
        rows, b, cpus = h3(libraries, env, record)
    else:
        rows = [host(libraries, env, record, "H1")]
        b, cpus = b_and_cpus(rows)
    out = f"s3://{s3.bucket}/lb03/out-{where}"
    code = snippet("data.md", 0, [("max_hash_shuffle_aggregators = 2", f"max_hash_shuffle_aggregators = {b // 4}"),
                                  ("ExecutionResources(cpu=2)", f"ExecutionResources(cpu={b // 4})"),
                                  ('"s3://my-bucket/in"', f'"{parquet_in["path"]}"'),
                                  ("concurrency=2,", f"concurrency={b // 4},"),
                                  ("override_num_blocks=5)", f"override_num_blocks={b // 2})"),
                                  ("TaskPoolStrategy(size=2)", f"TaskPoolStrategy(size={b // 4})"),
                                  ('"s3://my-bucket/out"', f'"{out}"')], record, "data")
    start = log_mark(libraries)
    status, word, data, log = snippet_job(libraries, env, record, "data", code, "data_snippet", timeout=3600, s3=s3,
                                          extra_env={"LB_OUT": out, "LB_ROWS": str(parquet_in["rows"])})
    lines = libraries.monitor_log(start)
    record.evidence("monitor_slice.log", "\n".join(lines))
    record.data("data_snippet", data)
    after = probe(libraries, env, record.dir, "probe_after")
    alive = {r["node_id"] for rs in after.values() for r in rs}
    check(record, f"the data.md snippet at B {b} on {where}: SUCCEEDED, all rows counted from the S3 output",
          status == "SUCCEEDED" and word == "PASS", (status, data))
    check(record, "output in S3", len(s3.objects(out.split("/", 3)[3] + "/")) > 0, out)
    check(record, "no refused fork in a WARNING", not [w for w in warnings_for(lines, rows) if REFUSED_PATTERN.search(w)], "")
    check(record, "no node crash: the same Ray nodes are alive", {r["node_id"] for r in rows} <= alive, "")
    check(record, "no out-of-memory error", not re.search(r"OutOfMemoryError|out of memory", log), "")


# --- LB11: destructive -----------------------------------------------------------------------------------------
PAIRS = [  # (library, tuned (script, args), many (script, args), torch, job timeout in s): run_c10.PLAN "a"
    ("core", ("c10_core.py", "tuned"), ("c10_core.py", "many"), False, 900),
    ("data", ("c10_data.py", "tuned2"), ("c10_data.py", "many"), False, 1200),
    ("tune", ("lb_tuned.py", "tune"), ("c10_tune.py", "many"), False, 900),
    ("serve", ("c10_serve.py", "full"), ("c10_serve.py", "many"), False, 900),
    ("train", ("lb_tuned.py", "train"), ("c10_train.py", "many"), True, 2400),
    ("rllib", ("lb_tuned.py", "rllib"), ("c10_rllib.py", "many"), True, 1500),
]


def c10_probe(cluster, env, out_dir, wd, name):
    """c10_probe.py: {node id: read_limits()}; again every 30 s for up to 5 min while a node gives an error."""
    for attempt in range(10):
        _, log = cluster.job(env, wd, ["python", "c10_probe.py"], out_dir=out_dir, name=f"{name}_{attempt}", timeout=300)
        lines = [x for x in log.splitlines() if x.startswith("C10_PROBE ")]
        found = json.loads(lines[-1].split(" ", 1)[1]) if lines else {}
        if found and not any("error" in v for v in found.values()):
            return found
        time.sleep(30)
    return found


def diagnose(cluster, env, node, sid, out):
    """During serve.many on H1: the 3 "If it fails" steps of the library pages, as a user does them."""
    time.sleep(240)  # the replicas that get no port fail to register
    token = Path(env["RAY_AUTH_TOKEN_PATH"]).read_text().strip()
    headers = {"Authorization": "Bearer " + token}  # never written
    base = env["RAY_ADDRESS"]
    with urllib.request.urlopen(urllib.request.Request(f"{base}/api/v0/logs?node_id={node['node_id']}&glob=worker-*.err", headers=headers),
                                timeout=60) as r:
        listing = json.loads(r.read())
    files = sorted({f for group in ((listing.get("data") or {}).get("result") or {}).values() for f in group if f.endswith(".err")})
    out["err_files"] = len(files)
    for name in files:
        url = f"{base}/api/v0/logs/file?node_id={node['node_id']}&filename={name}&lines=200"
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60) as r:
            text = r.read().decode(errors="replace")
        if "No available ports" in text:
            out.update(no_port_file=name, no_port_text=text[-3000:])
            break
    out["job_status"] = cluster.job_status(env, sid)
    out["job_log"] = cluster.job_logs(env, sid)
    done = cluster.cli("attach", cluster.yaml, input='sudo docker exec ray-vast-head grep "process limit" '
                       '/tmp/ray/session_latest/logs/monitor.log; echo GREP_EXIT $?\nexit\n', timeout=180)
    out["grep"] = done.stdout + done.stderr


@pytest.mark.limits
@pytest.mark.parametrize("where", ["H1", "H2"])
def test_LB11_process_limit_pairs_on_h1_and_h2(record, libraries, env, where):
    """The pairs of each library; on H1 the diagnostics during serve.many; then, as a measurement only, a task that starts
    threads until the container refuses one (the refused forks, the head WARNING, and whether the node lives on)."""
    node = host(libraries, env, record, where)
    limit = livekit.host_facts(node, node["machine_id"])["pids_max"]
    check(record, f"{where} has a pids limit", limit is not None, node.get("pids.max"))
    record.data("host", {k: node.get(k) for k in ("machine_id", "container_id", "pids.max", "cgroup", "ray_cpus", "ports")})
    wd = workdir(*C10_JOBS.glob("*.py"), JOBS / "lb_tuned.py")
    outcomes, diag = [], {}
    for lib, tuned, many, torch, timeout in PAIRS:
        for kind, (script, arg) in (("tuned", tuned), ("many", many)):
            pair = f"{lib}.{kind}"
            pre = c10_probe(libraries, env, record.dir, wd, f"{pair}_pre")
            target = next(iter(pre)) if len(pre) == 1 else None
            check(record, f"{pair}: one worker node", target is not None, len(pre))
            start = log_mark(libraries)
            during = None
            if where == "H1" and pair == "serve.many":
                during = lambda sid: diagnose(libraries, env, node, sid, diag)  # noqa: E731
            runtime_env = dict(TORCH_ENV if torch else {}, env_vars={"C10_PORTS": str(pre[target]["ports"]), "C10_CPUS": str(pre[target]["CPU"])})
            status, log = job(libraries, env, record, wd, ["python", script, arg], pair, runtime_env=runtime_env, timeout=timeout, during=during)
            libraries.job(env, wd, ["python", "c10_cleanup.py"], out_dir=record.dir, name=f"{pair}_cleanup", timeout=300)
            time.sleep(30)  # the raylet ends the job's worker processes
            post = c10_probe(libraries, env, record.dir, wd, f"{pair}_post")
            lines = libraries.monitor_log(start)
            record.evidence(f"{pair}_head_slice.log", "\n".join(lines))
            warnings = node_warnings(lines, node["container_id"], node["ip"])
            found = result_line(log, "C10_JOB")
            job_line = {"kind": found[1], **found[2]} if found else {"kind": None}
            # run_c10 calls a many run INCONCLUSIVE above 3328 (its phase A rule). The many sizes here are max(ports + 4, ...),
            # above the ports at any limit, so the limit of this host is accepted.
            verdict_word, reasons = verdict(kind, job_line, pre.get(target), post.get(target), warnings, limit_max=max(3328, limit))
            refused = (events_max(post.get(target)) or 0) - (events_max(pre.get(target)) or 0)
            if kind == "many":
                full = [w for w in warnings if re.search(r"worker ports in use (\d+) of \1\b", w)]
                if verdict_word == "PASS" and (not full or refused):
                    verdict_word, reasons = "FAIL", reasons + [f"no `ports in use N of N` WARNING ({len(full)}) or refused forks {refused}"]
            outcomes.append((pair, verdict_word, reasons))
            record.data(pair, {"verdict": verdict_word, "reasons": reasons, "job_status": status, "job": job_line, "refused_forks": refused,
                               "warnings": warnings[:5], "params": next((x for x in log.splitlines() if x.startswith("C10_PARAMS")), None)})
    failed = [pair for pair, word, _ in outcomes if word != "PASS"]
    for pair, verdict_word, reasons in outcomes:
        record.check(f"{pair}: {verdict_word}", verdict_word == "PASS", "; ".join(reasons)[:300])
    if where == "H1":  # the diagnostics: the "If it fails" steps of each library page, during the serve.many hang
        record.evidence("worker_err.txt", diag.get("no_port_text", ""))
        record.evidence("job_log_during.txt", diag.get("job_log", ""))
        record.evidence("grep_process_limit.txt", diag.get("grep", ""))
        record.data("err_files", diag.get("err_files"))
        named = [x for x in diag.get("grep", "").splitlines() if WARNING_PATTERN.search(x) and f"(instance {node['container_id']}, " in x]
        for name, ok, detail in (
                ("diagnostics step 3: the dashboard log API opens a worker-*.err with `No available ports`", "no_port_file" in diag,
                 diag.get("no_port_file")),
                ("diagnostics step 2: `ray job logs` shows serve.run that does not return (serve.md:101), the job RUNNING",
                 diag.get("job_status") == "RUNNING" and "C10_PARAMS" in diag.get("job_log", "") and "C10_JOB" not in diag.get("job_log", ""),
                 diag.get("job_status")),
                ("diagnostics step 1: the `grep \"process limit\"` through attach shows the WARNING that names the worker", bool(named),
                 named[-1][:300] if named else diag.get("grep", "")[-300:])):
            if not record.check(name, ok, detail):
                failed.append(name)
    # Refused forks, a measurement (data, no check): a task starts threads until EAGAIN; the next heartbeats report the
    # refused count to the head; then the node lives on, or the bootstrap reports `ray start exited` and destroys it.
    start = log_mark(libraries)
    status, log = libraries.job(env, workdir(JOBS / "lb_forks.py"), ["python", "lb_forks.py", node["node_id"]], out_dir=record.dir,
                                name="forks", timeout=1200)
    time.sleep(120)
    lines = libraries.monitor_log(start)
    record.evidence("forks_monitor_slice.log", "\n".join(lines))
    found = result_line(log, "LB_RESULT")
    refused = [w for w in node_warnings(lines, node["container_id"], node["ip"]) if re.search(r"[1-9]\d* refused forks/threads", w)]
    alive = node["node_id"] in [n for ids in alive_workers(libraries).values() for n in ids]
    record.data("refused_forks", {"job": found[2] if found else status, "warning": refused[-1][:300] if refused else None,
                                  "outcome": "node alive" if alive else ("destroyed itself" if any("ray start exited" in x for x in lines)
                                                                         else "gone")})
    assert not failed, f"not PASS: {failed}"
