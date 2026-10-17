"""Group `libraries` (docs/test-plan.md, claim matrix LB00-LB11): the Ray libraries on vast, with the snippets of the user
docs (docs/libraries/*.md) taken from the docs at run time (libraries_setup.doc_snippet), the C9 jobs and the C10 pairs.

One cluster for each host (clusters/libraries.yaml with one worker type, admin profile, us-west-2), so the hosts run in
parallel with `pytest -n` (each host is one xdist group): H1 = 1 `h1` worker (64 or more Ray CPUs, so more CPUs than
B = 54, any pids limit), H2 = 1 `h2` worker (the same on cgroup v2), H3 = 2 `gpu` workers, gpu2 = 1 `gpu2` worker
(2 GPUs), H4 = 1 `h4` worker (4 or more visible CPUs for each core of its CPU quota: LB15). A test names its host with the
`where` parameter or the `host` marker. Each test sets the workers that it needs with `libraries.ensure_shape()` (only
these workers run), so the library code needs no node pinning. H1, H2 and H4 come from `libraries.find()` (probe, at
most 3 rentals). The tests of one host run one after the other, in the file order (destructive scenarios last), so a "many" run
of LB11 is alone on its host:
  H3:   LB00, LB01, LB02, LB04, LB05, LB06, LB09, LB10, LB03
  gpu2: LB00, LB10, LB07
  H1:   LB00, LB03, LB11 (with the diagnostics during serve.many)
  H2:   LB00, LB11
  H4:   LB15
The teardown of each host's cluster (conftest.group_lifetime) checks the `down` and the S3 cleanup.
S3: for each host that needs it (H3, gpu2, H1), one bucket `ray-vast-rvtest-<date>-<hex>` and an IAM user
`ray-vast-livetest-<date>-s3-<host>` that can use only that bucket (`libraries.s3()`). Its keys go to the jobs in
the runtime env of the job; they are never written (the `*_submit.txt` files are redacted).
In parallel: about 4.5 h (H1 with LB11 is the longest); vast about $5.5 ($4 to $8), EC2 about $1.6 (5 heads)."""
import itertools
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

import livejob
from cluster import BIN, JOBS, MANUAL, free_port, wait_port
from libraries_setup import HOST_TYPE, PINS, alive_workers, b_and_cpus, doc_snippet, expected_limits, host_facts, probe
from liverun import wait_until

sys.path.insert(0, str(MANUAL / "c10"))
from run_c10 import TORCH_ENV, events_max, verdict  # noqa: E402  (the C10 runner)

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


# --- helpers ------------------------------------------------------------------------------------------------------------
def c9(cluster, env, record, script, torch=False, timeout=1800, env_vars=None):
    """One C9 job (working dir tests/manual/c9/jobs, as run_c9.py). Returns (word, data) of its C9_RESULT line."""
    runtime_env = {**(TORCH_ENV if torch else {}), **({"env_vars": env_vars} if env_vars else {})}
    job = cluster.job(env, ["python", script], record.dir, name=script.removesuffix(".py"), working_dir=C9_JOBS,
                      runtime_env=runtime_env, prefix="C9_RESULT", timeout=timeout)
    return job.word, job.data


def snippet(doc, index, subs, record, name):
    """The doc snippet with its substitutions; both go into the evidence."""
    code = doc_snippet((DOCS / "libraries" / doc).read_text(), "## Settings", index, subs)
    record.evidence(f"{name}_snippet.py", code)
    record.data(f"{name}_substitutions", [{"doc": f"libraries/{doc}", "old": old, "new": new} for old, new in subs])
    return code


def snippet_job(cluster, env, record, lib, code, name, runtime_env=None, timeout=1800, s3=None, extra_env=None):
    """Run a doc snippet with jobs/lb_snippet.py (with the S3 keys of `s3` in the env vars, redacted). Returns the Job."""
    wd = livejob.workdir(JOBS / "lb_snippet.py")
    (wd / "snippet.py").write_text(code)
    env_vars = {**(s3.env_vars if s3 else {}), "LB_GUARD_S": str(timeout - 120), **(extra_env or {})}
    return cluster.job(env, ["python", "lb_snippet.py", lib], record.dir, name=name, working_dir=wd,
                       runtime_env={**(runtime_env or {}), "env_vars": env_vars}, secrets=[s3.secret] if s3 else [],
                       prefix="LB_RESULT", timeout=timeout)


def warnings_for(w, rows):
    """The head process-limit WARNINGs (window events) of these worker nodes (NODE_PROBE rows) since the window `w` opened."""
    return w.events("process_limit", instances=[r["container_id"] for r in rows])


def with_refused_forks(warnings):
    """The WARNING lines with new refused forks or thread starts (`(+N)`, N >= 1)."""
    return [e.line for e in warnings if e.fields["refused_new"] > 0]


def h3(cluster, env, record, name="probe_h3"):
    """The H3 shape (2 `gpu` workers): (rows, B, CPUS)."""
    record.data("shape_wait_s", cluster.ensure_shape({"gpu": 2}))
    rows = probe(cluster, env, record.dir, name)["gpu"]
    b, cpus = b_and_cpus(rows)
    record.data("H3", {"B": b, "CPUS": cpus, "hosts": [{k: r.get(k) for k in ("container_id", "machine_id", "ports", "ray_cpus", "pids.max")}
                                                        for r in rows]})
    return rows, b, cpus


def host(cluster, env, record, name):
    """H1, H2 or H4 alone: reuse the matched host when it still runs, else find one. Returns its NODE_PROBE row."""
    kind = {"H1": "h1", "H2": "h2", "H4": "h4"}[name]
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
    w = libraries.window()
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
        record.check("H1: 64 or more Ray CPUs (more than B), a pids limit", hosts["H1"][0]["ray_cpus"] >= 64 and hosts["H1"][0].get("pids.max") not in (None, "max"),
             {k: hosts["H1"][0].get(k) for k in ("machine_id", "ray_cpus", "pids.max", "cgroup")})
    if where == "H2":
        record.check("H2: cgroup v2", hosts["H2"][0]["cgroup"] == "v2", {k: hosts["H2"][0].get(k) for k in ("machine_id", "pids.max", "cgroup")})
    enroll = w.enroll_seconds()
    for name, rows in hosts.items():
        for n in rows:
            cid = n["container_id"]
            ports, soft_limit = expected_limits(n)
            env_ray = n.get("ray_env") or {}
            enroll_s = enroll.get(int(cid))
            record.data(f"{name}_{cid}", {**{k: n.get(k) for k in ("machine_id", "pids.max", "cgroup", "cpu_quota", "affinity", "ray_cpus",
                                                                   "ports", "ray_env", "worker_threads", "cpu_count")},
                                          "enroll_s_from_first_running_sample": enroll_s})
            record.check(f"{name} {cid}: worker ports of the bootstrap formula (pids.max {n.get('pids.max')})", n.get("ports") == ports,
                 (n.get("ports"), ports))
            record.check(f"{name} {cid}: soft limit of the bootstrap formula, grpc threads 4, startup concurrency 8",
                 env_ray.get("RAY_num_workers_soft_limit") == str(soft_limit) and
                 env_ray.get("RAY_num_grpc_internal_threads") == "4" and env_ray.get("RAY_worker_maximum_startup_concurrency") == "8", env_ray)
            declared = libraries.config["available_node_types"][n["node_type"]]["resources"]["CPU"]
            quota = host_facts(n, n.get("machine_id"))["quota"]
            cores = n["affinity"] if quota is None else min(n["affinity"], quota)
            record.check(f"{name} {cid}: Ray CPUs = the declared CPU, the cores (affinity, quota) at least 90% of it",
                 n["ray_cpus"] == declared and cores >= 0.9 * declared, (n["ray_cpus"], declared, n["affinity"], quota))
            # design.md:56, :86: enrollment within 1 min of `running`; the suite's 10 s samples allow -15 to 75 s.
            record.check(f"{name} {cid}: enrolled within 1 min of `running` (design.md:86; -15 to 75 s on the suite's samples)",
                 enroll_s is not None and -15 <= enroll_s <= 75, enroll_s)


@pytest.mark.host("H3")
def test_LB01_jobs_workflow(record, libraries, env):
    h3(libraries, env, record)
    word, data = c9(libraries, env, record, "c9_0_2_hello.py", timeout=900)
    record.data("hello", data)
    record.check("--working-dir: data/hello.txt read by tasks on both workers", word == "PASS", data)
    word, data = c9(libraries, env, record, "c9_0_3_torch_cuda.py", torch=True, timeout=4200)
    record.data("torch_cuda", data)
    record.check("runtime_env pip torch==2.7.1: CUDA on both GPU workers", word == "PASS", data)
    sleeper = libraries.job(env, ["python", "c9_0_4_sleeper.py"], record.dir, name="sleeper", working_dir=C9_JOBS, wait=False)
    sid = sleeper.sid
    running = wait_until(sleeper.status_now, 300, done=lambda status: status == "RUNNING", poll_s=5)
    record.check("the sleeper job is RUNNING (within 300 s)", running.ok, running.seen or running.error)
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
    stopped = sleeper.stop()
    status = sleeper.wait(120).status
    record.evidence("job_cli.txt", f"== ray job list (exit {listed.returncode})\n{listed.stdout}{listed.stderr}\n"
                                   f"== ray job stop (exit {stopped.returncode})\n{stopped.stdout}{stopped.stderr}\n")
    ticks = (record.dir / "follow.out").read_text().count("C9_SLEEPER tick")
    record.check("ray job list shows the job", sid in listed.stdout, listed.returncode)
    record.check("ray job logs --follow streams (>= 3 ticks in 30 s)", ticks >= 3, ticks)
    record.check("ray job stop: exit 0, then STOPPED", stopped.returncode == 0 and status == "STOPPED", (stopped.returncode, status))


@pytest.mark.limits
@pytest.mark.host("H3")
def test_LB02_core_functions_and_the_core_snippet(record, libraries, env):
    rows, b, cpus = h3(libraries, env, record)
    for script, claim in (("c9_5_1_placement.py", "PACK on one worker, STRICT_SPREAD on both"),
                          ("c9_5_2_actor_fault.py", "actors restart after os._exit and ray.kill"),
                          ("c9_5_3_large_object.py", "a 2 GB object between the hosts")):
        word, data = c9(libraries, env, record, script, timeout=2100)
        record.data(script.removesuffix(".py"), data)
        record.check(claim, word == "PASS", data)
    share = round(cpus / b, 3)
    code = (snippet("core.md", 1, [], record, "cpus") +
            snippet("core.md", 0, [("@ray.remote(num_cpus=2.4)          # CPUS / B\ndef child",
                                    f"@ray.remote(num_cpus={share})          # CPUS / B\ndef child"),
                                   ("@ray.remote(num_cpus=2.4)          # CPUS / B\nclass Runner",
                                    f"@ray.remote(num_cpus={share})          # CPUS / B\nclass Runner"),
                                   ("range(5)", f"range({b // 2})")], record, "core"))
    w = libraries.window()
    job = snippet_job(libraries, env, record, "core", code, "core_snippet", timeout=1500)
    record.evidence("monitor_slice.log", "\n".join(w.lines()))
    record.data("core_snippet", job.data)
    record.check(f"the core snippet (num_cpus {share}, pool {b // 2}) completes", job.status == "SUCCEEDED" and job.word == "PASS",
         (job.status, job.data))
    warnings = [e.line for e in warnings_for(w, rows)]
    record.check("no process-limit WARNING for the H3 workers", not warnings, warnings[:2])


@pytest.mark.limits
@pytest.mark.host("H3")
def test_LB04_data_shuffles_with_the_tuned_settings(record, libraries, env):
    rows, b, cpus = h3(libraries, env, record)
    for mode, torch in (("shuffle", False), ("gpu", True)):
        job = libraries.job(env, ["python", "lb_data_tuned.py", mode], record.dir, name=f"data_{mode}",
                            working_dir=livejob.workdir(JOBS / "lb_data_tuned.py", C9_JOBS / "c9common.py"),
                            runtime_env={**(TORCH_ENV if torch else {}), "env_vars": {"LB_B": str(b)}}, prefix="C9_RESULT", timeout=2700)
        record.data(mode, job.data)
        record.check(f"C9.1.{'3' if mode == 'shuffle' else '4'} with the data.md settings at B {b}: SUCCEEDED",
             job.status == "SUCCEEDED" and job.word == "PASS", (job.status, job.data))


@pytest.mark.host("H3")
def test_LB05_data_defaults_without_a_shuffle(record, libraries, env):
    h3(libraries, env, record)
    for script in ("c9_1_1_data_cpu.py", "c9_1_2_data_objstore.py", "c9_1_5_data_io.py", "c9_1_6_data_many.py"):
        word, data = c9(libraries, env, record, script, timeout=2700)
        record.data(script.removesuffix(".py"), data)
        record.check(f"{script} with the defaults", word == "PASS", data)
    io = record.result["data"]["c9_1_5_data_io"]
    record.check("a local output path stays on the workers: the driver reads none of it",
         sum(v["files"] for v in io["files_per_node"].values()) > 0 and ("readback_error" in io or io.get("readback_rows") == 0), io)


@pytest.mark.limits
@pytest.mark.host("H3")
def test_LB06_tune_snippet_with_s3(record, libraries, env, s3):
    rows, b, cpus = h3(libraries, env, record)
    share = round(cpus / b, 3)
    code = snippet("tune.md", 0, [('{"cpu": 2.4}', f'{{"cpu": {share}}}'), ("max_concurrent_trials=5", f"max_concurrent_trials={b // 2}"),
                                  ('"s3://my-bucket/experiments"', f'"s3://{s3.bucket}/lb06/experiments"')], record, "tune")
    w = libraries.window()
    job = snippet_job(libraries, env, record, "tune", code, "tune_snippet", timeout=2400, s3=s3)
    word, data = job.word, job.data
    record.data("tune_snippet", data)
    checkpoints = {o["Key"].split("/checkpoint_")[0] for o in s3.objects("lb06/experiments/") if "/checkpoint_" in o["Key"]}
    record.data("s3_trials_with_checkpoints", len(checkpoints))
    record.check("the tune.md snippet: 60 trials, 0 errors", word == "PASS" and data.get("results") == 60 and data.get("errors") == 0, data)
    record.check("trials on both workers", len([n for n in data.get("trial_nodes", []) if n]) >= 2, data.get("trial_nodes"))
    record.check("checkpoints in S3 for every trial", len(checkpoints) == 60, len(checkpoints))
    forks = with_refused_forks(warnings_for(w, rows))
    record.check("no refused fork in a WARNING for the H3 workers", not forks, forks[:2])
    for script, claim, torch in (("c9_2_2_tune_asha.py", "ASHA stops trials early", False),
                                 ("c9_2_3_tune_gpu.py", "GPU trials get 1 GPU each, on both workers", False)):
        word, data = c9(libraries, env, record, script, torch=torch, timeout=1500)
        record.data(script.removesuffix(".py"), data)
        record.check(claim, word == "PASS", data)
    word, data = c9(libraries, env, record, "c9_2_4_tune_ckpt.py", timeout=1500)
    record.data("default_storage", data)
    # The job cuts each trial error to 300 characters in its result line; the full error is in the job log.
    log = (record.dir / "c9_2_4_tune_ckpt.log").read_text()
    shutil.copy(record.dir / "c9_2_4_tune_ckpt.log", record.dir / "c9_2_4_tune_ckpt_default.log")
    record.check("the default storage_path: `Unable to set up cluster storage ... not accessible by worker nodes` (tune.md:83)",
         word == "LIMIT" and "Unable to set up cluster storage" in log and "not accessible by worker nodes" in log,
         (word, data.get("errors"), data.get("experiment_path")))
    word, data = c9(libraries, env, record, "c9_2_4_tune_ckpt.py", timeout=1500, env_vars={"C9_STORAGE": f"s3://anonymous@{s3.bucket}/lb06/anon"})
    record.data("anonymous_s3", data)
    record.check("anonymous S3 through pyarrow: S3 refuses the uploads (tune.md:84)",
         word == "LIMIT" and re.search(r"(?i)access_?denied|access denied|forbidden|\b403\b",
                                        json.dumps(data) + (record.dir / "c9_2_4_tune_ckpt.log").read_text()), data)


@pytest.mark.limits
@pytest.mark.host("H3")
def test_LB09_serve_snippet_http_and_autoscaling(record, libraries, env):
    rows, b, cpus = h3(libraries, env, record)
    share, replicas = round(cpus / b, 3), b * len(rows)
    code = snippet("serve.md", 0, [("num_replicas=10,", f"num_replicas={replicas},"), ('{"num_cpus": 2.4}', f'{{"num_cpus": {share}}}')],
                   record, "serve")
    w = libraries.window()
    job = snippet_job(libraries, env, record, "serve", code, "serve_snippet", timeout=1500)
    record.data("serve_snippet", job.data)
    per_node = job.data.get("replica_nodes", {})
    record.check(f"the serve.md snippet: {replicas} replicas, at most B = {b} on each worker", job.status == "SUCCEEDED" and
         job.word == "PASS" and sum(per_node.values()) == replicas and max(per_node.values(), default=b + 1) <= b, (job.status, per_node))
    threads = probe(libraries, env, record.dir, "probe_with_replicas")["gpu"]
    record.data("worker_threads_with_replicas", {r["container_id"]: r["worker_threads"] for r in threads})
    # HTTP through the tunnel of serve.md, as written. Changed: the cluster name, the head IP, `~` (ssh expands `~` in
    # UserKnownHostsFile from the passwd home, not $HOME, and the suite keeps its state in a private HOME), and the local
    # port if 18000 is in use.
    line = next(x for x in (DOCS / "libraries" / "serve.md").read_text().splitlines()
                if x.startswith("ssh -i ~/.ray-vast/<cluster_name>/ssh_key.pem"))
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
    record.check("100 HTTP calls through the serve.md tunnel after the job ended: all 200", codes == [200] * 100,
         {str(c): codes.count(c) for c in set(codes)})
    head = libraries.ssh("curl -s -o /dev/null -m 15 -w '%{http_code}' http://127.0.0.1:8000/", check=False).strip()
    record.check("curl on the head, port 8000: 200", head == "200", head)
    libraries.job(env, ["python", "c10_cleanup.py"], record.dir, name="serve_cleanup", working_dir=C10_JOBS, timeout=300)
    forks = with_refused_forks(warnings_for(w, rows))
    record.check("no refused fork in a WARNING for the H3 workers", not forks, forks[:2])
    word, data = c9(libraries, env, record, "c9_4_4_serve_gpu.py", torch=True, timeout=2700)
    record.data("serve_gpu", data)
    record.check("GPU replicas: 1 GPU id each, on both workers", word == "PASS", data)
    job = libraries.job(env, ["python", "lb_serve_autoscale.py"], record.dir, name="serve_autoscale",
                        working_dir=livejob.workdir(JOBS / "lb_serve_autoscale.py"),
                        runtime_env={"env_vars": {"LB_B": str(b), "LB_CPUS": str(cpus), "LB_WORKERS": str(len(rows))}},
                        prefix="LB_RESULT", timeout=1200)
    record.data("serve_autoscale", job.data)
    record.check(f"autoscaling: scales out, up to max_replicas {replicas}, at most B on each worker", job.word == "PASS",
         {k: job.data.get(k) for k in ("peak_replicas", "per_node_max", "max_replicas")})
    libraries.job(env, ["python", "c10_cleanup.py"], record.dir, name="serve_cleanup_2", working_dir=C10_JOBS, timeout=300)


@pytest.mark.limits
@pytest.mark.parametrize("where", ["H3", "gpu2"])
def test_LB10_rllib_env_runners_and_learners(record, libraries, env, where):
    if where == "H3":
        rows, b, cpus = h3(libraries, env, record)
        share = round(cpus / b, 3)
        code = snippet("rllib.md", 0, [("num_cpus_per_env_runner=2.4", f"num_cpus_per_env_runner={share}")], record, "rllib_runners")
        w = libraries.window()
        job = snippet_job(libraries, env, record, "rllib_runners", code, "rllib_runners", runtime_env=TORCH_ENV, timeout=2700)
        record.data("rllib_runners", job.data)
        record.check(f"the env runner snippet (2 runners, num_cpus {share}): 2 iterations", job.word == "PASS", job.data)
        warnings = [e.line for e in warnings_for(w, rows)]
        record.check("no process-limit WARNING for the H3 workers", not warnings, warnings[:2])
        return
    record.data("shape_wait_gpu2_s", libraries.ensure_shape({"gpu2": 1}))
    gpu2 = probe(libraries, env, record.dir, "probe_gpu2")["gpu2"]
    b2, cpus2 = b_and_cpus(gpu2)
    share2 = round(cpus2 / b2, 3)
    record.data("num_cpus_per_learner", share2)
    code = snippet("rllib.md", 1, [("num_cpus_per_learner=2.4", f"num_cpus_per_learner={share2}"),
                                   ("tune.Tuner(config.algo_class", "grid = tune.Tuner(config.algo_class")], record, "rllib_learners")
    job = snippet_job(libraries, env, record, "rllib_learners", code, "rllib_learners", runtime_env=TORCH_ENV, timeout=2700)
    record.data("rllib_learners", job.data)
    record.check(f"the learner snippet on gpu2 (num_cpus_per_learner {share2}): 2 iterations, 0 errors", job.word == "PASS", job.data)


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
    w = libraries.window()
    job = snippet_job(libraries, env, record, "train", code, "train_snippet", runtime_env=TORCH_ENV, timeout=3600, s3=s3)
    status, word, data = job.status, job.word, job.data
    ranks = job.lines("LB_RANK")
    last = [r for r in ranks if r["epoch"] == 1]
    record.data("train_ranks", ranks)
    gpu2_node = gpu2[0]["node_id"][:8]
    record.check("the train.md snippet: fit returns", status == "SUCCEEDED" and word == "PASS", (status, data))
    record.check("2 epochs on 2 ranks on the gpu2 worker, cuda, one GPU id each, 2 different GPUs",
         len(last) == 2 and {r["node"] for r in last} == {gpu2_node} and all(r["device"].startswith("cuda") and len(r["gpu_ids"]) == 1 for r in last)
         and len({r["gpu_ids"][0] for r in last}) == 2, last)
    checkpoints = [o["Key"] for o in s3.objects("lb07/experiments/") if "/checkpoint_" in o["Key"]]
    record.check("checkpoints in S3", len(checkpoints) > 0, len(checkpoints))
    warnings = [e.line for e in warnings_for(w, gpu2)]
    record.check("no process-limit WARNING for the gpu2 worker", not warnings, warnings[:2])
    word, data = c9(libraries, env, record, "c9_3_1_train_1gpu.py", torch=True, timeout=2700)
    record.data("default_storage", data)
    record.check("1 GPU with the default storage_path: `A local path on the head node is not accessible by worker nodes`",
         word == "LIMIT" and "not accessible by worker nodes" in json.dumps(data) + (record.dir / "c9_3_1_train_1gpu.log").read_text(),
         data)


@pytest.mark.limits
@pytest.mark.parametrize("where", ["H3", "H1"])
def test_LB03_data_snippet_verbatim(record, libraries, env, s3, where):
    if where == "H3":
        rows, b, cpus = h3(libraries, env, record)
    else:
        rows = [host(libraries, env, record, "H1")]
        b, cpus = b_and_cpus(rows)
    # The input: about 3 GB of parquet in s3://<bucket>/lb03/in (LB03 runs once on each host), written by a job on the
    # host of the test. So it runs after the host is in place: the cluster can have no worker before (2026-10-17: after
    # LB00[H1] found no host, the input job made no progress for 1800 s).
    parquet_in = {"path": f"s3://{s3.bucket}/lb03/in", "rows": PARQUET_ROWS}
    job = libraries.job(env, ["python", "lb_make_parquet.py", parquet_in["path"], str(PARQUET_ROWS)], record.dir, name="make_parquet",
                        working_dir=livejob.workdir(JOBS / "lb_make_parquet.py"), runtime_env={"env_vars": s3.env_vars},
                        secrets=[s3.secret], prefix="LB_RESULT", timeout=3600)
    parquet_in["bytes"] = sum(o["Size"] for o in s3.objects("lb03/in/"))
    record.data("input", {**parquet_in, "status": job.status, "result": [job.word, job.data]})
    record.require("the LB03 input is written (lb_make_parquet.py)", job.word == "PASS", (job.status, job.word, job.data))
    out = f"s3://{s3.bucket}/lb03/out-{where}"
    code = snippet("data.md", 0, [("max_hash_shuffle_aggregators = 2", f"max_hash_shuffle_aggregators = {b // 4}"),
                                  ("ExecutionResources(cpu=2)", f"ExecutionResources(cpu={b // 4})"),
                                  ('"s3://my-bucket/in"', f'"{parquet_in["path"]}"'),
                                  ("concurrency=2,", f"concurrency={b // 4},"),
                                  ("override_num_blocks=5)", f"override_num_blocks={b // 2})"),
                                  ("TaskPoolStrategy(size=2)", f"TaskPoolStrategy(size={b // 4})"),
                                  ('"s3://my-bucket/out"', f'"{out}"')], record, "data")
    w = libraries.window()
    job = snippet_job(libraries, env, record, "data", code, "data_snippet", timeout=3600, s3=s3,
                      extra_env={"LB_OUT": out, "LB_ROWS": str(parquet_in["rows"])})
    status, word, data, log = job.status, job.word, job.data, job.log
    lines = w.lines()
    record.evidence("monitor_slice.log", "\n".join(lines))
    record.data("data_snippet", data)
    after = probe(libraries, env, record.dir, "probe_after")
    alive = {r["node_id"] for rs in after.values() for r in rs}
    record.check(f"the data.md snippet at B {b} on {where}: SUCCEEDED, all rows counted from the S3 output",
         status == "SUCCEEDED" and word == "PASS", (status, data))
    record.check("output in S3", len(s3.objects(out.split("/", 3)[3] + "/")) > 0, out)
    forks = with_refused_forks(w.events("process_limit", instances=[r["container_id"] for r in rows], lines=lines))
    record.check("no refused fork in a WARNING", not forks, forks[:2])
    record.check("no node crash: the same Ray nodes are alive", {r["node_id"] for r in rows} <= alive, "")
    record.check("no out-of-memory error", not re.search(r"OutOfMemoryError|out of memory", log), "")


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
    """c10_probe.py: {node id: read_limits()}; again every 30 s for up to 10 min while a node gives an error (or no row).
    Still an error after 10 min: a RuntimeError (the pair cannot be judged)."""
    attempts = itertools.count()

    def look():
        rows = cluster.job(env, ["python", "c10_probe.py"], out_dir, name=f"{name}_{next(attempts)}", working_dir=wd,
                           timeout=300).lines("C10_PROBE")
        return rows[-1] if rows else {}

    waited = wait_until(look, 600, done=lambda found: found and not any("error" in v for v in found.values()), poll_s=30)
    if not waited:
        raise RuntimeError(f"c10 probe {name}: no answer without an error in 600 s (last {waited.seen}, {waited.error})")
    return waited.seen


def diagnose(cluster, env, node, job, out):
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
    out["job_status"] = job.status_now()
    out["job_log"] = job.logs()
    done = cluster.cli("attach", cluster.yaml, input='sudo docker exec ray-vast-head grep "process limit" '
                       '/tmp/ray/session_latest/logs/monitor.log; echo GREP_EXIT $?\nexit\n', timeout=180)
    out["grep"] = done.stdout + done.stderr


@pytest.mark.limits
@pytest.mark.parametrize("where", ["H1", "H2"])
def test_LB11_process_limit_pairs_on_h1_and_h2(record, libraries, env, where):
    """The pairs of each library; on H1 the diagnostics during serve.many; then, as a measurement only, a task that starts
    threads until the container refuses one (the refused forks, the head WARNING, and whether the node lives on)."""
    node = host(libraries, env, record, where)
    limit = host_facts(node, node["machine_id"])["pids_max"]
    record.require(f"{where} has a pids limit", limit is not None, node.get("pids.max"))
    record.data("host", {k: node.get(k) for k in ("machine_id", "container_id", "pids.max", "cgroup", "ray_cpus", "ports")})
    wd = livejob.workdir(*C10_JOBS.glob("*.py"), JOBS / "lb_tuned.py")
    outcomes, diag = [], {}
    for lib, tuned, many, torch, timeout in PAIRS:
        for kind, (script, arg) in (("tuned", tuned), ("many", many)):
            pair = f"{lib}.{kind}"
            pre = c10_probe(libraries, env, record.dir, wd, f"{pair}_pre")
            target = next(iter(pre)) if len(pre) == 1 else None
            record.require(f"{pair}: one worker node", target is not None, len(pre))
            w = libraries.window()
            job = libraries.job(env, ["python", script, arg], record.dir, name=pair, working_dir=wd,
                                runtime_env={**(TORCH_ENV if torch else {}),
                                             "env_vars": {"C10_PORTS": str(pre[target]["ports"]), "C10_CPUS": str(pre[target]["CPU"])}},
                                prefix="C10_RESULT", timeout=timeout, wait=False)
            if where == "H1" and pair == "serve.many":
                diagnose(libraries, env, node, job, diag)
            status, log = job.wait().status, job.log
            libraries.job(env, ["python", "c10_cleanup.py"], record.dir, name=f"{pair}_cleanup", working_dir=wd, timeout=300)
            time.sleep(30)  # the raylet ends the job's worker processes
            post = c10_probe(libraries, env, record.dir, wd, f"{pair}_post")
            lines = w.lines()
            record.evidence(f"{pair}_head_slice.log", "\n".join(lines))
            events = w.events("process_limit", instances=[node["container_id"]], lines=lines)
            warnings = [e.line for e in events]
            job_line = {"kind": job.word, **job.data}
            # run_c10 calls a many run INCONCLUSIVE above 3328 (its phase A rule). The many sizes here are max(ports + 4, ...),
            # above the ports at any limit, so the limit of this host is accepted.
            # A refused fork fails a tuned run; in a many run it is data (2026-10-17): `refused_forks` of each pair.
            verdict_word, reasons = verdict(kind, job_line, pre.get(target), post.get(target), warnings, limit_max=max(3328, limit))
            refused = (events_max(post.get(target)) or 0) - (events_max(pre.get(target)) or 0)
            if kind == "many":
                full = [e.line for e in events if e.fields["ports_in_use"] == e.fields["ports_total"]]
                if verdict_word == "PASS" and not full:
                    verdict_word, reasons = "FAIL", reasons + ["no `ports in use N of N` WARNING"]
            outcomes.append((pair, verdict_word, reasons))
            record.data(pair, {"verdict": verdict_word, "reasons": reasons, "job_status": status, "job": job_line, "refused_forks": refused,
                               "warnings": warnings[:5], "params": next((x for x in log.splitlines() if x.startswith("C10_PARAMS")), None)})
    for pair, verdict_word, reasons in outcomes:
        record.check(f"{pair}: {verdict_word}", verdict_word == "PASS", "; ".join(reasons)[:300])
    if where == "H1":  # the diagnostics: the "If it fails" steps of each library page, during the serve.many hang
        record.evidence("worker_err.txt", diag.get("no_port_text", ""))
        record.evidence("job_log_during.txt", diag.get("job_log", ""))
        record.evidence("grep_process_limit.txt", diag.get("grep", ""))
        record.data("err_files", diag.get("err_files"))
        named = [e.line for e in w.events("process_limit", instances=[node["container_id"]], lines=diag.get("grep", "").splitlines())]
        for name, ok, detail in (
                ("diagnostics step 3: the dashboard log API opens a worker-*.err with `No available ports`", "no_port_file" in diag,
                 diag.get("no_port_file")),
                ("diagnostics step 2: `ray job logs` shows serve.run that does not return (serve.md:101), the job RUNNING",
                 diag.get("job_status") == "RUNNING" and "C10_PARAMS" in diag.get("job_log", "") and "C10_RESULT" not in diag.get("job_log", ""),
                 diag.get("job_status")),
                ("diagnostics step 1: the `grep \"process limit\"` through attach shows the WARNING that names the worker", bool(named),
                 named[-1][:300] if named else diag.get("grep", "")[-300:])):
            record.check(name, ok, detail)
    # Refused forks, a measurement (data, no check): a task starts threads until EAGAIN; the next heartbeats report the
    # refused count to the head; then the node lives on, or the bootstrap reports `ray start exited` and destroys it.
    w = libraries.window()
    forks = libraries.job(env, ["python", "lb_forks.py", node["node_id"]], record.dir, name="forks", working_dir=livejob.workdir(JOBS / "lb_forks.py"),
                          prefix="LB_RESULT", timeout=1200)
    time.sleep(120)
    lines = w.lines()
    record.evidence("forks_monitor_slice.log", "\n".join(lines))
    refused_lines = [e.line for e in w.events("process_limit", instances=[node["container_id"]], lines=lines) if e.fields["refused"] > 0]
    destroyed = any("ray start exited" in e.reason for e in w.events("destroy_request", instances=[node["container_id"]], lines=lines))
    alive = node["node_id"] in [n for ids in alive_workers(libraries).values() for n in ids]
    record.data("refused_forks", {"job": forks.data, "warning": refused_lines[-1][:300] if refused_lines else None,
                                  "outcome": "node alive" if alive else ("destroyed itself" if destroyed else "gone")})


# --- LB15: threads per process (a measurement) -----------------------------------------------------------------------
THREAD_WORKLOADS = [("data", False), ("train", True), ("rllib", True), ("task", True)]  # (workload of lb_threads.py, torch)
FIX_THREADS = 2  # the fix of rule 2: the thread pools of Arrow, torch and OpenMP get this many threads in each process


@pytest.mark.limits
@pytest.mark.host("H4")
def test_LB15_threads_per_process_on_a_host_with_many_visible_cpus(record, libraries, env):
    """Rule 2 of the library pages (threads and child processes) on a container that sees 4 or more CPUs for each core of
    its CPU quota. Each workload of jobs/lb_threads.py runs with Ray's defaults and then with the fixes (OMP_NUM_THREADS,
    pyarrow.set_cpu_count, torch.set_num_threads). The threads of each process, the pids.current peak and the refused
    forks are the data. The check: each measurement is recorded. A fixed run with a Ray worker process above 60 threads
    is recorded (`<workload>.fixed_over_60`), not a failure: then the docs need another fix."""
    node = host(libraries, env, record, "H4")
    facts = host_facts(node, node["machine_id"])
    record.data("host", {**facts, **{k: node.get(k) for k in ("container_id", "cpu_count", "affinity", "ports", "ray_cpus")}})
    record.require("H4: 4 or more visible CPUs for each core of the CPU quota, a pids limit",
                   (facts["visible_per_quota"] or 0) >= 4 and facts["limited"], facts)
    wd = livejob.workdir(JOBS / "lb_threads.py", JOBS / "lb_threads_fix.py")
    for workload, torch in THREAD_WORKLOADS:
        for mode in ("default", "fixed"):
            env_vars = {"LB_NODE": node["node_id"], "LB_FIX_THREADS": str(FIX_THREADS)}
            if mode == "fixed":
                env_vars["OMP_NUM_THREADS"] = str(FIX_THREADS)
            job = libraries.job(env, ["python", "lb_threads.py", workload, mode], record.dir, name=f"threads_{workload}_{mode}",
                                working_dir=wd, runtime_env={**(TORCH_ENV if torch else {}), "env_vars": env_vars},
                                prefix="LB_RESULT", timeout=2400 if torch else 1200)
            record.data(f"{workload}.{mode}", job.data)
            record.check(f"{workload} {mode}: the workload completed and the threads of each process are recorded",
                         job.word == "PASS" and job.data.get("samples", 0) > 0 and job.data.get("kinds"),
                         (job.status, job.word, (job.data.get("workload") or {}) if isinstance(job.data, dict) else job.data))
            if mode == "fixed":
                record.data(f"{workload}.fixed_over_60", job.data.get("over_60"))
