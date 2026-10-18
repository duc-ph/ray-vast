"""The Ray libraries on vast, with the snippets of the user docs (docs/libraries/*.md) taken from the docs at run time
(libraries_setup.doc_snippet), the library jobs (jobs/) and the process-limit pairs (pairs.py).

One cluster for each host (clusters/libraries.yaml with one worker type, admin profile, us-west-2), so the hosts run in
parallel with `pytest -n` (each host is one xdist group). Every test names its host with the `where` parameter:
  gpu:       2 `gpu` workers: the multi-node tests (jobs, core, data, tune, serve, RLlib env runners, the data snippet).
  gpu2:      1 `gpu2` worker (2 GPUs): Train and the RLlib learners on 2 GPUs of one worker.
  big-a:     1 `big-a` worker (CPU 64, more than its processes at once): the data snippet, then the Data, Tune and Serve
             pairs, with the troubleshooting steps during the Serve hang.
  big-b:     1 `big-b` worker (the same kind): the Core, Train and RLlib pairs.
  few-ports: 1 `few-ports` worker (14 to 63 direct ports).
Each test sets the workers that it needs with `libraries.ensure_shape()` (only these workers run), so the library code
needs no node pinning. The tests of one host run one after the other, in the file order (the process-limit pairs last).
N is the number of processes at once of a worker: the start line of its container log gives it.
The teardown of each host's cluster (conftest.group_lifetime) checks the `down` and the S3 cleanup.
S3: for each host that needs it (gpu, gpu2, big-a), one bucket `ray-vast-rvtest-<date>-<hex>` and an IAM user
`ray-vast-livetest-<date>-s3-<host>` that can use only that bucket (`libraries.s3()`). Its keys go to the jobs in
the runtime env of the job; they are never written (the `*_submit.txt` files are redacted).
In parallel (`-n` of 6 or more: one process for each of the 5 hosts): about 1.5 h (gpu is the longest); vast about $5,
EC2 about $0.8 (5 heads)."""
import json
import re
import socket
import subprocess
import time
import urllib.request
from pathlib import Path

import pytest
from ray_vast.worker.bootstrap import worker_ports_and_soft_limit

import livejob
from cluster import BIN, JOBS, free_port, vast, wait_port
from libraries_setup import doc_snippet, expected_limits, host_facts, n_and_cpu, node_facts
from liverun import wait_until
from pairs import TORCH_ENV, events_max, verdict

pytestmark = pytest.mark.timeout(0)  # hours-long tests: no pytest-timeout (each job has its own)
DOCS = Path(__file__).resolve().parents[2] / "docs"
PARQUET_ROWS = 3_000_000  # about 3 GB: 1000 random bytes in each row
JOB_COMMON = JOBS / "jobcommon.py"


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
def library_job(cluster, env, record, script, torch=False, timeout=1800, env_vars=None):
    """One library job (jobs/<script> with jobs/jobcommon.py). Returns (word, data) of its JOB_RESULT line."""
    runtime_env = {**(TORCH_ENV if torch else {}), **({"env_vars": env_vars} if env_vars else {})}
    files = [JOBS / script, JOB_COMMON, JOBS / "trainjob.py", JOBS / "data"]
    job = cluster.job(env, ["python", script], record.dir, name=script.removesuffix(".py"), working_dir=livejob.workdir(*files),
                      runtime_env=runtime_env, prefix="JOB_RESULT", timeout=timeout)
    return job.word, job.data


def snippet(doc, index, subs, record, name):
    """The doc snippet with its substitutions; both go into the evidence."""
    code = doc_snippet((DOCS / "libraries" / doc).read_text(), "## Settings", index, subs)
    record.evidence(f"{name}_snippet.py", code)
    record.data(f"{name}_substitutions", [{"doc": f"libraries/{doc}", "old": old, "new": new} for old, new in subs])
    return code


def snippet_job(cluster, env, record, lib, code, name, runtime_env=None, timeout=1800, s3=None, extra_env=None):
    """Run a doc snippet with jobs/snippet_runner.py (with the S3 keys of `s3` in the env vars, redacted). Returns the Job."""
    wd = livejob.workdir(JOBS / "snippet_runner.py")
    (wd / "snippet.py").write_text(code)
    env_vars = {**(s3.env_vars if s3 else {}), "JOB_GUARD_S": str(timeout - 120), **(extra_env or {})}
    return cluster.job(env, ["python", "snippet_runner.py", lib], record.dir, name=name, working_dir=wd,
                       runtime_env={**(runtime_env or {}), "env_vars": env_vars}, secrets=[s3.secret] if s3 else [],
                       prefix="JOB_RESULT", timeout=timeout)


def warnings_for(w, rows):
    """The head process-limit WARNINGs (window events) of these worker nodes (NODE_FACTS rows) since the window `w` opened."""
    return w.events("process_limit", instances=[r["container_id"] for r in rows])


def with_refused_forks(warnings):
    """The WARNING lines with new refused forks or thread starts (`(+N)`, N >= 1)."""
    return [e.line for e in warnings if e.fields["refused_new"] > 0]


def gpu_workers(cluster, env, record, name="node_facts_gpu"):
    """The 2 `gpu` workers: (rows, N, CPU)."""
    record.data("shape_wait_s", cluster.ensure_shape({"gpu": 2}))
    rows = node_facts(cluster, env, record.dir, name)["gpu"]
    n, cpu = n_and_cpu(cluster, rows)
    record.data("gpu", {"N": n, "CPU": cpu, "hosts": [{k: r.get(k) for k in ("container_id", "machine_id", "ports", "ray_cpus", "pids.max")}
                                                       for r in rows]})
    return rows, n, cpu


def one_worker(cluster, env, record):
    """The one worker of the test's host (the node type of the same name): its NODE_FACTS row."""
    record.data("shape_wait_s", cluster.ensure_shape({cluster.host: 1}))
    return node_facts(cluster, env, record.dir, f"node_facts_{cluster.host}")[cluster.host][0]


# --- not destructive ----------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("where", ["gpu", "gpu2", "big-a"])
def test_worker_limits_match_the_bootstrap_formula(record, libraries, env, where):
    w = libraries.window()
    if where == "gpu":
        rows = gpu_workers(libraries, env, record)[0]
    else:
        rows = [one_worker(libraries, env, record)]
    record.evidence("node_facts.json", json.dumps(rows, indent=1))
    if where == "big-a":
        record.check("big-a: 64 or more Ray CPUs (more than N), a pids limit", rows[0]["ray_cpus"] >= 64 and
                     rows[0].get("pids.max") not in (None, "max"), {k: rows[0].get(k) for k in ("machine_id", "ray_cpus", "pids.max", "cgroup")})
    enroll = w.enroll_seconds()
    for n in rows:
        cid = n["container_id"]
        ports, soft_limit = expected_limits(n)
        env_ray = n.get("ray_env") or {}
        enroll_s = enroll.get(int(cid))
        record.data(f"{where}_{cid}", {**{k: n.get(k) for k in ("machine_id", "pids.max", "cgroup", "cpu_quota", "affinity", "ray_cpus",
                                                               "ports", "ray_env", "worker_threads", "cpu_count")},
                                       "enroll_s_from_first_running_sample": enroll_s})
        record.check(f"{cid}: worker ports of the bootstrap formula (pids.max {n.get('pids.max')})", n.get("ports") == ports,
                     (n.get("ports"), ports))
        record.check(f"{cid}: soft limit of the bootstrap formula, grpc threads 4, startup concurrency 8",
                     env_ray.get("RAY_num_workers_soft_limit") == str(soft_limit) and
                     env_ray.get("RAY_num_grpc_internal_threads") == "4" and env_ray.get("RAY_worker_maximum_startup_concurrency") == "8", env_ray)
        processes = libraries.processes_at_once(int(cid))
        record.check(f"{cid}: the start line gives processes at once = worker ports - 4", processes == ports - 4, (processes, ports))
        declared = libraries.config["available_node_types"][n["node_type"]]["resources"]["CPU"]
        quota = host_facts(n, n.get("machine_id"))["quota"]
        cores = n["affinity"] if quota is None else min(n["affinity"], quota)
        record.check(f"{cid}: Ray CPUs = the declared CPU, the cores (affinity, quota) at least 90% of it",
                     n["ray_cpus"] == declared and cores >= 0.9 * declared, (n["ray_cpus"], declared, n["affinity"], quota))
        if declared <= 54:
            record.check(f"{cid}: a worker with CPU {declared} runs one process for each CPU at once", processes is not None and
                         processes >= declared, processes)
        # Enrollment within 1 min of `running`; the suite's 10 s samples allow -15 to 75 s.
        record.check(f"{cid}: enrolled within 1 min of `running` (-15 to 75 s on the suite's samples)",
                     enroll_s is not None and -15 <= enroll_s <= 75, enroll_s)


@pytest.mark.parametrize("where", ["gpu"])
def test_jobs_workflow(record, libraries, env, where):
    gpu_workers(libraries, env, record)
    word, data = library_job(libraries, env, record, "hello.py", timeout=900)
    record.data("hello", data)
    record.check("--working-dir: data/hello.txt read by tasks on both workers", word == "PASS", data)
    word, data = library_job(libraries, env, record, "torch_cuda.py", torch=True, timeout=4200)
    record.data("torch_cuda", data)
    record.check("runtime_env pip torch==2.7.1: CUDA on both GPU workers", word == "PASS", data)
    sleeper = libraries.job(env, ["python", "sleeper.py"], record.dir, name="sleeper", working_dir=livejob.workdir(JOBS / "sleeper.py"),
                            wait=False)
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
    ticks = (record.dir / "follow.out").read_text().count("JOB_SLEEPER tick")
    record.check("ray job list shows the job", sid in listed.stdout, listed.returncode)
    record.check("ray job logs --follow streams (>= 3 ticks in 30 s)", ticks >= 3, ticks)
    record.check("ray job stop: exit 0, then STOPPED", stopped.returncode == 0 and status == "STOPPED", (stopped.returncode, status))


@pytest.mark.parametrize("where", ["gpu"])
def test_core_functions_and_the_core_snippet(record, libraries, env, where):
    rows, n, cpu = gpu_workers(libraries, env, record)
    for script, claim in (("placement.py", "PACK on one worker, STRICT_SPREAD on both"),
                          ("actor_fault.py", "actors restart after os._exit and ray.kill"),
                          ("large_object.py", "a 2 GB object between the hosts")):
        word, data = library_job(libraries, env, record, script, timeout=2100)
        record.data(script.removesuffix(".py"), data)
        record.check(claim, word == "PASS", data)
    share = round(cpu / n, 3)
    code = (snippet("core.md", 1, [], record, "cpus") +
            snippet("core.md", 0, [("@ray.remote(num_cpus=2.4)          # CPU / N\ndef child",
                                    f"@ray.remote(num_cpus={share})          # CPU / N\ndef child"),
                                   ("@ray.remote(num_cpus=2.4)          # CPU / N\nclass Runner",
                                    f"@ray.remote(num_cpus={share})          # CPU / N\nclass Runner"),
                                   ("range(5)", f"range({n // 2})")], record, "core"))
    w = libraries.window()
    job = snippet_job(libraries, env, record, "core", code, "core_snippet", timeout=1500)
    record.evidence("monitor_slice.log", "\n".join(w.lines()))
    record.data("core_snippet", job.data)
    record.check(f"the core snippet (num_cpus {share}, pool {n // 2}) completes", job.status == "SUCCEEDED" and job.word == "PASS",
                 (job.status, job.data))
    warnings = [e.line for e in warnings_for(w, rows)]
    record.check("no process-limit WARNING for the gpu workers", not warnings, warnings[:2])


@pytest.mark.parametrize("where", ["gpu"])
def test_data_shuffles_with_the_tuned_settings(record, libraries, env, where):
    rows, n, cpu = gpu_workers(libraries, env, record)
    for mode, torch in (("shuffle", False), ("gpu", True)):
        job = libraries.job(env, ["python", "data_tuned.py", mode], record.dir, name=f"data_{mode}",
                            working_dir=livejob.workdir(JOBS / "data_tuned.py", JOB_COMMON),
                            runtime_env={**(TORCH_ENV if torch else {}), "env_vars": {"JOB_N": str(n)}}, prefix="JOB_RESULT", timeout=2700)
        record.data(mode, job.data)
        record.check(f"data {mode} with the data.md settings at N {n}: SUCCEEDED", job.status == "SUCCEEDED" and job.word == "PASS",
                     (job.status, job.data))


@pytest.mark.parametrize("where", ["gpu"])
def test_data_defaults_without_a_shuffle(record, libraries, env, where):
    gpu_workers(libraries, env, record)
    for script in ("data_cpu.py", "data_object_store.py", "data_io.py", "data_many_blocks.py"):
        word, data = library_job(libraries, env, record, script, timeout=2700)
        record.data(script.removesuffix(".py"), data)
        record.check(f"{script} with the defaults", word == "PASS", data)
    io = record.result["data"]["data_io"]
    record.check("a local output path stays on the workers: the driver reads none of it",
                 sum(v["files"] for v in io["files_per_node"].values()) > 0 and ("readback_error" in io or io.get("readback_rows") == 0), io)


@pytest.mark.parametrize("where", ["gpu"])
def test_tune_snippet_with_s3(record, libraries, env, s3, where):
    rows, n, cpu = gpu_workers(libraries, env, record)
    share = round(cpu / n, 3)
    code = snippet("tune.md", 0, [('{"cpu": 2.4}', f'{{"cpu": {share}}}'), ("max_concurrent_trials=5", f"max_concurrent_trials={n // 2}"),
                                  ('"s3://my-bucket/experiments"', f'"s3://{s3.bucket}/tune/experiments"')], record, "tune")
    w = libraries.window()
    job = snippet_job(libraries, env, record, "tune", code, "tune_snippet", timeout=2400, s3=s3)
    word, data = job.word, job.data
    record.data("tune_snippet", data)
    checkpoints = {o["Key"].split("/checkpoint_")[0] for o in s3.objects("tune/experiments/") if "/checkpoint_" in o["Key"]}
    record.data("s3_trials_with_checkpoints", len(checkpoints))
    record.check("the tune.md snippet: 60 trials, 0 errors", word == "PASS" and data.get("results") == 60 and data.get("errors") == 0, data)
    record.check("trials on both workers", len([x for x in data.get("trial_nodes", []) if x]) >= 2, data.get("trial_nodes"))
    record.check("checkpoints in S3 for every trial", len(checkpoints) == 60, len(checkpoints))
    forks = with_refused_forks(warnings_for(w, rows))
    record.check("no refused fork in a WARNING for the gpu workers", not forks, forks[:2])
    for script, claim in (("tune_asha.py", "ASHA stops trials early"), ("tune_gpu.py", "GPU trials get 1 GPU each, on both workers")):
        word, data = library_job(libraries, env, record, script, timeout=1500)
        record.data(script.removesuffix(".py"), data)
        record.check(claim, word == "PASS", data)
    word, data = library_job(libraries, env, record, "tune_checkpoint.py", timeout=1500)
    record.data("default_storage", data)
    # The job cuts each trial error to 300 characters in its result line; the full error is in the job log.
    log_path = record.dir / "tune_checkpoint.log"
    log = log_path.read_text()
    (record.dir / "tune_checkpoint_default.log").write_text(log)
    record.check("the default storage_path: `Unable to set up cluster storage ... not accessible by worker nodes` (tune.md)",
                 word == "LIMIT" and "Unable to set up cluster storage" in log and "not accessible by worker nodes" in log,
                 (word, data.get("errors"), data.get("experiment_path")))
    word, data = library_job(libraries, env, record, "tune_checkpoint.py", timeout=1500,
                             env_vars={"JOB_STORAGE": f"s3://anonymous@{s3.bucket}/tune/anon"})
    record.data("anonymous_s3", data)
    record.check("anonymous S3 through pyarrow: S3 refuses the uploads (tune.md)",
                 word == "LIMIT" and re.search(r"(?i)access_?denied|access denied|forbidden|\b403\b",
                                               json.dumps(data) + log_path.read_text()), data)


@pytest.mark.parametrize("where", ["gpu"])
def test_serve_snippet_http_and_autoscaling(record, libraries, env, where):
    rows, n, cpu = gpu_workers(libraries, env, record)
    share, replicas = round(cpu / n, 3), n * len(rows)
    code = snippet("serve.md", 0, [("num_replicas=10,", f"num_replicas={replicas},"), ('{"num_cpus": 2.4}', f'{{"num_cpus": {share}}}')],
                   record, "serve")
    w = libraries.window()
    job = snippet_job(libraries, env, record, "serve", code, "serve_snippet", timeout=1500)
    record.data("serve_snippet", job.data)
    per_node = job.data.get("replica_nodes", {})
    record.check(f"the serve.md snippet: {replicas} replicas, at most N = {n} on each worker", job.status == "SUCCEEDED" and
                 job.word == "PASS" and sum(per_node.values()) == replicas and max(per_node.values(), default=n + 1) <= n,
                 (job.status, per_node))
    threads = node_facts(libraries, env, record.dir, "node_facts_with_replicas")["gpu"]
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
    cleanup = livejob.workdir(JOBS / "cleanup.py")
    libraries.job(env, ["python", "cleanup.py"], record.dir, name="serve_cleanup", working_dir=cleanup, timeout=300)
    forks = with_refused_forks(warnings_for(w, rows))
    record.check("no refused fork in a WARNING for the gpu workers", not forks, forks[:2])
    word, data = library_job(libraries, env, record, "serve_gpu.py", torch=True, timeout=2700)
    record.data("serve_gpu", data)
    record.check("GPU replicas: 1 GPU id each, on both workers", word == "PASS", data)
    job = libraries.job(env, ["python", "serve_autoscale.py"], record.dir, name="serve_autoscale",
                        working_dir=livejob.workdir(JOBS / "serve_autoscale.py"),
                        runtime_env={"env_vars": {"JOB_N": str(n), "JOB_CPUS": str(cpu), "JOB_WORKERS": str(len(rows))}},
                        prefix="JOB_RESULT", timeout=1200)
    record.data("serve_autoscale", job.data)
    record.check(f"autoscaling: scales out, up to max_replicas {replicas}, at most N on each worker", job.word == "PASS",
                 {k: job.data.get(k) for k in ("peak_replicas", "per_node_max", "max_replicas")})
    libraries.job(env, ["python", "cleanup.py"], record.dir, name="serve_cleanup_2", working_dir=cleanup, timeout=300)


@pytest.mark.parametrize("where", ["gpu", "gpu2"])
def test_rllib_env_runners_and_learners(record, libraries, env, where):
    if where == "gpu":
        rows, n, cpu = gpu_workers(libraries, env, record)
        share = round(cpu / n, 3)
        code = snippet("rllib.md", 0, [("num_cpus_per_env_runner=2.4", f"num_cpus_per_env_runner={share}")], record, "rllib_runners")
        w = libraries.window()
        job = snippet_job(libraries, env, record, "rllib_runners", code, "rllib_runners", runtime_env=TORCH_ENV, timeout=2700)
        record.data("rllib_runners", job.data)
        record.check(f"the env runner snippet (2 runners, num_cpus {share}): 2 iterations", job.word == "PASS", job.data)
        warnings = [e.line for e in warnings_for(w, rows)]
        record.check("no process-limit WARNING for the gpu workers", not warnings, warnings[:2])
        return
    rows = [one_worker(libraries, env, record)]
    n, cpu = n_and_cpu(libraries, rows)
    share = round(cpu / n, 3)
    record.data("num_cpus_per_learner", share)
    code = snippet("rllib.md", 1, [("num_cpus_per_learner=2.4", f"num_cpus_per_learner={share}"),
                                   ("tune.Tuner(config.algo_class", "grid = tune.Tuner(config.algo_class")], record, "rllib_learners")
    job = snippet_job(libraries, env, record, "rllib_learners", code, "rllib_learners", runtime_env=TORCH_ENV, timeout=2700)
    record.data("rllib_learners", job.data)
    record.check(f"the learner snippet on gpu2 (num_cpus_per_learner {share}): 2 iterations, 0 errors", job.word == "PASS", job.data)


@pytest.mark.parametrize("where", ["gpu2"])
def test_train_snippet_with_2_gpus_and_s3(record, libraries, env, s3, where):
    rows = [one_worker(libraries, env, record)]
    n, cpu = n_and_cpu(libraries, rows)
    share = round(cpu / n, 3)
    code = snippet("train.md", 0, [('{"CPU": 2.4, "GPU": 1}', f'{{"CPU": {share}, "GPU": 1}}'),
                                   ('"s3://my-bucket/experiments"', f'"s3://{s3.bucket}/train/experiments"'),
                                   ("    ...\n", "    train_body(loader)\n")], record, "train")
    w = libraries.window()
    job = snippet_job(libraries, env, record, "train", code, "train_snippet", runtime_env=TORCH_ENV, timeout=3600, s3=s3)
    status, word, data = job.status, job.word, job.data
    ranks = job.lines("JOB_RANK")
    last = [r for r in ranks if r["epoch"] == 1]
    record.data("train_ranks", ranks)
    gpu2_node = rows[0]["node_id"][:8]
    record.check("the train.md snippet: fit returns", status == "SUCCEEDED" and word == "PASS", (status, data))
    record.check("2 epochs on 2 ranks on the gpu2 worker, cuda, one GPU id each, 2 different GPUs",
                 len(last) == 2 and {r["node"] for r in last} == {gpu2_node} and all(r["device"].startswith("cuda") and len(r["gpu_ids"]) == 1
                                                                                for r in last)
                 and len({r["gpu_ids"][0] for r in last}) == 2, last)
    checkpoints = [o["Key"] for o in s3.objects("train/experiments/") if "/checkpoint_" in o["Key"]]
    record.check("checkpoints in S3", len(checkpoints) > 0, len(checkpoints))
    warnings = [e.line for e in warnings_for(w, rows)]
    record.check("no process-limit WARNING for the gpu2 worker", not warnings, warnings[:2])
    word, data = library_job(libraries, env, record, "train_one_gpu.py", torch=True, timeout=2700)
    record.data("default_storage", data)
    record.check("1 GPU with the default storage_path: `A local path on the head node is not accessible by worker nodes`",
                 word == "LIMIT" and "not accessible by worker nodes" in json.dumps(data) + (record.dir / "train_one_gpu.log").read_text(),
                 data)


@pytest.mark.parametrize("where", ["gpu", "big-a"])
def test_data_snippet_on_s3(record, libraries, env, s3, where):
    if where == "gpu":
        rows, n, cpu = gpu_workers(libraries, env, record)
    else:
        rows = [one_worker(libraries, env, record)]
        n, cpu = n_and_cpu(libraries, rows)
    # The input: about 3 GB of parquet in s3://<bucket>/data/in, written by a job on the host of the test. So it runs
    # after the workers are in place: with no worker, the input job makes no progress.
    parquet_in = {"path": f"s3://{s3.bucket}/data/in", "rows": PARQUET_ROWS}
    job = libraries.job(env, ["python", "make_parquet.py", parquet_in["path"], str(PARQUET_ROWS)], record.dir, name="make_parquet",
                        working_dir=livejob.workdir(JOBS / "make_parquet.py"), runtime_env={"env_vars": s3.env_vars},
                        secrets=[s3.secret], prefix="JOB_RESULT", timeout=3600)
    parquet_in["bytes"] = sum(o["Size"] for o in s3.objects("data/in/"))
    record.data("input", {**parquet_in, "status": job.status, "result": [job.word, job.data]})
    record.require("the input is written (make_parquet.py)", job.word == "PASS", (job.status, job.word, job.data))
    out = f"s3://{s3.bucket}/data/out-{where}"
    code = snippet("data.md", 0, [("max_hash_shuffle_aggregators = 2", f"max_hash_shuffle_aggregators = {n // 4}"),
                                  ("ExecutionResources(cpu=2)", f"ExecutionResources(cpu={n // 4})"),
                                  ('"s3://my-bucket/in"', f'"{parquet_in["path"]}"'),
                                  ("concurrency=2,", f"concurrency={n // 4},"),
                                  ("override_num_blocks=5)", f"override_num_blocks={n // 2})"),
                                  ("TaskPoolStrategy(size=2)", f"TaskPoolStrategy(size={n // 4})"),
                                  ('"s3://my-bucket/out"', f'"{out}"')], record, "data")
    w = libraries.window()
    job = snippet_job(libraries, env, record, "data", code, "data_snippet", timeout=3600, s3=s3,
                      extra_env={"JOB_OUT": out, "JOB_ROWS": str(parquet_in["rows"])})
    status, word, data, log = job.status, job.word, job.data, job.log
    lines = w.lines()
    record.evidence("monitor_slice.log", "\n".join(lines))
    record.data("data_snippet", data)
    after = node_facts(libraries, env, record.dir, "node_facts_after")
    alive = {r["node_id"] for rs in after.values() for r in rs}
    record.check(f"the data.md snippet at N {n} on {where}: SUCCEEDED, all rows counted from the S3 output",
                 status == "SUCCEEDED" and word == "PASS", (status, data))
    record.check("output in S3", len(s3.objects(out.split("/", 3)[3] + "/")) > 0, out)
    forks = with_refused_forks(w.events("process_limit", instances=[r["container_id"] for r in rows], lines=lines))
    record.check("no refused fork in a WARNING", not forks, forks[:2])
    record.check("no node crash: the same Ray nodes are alive", {r["node_id"] for r in rows} <= alive, "")
    record.check("no out-of-memory error", not re.search(r"OutOfMemoryError|out of memory", log), "")


# --- destructive: the process-limit pairs ---------------------------------------------------------------------------------
PAIRS = [  # (library, tuned (script, args), many (script, args), torch, job timeout in s)
    ("core", ("pair_core.py", "tuned"), ("pair_core.py", "many"), False, 900),
    ("data", ("pair_data.py", "tuned2"), ("pair_data.py", "many"), False, 1200),
    ("tune", ("pair_tuned.py", "tune"), ("pair_tune.py", "many"), False, 900),
    ("serve", ("pair_serve.py", "full"), ("pair_serve.py", "many"), False, 900),
    ("train", ("pair_tuned.py", "train"), ("pair_train.py", "many"), True, 2400),
    ("rllib", ("pair_tuned.py", "rllib"), ("pair_rllib.py", "many"), True, 1500),
]
PAIR_LIBRARIES = {"big-a": ("data", "tune", "serve"), "big-b": ("core", "train", "rllib")}  # two hosts at the same time


def node_limits(cluster, env, out_dir, wd, name):
    """jobs/node_limits.py: {node id: read_limits()}; again every 30 s for up to 10 min while a node gives an error (or no
    row). Still an error after 10 min: a RuntimeError (the pair cannot be judged)."""
    attempts = iter(range(10**6))

    def look():
        rows = cluster.job(env, ["python", "node_limits.py"], out_dir, name=f"{name}_{next(attempts)}", working_dir=wd,
                           timeout=300).lines("NODE_LIMITS")
        return rows[-1] if rows else {}

    waited = wait_until(look, 600, done=lambda found: found and not any("error" in v for v in found.values()), poll_s=30)
    if not waited:
        raise RuntimeError(f"node limits {name}: no answer without an error in 600 s (last {waited.seen}, {waited.error})")
    return waited.seen


def diagnose(cluster, env, node, job, out):
    """During serve.many on big-a: the 3 "If it fails" steps of the library pages, as a user does them."""
    out["started"] = time.time()  # the serve.many job started just now
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


@pytest.mark.parametrize("where", ["big-a", "big-b"])
def test_process_limit_pairs_on_a_worker_with_more_cpus_than_processes(record, libraries, env, where):
    """The pairs of the libraries of this host (PAIR_LIBRARIES); on big-a also the troubleshooting steps during serve.many.
    A many run ends early when its hang is proven (livejob.HangRule: the head's WARNING for the node and no progress for
    120 s); else the job's own guard (600 s) ends it."""
    node = one_worker(libraries, env, record)
    limit = host_facts(node, node["machine_id"])["pids_max"]
    record.require(f"{where} has a pids limit", limit is not None, node.get("pids.max"))
    record.data("host", {k: node.get(k) for k in ("machine_id", "container_id", "pids.max", "cgroup", "ray_cpus", "ports")})
    wd = livejob.workdir(*JOBS.glob("pair*.py"), JOBS / "node_limits.py", JOBS / "cleanup.py")
    outcomes, diag = [], {}
    for lib, tuned, many, torch, timeout in [p for p in PAIRS if p[0] in PAIR_LIBRARIES[where]]:
        for kind, (script, arg) in (("tuned", tuned), ("many", many)):
            pair = f"{lib}.{kind}"
            pre = node_limits(libraries, env, record.dir, wd, f"{pair}_pre")
            target = next(iter(pre)) if len(pre) == 1 else None
            record.require(f"{pair}: one worker node", target is not None, len(pre))
            w = libraries.window()
            job = libraries.job(env, ["python", script, arg], record.dir, name=pair, working_dir=wd,
                                runtime_env={**(TORCH_ENV if torch else {}),
                                             "env_vars": {"JOB_PORTS": str(pre[target]["ports"]), "JOB_CPUS": str(pre[target]["CPU"])}},
                                prefix="JOB_RESULT", timeout=timeout, wait=False)
            if where == "big-a" and pair == "serve.many":
                diagnose(libraries, env, node, job, diag)
            hung = None
            if kind == "many":
                hung = livejob.HangRule(lambda: next((e.line for e in w.events("process_limit", instances=[node["container_id"]])), None),
                                        job.progress)
            status, log = job.wait(hung=hung).status, job.log
            libraries.job(env, ["python", "cleanup.py"], record.dir, name=f"{pair}_cleanup", working_dir=wd, timeout=300)
            time.sleep(30)  # the raylet ends the job's worker processes
            post = node_limits(libraries, env, record.dir, wd, f"{pair}_post")
            lines = w.lines()
            record.evidence(f"{pair}_head_slice.log", "\n".join(lines))
            events = w.events("process_limit", instances=[node["container_id"]], lines=lines)
            warnings = [e.line for e in events]
            job_line = {"kind": job.word, **job.data}
            # The many sizes are max(ports + 4, ...), above the ports at any limit, so the limit of this host is accepted.
            # A refused fork fails a tuned run; in a many run it is data (`refused_forks` of each pair).
            verdict_word, reasons = verdict(kind, job_line, pre.get(target), post.get(target), warnings, limit_max=max(3328, limit))
            refused = (events_max(post.get(target)) or 0) - (events_max(pre.get(target)) or 0)
            if kind == "many":
                full = [e.line for e in events if e.fields["ports_in_use"] == e.fields["ports_total"]]
                if verdict_word == "PASS" and not full:
                    verdict_word, reasons = "FAIL", reasons + ["no `ports in use N of N` WARNING"]
            outcomes.append((pair, verdict_word, reasons))
            record.data(pair, {"verdict": verdict_word, "reasons": reasons, "job_status": status, "job": job_line, "refused_forks": refused,
                               "warnings": warnings[:5], "params": next((x for x in log.splitlines() if x.startswith("JOB_PARAMS")), None)})
    for pair, verdict_word, reasons in outcomes:
        record.check(f"{pair}: {verdict_word}", verdict_word == "PASS", "; ".join(reasons)[:300])
    if where != "big-a":
        return
    # The troubleshooting steps of each library page, during the serve.many hang.
    record.evidence("worker_err.txt", diag.get("no_port_text", ""))
    record.evidence("job_log_during.txt", diag.get("job_log", ""))
    record.evidence("grep_process_limit.txt", diag.get("grep", ""))
    record.data("err_files", diag.get("err_files"))
    # Only a WARNING of the serve.many hang counts: the grep also shows the WARNINGs of the runs before.
    named = [e.line for e in libraries.window().events("process_limit", instances=[node["container_id"]],
                                                       lines=diag.get("grep", "").splitlines())
             if e.t >= diag.get("started", float("inf"))]
    for name, ok, detail in (
            ("step 3: the dashboard log API opens a worker-*.err with `No available ports`", "no_port_file" in diag,
             diag.get("no_port_file")),
            ("step 2: `ray job logs` shows serve.run that does not return (serve.md), the job RUNNING",
             diag.get("job_status") == "RUNNING" and "JOB_PARAMS" in diag.get("job_log", "") and "JOB_RESULT" not in diag.get("job_log", ""),
             diag.get("job_status")),
            ("step 1: the `grep \"process limit\"` through attach shows the WARNING that names the worker", bool(named),
             named[-1][:300] if named else diag.get("grep", "")[-300:])):
        record.check(name, ok, detail)


# --- a host with few direct ports ---------------------------------------------------------------------------------------
@pytest.mark.parametrize("where", ["few-ports"])
def test_a_host_with_14_to_63_direct_ports(record, libraries, env, where):
    """On a host whose offer has 14 to 63 direct ports, the provider rents min(64, direct_port_count) identity ports, the
    worker enrolls (the head port-checks each port that the worker reports: all the ports that vast mapped), the bootstrap
    gives Ray min(mapped - 6, the pids formula) worker ports (`worker_ports_and_soft_limit`, the product's formula), and a
    small job runs."""
    w = libraries.window()
    node = one_worker(libraries, env, record)
    cid = str(node["container_id"])
    instance = next((i for i in libraries.instances() if str(i["id"]) == cid), {})
    # The provider rents with the offer's `direct_port_count`. The instance listing can give another value (an offer for 1
    # of 4 GPUs had 24, the instance 25), so the test reads the offer of the same machine and GPU count.
    same = [o for o in vast().search_offers({"machine_id": {"eq": instance.get("machine_id")}, "num_gpus": {"eq": instance.get("num_gpus")}})
            if o.get("machine_id") == instance.get("machine_id")]
    offered, mapped = (same[0]["direct_port_count"] if same else None), node.get("identity_ports")
    vast_ports = sorted(int(k.split("/")[0]) for k in (instance.get("ports") or {}) if k.split("/")[0].isdigit()
                        and 70000 <= int(k.split("/")[0]) < 70064)
    facts = host_facts(node, node.get("machine_id"))
    ports, soft_limit = expected_limits(node)
    record.data("host", {**facts, "direct_port_count": offered, "instance_direct_port_count": instance.get("direct_port_count"),
                         "identity_ports_mapped": mapped, "vast_identity_ports": len(vast_ports),
                         "ports": node.get("ports"), "expected_ports": ports, "soft_limit": soft_limit})
    record.check("the offer has 14 to 63 direct ports", offered is not None and 14 <= offered <= 63, offered)
    # The few-ports query asks for 17 or more effective cores, so the pids formula alone gives more ports than mapped - 6.
    pids_only = worker_ports_and_soft_limit(facts["cpus"], facts["pids_max"], 64)[0]
    record.check("the port cap sets the count: mapped - 6 is below the pids formula alone", mapped is not None and mapped - 6 < pids_only,
                 (mapped, pids_only, facts["pids_max"]))
    record.check("the candidate rents min(64, direct_port_count) identity ports: vast maps that many into the container",
                 offered is not None and mapped == min(64, offered), (mapped, offered, len(vast_ports)))
    enrolled = w.events("enrolled", instances=[cid])
    record.check("the worker enrolled (the head checked each of its mapped ports)", bool(enrolled), enrolled[0].line if enrolled else None)
    record.data("port_check_endings", [e.line for e in w.events("ending") if "port check failed" in (e.reason or "")])
    text = libraries.container_log(int(cid))
    record.evidence(f"container_{cid}.log", text)
    start = re.search(r"bootstrap: limits: pids\.max \S+ \(cgroup v[12]\), cpus (\d+), processes at once (\d+), worker ports (\d+), "
                      r"soft limit (\d+)", text)
    record.check("the start line: worker ports = min(mapped - 6, the pids formula) (worker_ports_and_soft_limit)",
                 start is not None and int(start.group(3)) == ports == node.get("ports") and int(start.group(4)) == soft_limit,
                 (start.group(0) if start else None, ports, node.get("ports"), mapped))
    record.check("the start line: processes at once = worker ports - 4, at least the CPU", start is not None and
                 int(start.group(2)) == int(start.group(3)) - 4 >= int(start.group(1)), start.group(0) if start else None)
    job = libraries.job(env, ["python", "tasks.py"], record.dir, name="tasks", working_dir=livejob.workdir(JOBS / "tasks.py"),
                        prefix="JOB_RESULT", timeout=900)
    record.data("tasks", job.data)
    record.check("a small job runs (one task for each Ray CPU at the same time)", job.status == "SUCCEEDED" and job.word == "PASS",
                 (job.status, job.word, job.data))
