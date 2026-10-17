"""Group `lifecycle` (docs/test-plan.md, claim matrix L01-L10): the C8 shape (2 GPU node types x 2, k = 2), run as the
restricted IAM user, in us-east-2 with a t3.medium head, no top-level max_workers. Order: up and scale (L01, L02), then a
hold of 4 workers for the probes (L03-L05), then the destructive scenarios (L07, L10). The teardown of the cluster
(conftest.group_lifetime) checks the `down`.
About 2 h; vast about $2, EC2 about $0.10."""
import json
import re
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import boto3
import pytest

from livekit import check
from cluster import ADMIN_PROFILE, BIN, MANUAL

pytestmark = pytest.mark.lifecycle
JOBS = Path(__file__).parent / "jobs"


def workdir(*files):
    """A temp working directory for `ray job submit` with these files."""
    d = Path(tempfile.mkdtemp(prefix="rvjob-"))
    for f in files:
        shutil.copy(f, d / Path(f).name)
    return d



def worker_rules(cluster):
    ec2 = boto3.Session(profile_name=ADMIN_PROFILE, region_name=cluster.region).client("ec2")
    groups = ec2.describe_security_groups(Filters=[{"Name": "tag:ray-vast-cluster", "Values": [cluster.name]}])["SecurityGroups"]
    return sum(1 for g in groups for p in g["IpPermissions"] for r in p.get("IpRanges", []) if r.get("Description") == f"ray-vast:{cluster.name}")


def probe_nodes(cluster, env, out_dir, name="probe"):
    status, log = cluster.job(env, workdir(JOBS / "node_probe.py", MANUAL / "c10" / "jobs" / "c10common.py"),
                              ["python", "node_probe.py"], out_dir=out_dir, name=name, timeout=900)
    return status, [json.loads(line.split(" ", 1)[1]) for line in log.splitlines() if line.startswith("NODE_PROBE ")]


@pytest.fixture(scope="module")
def session_env(lifecycle):
    with lifecycle.tunnel() as env:
        yield env


@pytest.fixture(scope="module")
def hold(lifecycle, session_env):
    """4 workers held by a placement group (hold.py) for L03-L10. Stopped at the end of the module."""
    sid, _ = lifecycle.job(session_env, workdir(JOBS / "hold.py"), ["python", "hold.py", "4"], name="hold", no_wait=True,
                           out_dir=lifecycle.dir)
    deadline = time.time() + 1800
    while time.time() < deadline and "HOLD_READY" not in lifecycle.job_logs(session_env, sid):
        time.sleep(20)
    yield sid
    subprocess.run([str(BIN / "ray"), "job", "stop", "--address", session_env["RAY_ADDRESS"], sid], env=session_env,
                   capture_output=True, timeout=120)


@pytest.mark.security
def test_L01_up_as_the_restricted_user_in_another_region(record, lifecycle, restricted_user):
    if lifecycle.up_output is None:
        pytest.skip("--keep-cluster reused a running cluster: this run had no `up`")
    up = lifecycle.up_output
    record.evidence("up.txt", up["stdout"] + up["stderr"])
    record.evidence("iam_commands.log", (restricted_user.log).read_text())
    record.data("up_s", up["wall_s"])
    check(record, "up as the restricted user exits 0", up["returncode"] == 0, up["wall_s"])
    ec2 = boto3.Session(profile_name=ADMIN_PROFILE, region_name="us-east-2").client("ec2")
    head = ec2.describe_instances(InstanceIds=[lifecycle.state["head_instance_id"]])["Reservations"][0]["Instances"][0]
    check(record, "the head runs in us-east-2 with t3.medium", head["Placement"]["AvailabilityZone"].startswith("us-east-2") and
          head["InstanceType"] == "t3.medium", (head["Placement"]["AvailabilityZone"], head["InstanceType"]))
    role = boto3.Session(profile_name=ADMIN_PROFILE).client("iam").get_role(RoleName=f"ray-vast-{lifecycle.name}")["Role"]
    check(record, "the head role has the boundary", role.get("PermissionsBoundary", {}).get("PermissionsBoundaryArn", "").endswith(
        ":policy/ray-vast-head-boundary"), "")
    check(record, "the state file saved the restricted profile", lifecycle.state.get("profile") == restricted_user.profile, "")


def test_L02_scale_two_types_and_idle_scale_down(record, lifecycle, session_env):
    """C8: 4 one-GPU tasks in a STRICT_SPREAD group over 2 node types; then idle scale-down to 0."""
    rules_seen = []
    stop = threading.Event()

    def sample_rules():
        while not stop.is_set():
            try:
                rules_seen.append((time.time(), worker_rules(lifecycle)))
            except Exception:  # noqa: BLE001
                pass
            stop.wait(15)
    threading.Thread(target=sample_rules, daemon=True).start()
    t_submit = time.time()
    log_start = len(lifecycle.monitor_log()) + 1
    status, log = lifecycle.job(session_env, workdir(MANUAL / "c8_job.py"), ["python", "c8_job.py"], out_dir=record.dir, name="c8_job",
                                timeout=2400)
    t_end = time.time()
    results = [json.loads(line) for line in log.splitlines() if line.startswith("{")]
    check(record, "c8_job SUCCEEDED with 4 containers", status == "SUCCEEDED" and len({r["container_id"] for r in results}) == 4, status)
    check(record, "both node types", {r["node_type"] for r in results} == {"gpu-a", "gpu-b"}, sorted({str(r["node_type"]) for r in results}))
    for r in results:
        check(record, f"bundle {r['bundle']}: 1 GPU, gpu_ids [0]", r["gpu_ids"] == [0] and r["nvidia_smi"].count("GPU ") == 1,
              (r["gpu_ids"], r["nvidia_smi"][:80]))
    # idle scale-down: all instances gone within idle_timeout_minutes (2) + 120 s
    deadline = t_end + 120 + 420
    while time.time() < deadline and lifecycle.instances():
        time.sleep(15)
    t_zero = time.time()
    stop.set()
    # control-cost.md:24: Ray removes a worker after `idle_timeout_minutes` (2) idle. 300 s more is a safety bound for the
    # autoscaler loop, the drain and the destroy; the time is data.
    record.data("idle_to_zero_s", round(t_zero - t_end))
    check(record, "idle scale-down to 0 within idle_timeout + 300 s", not lifecycle.instances() and t_zero - t_end <= 420,
          round(t_zero - t_end))
    time.sleep(20)
    rules_after = worker_rules(lifecycle)
    record.data("rules_seen_max", max((n for _, n in rules_seen), default=None))
    check(record, "worker rules were added and are all revoked", max((n for _, n in rules_seen), default=0) >= 1 and rules_after == 0,
          (max((n for _, n in rules_seen), default=0), rules_after))
    lines = lifecycle.monitor_log(log_start)
    record.evidence("monitor_slice.log", "\n".join(lines))
    rows = [r for r in lifecycle.timeline if r["t"] >= t_submit - 5 and "instances" in r]
    rented = {i["id"] for r in rows for i in r["instances"]}
    peak = max((sum(i.get("dph_total") or 0 for i in r["instances"]) for r in rows), default=0)
    enrolled = {int(m.group(1)): lifecycle.log_epoch(line) for line in lines for m in [re.search(r"enrolled instance (\d+)", line)] if m}
    first_running = {}
    for r in rows:
        for i in r["instances"]:
            if i.get("actual_status") == "running":
                first_running.setdefault(i["id"], r["t"])
    enroll_s = {i: round(enrolled[i] - first_running[i], 1) for i in enrolled if i in first_running and enrolled[i]}
    record.data("rented", len(rented))
    record.data("peak_dph", round(peak, 3))
    record.data("enroll_s_from_first_running_sample", enroll_s)
    check(record, "rentals <= candidate_factor x slots (8)", 4 <= len(rented) <= 8, len(rented))
    check(record, "peak $/h below max_dollars_per_hour (3.0)", peak <= 3.0, round(peak, 3))
    # design.md:56, :86: a candidate that does not enroll 1 min after the provider sees it `running` is ended. This suite's
    # own 10 s samples of vast differ from the provider's by up to 15 s either way: -15 to 75 s.
    check(record, "enroll within 1 min of running (design.md:86; -15 to 75 s on the suite's samples)",
          enroll_s and max(enroll_s.values()) <= 75 and min(enroll_s.values()) >= -15, enroll_s)
    check(record, "no candidate ended on the pull limit", not [x for x in lines if "ending instance" in x and "pull" in x.lower()], "")
    check(record, "no `missing from vast` ending", not [x for x in lines if "ending instance" in x and "missing" in x], "")
    unknown = [x for x in lines if "unknown vast actual_status" in x and "rebooting" not in x]
    check(record, "no unknown vast status", not unknown, unknown[:2])
    check(record, "rent lines", any(re.search(r"create \d+ x gpu-[ab]: rented \d+ candidates", x) for x in lines), "")


@pytest.mark.security
@pytest.mark.limits
def test_L03_host_and_task_probe_on_each_worker(record, lifecycle, session_env, hold):
    status, nodes = probe_nodes(lifecycle, session_env, record.dir)
    record.evidence("node_probe.json", json.dumps(nodes, indent=1))
    check(record, "probe job SUCCEEDED on 4 workers", status == "SUCCEEDED" and len(nodes) == 4, (status, len(nodes)))
    for n in nodes:
        cid = n["container_id"]
        check(record, f"{cid}: uid 1000, Ray 2.59.0", n["uid"] == 1000 and n["ray_version"] == "2.59.0", (n["uid"], n["ray_version"]))
        check(record, f"{cid}: no CONTAINER_API_KEY or RAYVAST_JOIN_TOKEN in os.environ",
              not n["CONTAINER_API_KEY_in_environ"] and not n["RAYVAST_JOIN_TOKEN_in_environ"], "")
        check(record, f"{cid}: PID 1 is the bootstrap (GPU image ENTRYPOINT kept)", n["pid1_is_bootstrap"], "")
        check(record, f"{cid}: /tmp/rayvast 700, files 600 or 644 for certs", n["rayvast_dir_mode"] == "0o700" and
              all(m == "0o600" for f, m in n["rayvast_file_modes"].items() if f in ("node.key", "token")), n["rayvast_file_modes"])
        check(record, f"{cid}: CapBnd without NET_RAW and NET_ADMIN", not n["cap_net_raw_bnd"] and not n["cap_net_admin_bnd"], "")
        quota = n["cpu_quota"]
        record.data(f"host_{cid}", {k: n.get(k) for k in ("pids.max", "cgroup", "cpu_quota", "affinity", "ray_cpus", "ports", "ray_env",
                                                          "worker_threads", "cpu_count")})
        check(record, f"{cid}: Ray CPUs = the real core count (affinity, quota)", n["ray_cpus"] <= n["affinity"] and n["ray_cpus"] >= 4,
              (n["ray_cpus"], n["affinity"], quota))
        limit = n.get("pids.max")
        expected = 58 if limit in (None, "max") else max(4, min(58, (int(limit) - 400) // 60))
        check(record, f"{cid}: worker ports = formula of pids.max {limit}", n.get("ports") == expected, (n.get("ports"), expected))
        env = n.get("ray_env") or {}
        check(record, f"{cid}: soft limit min(CPUs, ports // 2), grpc 4, startup 8",
              env.get("RAY_num_workers_soft_limit") == str(min(int(n["ray_cpus"]), expected // 2)) and
              env.get("RAY_num_grpc_internal_threads") == "4" and env.get("RAY_worker_maximum_startup_concurrency") == "8", env)
        check(record, f"{cid}: GPU device nodes = 1", n["gpu_device_nodes"] == 1 and n["ray_gpus"] == 1, (n["gpu_device_nodes"], n["ray_gpus"]))
    for i in lifecycle.instances():
        text = lifecycle.container_log(i["id"])
        start = re.search(r"bootstrap: limits: pids\.max \S+ \(cgroup v[12]\), cpus \d+, worker ports \d+, soft limit \d+, grpc threads 4, "
                          r"startup concurrency 8", text)
        record.evidence(f"container_{i['id']}.log", text)
        check(record, f"{i['id']}: the start line in the container log", start is not None, start.group(0) if start else "")
        checks = re.search(r"bootstrap: host checks: (\{.*\})", text)
        check(record, f"{i['id']}: host checks all True", checks is not None and all(
            f"'{k}': True" in checks.group(1) for k in ("egress_ip_ok", "no_net_raw_admin", "own_netns", "shm_empty")),
              checks.group(1)[:200] if checks else "")


@pytest.mark.security
def test_L04_network_battery_tls_and_token(record, lifecycle, session_env, hold):
    status, nodes = probe_nodes(lifecycle, session_env, record.dir)
    for n in nodes:
        cid = n["container_id"]
        check(record, f"{cid}: agent port answers 401 without the token", n["agent_http_no_token"] == 401, n["agent_http_no_token"])
        check(record, f"{cid}: runtime-env port answers 401 without the token", n["runtime_env_no_token"] == 401, n["runtime_env_no_token"])
        for port in ("dashboard_agent_listen_port", "runtime_env_agent_port"):
            check(record, f"{cid}: {port} listens on 127.0.0.1 only", n["listen"].get(port) == ["127.0.0.1"], n["listen"].get(port))
    ips = sorted({n["ip"] for n in nodes})
    secrets_dir = f"/home/ubuntu/ray-vast-{lifecycle.name}/secrets"
    lifecycle.ssh("sudo rm -f /tmp/rv_cap.pcap; (sudo timeout 600 tcpdump -i ens5 -w /tmp/rv_cap.pcap not port 22 >/dev/null 2>&1 &)")
    status, log = lifecycle.job(session_env, workdir(MANUAL / "battery.py"), ["python", "battery.py", ips[0], ips[1]],
                                out_dir=record.dir, name="battery", timeout=1800)
    lifecycle.ssh("sudo pkill -INT tcpdump; sleep 3", check=False)
    hits = lifecycle.ssh(f"sudo grep -c -a -F -f {secrets_dir}/token /tmp/rv_cap.pcap; ls -l /tmp/rv_cap.pcap", check=False)
    lifecycle.ssh("sudo rm -f /tmp/rv_cap.pcap", check=False)
    record.evidence("token_in_capture.txt", hits)
    battery = re.search(r"BATTERY_JSON (\{.*\})", log)
    data = json.loads(battery.group(1)) if battery else {}
    record.data("battery", {k: v for k, v in data.items() if k.startswith("step")})
    check(record, "battery ran (actor RTT, pip, 50 MB both ways, worker to worker)", status == "SUCCEEDED" and all(
        f"step{i}" in " ".join(data) for i in (1, 2, 3, 4)), status)
    check(record, "no token bytes on the head's public interface", hits.strip().splitlines()[0] == "0" if hits.strip() else False, hits[:80])
    metrics = []
    for n in nodes:
        port = n["agent_ports"].get("metrics_export_port")
        try:
            body = subprocess.run(["curl", "-s", "-m", "15", f"http://{n['ip']}:{port}/metrics"], capture_output=True, text=True, timeout=30).stdout
        except subprocess.TimeoutExpired:
            body = ""
        series = len({line.split("{")[0].split(" ")[0] for line in body.splitlines() if line.startswith("ray_")})
        metrics.append(series)
    record.data("metrics_ray_series", metrics)
    check(record, "the metrics port answers without auth (ray_ series)", all(m > 50 for m in metrics), metrics)


def test_L05_logs_a_user_reads(record, lifecycle, hold):
    lines = lifecycle.monitor_log()
    record.evidence("monitor.log", "\n".join(lines))
    for name, pattern in (("rent", r"create \d+ x \S+: rented \d+ candidates"), ("enrolled", r"enrolled instance \d+ \(machine \d+, ip [\d.]+\) into"),
                          ("ending", r"ending instance \d+ \(machine")):
        check(record, f"monitor.log has the {name} line", any(re.search(pattern, x) for x in lines), "")
    legacy = [x for x in lines if "Dropping the empty legacy field" in x]
    record.data("legacy_field_lines", len(legacy))
    check(record, "the `Dropping the empty legacy field` lines are not errors", all("ERROR" not in x for x in legacy), legacy[:1])
    done = lifecycle.cli("attach", lifecycle.yaml, input='sudo docker exec ray-vast-head grep "process limit" '
                         '/tmp/ray/session_latest/logs/monitor.log; echo GREP_EXIT $?\nexit\n', timeout=180)
    record.evidence("grep_process_limit.txt", done.stdout + done.stderr)
    check(record, "the documented grep runs through attach", re.search(r"GREP_EXIT [01]", done.stdout) is not None, done.stdout[-200:])


def _node_of(nodes, index):
    return sorted(nodes, key=lambda n: n["container_id"])[index]


@pytest.mark.failure
@pytest.mark.security
def test_L07_signals_to_pid_1(record, lifecycle, session_env, hold):
    _, nodes = probe_nodes(lifecycle, session_env, record.dir)
    a = _node_of(nodes, 0)
    wd = workdir(MANUAL / "task_probe.py")
    status, log = lifecycle.job(session_env, wd, ["python", "task_probe.py", "signals", a["node_id"], "120"], out_dir=record.dir,
                                name="signals", timeout=900)
    result = json.loads(next(line for line in log.splitlines() if line.startswith('{"orphan_parents"')))
    check(record, "KILL, QUIT, USR1, USR2, CONT: same PID 1 after 120 s", result["pid1_same"], result["kill"])
    check(record, "0 zombies of PID 1", result["zombies_of_pid1_end"] == 0, result["zombies_of_pid1_end"])
    log_start = len(lifecycle.monitor_log()) + 1
    status, log = lifecycle.job(session_env, wd, ["python", "task_probe.py", "sigterm", a["node_id"]], out_dir=record.dir,
                                name="sigterm", timeout=300)
    t0 = json.loads(log.splitlines()[[i for i, x in enumerate(log.splitlines()) if '"t0"' in x][0]])["t0"]
    # t0 is before the driver (on the head) schedules the signal task; the task prints the time of the signal (worker clock).
    sent = next((json.loads(x)["utc"] for x in log.splitlines() if x.startswith('{"sigterm": "sent"')), None)
    record.data("signal_after_t0_s", sent and round(sent - t0, 2))
    gone = None
    while time.time() < t0 + 120:
        if int(a["container_id"]) not in {i["id"] for i in lifecycle.instances()}:
            gone = time.time()
            break
        time.sleep(2)
    lines = lifecycle.monitor_log(log_start)
    report = next((lifecycle.log_epoch(x) for x in lines if f"worker {a['container_id']} asks to be destroyed" in x and "SIGTERM" in x), None)
    record.evidence("monitor_slice.log", "\n".join(lines))
    # design.md:262 (a TERM to PID 1: "Report, then destroy") states no time. Pass: the head logs the report, vast lists
    # the instance as gone within 60 s (a safety bound: a live instance costs money), Ray marks the node DEAD (safety
    # bound 300 s). The times are measurements (data), not pass criteria. `report_after_signal_s` mixes the worker and
    # head clocks.
    record.data("report_after_signal_s", report and sent and round(report - sent, 2))
    record.data("gone_after_t0_s", gone and round(gone - t0, 1))
    check(record, "TERM: the head logs the report `asks to be destroyed: 'received SIGTERM'`", report is not None, "")
    check(record, "TERM: gone from vast within 60 s (safety bound)", gone is not None and gone - t0 <= 60, gone and round(gone - t0, 1))
    dead = None
    while time.time() < t0 + 300:
        state = lifecycle.head_exec(f"ray list nodes --filter node_id={a['node_id']} --format json", check=False)
        if '"DEAD"' in state:
            dead = time.time()
            break
        time.sleep(5)
    record.data("dead_after_t0_s", dead and round(dead - t0, 1))
    check(record, "Ray marks the node DEAD (safety bound 300 s)", dead is not None, dead and round(dead - t0, 1))
    deadline = time.time() + 1200
    while time.time() < deadline:
        _, after = probe_nodes(lifecycle, session_env, record.dir, name="probe_after")
        if len(after) == 4 and a["container_id"] not in {n["container_id"] for n in after}:
            break
        time.sleep(30)
    check(record, "a replacement joins", len(after) == 4 and a["container_id"] not in {n["container_id"] for n in after}, len(after))


@pytest.mark.failure
def test_L10_head_loss_then_down_without_a_vast_key(record, lifecycle, session_env, hold):
    """Stop the hold, wait for 0 workers, ask for 4 again; when one worker is enrolled and another instance still loads,
    stop the head container. Every instance must destroy itself; then `down` without a vast key."""
    if lifecycle.kept:
        pytest.skip("--keep-cluster: the cluster stays")
    subprocess.run([str(BIN / "ray"), "job", "stop", "--address", session_env["RAY_ADDRESS"], hold], env=session_env,
                   capture_output=True, timeout=120)
    deadline = time.time() + 900
    while time.time() < deadline and lifecycle.instances():
        time.sleep(15)
    log_start = len(lifecycle.monitor_log()) + 1
    lifecycle.job(session_env, workdir(JOBS / "hold.py"), ["python", "hold.py", "4"], name="hold_again", no_wait=True, out_dir=record.dir)
    t_stop, deadline = None, time.time() + 1800
    while time.time() < deadline:
        enrolled = {int(m.group(1)) for x in lifecycle.monitor_log(log_start) for m in [re.search(r"enrolled instance (\d+)", x)] if m}
        instances = lifecycle.instances()
        loading = [i for i in instances if i["id"] not in enrolled and i.get("actual_status") != "running"]
        if enrolled and loading:
            lifecycle.ssh("sudo docker stop ray-vast-head")
            t_stop = time.time()
            break
        time.sleep(5)
    check(record, "head stopped with a worker enrolled and a candidate loading", t_stop is not None, "")
    workers = {i["id"] for i in lifecycle.instances()} & enrolled
    record.data("at_stop", {"workers": sorted(workers), "others": sorted({i["id"] for i in lifecycle.instances()} - enrolled)})
    gone_at = {}
    while time.time() < t_stop + 30 * 60:
        listed = {i["id"]: i for i in lifecycle.instances()}
        for iid in set(record.result["data"]["at_stop"]["workers"]) | set(record.result["data"]["at_stop"]["others"]):
            if iid not in listed:
                gone_at.setdefault(iid, round(time.time() - t_stop))
        if not listed:
            break
        time.sleep(10)
    record.data("gone_s_after_head_stop", gone_at)
    # Workers: design.md:246, :258 (no heartbeat success for 300 s, one each 30 s) and :261 (Ray exits 60 s after the GCS
    # is lost): at most 330 s, plus 30 s for vast's list and this loop. Candidates: design.md:144, :155, :260 (10 min of
    # failed downloads or no enrollment within 10 min) after a pull that the dead provider no longer limits: 30 min.
    for iid in record.result["data"]["at_stop"]["workers"]:
        check(record, f"worker {iid} destroys itself within 360 s", gone_at.get(iid, 10**6) <= 360, gone_at.get(iid))
    for iid in record.result["data"]["at_stop"]["others"]:
        check(record, f"candidate {iid} is gone within 30 min (10 min after its pull)", gone_at.get(iid, 10**6) <= 30 * 60, gone_at.get(iid))
    done = lifecycle.down(env={"VAST_API_KEY": ""})  # the group teardown checks AWS and vast after it (livekit.end_cluster)
    record.evidence("down_without_vast_key.txt", done.stdout + done.stderr)
    check(record, "down without a vast key: the warning, exit 0", done.returncode == 0 and "warning: no vast key" in done.stdout, done.stdout[-300:])
