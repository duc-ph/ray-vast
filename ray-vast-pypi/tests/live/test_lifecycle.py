"""Group `lifecycle` (docs/test-plan.md, claim matrix L01-L10): the C8 shape (2 GPU node types x 2, k = 2), run as the
restricted IAM user, in us-east-2 with a t3.medium head, no top-level max_workers. Order: up and scale (L01, L02), then a
hold of 4 workers for the probes (L03-L05), then the destructive scenarios (L07, L10). The teardown of the cluster
(conftest.group_lifetime) checks the `down`.
About 2 h; vast about $2, EC2 about $0.10."""
import json
import re
import subprocess
import threading
import time

import boto3
import pytest

from livekit import check, expected_limits
from cluster import ADMIN_PROFILE, JOBS, MANUAL
from window import phase, wait_until

pytestmark = pytest.mark.lifecycle


def worker_rules(cluster):
    ec2 = boto3.Session(profile_name=ADMIN_PROFILE, region_name=cluster.region).client("ec2")
    groups = ec2.describe_security_groups(Filters=[{"Name": "tag:ray-vast-cluster", "Values": [cluster.name]}])["SecurityGroups"]
    return sum(1 for g in groups for p in g["IpPermissions"] for r in p.get("IpRanges", []) if r.get("Description") == f"ray-vast:{cluster.name}")


@pytest.fixture(scope="module")
def session_env(lifecycle):
    with lifecycle.tunnel() as env:
        yield env


@pytest.fixture(scope="module")
def hold(lifecycle, session_env):
    """4 workers held by a placement group (hold.py) for L03-L10. Stopped at the end of the module. No HOLD_READY in
    1800 s is an error of each test that uses it."""
    job = lifecycle.job(session_env, ["python", "hold.py", "4"], name="hold", files=[JOBS / "hold.py"], wait=False)
    ready = wait_until(job.logs, 1800, done=lambda log: "HOLD_READY" in log, poll_s=20)
    if not ready:
        job.stop()
        raise RuntimeError(f"the hold job printed no HOLD_READY in 1800 s ({ready.error or 'no error'})")
    yield job
    job.stop()


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
    w = lifecycle.window()
    job = lifecycle.job(session_env, ["python", "c8_job.py"], record.dir, name="c8_job", files=[MANUAL / "c8_job.py"], timeout=2400)
    status = job.status
    t_end = time.time()
    results = [json.loads(line) for line in job.log.splitlines() if line.startswith("{")]
    check(record, "c8_job SUCCEEDED with 4 containers", status == "SUCCEEDED" and len({r["container_id"] for r in results}) == 4, status)
    check(record, "both node types", {r["node_type"] for r in results} == {"gpu-a", "gpu-b"}, sorted({str(r["node_type"]) for r in results}))
    for r in results:
        check(record, f"bundle {r['bundle']}: 1 GPU, gpu_ids [0]", r["gpu_ids"] == [0] and r["nvidia_smi"].count("GPU ") == 1,
              (r["gpu_ids"], r["nvidia_smi"][:80]))
    # idle scale-down: all instances gone within idle_timeout_minutes (2) + 120 s
    idle = wait_until(w.vast_now, t_end + 120 + 420 - time.time(), done=lambda seen: not seen, poll_s=15)
    t_zero = time.time()
    stop.set()
    # control-cost.md:24: Ray removes a worker after `idle_timeout_minutes` (2) idle. 300 s more is a safety bound for the
    # autoscaler loop, the drain and the destroy; the time is data.
    record.data("idle_to_zero_s", round(t_zero - t_end))
    check(record, "idle scale-down to 0 within idle_timeout + 300 s", idle.ok and t_zero - t_end <= 420,
          round(t_zero - t_end))
    time.sleep(20)
    rules_after = worker_rules(lifecycle)
    record.data("rules_seen_max", max((n for _, n in rules_seen), default=None))
    check(record, "worker rules were added and are all revoked", max((n for _, n in rules_seen), default=0) >= 1 and rules_after == 0,
          (max((n for _, n in rules_seen), default=0), rules_after))
    lines = w.lines()
    record.evidence("monitor_slice.log", "\n".join(lines))
    events = w.events(lines=lines)
    rows = [r for r in lifecycle.timeline if r["t"] >= t_submit - 5 and "instances" in r]
    rented = set(w.vast_seen())
    peak = max((sum(i.get("dph_total") or 0 for i in r["instances"]) for r in rows), default=0)
    enroll_s = w.enroll_seconds()
    record.data("rented", len(rented))
    record.data("peak_dph", round(peak, 3))
    record.data("enroll_s_from_first_running_sample", enroll_s)
    check(record, "rentals <= candidate_factor x slots (8)", 4 <= len(rented) <= 8, len(rented))
    check(record, "peak $/h below max_dollars_per_hour (3.0)", peak <= 3.0, round(peak, 3))
    # design.md:56, :86: a candidate that does not enroll 1 min after the provider sees it `running` is ended. This suite's
    # own 10 s samples of vast differ from the provider's by up to 15 s either way: -15 to 75 s.
    check(record, "enroll within 1 min of running (design.md:86; -15 to 75 s on the suite's samples)",
          enroll_s and max(enroll_s.values()) <= 75 and min(enroll_s.values()) >= -15, enroll_s)
    pull = [e.line for e in events if e.kind == "ending" and e.cause == "pull_limit"]
    check(record, "no candidate ended on the pull limit", not pull, pull[:2])
    missing = [e.line for e in events if e.kind == "ending" and e.cause == "gone_from_vast"]
    check(record, "no `missing from vast` ending", not missing, missing[:2])
    unknown = [e.line for e in events if e.kind == "unknown_status"]
    check(record, "no unknown vast status", not unknown, unknown[:2])
    check(record, "rent lines", any(e.kind == "rented" and e.fields["node_type"] in ("gpu-a", "gpu-b") for e in events), "")


@pytest.mark.security
@pytest.mark.limits
def test_L03_host_and_task_probe_on_each_worker(record, lifecycle, session_env, hold):
    nodes = lifecycle.probe(session_env, record.dir)  # raises unless the probe job SUCCEEDED
    record.evidence("node_probe.json", json.dumps(nodes, indent=1))
    check(record, "probe job SUCCEEDED on 4 workers", len(nodes) == 4, len(nodes))
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
        ports, soft_limit = expected_limits(n)
        check(record, f"{cid}: worker ports of the bootstrap formula (pids.max {n.get('pids.max')})", n.get("ports") == ports,
              (n.get("ports"), ports))
        env = n.get("ray_env") or {}
        check(record, f"{cid}: soft limit of the bootstrap formula, grpc 4, startup 8",
              env.get("RAY_num_workers_soft_limit") == str(soft_limit) and
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
    nodes = lifecycle.probe(session_env, record.dir)
    for n in nodes:
        cid = n["container_id"]
        check(record, f"{cid}: agent port answers 401 without the token", n["agent_http_no_token"] == 401, n["agent_http_no_token"])
        check(record, f"{cid}: runtime-env port answers 401 without the token", n["runtime_env_no_token"] == 401, n["runtime_env_no_token"])
        for port in ("dashboard_agent_listen_port", "runtime_env_agent_port"):
            check(record, f"{cid}: {port} listens on 127.0.0.1 only", n["listen"].get(port) == ["127.0.0.1"], n["listen"].get(port))
    ips = sorted({n["ip"] for n in nodes})
    secrets_dir = f"/home/ubuntu/ray-vast-{lifecycle.name}/secrets"
    lifecycle.ssh("sudo rm -f /tmp/rv_cap.pcap; (sudo timeout 600 tcpdump -i ens5 -w /tmp/rv_cap.pcap not port 22 >/dev/null 2>&1 &)")
    battery = lifecycle.job(session_env, ["python", "battery.py", ips[0], ips[1]], record.dir, name="battery",
                            files=[MANUAL / "battery.py"], timeout=1800)
    lifecycle.ssh("sudo pkill -INT tcpdump; sleep 3", check=False)
    hits = lifecycle.ssh(f"sudo grep -c -a -F -f {secrets_dir}/token /tmp/rv_cap.pcap; ls -l /tmp/rv_cap.pcap", check=False)
    lifecycle.ssh("sudo rm -f /tmp/rv_cap.pcap", check=False)
    record.evidence("token_in_capture.txt", hits)
    data = (battery.lines("BATTERY_JSON") or [{}])[-1]
    record.data("battery", {k: v for k, v in data.items() if k.startswith("step")})
    check(record, "battery ran (actor RTT, pip, 50 MB both ways, worker to worker)", battery.status == "SUCCEEDED" and all(
        f"step{i}" in " ".join(data) for i in (1, 2, 3, 4)), battery.status)
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
    w = lifecycle.window(from_start=True)
    lines = w.lines()
    record.evidence("monitor.log", "\n".join(lines))
    events = w.events(lines=lines)
    for name, kind in (("rent", "rented"), ("enrolled", "enrolled"), ("ending", "ending")):
        check(record, f"monitor.log has the {name} line", any(e.kind == kind for e in events), "")
    legacy = [e.line for e in events if e.kind == "legacy_field"]
    record.data("legacy_field_lines", len(legacy))
    check(record, "the `Dropping the empty legacy field` lines are not errors", all("ERROR" not in x for x in legacy), legacy[:1])
    done = lifecycle.cli("attach", lifecycle.yaml, input='sudo docker exec ray-vast-head grep "process limit" '
                         '/tmp/ray/session_latest/logs/monitor.log; echo GREP_EXIT $?\nexit\n', timeout=180)
    record.evidence("grep_process_limit.txt", done.stdout + done.stderr)
    check(record, "the documented grep runs through attach", re.search(r"GREP_EXIT [01]", done.stdout) is not None, done.stdout[-200:])


@pytest.mark.failure
@pytest.mark.security
def test_L07_signals_to_pid_1(record, lifecycle, session_env, hold):
    nodes = lifecycle.probe(session_env, record.dir)
    a = min(nodes, key=lambda n: n["container_id"])
    log = lifecycle.job(session_env, ["python", "task_probe.py", "signals", a["node_id"], "120"], record.dir, name="signals",
                        files=[MANUAL / "task_probe.py"], timeout=900).log
    result = json.loads(next(line for line in log.splitlines() if line.startswith('{"orphan_parents"')))
    check(record, "KILL, QUIT, USR1, USR2, CONT: same PID 1 after 120 s", result["pid1_same"], result["kill"])
    check(record, "0 zombies of PID 1", result["zombies_of_pid1_end"] == 0, result["zombies_of_pid1_end"])
    w = lifecycle.window()
    log = lifecycle.job(session_env, ["python", "task_probe.py", "sigterm", a["node_id"]], record.dir, name="sigterm",
                        files=[MANUAL / "task_probe.py"], timeout=300).log
    t0 = json.loads(log.splitlines()[[i for i, x in enumerate(log.splitlines()) if '"t0"' in x][0]])["t0"]
    # t0 is before the driver (on the head) schedules the signal task; the task prints the time of the signal (worker clock).
    sent = next((json.loads(x)["utc"] for x in log.splitlines() if x.startswith('{"sigterm": "sent"')), None)
    record.data("signal_after_t0_s", sent and round(sent - t0, 2))
    waited = wait_until(w.vast_now, t0 + 120 - time.time(), done=lambda seen: int(a["container_id"]) not in {i["id"] for i in seen},
                        poll_s=2)
    gone = time.time() if waited else None
    lines = w.lines()
    report = next((e.t for e in w.events("destroy_request", instances=[a["container_id"]], lines=lines) if "SIGTERM" in e.reason), None)
    record.evidence("monitor_slice.log", "\n".join(lines))
    # design.md:262 (a TERM to PID 1: "Report, then destroy") states no time. Pass: the head logs the report, vast lists
    # the instance as gone within 60 s (a safety bound: a live instance costs money), Ray marks the node DEAD (safety
    # bound 300 s). The times are measurements (data), not pass criteria. `report_after_signal_s` mixes the worker and
    # head clocks.
    record.data("report_after_signal_s", report and sent and round(report - sent, 2))
    record.data("gone_after_t0_s", gone and round(gone - t0, 1))
    check(record, "TERM: the head logs the report `asks to be destroyed: 'received SIGTERM'`", report is not None, "")
    check(record, "TERM: gone from vast within 60 s (safety bound)", gone is not None and gone - t0 <= 60, gone and round(gone - t0, 1))
    waited = wait_until(lambda: lifecycle.head_exec(f"ray list nodes --filter node_id={a['node_id']} --format json", check=False),
                        t0 + 300 - time.time(), done=lambda state: '"DEAD"' in state, poll_s=5)
    dead = time.time() if waited else None
    record.data("dead_after_t0_s", dead and round(dead - t0, 1))
    check(record, "Ray marks the node DEAD (safety bound 300 s)", dead is not None, dead and round(dead - t0, 1))
    joined = wait_until(lambda: lifecycle.probe(session_env, record.dir, name="probe_after", check=False), 1200,
                        done=lambda after: len(after) == 4 and a["container_id"] not in {n["container_id"] for n in after}, poll_s=30)
    check(record, "a replacement joins", joined.ok, (len(joined.seen or []), joined.error))


@pytest.mark.failure
def test_L10_head_loss_then_down_without_a_vast_key(record, lifecycle, session_env, hold):
    """Stop the hold, wait for 0 workers, ask for 4 again; when one worker is enrolled and another instance still loads,
    stop the head container. Every instance must destroy itself; then `down` without a vast key."""
    if lifecycle.kept:
        pytest.skip("--keep-cluster: the cluster stays")
    hold.stop()
    idle = wait_until(lifecycle.instances, 900, done=lambda seen: not seen, poll_s=15)
    check(record, "0 workers before the second hold (within 900 s)", idle.ok, [i["id"] for i in idle.seen or []] or idle.error)
    w = lifecycle.window()
    lifecycle.job(session_env, ["python", "hold.py", "4"], record.dir, name="hold_again", files=[JOBS / "hold.py"], wait=False)

    def enrolled_and_loading():  # (the enrolled instances, the pending candidates that are not enrolled)
        enrolled = {e.instance for e in w.events("enrolled")}
        return enrolled, [i for i in w.vast_now() if i["id"] not in enrolled and phase(i.get("actual_status")) == "pending"]

    ready = wait_until(enrolled_and_loading, 1800, done=lambda seen: seen[0] and seen[1], poll_s=5)
    t_stop = None
    if ready:
        lifecycle.ssh("sudo docker stop ray-vast-head")
        t_stop = time.time()
    check(record, "head stopped with a worker enrolled and a candidate loading", t_stop is not None, ready.error or "")
    enrolled, listed = ready.seen[0], {i["id"] for i in w.vast_now()}
    at_stop = {"workers": sorted(listed & enrolled), "others": sorted(listed - enrolled)}
    record.data("at_stop", at_stop)
    empty = wait_until(w.vast_now, t_stop + 30 * 60 - time.time(), done=lambda seen: not seen, poll_s=10)
    record.check("vast lists no instance of the cluster within 30 min of the head stop", empty.ok,
                 [i["id"] for i in empty.seen or []] or empty.error)
    seen = w.vast_seen()  # the time of each instance's `gone`: the first sample (the poller's or the wait's) without it
    gone_at = {iid: round(seen[iid][-1][0] - t_stop) for iid in at_stop["workers"] + at_stop["others"]
               if seen.get(iid) and seen[iid][-1][1] == "gone"}
    record.data("gone_s_after_head_stop", gone_at)
    # Workers: design.md:246, :258 (no heartbeat success for 300 s, one each 30 s) and :261 (Ray exits 60 s after the GCS
    # is lost): at most 330 s, plus 30 s for vast's list and this loop. Candidates: design.md:144, :155, :260 (10 min of
    # failed downloads or no enrollment within 10 min) after a pull that the dead provider no longer limits: 30 min.
    for iid in at_stop["workers"]:
        check(record, f"worker {iid} destroys itself within 360 s", gone_at.get(iid, 10**6) <= 360, gone_at.get(iid))
    for iid in at_stop["others"]:
        check(record, f"candidate {iid} is gone within 30 min (10 min after its pull)", gone_at.get(iid, 10**6) <= 30 * 60, gone_at.get(iid))
    done = lifecycle.down(env={"VAST_API_KEY": ""})  # the group teardown checks AWS and vast after it (livekit.end_cluster)
    record.evidence("down_without_vast_key.txt", done.stdout + done.stderr)
    check(record, "down without a vast key: the warning, exit 0", done.returncode == 0 and "warning: no vast key" in done.stdout, done.stdout[-300:])
