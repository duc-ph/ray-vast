"""Step 11: the whole flow on the dev box, with a fake vast that runs instances as local containers."""
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from e2e_harness import *  # noqa: F401,F403  (the fixtures)

pytestmark = pytest.mark.local_e2e


def test_scale_up_three_workers_with_k_2_runs_the_battery_then_idle_scale_down_to_zero(make_cluster):
    """6 candidates for 3 slots: 3 fill the slots, the provider destroys the 3 unused. The battery. Idle scale-down."""
    cluster = make_cluster(offers=8)
    cluster.write_config(candidate_factor=2)
    cluster.start_head()

    out = cluster.run_driver("battery_subset.py", 3)
    result = json.loads(out.split("BATTERY_JSON ", 1)[1].splitlines()[0])
    assert result["workers"] == 3, result
    assert result["tasks_on_nodes"] == 3, result
    assert result["actors_ping"] == [1, 1, 1], result
    assert result["head_to_worker"] == [True] * 3 and result["worker_to_head"] == [True] * 3, result
    assert result["worker_to_worker"] == [True] * 3, result
    assert result["job_succeeded"], result
    rentals, workers = cluster.fake.rentals, cluster.fake.live_ids()
    assert len(rentals) == 6 and len(workers) == 3, cluster.diagnostics()
    unused = [d[0] for d in cluster.fake.destroys if d[1] == "account"]
    assert sorted(unused + workers) == rentals and len(cluster.fake.destroys) == 3  # the provider, not the teardown
    assert cluster.sg_cidrs() == [f"{cluster.box_ip}/32"]

    cluster.wait_for(lambda: cluster.fake.live_ids() == [], 300, "idle scale-down to 0")
    assert {d[1] for d in cluster.fake.destroys} == {"account"}
    assert sorted(d[0] for d in cluster.fake.destroys) == rentals
    cluster.wait_for(lambda: cluster.sg_cidrs() == [], 30, "the security group rule is removed")
    cluster.assert_live([])


def test_a_candidate_that_never_enrolls_is_ended_and_its_slot_is_asked_for_again(make_cluster):
    cluster = make_cluster(silent_hosts=1)  # the first rental runs, but never asks to enroll
    cluster.write_config(idle_timeout_minutes=30)
    cluster.start_head()

    result = hold(cluster, 1, timeout=240)
    assert len(result["nodes"]) == 1
    rentals = cluster.fake.rentals
    assert len(rentals) == 2, cluster.diagnostics()
    silent, again = rentals
    assert [d[:2] for d in cluster.fake.destroys] == [(silent, "account")]
    fake = cluster.fake
    assert fake.destroyed_at[silent] - fake.rented_at[silent] >= 60  # the 1 min limit after `running`
    assert fake.rented_at[again] > fake.destroyed_at[silent]  # v2 asked again after the slot was removed
    monitor = docker("exec", HEAD, "cat", "/tmp/ray/session_latest/logs/monitor.log").stdout
    assert f"ending instance {silent}" in monitor and "not enrolled 60 s after running" in monitor
    cluster.assert_live([again])


def hold(cluster, n, *extra, timeout=600):
    """Make the autoscaler start n workers (a placement group of n nodes); return what the driver printed."""
    out = cluster.run_driver("hold_nodes.py", n, *extra, timeout=timeout)
    return json.loads(out.split("HOLD_JSON ", 1)[1].splitlines()[0])


def test_two_workers_with_the_same_public_ip_both_join_and_the_idle_one_is_removed(make_cluster):
    cluster = make_cluster()
    cluster.write_config()
    cluster.start_head()

    result = hold(cluster, 2, "busy")
    assert len(result["nodes"]) == 2
    busy = int(result["busy_container"])
    live = cluster.fake.live_ids()
    assert len(live) == 2 and busy in live
    assert len({cluster.fake._record(i)["public_ipaddr"] for i in live}) == 1  # one public IP for both
    idle = next(i for i in live if i != busy)

    cluster.wait_for(lambda: cluster.fake.live_ids() == [busy], 300, "the idle worker is removed")
    assert [d[:2] for d in cluster.fake.destroys] == [(idle, "account")]
    ping = cluster.run_driver("ping_busy.py")
    assert f"PING {busy}" in ping  # the busy worker still serves its actor
    cluster.assert_live([busy])


def test_host_with_more_cores_than_declared_runs_with_the_real_core_count(make_cluster):
    cluster = make_cluster(docker_args=["--cpus", "2"])  # the box has 4 cores; the cgroup allows 2
    cluster.write_config(cpu=1)  # the node type declares 1
    cluster.start_head()

    result = hold(cluster, 1)
    assert [n["cpu"] for n in result["nodes"]] == [2]


def hold_two_workers_that_passed_the_token_check(cluster):
    hold(cluster, 2)
    for instance_id in cluster.fake.live_ids():
        cluster.wait_for(lambda: cluster.agents_refuse_without_token(instance_id), 60,
                         f"worker {instance_id} passes the token check")


def wait_until_each_worker_reported_and_destroyed_itself(cluster, timeout, reasons):
    """Each worker tries to report its reason to the silent head (the report fails), then destroys its own instance."""
    cluster.wait_for(lambda: cluster.fake.live_ids() == [], timeout, "both workers destroy themselves")
    assert sorted(d[1] for d in cluster.fake.destroys) == ["instance", "instance"]
    for _, _, log in cluster.fake.destroys:
        assert any(reason in log for reason in reasons), log[-2000:]
        assert "destroy report failed" in log, log[-2000:]
    cluster.assert_live([])


def test_head_silence_makes_each_worker_report_and_destroy_itself(make_cluster):
    cluster = make_cluster(worker_env={"RAYVAST_TIME_SCALE": "5"})  # heartbeat timeout 300 s -> 60 s
    cluster.write_config(idle_timeout_minutes=30)
    cluster.start_head()
    hold_two_workers_that_passed_the_token_check(cluster)

    docker("pause", HEAD)
    wait_until_each_worker_reported_and_destroyed_itself(cluster, 240, ["no successful heartbeat"])


def test_container_restart_is_refused_a_second_enrollment_and_destroys_itself(make_cluster):
    cluster = make_cluster()
    cluster.write_config(idle_timeout_minutes=30)
    cluster.start_head()
    hold(cluster, 1)
    (instance,) = cluster.fake.live_ids()
    name = cluster.fake.name_of(instance)

    docker("kill", name)  # a hard stop, as when a host reboots (`docker restart` sends SIGTERM, which destroys at once)
    docker("start", name)
    cluster.wait_for(lambda: cluster.fake.live_ids() == [], 120, "the restarted worker destroys itself")
    (destroyed, who, log) = cluster.fake.destroys[0]
    assert (destroyed, who) == (instance, "instance")
    assert "head refused enrollment: already enrolled" in log
    cluster.assert_live([])


@pytest.mark.timeout(900)
def test_monitor_crash_stops_the_head_and_every_worker_destroys_itself(make_cluster):
    """Design 7.3: no self-healing. Ray stops the head when the monitor dies (F11); the head stays stopped; each worker
    loses the GCS and the heartbeat, tries to report to the head, and destroys its own instance."""
    cluster = make_cluster()
    cluster.write_config(idle_timeout_minutes=30)
    cluster.start_head()
    hold_two_workers_that_passed_the_token_check(cluster)

    docker("exec", HEAD, "pkill", "-9", "-f", "autoscaler/v2/monitor.py")
    cluster.wait_for(lambda: docker("inspect", "-f", "{{.State.Status}}", HEAD).stdout.strip() == "exited",
                     180, "ray start --block stops the head after the monitor dies")
    assert docker("inspect", "-f", "{{.HostConfig.RestartPolicy.Name}}", HEAD).stdout.strip() in ("", "no")
    # A worker raylet exits 60 s after it loses the GCS (F11); the heartbeat limit is 300 s. Either one ends it.
    wait_until_each_worker_reported_and_destroyed_itself(cluster, 420,
                                                         ["ray start exited with code", "no successful heartbeat"])
    assert docker("inspect", "-f", "{{.State.Status}}", HEAD).stdout.strip() == "exited"  # nothing restarted it


@pytest.mark.timeout(900)
def test_three_enrollment_failures_in_a_row_pause_the_node_type_then_it_launches_again(make_cluster):
    scale = 10  # the 10 min breaker pause becomes 60 s
    cluster = make_cluster(offers=6, failing_hosts=3)
    cluster.write_config(idle_timeout_minutes=30, provider_module="scaled_provider.VastNodeProvider")
    cluster.start_head(RAYVAST_TIME_SCALE=str(scale))

    with ThreadPoolExecutor(1) as pool:
        held = pool.submit(hold, cluster, 1)  # the slot fails 3 times (the head refuses the hosts), then the 4th enrolls
        cluster.wait_for(lambda: len(cluster.fake.destroys) == 3, 400, "the head refuses and destroys 3 hosts")
        status = {}

        def breaker_shown():
            status["plain"] = docker("exec", HEAD, "ray", "status", check=False).stdout
            return "cpu: LaunchFailed" in status["plain"]
        cluster.wait_for(breaker_shown, 600 / scale, "`ray status` shows the failed launches of the paused node type")
        status["verbose"] = docker("exec", HEAD, "ray", "status", "-v", check=False).stdout
        print(status["plain"], status["verbose"], sep="\n")
        result = held.result()

    assert len(result["nodes"]) == 1
    fake = cluster.fake
    rentals = fake.rentals
    assert len(rentals) == 4, rentals
    assert [d[:2] for d in fake.destroys] == [(i, "account") for i in rentals[:3]]  # the head destroyed the 3 hosts
    pause = fake.rented_at[rentals[3]] - fake.destroyed_at[rentals[2]]
    assert pause >= 600 / scale, f"no pause after the third failure: the 4th rental came {pause:.0f} s after it"
    monitor = docker("exec", HEAD, "cat", "/tmp/ray/session_latest/logs/monitor.log").stdout
    assert monitor.count("worker checks failed: no_net_raw_admin") == 3, monitor[-3000:]
    assert f"3 slots in a row ended with no enrolled worker; no new launches of this type for {600 // scale} s" in monitor
    cluster.assert_live([rentals[3]])
