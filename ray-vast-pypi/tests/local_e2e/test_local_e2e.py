"""Step 11: the whole flow on the dev box, with a fake vast that runs instances as local containers."""
import json
import time
import subprocess

import pytest

from e2e_harness import *  # noqa: F401,F403  (the fixtures)

pytestmark = pytest.mark.local_e2e


def test_scale_up_two_workers_run_the_battery_then_idle_scale_down_to_zero(make_cluster):
    cluster = make_cluster()
    cluster.write_config()
    cluster.start_head()

    out = cluster.run_driver("battery_subset.py", 2)
    result = json.loads(out.split("BATTERY_JSON ", 1)[1].splitlines()[0])
    assert result["workers"] == 2, result
    assert result["tasks_on_nodes"] == 2, result
    assert result["actors_ping"] == [1, 1], result
    assert result["head_to_worker"] == [True, True] and result["worker_to_head"] == [True, True], result
    assert result["worker_to_worker"] is True, result
    assert result["job_succeeded"], result
    assert len(cluster.fake.rentals) >= 2  # a cold start can rent extra nodes (F11); they leave with the others
    assert cluster.sg_cidrs() == [f"{cluster.box_ip}/32"]

    cluster.wait_for(lambda: cluster.fake.live_ids() == [], 300, "idle scale-down to 0")
    assert {d[1] for d in cluster.fake.destroys} == {"account"}
    assert sorted(d[0] for d in cluster.fake.destroys) == sorted(cluster.fake.rentals)
    cluster.wait_for(lambda: cluster.sg_cidrs() == [], 30, "the security group rule is removed")


def hold(cluster, n, *extra):
    """Make the autoscaler start n workers (a placement group of n nodes); return what the driver printed."""
    out = cluster.run_driver("hold_nodes.py", n, *extra)
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
    assert [d[0] for d in cluster.fake.destroys] == [idle]
    ping = cluster.run_driver("ping_busy.py")
    assert f"PING {busy}" in ping  # the busy worker still serves its actor


def test_host_with_more_cores_than_declared_runs_with_the_real_core_count(make_cluster):
    cluster = make_cluster(docker_args=["--cpus", "2"])  # the box has 4 cores; the cgroup allows 2
    cluster.write_config(cpu=1)  # the node type declares 1
    cluster.start_head()

    result = hold(cluster, 1)
    assert [n["cpu"] for n in result["nodes"]] == [2]


def test_head_silence_makes_each_worker_report_and_destroy_itself(make_cluster):
    cluster = make_cluster(worker_env={"RAYVAST_TIME_SCALE": "5"})  # heartbeat timeout 300 s -> 60 s
    cluster.write_config(idle_timeout_minutes=30)
    cluster.start_head()
    hold(cluster, 2)
    time.sleep(20)  # the token check is done by now

    docker("pause", "st11-head")
    cluster.wait_for(lambda: cluster.fake.live_ids() == [], 240, "both workers destroy themselves")
    assert sorted(d[1] for d in cluster.fake.destroys) == ["instance", "instance"]
    for _, _, log in cluster.fake.destroys:
        assert "no successful heartbeat" in log
        assert "destroy report failed" in log  # it tried to report to the silent head first


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


def test_monitor_crash_stops_the_head_and_the_restarted_head_sweeps_and_scales_again(make_cluster):
    cluster = make_cluster()
    cluster.write_config(idle_timeout_minutes=30)
    cluster.start_head()
    hold(cluster, 1)
    (old,) = cluster.fake.live_ids()

    docker("exec", "st11-head", "pkill", "-9", "-f", "autoscaler/v2/monitor.py")
    cluster.wait_for(lambda: docker("inspect", "-f", "{{.State.Status}}", "st11-head").stdout.strip() == "exited",
                     180, "ray start --block stops the head after the monitor dies")
    docker("start", "st11-head")  # what the restart policy of Docker does
    cluster.wait_for(cluster.head_server_answers, 120, "the new provider serves")
    cluster.wait_for(lambda: docker("exec", "st11-head", "ray", "status", check=False).returncode == 0, 90, "ray status")

    cluster.wait_for(lambda: old not in cluster.fake.live_ids(), 60, "the new provider sweeps the old worker")
    assert [d[:2] for d in cluster.fake.destroys] == [(old, "account")]
    result = hold(cluster, 2)
    assert len(result["nodes"]) == 2 and len(cluster.fake.rentals) == 3


def test_k_2_rents_four_candidates_for_two_nodes_and_destroys_the_two_unused(make_cluster):
    cluster = make_cluster(offers=8)
    cluster.write_config(idle_timeout_minutes=30, candidate_factor=2)
    cluster.start_head()
    result = hold(cluster, 2)
    started = time.monotonic()
    assert len(result["nodes"]) == 2

    cluster.wait_for(lambda: len(cluster.fake.live_ids()) == 2, 60, "the two unused candidates are destroyed")
    assert len(cluster.fake.rentals) == 4 and len(cluster.fake.destroys) == 2
    assert time.monotonic() - started < 60
