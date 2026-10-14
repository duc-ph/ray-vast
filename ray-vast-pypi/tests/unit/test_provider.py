import logging
import re
import socket
import threading

import botocore.exceptions
import pytest
import requests
from ray.autoscaler.node_launch_exception import NodeLaunchException
from ray.autoscaler.tags import TAG_RAY_NODE_KIND, TAG_RAY_NODE_STATUS, TAG_RAY_USER_NODE_TYPE

import ray_vast.state
from conftest import offer  # noqa: E402

LABEL = "ray-vast:c1"
NODE_CONFIG = {"offer_query": {"gpu_ram": {"gte": 24000}}, "disk_gb": 40}
TAGS = {TAG_RAY_NODE_KIND: "worker", TAG_RAY_USER_NODE_TYPE: "gpu", TAG_RAY_NODE_STATUS: "uninitialized"}
RESOURCES = {"CPU": 4, "GPU": 1}


def create(provider, count=1):
    return provider.create_node_with_resources_and_labels(NODE_CONFIG, dict(TAGS), count, RESOURCES, {})


def instance_ids(env):
    return sorted(env.vast.instances)


def enroll(provider, instance_id, slot_id=None):
    """What the head server does on a good /enroll: mark enrolled and fill a slot."""
    with provider.state.lock:
        cand = provider.state.candidates[instance_id]
        slot = provider.state.slots[slot_id or cand.slot_id]
        provider.state.mark_enrolled(instance_id)
        slot.instance_id, slot.status = instance_id, "setting-up"


def test_head_is_always_listed_with_the_tags_v2_needs(env):
    p = env.make()
    assert p.non_terminated_nodes({}) == ["head"]
    assert p.node_tags("head") == {TAG_RAY_NODE_KIND: "head", TAG_RAY_USER_NODE_TYPE: "head",
                                   TAG_RAY_NODE_STATUS: "up-to-date"}
    assert p.non_terminated_nodes({TAG_RAY_NODE_KIND: "worker"}) == []
    assert p.is_running("head") and not p.is_terminated("head") and p.external_ip("head") == "9.9.9.9"
    p.terminate_node("head")
    assert p.non_terminated_nodes({}) == ["head"]


def test_create_rents_candidates_and_returns_slots_without_waiting(env):
    env.vast.offers = [offer(1, 11), offer(2, 12), offer(3, 13)]
    p = env.make()
    created = create(p, count=1)
    (slot_id,) = created
    assert len(env.vast.instances) == 2  # candidate_factor 2 x count 1
    assert p.non_terminated_nodes({TAG_RAY_USER_NODE_TYPE: "gpu"}) == [slot_id]
    assert not p.is_running(slot_id) and not p.is_terminated(slot_id)
    assert p.node_tags(slot_id)[TAG_RAY_NODE_STATUS] == "uninitialized"
    request = env.vast.requests[0]
    assert request["label"] == LABEL and request["disk_gb"] == 40 and request["image"] == "img:1"
    assert request["args"][:2] == ["sh", "-c"] and "bootstrap" in request["args"][2]
    assert request["env"]["RAYVAST_HEAD_IP"] == "9.9.9.9" and request["env"]["RAYVAST_PORT_A"] == "7443"
    assert request["env"]["RAYVAST_CLUSTER"] == "c1" and len(request["env"]["RAYVAST_CA_SHA256"]) == 64
    assert env.vast.last_query["num_gpus"] == {"eq": 1}


def test_create_without_offers_raises_and_leaves_no_slot(env):
    p = env.make()
    with pytest.raises(NodeLaunchException) as e:
        create(p)
    assert e.value.category == "no offers"
    assert p.non_terminated_nodes({}) == ["head"]


def test_budget_limits_rentals(env):
    env.vast.offers = [offer(1, 11, dph=4), offer(2, 12, dph=4), offer(3, 13, dph=4)]
    p = env.make(max_dollars_per_hour=9, candidate_factor=3)
    create(p)
    assert len(env.vast.instances) == 2  # the third would reach 12 dollars per hour
    with pytest.raises(NodeLaunchException) as e:
        create(p)
    assert e.value.category == "budget"


def test_blocklisted_machine_is_skipped(env):
    env.vast.offers = [offer(1, 11), offer(2, 12)]
    p = env.make(candidate_factor=1)
    p.state.block(11, "bad", "security", env.clock())
    create(p)
    assert [r["offer_id"] for r in env.vast.requests] == [2]


def test_rental_error_tries_next_offer_and_all_failing_raises(env):
    env.vast.offers = [offer(1, 11)]
    env.vast.fail_create = True
    p = env.make()
    with pytest.raises(NodeLaunchException):
        create(p)
    assert p.non_terminated_nodes({}) == ["head"]


def test_read_methods_make_no_vast_calls(env):
    env.vast.offers = [offer(1, 11)]
    p = env.make()
    (slot_id,) = create(p)
    before = len(env.vast.calls)
    p.non_terminated_nodes({}), p.is_running(slot_id), p.is_terminated(slot_id), p.node_tags(slot_id)
    p.set_node_tags(slot_id, {"x": "y"}), p.internal_ip(slot_id), p.external_ip(slot_id)
    assert len(env.vast.calls) == before
    assert p.node_tags(slot_id)["x"] == "y"
    assert p.node_tags("slot-gone") == {} and not p.is_running("slot-gone") and p.is_terminated("slot-gone")


def test_running_candidate_gets_ip_and_sg_rule(env):
    env.vast.offers = [offer(1, 11)]
    p = env.make(candidate_factor=1)
    create(p)
    p.reconcile_once()
    assert env.rules() == []
    env.vast.run(101, "5.5.5.5")
    p.reconcile_once()
    assert env.rules() == ["5.5.5.5/32"]
    assert p.state.ip_refs == {"5.5.5.5": 1}
    env.vast.instances[101]["public_ipaddr"] = "6.6.6.6"  # an IP change before enrollment ends it too
    p.reconcile_once()
    assert instance_ids(env) == [] and env.rules() == []


def test_sg_quota_destroys_candidate(env, monkeypatch):
    env.vast.offers = [offer(1, 11)]
    p = env.make(candidate_factor=1)
    create(p)
    env.vast.run(101, "5.5.5.5")
    p.reconcile_once()  # healthy; now AWS refuses the next rule
    env.vast.offers = [offer(2, 12)]
    create(p)

    def refuse(**kwargs):
        raise botocore.exceptions.ClientError({"Error": {"Code": "RulesPerSecurityGroupLimitExceeded", "Message": "x"}}, "op")
    monkeypatch.setattr(env.ec2, "authorize_security_group_ingress", refuse)
    env.vast.run(102, "5.5.5.6")
    p.reconcile_once()
    assert instance_ids(env) == [101] and p.state.ip_refs == {"5.5.5.5": 1}


def test_pull_timeout_from_image_size_and_blocklist(env):
    env.vast.offers = [offer(1, 11, inet_down=1000)]
    p = env.make(candidate_factor=1, image_size_gb=10)  # 2 x 10 GB x 8000 / 1000 = 160 s, clamped to 300 s
    create(p)
    env.clock.now += 299
    p.reconcile_once()
    assert instance_ids(env) == [101]
    env.clock.now += 2
    p.reconcile_once()
    assert instance_ids(env) == []
    assert p.state.is_blocked(11, env.clock())
    assert p.non_terminated_nodes({TAG_RAY_NODE_KIND: "worker"}) == []  # slot removed: v2 asks again


def test_pull_timeout_defaults_to_15_minutes_without_image_size(env):
    env.vast.offers = [offer(1, 11)]
    p = env.make(candidate_factor=1)
    create(p)
    env.clock.now += 899
    p.reconcile_once()
    assert instance_ids(env) == [101]
    env.clock.now += 2
    p.reconcile_once()
    assert instance_ids(env) == []


def test_enrollment_timeout_one_minute_after_running(env):
    env.vast.offers = [offer(1, 11)]
    p = env.make(candidate_factor=1)
    create(p)
    env.clock.now += 500
    env.vast.run(101, "5.5.5.5")
    p.reconcile_once()
    env.clock.now += 59
    p.reconcile_once()
    assert instance_ids(env) == [101]
    env.clock.now += 2
    p.reconcile_once()
    assert instance_ids(env) == [] and env.rules() == []
    assert p.state.is_blocked(11, env.clock())


def test_exited_or_error_candidate_is_destroyed_and_blocked(env):
    env.vast.offers = [offer(1, 11), offer(2, 12)]
    p = env.make()
    create(p)
    env.vast.instances[101]["actual_status"] = "exited"
    env.vast.instances[102]["status_msg"] = "Error response from daemon: pull access denied"
    p.reconcile_once()
    assert instance_ids(env) == []
    assert p.state.is_blocked(11, env.clock()) and p.state.is_blocked(12, env.clock())


def test_filled_slot_destroys_the_other_candidates_of_its_request(env):
    env.vast.offers = [offer(i, 10 + i) for i in range(1, 7)]
    p = env.make()
    slots = list(create(p, count=3))
    assert len(env.vast.instances) == 6
    for iid in (101, 102, 103):
        env.vast.run(iid, f"5.5.5.{iid}")
    p.reconcile_once()
    for iid in (101, 102, 103):
        enroll(p, iid)
    p.reconcile_once()
    assert instance_ids(env) == [101, 102, 103]
    assert all(p.is_running(s) for s in slots)
    assert sorted(env.rules()) == ["5.5.5.101/32", "5.5.5.102/32", "5.5.5.103/32"]


def test_free_slot_stays_while_a_pending_candidate_of_its_type_remains(env):
    env.vast.offers = [offer(1, 11)]
    p = env.make(candidate_factor=1)
    slot_a, slot_b = create(p, count=2)  # one rental for two slots
    p.reconcile_once()
    assert len(p.non_terminated_nodes({TAG_RAY_NODE_KIND: "worker"})) == 2  # the head may fill either slot
    enroll(p, 101, slot_b)
    p.reconcile_once()
    assert p.non_terminated_nodes({TAG_RAY_NODE_KIND: "worker"}) == [slot_b]  # nothing is left for slot_a


def test_sibling_candidate_enrolls_into_a_slot_whose_own_candidates_died(env):
    env.vast.offers = [offer(i, 10 + i) for i in range(1, 5)]
    p = env.make()
    slot_a, slot_b = create(p, count=2)  # candidates 101 and 103 point at slot_a, 102 and 104 at slot_b
    env.vast.instances[102]["actual_status"] = "exited"
    env.vast.instances[104]["actual_status"] = "exited"
    p.reconcile_once()
    assert instance_ids(env) == [101, 103]
    assert p.non_terminated_nodes({TAG_RAY_NODE_KIND: "worker"}) == [slot_a, slot_b]
    enroll(p, 101, slot_a)
    enroll(p, 103, slot_b)
    p.reconcile_once()
    assert instance_ids(env) == [101, 103] and p.is_running(slot_a) and p.is_running(slot_b)


def test_running_candidate_without_ip_hits_the_pull_limit_then_gets_the_enrollment_limit_after_it_has_one(env):
    env.vast.offers = [offer(1, 11), offer(2, 12)]
    p = env.make(candidate_factor=1)
    create(p), create(p)
    env.vast.instances[101].update(actual_status="running")  # running, no public_ipaddr yet
    env.vast.instances[102].update(actual_status="running")
    env.clock.now += 800
    p.reconcile_once()
    env.vast.run(102, "5.5.5.6")  # 102 gets its IP in time; its enrollment clock starts now
    p.reconcile_once()
    env.clock.now += 101
    p.reconcile_once()  # 101 is past the 15 min pull limit; 102 is 101 s into its enrollment, past 60 s
    assert instance_ids(env) == []


def test_running_candidate_gets_its_ip_late_and_is_not_stuck(env):
    env.vast.offers = [offer(1, 11)]
    p = env.make(candidate_factor=1)
    create(p)
    env.vast.instances[101].update(actual_status="running")
    env.clock.now += 600
    p.reconcile_once()
    env.vast.run(101, "5.5.5.5")
    p.reconcile_once()
    env.clock.now += 59
    p.reconcile_once()
    assert instance_ids(env) == [101] and env.rules() == ["5.5.5.5/32"]


def test_instance_missing_from_vast_counts_only_after_the_grace_period(env):
    env.vast.offers = [offer(1, 11)]
    p = env.make(candidate_factor=1)
    create(p)
    env.vast.instances[101]["label"] = "listed-late"  # not in our label listing yet
    env.clock.now += 59
    p.reconcile_once()
    assert 101 in p.state.candidates
    env.clock.now += 2
    p.reconcile_once()
    assert 101 not in p.state.candidates


def test_failed_list_changes_nothing(env):
    env.vast.offers = [offer(1, 11)]
    p = env.make(candidate_factor=1)
    create(p)
    env.vast.instances[101]["actual_status"] = "exited"
    env.vast.fail_list = True
    before = len(env.vast.calls)
    p.reconcile_once()
    assert env.vast.calls[before:] == ["list_instances"] and 101 in p.state.candidates
    env.vast.fail_list = False
    p.reconcile_once()
    assert instance_ids(env) == []


def test_failed_destroy_is_retried_by_the_next_pass(env):
    env.vast.offers = [offer(1, 11)]
    p = env.make(candidate_factor=1)
    create(p)
    env.vast.instances[101]["actual_status"] = "exited"
    env.vast.fail_destroy = True
    p.reconcile_once()
    assert instance_ids(env) == [101] and 101 not in p.state.candidates  # the instance lives on, unrecorded
    env.vast.fail_destroy = False
    p.reconcile_once()
    assert instance_ids(env) == []


def test_unknown_vast_status_is_treated_as_pending_and_logged_once(env, caplog):
    env.vast.offers = [offer(1, 11)]
    p = env.make(candidate_factor=1)
    create(p)
    env.vast.instances[101]["actual_status"] = "weird"
    p.reconcile_once()
    p.reconcile_once()
    assert instance_ids(env) == [101]
    assert sum("unknown vast actual_status" in r.message for r in caplog.records) == 1


def test_worker_ip_change_exited_and_heartbeat_loss_destroy_the_worker(env):
    env.vast.offers = [offer(i, 10 + i) for i in range(1, 4)]
    p = env.make(candidate_factor=1)
    slots = [next(iter(create(p))) for _ in range(3)]
    for iid in (101, 102, 103):
        env.vast.run(iid, f"5.5.5.{iid}")
    p.reconcile_once()
    for iid in (101, 102, 103):
        enroll(p, iid)
        p.state.candidates[iid].last_heartbeat = env.clock()
    p.reconcile_once()
    assert p.node_tags(slots[0])[TAG_RAY_NODE_STATUS] == "up-to-date"
    env.vast.instances[101]["public_ipaddr"] = "7.7.7.7"
    env.vast.instances[102]["actual_status"] = "exited"
    env.clock.now += 301
    p.state.candidates[103].last_heartbeat = env.clock() - 299
    env.vast.instances[103]["public_ipaddr"] = "5.5.5.103"
    p.reconcile_once()
    assert instance_ids(env) == [103]
    assert p.non_terminated_nodes({TAG_RAY_NODE_KIND: "worker"}) == [slots[2]]
    assert env.rules() == ["5.5.5.103/32"]
    env.clock.now += 2
    p.reconcile_once()
    assert instance_ids(env) == [] and env.rules() == []


def test_worker_destroyed_by_head_server_loses_its_slot_and_its_rule(env):
    env.vast.offers = [offer(1, 11)]
    p = env.make(candidate_factor=1)
    (slot_id,) = create(p)
    env.vast.run(101, "5.5.5.5")
    p.reconcile_once()
    enroll(p, 101)
    peer_cert = {"subjectAltName": (("IP Address", "5.5.5.5"), ("URI", "rayvast:instance/101"))}
    status, _ = p.head_server.worker_request(
        "/report", {"instance_id": 101, "kind": "token_check", "ok": False, "reason": "open"}, peer_cert)
    assert status == 200 and instance_ids(env) == [] and env.rules() == []
    p.reconcile_once()
    assert p.is_terminated(slot_id)


def test_orphan_sweep_destroys_an_unknown_labelled_instance_and_not_other_labels(env, caplog):
    p = env.make()
    env.vast.instances[7] = {"id": 7, "label": LABEL, "actual_status": "running", "public_ipaddr": "1.1.1.1"}
    env.vast.instances[8] = {"id": 8, "label": "someone-else", "actual_status": "running", "public_ipaddr": "1.1.1.2"}
    p.reconcile_once()
    assert instance_ids(env) == [8]
    assert any(r.message.startswith("orphan sweep: destroying unknown instance 7") for r in caplog.records)


def test_two_workers_on_one_ip_share_one_rule(env):
    env.vast.offers = [offer(1, 11), offer(2, 12)]
    p = env.make(candidate_factor=1)
    a, b = next(iter(create(p))), next(iter(create(p)))
    for iid in (101, 102):
        env.vast.run(iid, "5.5.5.5")
    p.reconcile_once()
    enroll(p, 101), enroll(p, 102)
    assert env.rules() == ["5.5.5.5/32"] and p.state.ip_refs == {"5.5.5.5": 2}
    p.terminate_node(a)
    assert env.rules() == ["5.5.5.5/32"] and instance_ids(env) == [102]
    p.terminate_node(b)
    assert env.rules() == [] and instance_ids(env) == []
    assert p.non_terminated_nodes({}) == ["head"]
    p.terminate_node(b)  # a second terminate is harmless


def test_terminate_unfilled_slot_destroys_its_candidates(env):
    env.vast.offers = [offer(1, 11), offer(2, 12)]
    p = env.make()
    (slot_id,) = create(p)
    p.terminate_node(slot_id)
    assert instance_ids(env) == [] and p.is_terminated(slot_id)


def test_parallel_creates_respect_the_budget_and_make_distinct_slots(env):
    env.vast.offers = [offer(i, 10 + i, dph=1) for i in range(1, 21)]
    p = env.make(max_dollars_per_hour=8, candidate_factor=2)
    results, errors = [], []

    def run():
        try:
            results.append(create(p, count=2))
        except NodeLaunchException as e:
            errors.append(e)

    threads = [threading.Thread(target=run) for _ in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(env.vast.instances) == 8 and p.state.total_dph() == 8
    slot_ids = [s for r in results for s in r]
    assert len(slot_ids) == len(set(slot_ids)) == 2 * len(results)
    assert len(results) + len(errors) == 4


def test_concurrent_creates_never_pass_the_budget(env):
    env.vast.offers = [offer(i, 10 + i, dph=1) for i in range(1, 41)]
    p = env.make(max_dollars_per_hour=8, candidate_factor=3)
    def hammer():
        for _ in range(3):
            try:
                create(p)
            except NodeLaunchException:
                pass  # over budget: expected once the limit is reached

    threads = [threading.Thread(target=hammer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert p.state.total_dph() <= 8 and len(env.vast.instances) <= 8
    assert p._reserved_dph == 0


def test_create_in_the_gap_between_reservation_and_rental_cannot_pass_the_budget(env):
    env.vast.offers = [offer(1, 11, dph=1), offer(2, 12, dph=1)]
    p = env.make(max_dollars_per_hour=1.5, candidate_factor=1)
    errors = []

    def second_create_runs_while_the_first_is_renting():
        # The first create has taken its offer and has not recorded the rental yet.
        def second():
            try:
                create(p)
            except NodeLaunchException as e:
                errors.append(e)

        t = threading.Thread(target=second)
        t.start()
        t.join(timeout=10)

    env.vast.on_create = second_create_runs_while_the_first_is_renting
    create(p)
    assert len(env.vast.instances) == 1 and p.state.total_dph() == 1
    assert [e.category for e in errors] == ["budget"]


def test_startup_sweep_destroys_labelled_instances_and_rules_then_scales_from_zero(env):
    env.vast.offers = [offer(1, 11), offer(2, 12)]
    old = env.make()
    create(old)
    env.vast.run(101, "5.5.5.5")
    old.reconcile_once()
    assert env.rules() == ["5.5.5.5/32"]
    env.vast.instances[50] = {"id": 50, "label": "other", "actual_status": "running", "public_ipaddr": "1.1.1.1"}
    new = env.make()  # a restart: a new provider object on the same vast and group
    assert instance_ids(env) == [50] and env.rules() == []
    assert new.non_terminated_nodes({}) == ["head"]
    assert len(create(new)) == 1 and len(env.vast.instances) == 3


@pytest.mark.slow  # real threads and real timers
def test_threads_start_and_stop(env, tmp_path):
    ports = []
    for _ in range(2):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            ports.append(sock.getsockname()[1])
    p = env.make(head_ip="127.0.0.1", port_a=ports[0], port_b=ports[1], start_threads=True)
    ca = requests.get(f"https://127.0.0.1:{ports[0]}/ca.pem", verify=str(tmp_path / "secrets" / "ca.pem"), timeout=5)
    assert ca.status_code == 200 and ca.content == (tmp_path / "secrets" / "ca.pem").read_bytes()
    assert p._thread.is_alive()
    p.close()
    p._thread.join(5)
    assert not p._thread.is_alive()
    with pytest.raises(requests.ConnectionError):
        requests.get(f"https://127.0.0.1:{ports[0]}/ca.pem", verify=False, timeout=5)


def test_vast_api_url_in_the_provider_config_points_the_vast_client_at_it(env, tmp_path, monkeypatch):
    """Test-only key: the local end-to-end tests run a fake vast server."""
    import ray_vast.provider
    from ray_vast import pki

    seen = {}

    class RecordingVast:
        def __init__(self, api_key, base_url="default"):
            seen["base_url"] = base_url

        def list_instances(self):
            return []

    monkeypatch.setattr(ray_vast.provider, "VastClient", RecordingVast)
    pki.write_head_files(tmp_path / "s", "9.9.9.9")
    (tmp_path / "s" / "vast_api_key").write_text("k\n")
    config = {"head_ip": "9.9.9.9", "secrets_dir": str(tmp_path / "s"), "security_group_ids": env.group_ids,
              "region": "us-west-2", "image": "img:1", "max_dollars_per_hour": 10,
              "vast_api_url": "http://127.0.0.1:1/api/v0"}
    ray_vast.provider.VastNodeProvider(config, "c2", ec2=env.ec2, start_threads=False)
    assert seen["base_url"] == "http://127.0.0.1:1/api/v0"


def test_each_candidate_gets_its_own_join_token_in_the_create_env(env):
    env.vast.offers = [offer(1, 11), offer(2, 12)]
    p = env.make()
    create(p)
    tokens = [r["env"]["RAYVAST_JOIN_TOKEN"] for r in env.vast.requests]
    assert len(tokens) == 2 and tokens[0] != tokens[1]
    assert all(re.fullmatch("[0-9a-f]{64}", t) for t in tokens)
    assert [p.state.candidates[i].join_token for i in (101, 102)] == tokens


def test_a_worker_ip_rule_goes_into_the_second_group_when_the_first_is_full(env):
    for n in range(60):  # group 0 is full of rules of this cluster
        env.ec2.authorize_security_group_ingress(GroupId=env.group_ids[0], IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 0, "ToPort": 65535,
            "IpRanges": [{"CidrIp": f"10.1.0.{n + 1}/32", "Description": "other"}]}])
    env.vast.offers = [offer(1, 11)]
    p = env.make(candidate_factor=1)
    create(p)
    env.vast.run(101, "5.5.5.5")
    p.reconcile_once()
    group_1 = env.ec2.describe_security_groups(GroupIds=[env.group_ids[1]])["SecurityGroups"][0]["IpPermissions"]
    assert [r["CidrIp"] for perm in group_1 for r in perm["IpRanges"]] == ["5.5.5.5/32"]
    assert instance_ids(env) == [101]


def fail_one_slot(env, p, tags=TAGS):
    """Rent one candidate for one slot; it exits before enrollment, so the slot ends with no worker."""
    p.create_node_with_resources_and_labels(NODE_CONFIG, dict(tags), 1, RESOURCES, {})
    iid = max(p.state.candidates)  # the newest rental
    env.vast.instances[iid]["actual_status"] = "exited"
    p.reconcile_once()
    assert iid not in p.state.candidates


def test_three_failed_slots_in_a_row_stop_launches_of_that_node_type_for_10_min(env, caplog):
    env.vast.offers = [offer(i, 10 + i) for i in range(1, 10)]
    p = env.make(candidate_factor=1)
    for _ in range(3):
        fail_one_slot(env, p)
    rentals = len(env.vast.requests)
    with pytest.raises(NodeLaunchException) as e:
        create(p)
    assert e.value.category == "repeated enrollment failures" and len(env.vast.requests) == rentals
    assert any(r.levelname == "ERROR" and "gpu" in r.message for r in caplog.records)
    assert p.non_terminated_nodes({}) == ["head"]
    cpu_tags = {**TAGS, TAG_RAY_USER_NODE_TYPE: "cpu"}
    assert len(p.create_node_with_resources_and_labels(NODE_CONFIG, cpu_tags, 1, RESOURCES, {})) == 1  # other types go on
    env.clock.now += 599
    with pytest.raises(NodeLaunchException):
        create(p)
    env.clock.now += 2
    assert len(create(p)) == 1


def test_the_breaker_log_names_the_pause_in_force(env, caplog, monkeypatch):
    monkeypatch.setattr(ray_vast.state, "BREAKER_PAUSE_S", 60)  # what a scaled test provider sets
    env.vast.offers = [offer(i, 10 + i) for i in range(1, 10)]
    p = env.make(candidate_factor=1)
    for _ in range(3):
        fail_one_slot(env, p)
    assert "no new launches of this type for 60 s" in caplog.text


def test_after_the_pause_one_more_failed_slot_does_not_stop_launches_again(env):
    env.vast.offers = [offer(i, 10 + i) for i in range(1, 10)]
    p = env.make(candidate_factor=1)
    for _ in range(3):
        fail_one_slot(env, p)
    env.clock.now += 601
    fail_one_slot(env, p)
    assert len(create(p)) == 1  # the trip set the count to 0: 1 failure is not 3


def test_the_provider_log_never_holds_a_join_token(env, caplog):
    caplog.set_level(logging.DEBUG)
    env.vast.offers = [offer(1, 11), offer(2, 12)]
    p = env.make()
    create(p)
    env.vast.run(101, "5.5.5.5")
    env.vast.instances[102]["actual_status"] = "exited"
    p.reconcile_once()
    env.clock.now += 120  # 101 does not enroll in time
    p.reconcile_once()
    tokens = [r["env"]["RAYVAST_JOIN_TOKEN"] for r in env.vast.requests]
    assert len(tokens) == 2 and "ending instance 101" in caplog.text  # the log is not empty
    assert not any(t in caplog.text for t in tokens)


@pytest.fixture
def identity_listeners():
    """64 listeners on 127.0.0.1, for the head's port check of a candidate whose public IP is 127.0.0.1."""
    socks = []
    for _ in range(64):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        socks.append(s)
    yield {str(70000 + i): s.getsockname()[1] for i, s in enumerate(socks)}
    for s in socks:
        s.close()


def test_an_enrollment_with_the_join_token_resets_the_failure_count(env, identity_listeners):
    env.vast.offers = [offer(i, 10 + i) for i in range(1, 10)]
    p = env.make(candidate_factor=1)
    fail_one_slot(env, p)
    fail_one_slot(env, p)
    create(p)
    (iid,) = list(p.state.candidates)
    env.vast.run(iid, "127.0.0.1")
    p.reconcile_once()
    checks = {"egress_ip_ok": True, "no_net_raw_admin": True, "own_netns": True, "shm_empty": True, "details": ""}
    body = {"instance_id": iid, "join_token": env.vast.requests[-1]["env"]["RAYVAST_JOIN_TOKEN"], "checks": checks,
            "ports": identity_listeners, "cpu_count": 4, "gpu_count": 1}
    status, answer = p.head_server.enroll(body, "127.0.0.1")
    assert status == 200, answer
    fail_one_slot(env, p)
    fail_one_slot(env, p)
    assert len(create(p)) == 1  # 2 failures since the enrollment: no stop


@pytest.mark.parametrize("ca_days, warns", [(10, True), (365, False)])
def test_provider_start_logs_a_warning_when_the_ca_expires_within_14_days(env, tmp_path, caplog, ca_days, warns):
    from ray_vast import pki

    pki.write_head_files(tmp_path / "short", "9.9.9.9", ca_days)
    (tmp_path / "short" / "vast_api_key").write_text("k\n")
    env.make(secrets_dir=str(tmp_path / "short"))
    warnings = [r for r in caplog.records if r.levelname == "WARNING" and "expire" in r.message]
    assert len(warnings) == (1 if warns else 0)
