import logging
import random
import socket
import subprocess
import threading

import pytest
import requests

from ray_vast import pki
from ray_vast.head_server import HeadServer
from ray_vast.state import Candidate, ClusterState, Slot
from ray_vast.vast_api import VastError

CONTAINER_KEY = "container-key-SECRET-1111"
BAD_CHECKS = {"egress_ip_ok": True, "no_net_raw_admin": False, "own_netns": True, "shm_empty": True, "details": ""}
TOKEN = "cluster-token-SECRET-2222"


class StubVast:
    """The vast boundary: `keys` maps instance id -> its container key; a wrong key is HTTP 401, an unknown id None."""

    def __init__(self):
        self.keys, self.get_calls, self.destroyed = {}, [], []
        self.get_barrier = None  # when set, get_instance waits there, so parallel requests overlap
        self.destroy_error = None
        self.get_error = None

    def get_instance(self, instance_id, key=None):
        self.get_calls.append(instance_id)
        if self.get_barrier:
            self.get_barrier.wait(timeout=10)
        if self.get_error:
            raise self.get_error
        if instance_id not in self.keys:
            return None
        if self.keys[instance_id] != key:
            raise VastError("GET: HTTP 401", status=401)
        return {"id": instance_id}

    def destroy_instance(self, instance_id, key=None):
        self.destroyed.append(instance_id)
        if self.destroy_error:
            raise self.destroy_error


@pytest.fixture
def listeners():
    """64 local listening sockets standing in for the identity ports on the public IP."""
    socks = []
    for _ in range(64):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        socks.append(s)
    yield {str(70000 + i): s.getsockname()[1] for i, s in enumerate(socks)}
    for s in socks:
        s.close()


@pytest.fixture
def env(tmp_path, listeners):
    ca_cert, ca_key = pki.make_ca()
    head_cert, head_key = pki.make_leaf(ca_cert, ca_key, "127.0.0.1")
    state, vast, destroyed_ips = ClusterState(), StubVast(), []
    for _ in range(20):  # parallel test runs on the same box share the range 20000-20999
        port_a, port_b = random.sample(range(20000, 21000), 2)
        server = HeadServer(
            state, vast, ca_cert, ca_key, head_cert, head_key, TOKEN, b"SHIM", b"print('boot')", "127.0.0.1",
            port_a, port_b, "c1", on_destroy=lambda ip, remaining: destroyed_ips.append((ip, remaining)),
        )
        try:
            server.start()
            break
        except OSError:
            server.stop()
    else:
        raise RuntimeError("no free port pair")
    (tmp_path / "ca.pem").write_bytes(ca_cert)
    yield type("Env", (), dict(
        state=state, vast=vast, server=server, ca_cert=ca_cert, ca_key=ca_key, ca_file=str(tmp_path / "ca.pem"),
        port_a=port_a, port_b=port_b, ports=listeners, destroyed_ips=destroyed_ips, tmp=tmp_path,
    ))
    server.stop()


def add_candidate(env, instance_id, slot_id="slot-1", ip="127.0.0.1", key=CONTAINER_KEY, node_type="gpu", machine_id=7):
    with env.state.lock:
        if slot_id not in env.state.slots:
            env.state.add_slot(Slot(slot_id, node_type))
        env.state.add_candidate(Candidate(instance_id, slot_id, 1, machine_id, public_ip=ip))
        env.state.ip_ref_add(ip)
    env.vast.keys[instance_id] = key


def enroll(env, instance_id, key=CONTAINER_KEY, checks=None, ports=None):
    body = {
        "instance_id": instance_id, "container_api_key": key,
        "checks": checks or {"egress_ip_ok": True, "no_net_raw_admin": True, "own_netns": True, "shm_empty": True,
                             "details": ""},
        "ports": ports or env.ports, "cpu_count": 8, "gpu_count": 1,
    }
    return requests.post(f"https://127.0.0.1:{env.port_a}/enroll", json=body, verify=env.ca_file, timeout=30)


def test_good_enrollment_gives_files_and_a_verifiable_cert(env):
    add_candidate(env, 11)
    resp = enroll(env, 11)
    assert resp.status_code == 200
    answer = resp.json()
    assert answer["slot_id"] == "slot-1" and answer["node_type"] == "gpu" and answer["num_gpus"] == 1
    assert answer["token"] == TOKEN and answer["gcs_address"] == "127.0.0.1:6379"
    assert answer["ca_pem"].encode() == env.ca_cert
    (env.tmp / "node.pem").write_text(answer["cert_pem"])
    out = subprocess.run(["openssl", "verify", "-CAfile", env.ca_file, str(env.tmp / "node.pem")],
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert env.state.slots["slot-1"].status == "setting-up" and env.state.slots["slot-1"].instance_id == 11
    assert 11 in env.state.enrolled and env.vast.destroyed == []


def test_ca_and_bootstrap_are_served_on_port_a(env):
    assert requests.get(f"https://127.0.0.1:{env.port_a}/ca.pem", verify=env.ca_file, timeout=30).content == env.ca_cert
    resp = requests.get(f"https://127.0.0.1:{env.port_a}/bootstrap", verify=env.ca_file, timeout=30)
    assert resp.content == b"print('boot')"
    assert env.server.bootstrap_sha256 == "fa8196cd37ef904decb437400646a1d984feaf5c050efee114f8a8d619008ba0"


def test_another_instances_key_is_refused_without_a_destroy(env):
    add_candidate(env, 11)
    assert enroll(env, 11, key="some-other-instances-key").status_code == 403
    assert env.vast.destroyed == [] and 11 in env.state.candidates and env.state.ip_refs == {"127.0.0.1": 1}


def test_vast_server_error_during_the_key_check_gives_503_without_a_destroy(env):
    add_candidate(env, 11)
    env.vast.get_error = VastError("GET: HTTP 502", status=502)
    assert enroll(env, 11).status_code == 503
    assert env.vast.destroyed == [] and 11 in env.state.candidates and 11 not in env.state.enrolled


def test_two_parallel_failing_enrollments_lower_the_ip_ref_once(env):
    add_candidate(env, 11)
    add_candidate(env, 12)  # same IP: the ref count is 2
    env.vast.get_barrier = threading.Barrier(2)
    results = []
    threads = [threading.Thread(target=lambda: results.append(enroll(env, 11, checks=BAD_CHECKS).status_code))
               for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results == [403, 403]
    assert env.vast.destroyed == [11]
    assert env.state.ip_refs == {"127.0.0.1": 1} and env.destroyed_ips == [("127.0.0.1", 1)]


def test_ip_ref_is_lowered_even_if_the_vast_destroy_raises(env):
    add_candidate(env, 11)
    env.vast.destroy_error = RuntimeError("boom")
    assert enroll(env, 11, checks=BAD_CHECKS).status_code == 403
    assert 11 not in env.state.candidates and env.state.ip_refs == {} and env.destroyed_ips == [("127.0.0.1", 0)]


def test_second_enrollment_of_the_same_instance_is_refused(env):
    add_candidate(env, 11)
    assert enroll(env, 11).status_code == 200
    refs = dict(env.state.ip_refs)
    assert enroll(env, 11).status_code == 403
    assert env.vast.destroyed == [] and 11 in env.state.candidates  # the enrolled worker stays
    assert env.state.ip_refs == refs


def test_wrong_source_ip_is_refused(env):
    add_candidate(env, 11, ip="10.9.9.9")
    assert enroll(env, 11).status_code == 403
    assert 11 not in env.state.enrolled and env.vast.destroyed == []
    assert env.state.ip_refs == {"10.9.9.9": 1}


@pytest.mark.parametrize("key", ["", "   "])
def test_empty_container_key_is_refused_without_a_vast_call(env, key):
    add_candidate(env, 11)
    assert enroll(env, 11, key=key).status_code == 403
    assert env.vast.get_calls == [] and env.state.ip_refs == {"127.0.0.1": 1}


def test_duplicate_ports_are_a_400(env):
    add_candidate(env, 11)
    ports = {**env.ports, "70001": env.ports["70000"]}
    assert enroll(env, 11, ports=ports).status_code == 400
    assert env.vast.get_calls == []


@pytest.mark.parametrize("cpu, gpu", [(0, 1), (5000, 1), (8, -1), (8, 65)])
def test_out_of_range_cpu_or_gpu_count_is_a_400(env, cpu, gpu):
    add_candidate(env, 11)
    body = {"instance_id": 11, "container_api_key": CONTAINER_KEY, "checks": {}, "ports": env.ports,
            "cpu_count": cpu, "gpu_count": gpu}
    assert requests.post(f"https://127.0.0.1:{env.port_a}/enroll", json=body, verify=env.ca_file).status_code == 400


def test_candidate_whose_slot_is_gone_takes_a_free_slot_of_its_node_type(env):
    add_candidate(env, 11, slot_id="slot-gone")
    env.state.remove_slot("slot-gone")
    env.state.candidates[11].node_type = "gpu"
    env.state.add_slot(Slot("slot-2", "gpu"))
    env.state.add_slot(Slot("slot-3", "cpu"))
    assert enroll(env, 11).json()["slot_id"] == "slot-2"


def test_unknown_instance_is_refused_without_a_vast_call(env):
    assert enroll(env, 99).status_code == 403
    assert env.vast.get_calls == [] and env.state.ip_refs == {}


def test_two_candidates_for_one_slot_first_wins(env):
    add_candidate(env, 11)
    add_candidate(env, 12)
    assert enroll(env, 11).status_code == 200
    second = enroll(env, 12)
    assert second.status_code == 403 and second.json()["reason"] == "no free slot"
    assert env.vast.destroyed == [12] and 12 not in env.state.candidates


def test_second_candidate_takes_another_free_slot_of_the_same_node_type(env):
    add_candidate(env, 11)
    add_candidate(env, 12)
    env.state.add_slot(Slot("slot-2", "gpu"))
    assert enroll(env, 11).status_code == 200
    assert enroll(env, 12).json()["slot_id"] == "slot-2"


def test_failed_security_check_blocks_the_machine_as_security(env):
    add_candidate(env, 11)
    checks = {"egress_ip_ok": True, "no_net_raw_admin": False, "own_netns": True, "shm_empty": True, "details": "x"}
    assert enroll(env, 11, checks=checks).status_code == 403
    assert env.state.blocklist[7].kind == "security" and env.vast.destroyed == [11]


def test_failed_egress_check_blocks_the_machine_as_network(env):
    add_candidate(env, 11)
    checks = {"egress_ip_ok": False, "no_net_raw_admin": True, "own_netns": True, "shm_empty": True, "details": ""}
    assert enroll(env, 11, checks=checks).status_code == 403
    assert env.state.blocklist[7].kind == "network"


def test_outside_check_failure_is_refused_and_blocked_as_network(env):
    add_candidate(env, 11)
    dead = socket.socket()
    dead.bind(("127.0.0.1", 0))
    ports = {**env.ports, "70005": dead.getsockname()[1]}
    dead.close()  # nothing listens on it now
    assert enroll(env, 11, ports=ports).status_code == 403
    assert env.state.blocklist[7].kind == "network" and env.vast.destroyed == [11]


def worker_session(env, ip, instance_id=11, tmp_name="w"):
    """Return kwargs for requests with a node cert for `ip`, signed by the real CA."""
    cert, key = pki.make_leaf(env.ca_cert, env.ca_key, ip, instance_id=instance_id)
    (env.tmp / f"{tmp_name}.pem").write_bytes(cert)
    (env.tmp / f"{tmp_name}.key").write_bytes(key)
    return dict(cert=(str(env.tmp / f"{tmp_name}.pem"), str(env.tmp / f"{tmp_name}.key")), verify=env.ca_file, timeout=30)


def enrolled_worker(env, instance_id, ip):
    add_candidate(env, instance_id, slot_id=f"slot-{instance_id}", ip=ip)
    env.state.mark_enrolled(instance_id)
    env.state.slots[f"slot-{instance_id}"].instance_id = instance_id


def test_heartbeat_with_a_valid_cert_records_the_time(env):
    enrolled_worker(env, 11, "10.0.0.5")
    resp = requests.post(f"https://127.0.0.1:{env.port_b}/heartbeat", json={"instance_id": 11},
                         **worker_session(env, "10.0.0.5"))
    assert resp.status_code == 200
    assert env.state.candidates[11].last_heartbeat is not None


def test_heartbeat_with_another_workers_cert_for_the_same_ip_is_403(env):
    enrolled_worker(env, 11, "10.0.0.5")
    enrolled_worker(env, 12, "10.0.0.5")
    resp = requests.post(f"https://127.0.0.1:{env.port_b}/heartbeat", json={"instance_id": 11},
                         **worker_session(env, "10.0.0.5", instance_id=12))
    assert resp.status_code == 403 and env.state.candidates[11].last_heartbeat is None


def test_heartbeat_with_a_cert_for_another_ip_is_403(env):
    enrolled_worker(env, 11, "10.0.0.5")
    resp = requests.post(f"https://127.0.0.1:{env.port_b}/heartbeat", json={"instance_id": 11},
                         **worker_session(env, "10.0.0.9"))
    assert resp.status_code == 403 and resp.json() == {"reason": "unknown"}
    assert env.state.candidates[11].last_heartbeat is None


def test_heartbeat_of_a_worker_that_never_enrolled_is_403(env):
    add_candidate(env, 11, ip="10.0.0.5")
    resp = requests.post(f"https://127.0.0.1:{env.port_b}/heartbeat", json={"instance_id": 11},
                         **worker_session(env, "10.0.0.5"))
    assert resp.status_code == 403


def test_heartbeat_without_a_client_cert_is_refused(env):
    enrolled_worker(env, 11, "10.0.0.5")
    with pytest.raises(requests.exceptions.RequestException):
        requests.post(f"https://127.0.0.1:{env.port_b}/heartbeat", json={"instance_id": 11}, verify=env.ca_file, timeout=30)


def test_heartbeat_with_a_rogue_ca_cert_is_refused(env):
    enrolled_worker(env, 11, "10.0.0.5")
    rogue_cert, rogue_key = pki.make_ca()
    cert, key = pki.make_leaf(rogue_cert, rogue_key, "10.0.0.5")
    (env.tmp / "r.pem").write_bytes(cert)
    (env.tmp / "r.key").write_bytes(key)
    with pytest.raises(requests.exceptions.RequestException):
        requests.post(f"https://127.0.0.1:{env.port_b}/heartbeat", json={"instance_id": 11}, verify=env.ca_file,
                      cert=(str(env.tmp / "r.pem"), str(env.tmp / "r.key")), timeout=30)


def test_stalled_client_does_not_block_the_server(env):
    stalled = socket.create_connection(("127.0.0.1", env.port_a))  # connects, never sends a hello
    try:
        assert requests.get(f"https://127.0.0.1:{env.port_a}/ca.pem", verify=env.ca_file, timeout=5).status_code == 200
    finally:
        stalled.close()


def test_failed_token_check_destroys_the_worker(env):
    enrolled_worker(env, 11, "10.0.0.5")
    resp = requests.post(f"https://127.0.0.1:{env.port_b}/report",
                         json={"instance_id": 11, "kind": "token_check", "ok": False, "reason": "bad"},
                         **worker_session(env, "10.0.0.5"))
    assert resp.status_code == 200
    assert env.vast.destroyed == [11] and 11 not in env.state.candidates
    assert 11 in env.state.enrolled  # it can never enroll again


def test_token_check_without_ok_destroys_the_worker(env):
    enrolled_worker(env, 11, "10.0.0.5")
    resp = requests.post(f"https://127.0.0.1:{env.port_b}/report", json={"instance_id": 11, "kind": "token_check"},
                         **worker_session(env, "10.0.0.5"))
    assert resp.status_code == 200 and env.vast.destroyed == [11]


def test_passing_token_check_and_destroy_report_do_not_destroy(env):
    enrolled_worker(env, 11, "10.0.0.5")
    session = worker_session(env, "10.0.0.5")
    for body in ({"kind": "token_check", "ok": True, "reason": ""}, {"kind": "destroy", "ok": False, "reason": "x"}):
        resp = requests.post(f"https://127.0.0.1:{env.port_b}/report", json={"instance_id": 11, **body}, **session)
        assert resp.status_code == 200
    assert env.vast.destroyed == []


def test_report_before_enrollment_is_accepted(env):
    resp = requests.post(f"https://127.0.0.1:{env.port_a}/report", verify=env.ca_file, timeout=30,
                         json={"instance_id": 11, "container_api_key": CONTAINER_KEY, "kind": "destroy", "reason": "r"})
    assert resp.status_code == 200 and resp.json() == {}


def test_no_secret_reaches_the_log(env, caplog):
    caplog.set_level(logging.DEBUG)
    add_candidate(env, 11)
    add_candidate(env, 12)
    answer = enroll(env, 11).json()
    enroll(env, 12, key="wrong-key-SECRET-3333")
    requests.post(f"https://127.0.0.1:{env.port_a}/report", verify=env.ca_file, timeout=30,
                  json={"instance_id": 11, "container_api_key": CONTAINER_KEY, "kind": "destroy", "reason": "r"})
    text = caplog.text
    assert "enrolled instance 11" in text  # the log is not empty
    key_body = answer["key_pem"].split("\n")[1]
    for secret in (CONTAINER_KEY, "wrong-key-SECRET-3333", TOKEN, key_body, answer["cert_pem"].split("\n")[1]):
        assert secret not in text
