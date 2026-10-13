import re
import threading

import pytest

from ray_vast.state import Candidate, ClusterState, Slot


def make_candidate(instance_id, ip="1.2.3.4", dph=0.5, machine_id=7):
    return Candidate(instance_id=instance_id, slot_id="s", offer_id=1, machine_id=machine_id, public_ip=ip, dph_total=dph)


def test_concurrent_changes_lose_no_update():
    state = ClusterState()
    ids = []

    def work(t):
        for i in range(200):
            slot_id = state.new_slot_id()
            ids.append(slot_id)
            with state.lock:
                state.add_slot(Slot(slot_id, "worker"))
                state.add_candidate(make_candidate(t * 1000 + i, ip="9.9.9.9"))
                state.ip_ref_add("9.9.9.9")

    threads = [threading.Thread(target=work, args=(t,)) for t in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(ids)) == 1600
    assert len(state.slots) == 1600
    assert len(state.candidates) == 1600
    assert state.ip_refs["9.9.9.9"] == 1600
    assert state.total_dph() == 800.0


def test_enrolled_id_stays_after_slot_removed():
    state = ClusterState()
    state.add_slot(Slot("slot-1-abc", "worker", instance_id=42))
    state.add_candidate(make_candidate(42))
    state.mark_enrolled(42)
    state.remove_slot("slot-1-abc")
    state.remove_candidate(42)
    assert 42 in state.enrolled


def test_blocklist_expiry_by_kind():
    state = ClusterState()
    state.block(1, "bad token", "security", now=1000.0)
    state.block(2, "no route", "network", now=1000.0)
    assert state.is_blocked(1, now=1000.0) and state.is_blocked(2, now=1000.0)
    assert state.is_blocked(1, now=1000.0 + 29 * 86400)
    assert not state.is_blocked(2, now=1000.0 + 86400)
    assert state.is_blocked(1, now=1000.0 + 30 * 86400 - 1)
    assert not state.is_blocked(1, now=1000.0 + 30 * 86400)
    assert not state.is_blocked(3, now=1000.0)


def test_ip_ref_counts():
    state = ClusterState()
    assert state.ip_ref_add("1.2.3.4") == 1
    assert state.ip_ref_add("1.2.3.4") == 2
    assert state.ip_ref_remove("1.2.3.4") == 1
    assert state.ip_ref_remove("1.2.3.4") == 0
    assert state.ip_ref_remove("1.2.3.4") == 0
    assert state.ip_ref_add("1.2.3.4") == 1


def test_block_rejects_unknown_kind():
    with pytest.raises(ValueError, match="unknown blocklist kind"):
        ClusterState().block(1, "x", "bogus", now=0.0)


def test_single_calls_are_atomic_without_caller_lock():
    state = ClusterState()
    counts = []

    def work(t):
        for i in range(200):
            state.add_slot(Slot(state.new_slot_id(), "worker"))
            state.add_candidate(make_candidate(t * 1000 + i))
            counts.append(state.ip_ref_add("9.9.9.9"))
            state.mark_enrolled(t * 1000 + i)

    threads = [threading.Thread(target=work, args=(t,)) for t in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(state.slots) == 1600
    assert len(state.candidates) == 1600
    assert len(state.enrolled) == 1600
    assert sorted(counts) == list(range(1, 1601))
    for t in range(8):
        for i in range(200):
            state.ip_ref_remove("9.9.9.9")
    assert state.ip_ref_add("9.9.9.9") == 1


def test_total_dph_drops_when_candidate_removed():
    state = ClusterState()
    state.add_candidate(make_candidate(1, dph=0.25))
    state.add_candidate(make_candidate(2, dph=0.5))
    state.remove_candidate(1)
    assert state.total_dph() == 0.5


def test_ip_ref_at_zero_leaves_no_entry():
    state = ClusterState()
    state.ip_ref_add("1.2.3.4")
    state.ip_ref_remove("1.2.3.4")
    assert "1.2.3.4" not in state.ip_refs


def test_timeout_block_lasts_one_day():
    state = ClusterState()
    state.block(1, "slow", "timeout", now=1000.0)
    assert state.is_blocked(1, now=1000.0 + 86400 - 1)
    assert not state.is_blocked(1, now=1000.0 + 86400)


def test_block_keeps_reason_and_kind_and_a_new_block_replaces_the_old_one():
    state = ClusterState()
    state.block(1, "bad token", "network", now=0.0)
    assert (state.blocklist[1].reason, state.blocklist[1].kind) == ("bad token", "network")
    state.block(1, "worse", "security", now=100.0)
    assert (state.blocklist[1].reason, state.blocklist[1].kind) == ("worse", "security")
    assert state.is_blocked(1, now=86400 + 100.0)


def test_expired_block_is_forgotten():
    state = ClusterState()
    state.block(1, "x", "network", now=0.0)
    assert not state.is_blocked(1, now=86400.0)
    assert 1 not in state.blocklist


def test_slot_ids_count_up_and_end_in_six_random_hex_digits():
    state = ClusterState()
    first, second = state.new_slot_id(), state.new_slot_id()
    assert re.fullmatch(r"slot-1-[0-9a-f]{6}", first)
    assert re.fullmatch(r"slot-2-[0-9a-f]{6}", second)
    assert ClusterState().new_slot_id() != first  # a restarted provider must not reuse ids (1 in 16 million chance of a false failure)


def test_remove_returns_the_removed_entry_or_none():
    state = ClusterState()
    slot, candidate = Slot("s1", "worker"), make_candidate(5)
    state.add_slot(slot)
    state.add_candidate(candidate)
    assert state.remove_slot("s1") is slot
    assert state.remove_slot("s1") is None
    assert state.remove_candidate(5) is candidate
    assert state.remove_candidate(5) is None


def test_total_dph_counts_candidates_in_every_status():
    state = ClusterState()
    for iid, status in enumerate(["created", "rented", "running", "enrolled"]):
        candidate = make_candidate(iid, dph=0.25)
        candidate.status = status
        state.add_candidate(candidate)
    assert state.total_dph() == 1.0


@pytest.mark.parametrize("call", [
    lambda s: s.new_slot_id(),
    lambda s: s.add_slot(Slot("s1", "worker")),
    lambda s: s.remove_slot("s1"),
    lambda s: s.add_candidate(make_candidate(1)),
    lambda s: s.remove_candidate(1),
    lambda s: s.mark_enrolled(1),
    lambda s: s.ip_ref_add("1.2.3.4"),
    lambda s: s.ip_ref_remove("1.2.3.4"),
    lambda s: s.block(1, "x", "network", now=0.0),
    lambda s: s.is_blocked(1, now=0.0),
    lambda s: s.total_dph(),
], ids=["new_slot_id", "add_slot", "remove_slot", "add_candidate", "remove_candidate", "mark_enrolled",
        "ip_ref_add", "ip_ref_remove", "block", "is_blocked", "total_dph"])
def test_every_method_waits_for_the_state_lock(call):
    state = ClusterState()
    done = threading.Event()
    with state.lock:  # held by this thread; RLock re-entry only works in the holder
        caller = threading.Thread(target=lambda: (call(state), done.set()))
        caller.start()
        assert not done.wait(0.05)  # an unlocked call would finish in microseconds
    caller.join(timeout=5)
    assert done.is_set()
