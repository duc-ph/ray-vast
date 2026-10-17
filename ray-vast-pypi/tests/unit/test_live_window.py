"""The observation window of the live suite (`tests/live/window.py`): the head log events since a window opened, their
head-clock times, the vast instances that it saw, and the wait of the suite (`liverun.wait_until`). Through its
interface, with a fake head (recorded monitor.log excerpts and vast listings from archive/results/2026-10-1*/, in
tests/fixtures/window/, with documentation addresses in place of the real IP addresses) and a fake clock."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "live"))  # the live suite imports its modules by their own names
import liverun  # noqa: E402
import window  # noqa: E402

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "window"


def recorded(name):
    return (FIXTURES / name).read_text().splitlines()


def timeline(name):
    return [json.loads(line) for line in recorded(name)]


class FakeClock:
    """`time()` and `sleep()`; a sleep moves the time and runs `on_sleep` (the world changes while the suite waits)."""

    def __init__(self, t=1792197356.0, on_sleep=None):
        self.t, self.on_sleep, self.slept = t, on_sleep, []

    def time(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s
        if self.on_sleep:
            self.on_sleep(self.t)


class FakeHead:
    """The window's head adapter, with no cluster: the monitor.log lines, the head's time zone, the vast listings (each
    `instances()` call gives the next one, the last repeats) and the cluster's poller samples."""

    def __init__(self, log=(), zone="America/Los_Angeles", listings=([],), samples=()):
        self.log, self.zone, self.listings, self.timeline = list(log), zone, list(listings), list(samples)

    def monitor_log(self, start_line=1):
        return self.log[start_line - 1:]

    def monitor_log_length(self):
        return len(self.log)

    def head_time_zone(self):
        return self.zone

    def instances(self):
        return self.listings.pop(0) if len(self.listings) > 1 else self.listings[0]


# --- the head clock ---------------------------------------------------------------------------------------------------
def test_event_times_are_epochs_of_the_head_time_zone_in_summer_time_and_in_winter_time():
    # The rayproject/ray images set TZ=America/Los_Angeles. The expected epochs come from `TZ=America/Los_Angeles date -d`.
    enrolled = "2026-10-16 17:36:34,988\tINFO head_server.py:230 -- enrolled instance 56321086 (machine 8966, ip 203.0.113.11) into slot-2-e2d2df"
    winter = enrolled.replace("2026-10-16 17:36:34,988", "2026-01-15 09:30:00,500")
    head = FakeHead()
    w = window.Window(head, clock=FakeClock())
    head.log += [enrolled, winter]
    times = [e.t for e in w.events("enrolled")]
    assert times == [1792197394.988, 1768498200.5]  # PDT (-0700) and PST (-0800): one offset for both would be wrong

    head = FakeHead(zone="UTC")
    w = window.Window(head, clock=FakeClock())
    head.log += [winter]
    assert [e.t for e in w.events("enrolled")] == [1768469400.5]


# --- events since the window opened, by kind ----------------------------------------------------------------------------
def test_a_window_answers_only_the_events_of_the_lines_written_after_it_opened():
    log = recorded("L02_monitor.log")
    head = FakeHead(log[:100])  # lines 1-100 hold the enrollments of 56321086 and 56321114
    w = window.Window(head, clock=FakeClock())
    head.log += log[100:]
    assert [e.instance for e in w.events("enrolled")] == [56321097, 56321092]
    assert w.lines() == log[100:]
    assert [e.instance for e in window.Window(FakeHead(log), clock=FakeClock(), from_start=True).events("enrolled")] == [
        56321086, 56321114, 56321097, 56321092]


def test_the_rent_enroll_and_end_lines_of_a_scale_up_and_an_idle_scale_down():
    w = window.Window(FakeHead(recorded("L02_monitor.log")), clock=FakeClock(), from_start=True)
    rented = w.events("rented")
    assert [(e.fields["node_type"], e.fields["rented"]) for e in rented] == [("gpu-b", 2), ("gpu-a", 2), ("gpu-b", 2), ("gpu-a", 2)]
    enrolled = w.events("enrolled")[0]
    assert (enrolled.instance, enrolled.fields, enrolled.t) == (56321086, {"machine": 8966, "ip": "203.0.113.11", "slot": "slot-2-e2d2df"},
                                                                1792197394.988)
    ending = w.events("ending")
    assert [(e.instance, e.reason) for e in ending][:2] == [(56321088, "its request has no free slot left"),
                                                             (56321115, "its request has no free slot left")]
    assert [e.instance for e in ending if e.reason == "terminated by the autoscaler"] == [56321092, 56321114, 56321086, 56321097]
    assert {e.cause for e in ending} == {"no_free_slot", "autoscaler"}
    assert len(w.events("legacy_field")) > 0 and not w.events("unknown_status") and not w.events("refused")


def test_an_ending_on_the_pull_limit_or_after_a_missing_listing_has_its_cause():
    # The reason texts of provider.py (`not running with an IP after <pull limit> s`, `instance is gone from vast`), in the
    # format of a recorded ending line.
    line = "2026-10-16 17:36:42,867\tWARNING provider.py:274 -- ending instance 56321088 (machine 37217, ip None): {}"
    head = FakeHead([line.format("'not running with an IP after 1200 s'"), line.format("'instance is gone from vast'"),
                     line.format("'not enrolled 60 s after running'")])
    assert [e.cause for e in window.Window(head, clock=FakeClock(), from_start=True).events("ending")] == [
        "pull_limit", "gone_from_vast", "enroll_timeout"]


def test_a_reboot_gives_an_unknown_status_line_and_a_refused_enrollment():
    w = window.Window(FakeHead(recorded("L09_monitor.log")), clock=FakeClock(), from_start=True)
    [unknown] = w.events("unknown_status")
    assert unknown.fields == {"status": "rebooting"} and unknown.t == 1792198035.057
    [refused] = w.events("refused")
    assert (refused.instance, refused.reason, refused.fields) == (56322074, "already enrolled", {"ip": "198.51.100.16"})
    assert [e.instance for e in w.events("enrolled")] == [56322159]


def test_a_sigterm_to_pid_1_gives_a_destroy_request_with_its_reason():
    w = window.Window(FakeHead(recorded("L07_monitor.log")), clock=FakeClock(), from_start=True)
    [request] = w.events("destroy_request")
    assert (request.instance, request.reason) == (56321632, "received SIGTERM")
    [ending] = w.events("ending")
    assert (ending.instance, ending.cause) == (56322075, "no_free_slot")


def test_the_process_limit_warning_names_its_worker_and_its_counts():
    w = window.Window(FakeHead(recorded("LB11_H2_core_many_monitor.log")), clock=FakeClock(), from_start=True)
    warnings = w.events("process_limit")
    assert len(warnings) == 2
    assert warnings[0].instance == 56320907 and {k: v for k, v in warnings[0].fields.items() if k not in ("advice", "docs")} == {
        "slot": "slot-1-a37a29", "ip": "198.51.100.18", "refused": 0, "refused_new": 0, "pids": 589, "pids_max": 1536,
        "ports_in_use": 18, "ports_total": 18}
    assert w.events("process_limit", instances=[56320907]) == warnings and w.events("process_limit", instances=[1]) == []
    # The recorded runs had no refused fork; this line is the recorded one with refusals.
    refused = warnings[0].line.replace("0 refused forks/threads (+0)", "7 refused forks/threads (+3)")
    [event] = window.Window(FakeHead([refused]), clock=FakeClock(), from_start=True).events("process_limit")
    assert (event.fields["refused"], event.fields["refused_new"]) == (7, 3)


def test_head_log_lines_with_no_window_give_the_same_events_and_the_documented_advice_of_the_warning():
    # A head log in a file (the local C10 check of F16): the module function reads lines in a given time zone.
    lines = recorded("LB11_H2_core_many_monitor.log")
    [first, _] = window.events(lines, "America/Los_Angeles", "process_limit")
    assert first.t == 1792197564.83  # 2026-10-16 17:39:24,830 PDT
    assert first.fields["advice"] == "Ray work on this worker can queue, hang or fail."
    assert first.fields["docs"] == "https://duc-ph.github.io/ray-vast/how-to/troubleshoot/#a-worker-is-at-its-process-limit"


def test_other_head_log_lines_such_as_the_output_of_the_documented_grep_give_the_same_events():
    # `grep "process limit" monitor.log` through `ray-vast attach` (LB11): the shell's echo and prompt lines are no events.
    grep = ["ubuntu@ip-10-0-0-1:~$ sudo docker exec ray-vast-head grep \"process limit\" /tmp/ray/session_latest/logs/monitor.log",
            *[x for x in recorded("LB11_H2_core_many_monitor.log") if "process limit" in x], "GREP_EXIT 0"]
    w = window.Window(FakeHead(), clock=FakeClock())
    assert [e.instance for e in w.events("process_limit", instances=[56320907], lines=grep)] == [56320907, 56320907]


def test_a_launch_that_finds_no_offer_or_no_budget_gives_launch_failed_with_the_text():
    w = window.Window(FakeHead(recorded("A08_monitor.log")), clock=FakeClock(), from_start=True)
    failed = w.events("launch_failed")
    assert len(failed) == 8  # 4 of each type; Ray's `launch failed with Failed to launch` lines are not new events
    assert {(e.fields["node_type"], e.reason) for e in failed} == {
        ("impossible", "Node Launch Exception (no offers): node type 'impossible': no rentable offer matches the node type query"),
        ("poor", "Node Launch Exception (budget): node type 'poor': every matching offer would pass max_dollars_per_hour 0.001")}


# --- the vast instances that the window saw -----------------------------------------------------------------------------
def test_the_enroll_time_of_each_instance_is_its_enrolled_line_minus_its_first_running_sample():
    # The recorded L02 run (2026-10-17): the window opened at the submit; the expected values are that run's result.json.
    head = FakeHead(samples=timeline("L02_vast_timeline.jsonl"))
    w = window.Window(head, clock=FakeClock(1792197356.0))
    head.log += recorded("L02_monitor.log")
    assert w.enroll_seconds() == {56321086: -2.6, 56321114: 4.4, 56321097: 7.0, 56321092: 6.6}


def test_the_status_changes_of_each_instance_from_the_poller_samples_and_the_listings_of_the_window():
    clock = FakeClock(1792198000.0)
    samples = timeline("L09_vast_timeline.jsonl")
    head = FakeHead(samples=samples[:4] + [{"t": 1792198038.0, "error": "ConnectionError"}] + samples[4:])
    w = window.Window(head, clock=clock)
    seen = w.vast_seen()
    assert seen[56322074] == [(1792198002.0, "running"), (1792198033.1, "rebooting"), (1792198043.6, "gone")]
    assert seen[56322223] == [(1792198053.9, None), (1792198064.3, "created"), (1792198074.7, "running")]
    clock.t = 1792198100.0
    head.listings = [[i for i in samples[-1]["instances"] if i["id"] != 56322159]]
    assert [i["id"] for i in w.vast_now()] == [56321645, 56321651, 56322223, 56322224]
    assert w.vast_seen()[56322159][-1] == (1792198100.0, "gone")


def test_a_window_counts_only_the_samples_from_5_s_before_it_opened():
    head = FakeHead(samples=timeline("L09_vast_timeline.jsonl"))
    w = window.Window(head, clock=FakeClock(1792198060.0))
    assert sorted(w.vast_seen()) == [56321645, 56321651, 56322159, 56322223, 56322224]


def test_the_phase_of_a_vast_status_is_the_one_of_the_provider():
    assert [window.phase(s) for s in ("running", None, "", "created", "loading", "scheduling", "rebooting")] == [
        "running"] + ["pending"] * 6
    assert [window.phase(s) for s in ("exited", "offline", "migrating")] == ["failed", "failed", "unknown"]


# --- wait_until ---------------------------------------------------------------------------------------------------------
def test_wait_until_an_event_arrives_gives_success_the_elapsed_time_and_what_it_saw():
    log = recorded("L07_monitor.log")
    head = FakeHead(log[:3])
    clock = FakeClock(1792197900.0, on_sleep=lambda t: head.log.append(log[len(head.log)]))
    w = window.Window(head, clock=clock)
    waited = liverun.wait_until(lambda: w.events("destroy_request"), timeout=60, poll_s=2, clock=clock)
    assert waited and waited.ok and waited.elapsed == 2 and waited.seen[0].reason == "received SIGTERM"


def test_wait_until_with_a_done_test_on_the_vast_listing():
    listings = [[{"id": 1, "actual_status": "running"}]] * 3 + [[]]
    clock = FakeClock()
    w = window.Window(FakeHead(listings=listings), clock=clock)
    waited = liverun.wait_until(w.vast_now, timeout=600, done=lambda seen: not seen, poll_s=15, clock=clock)
    assert (waited.ok, waited.elapsed, waited.seen) == (True, 45, [])
    assert w.vast_seen()[1][-1][1] == "gone"


def test_a_wait_that_times_out_gives_the_elapsed_time_and_the_last_thing_seen_and_never_sleeps_past_its_timeout():
    clock = FakeClock()
    w = window.Window(FakeHead(listings=[[{"id": 7, "actual_status": "loading"}]]), clock=clock)
    waited = liverun.wait_until(w.vast_now, timeout=25, done=lambda seen: not seen, poll_s=10, clock=clock)
    assert not waited and (waited.ok, waited.elapsed, waited.seen) == (False, 25, [{"id": 7, "actual_status": "loading"}])
    assert clock.slept == [10, 10, 5]


def test_a_look_that_raises_gives_its_error_keeps_the_last_good_value_and_the_wait_goes_on():
    answers = iter([RuntimeError("ssh: exit 255"), "RUNNING"])

    def look():
        answer = next(answers)
        if isinstance(answer, Exception):
            raise answer
        return answer

    clock = FakeClock()
    assert liverun.wait_until(look, timeout=60, done=lambda s: s == "RUNNING", poll_s=5, clock=clock).elapsed == 5
    answers = iter([RuntimeError("ssh: exit 255")] * 3)
    waited = liverun.wait_until(look, timeout=10, poll_s=5, clock=clock)
    assert (waited.ok, waited.seen, waited.error) == (False, None, "RuntimeError: ssh: exit 255")
    answers = iter(["PENDING", RuntimeError("ssh: exit 255"), RuntimeError("ssh: exit 255")])  # a later error keeps "PENDING"
    waited = liverun.wait_until(look, timeout=10, done=lambda s: s == "RUNNING", poll_s=5, clock=clock)
    assert (waited.ok, waited.seen, waited.error) == (False, "PENDING", "RuntimeError: ssh: exit 255")
