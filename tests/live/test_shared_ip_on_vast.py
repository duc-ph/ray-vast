"""Two workers on one public IP. Two vast machines can sit behind one NAT IP; then the workers on them have the same
public IP, and each container has its own network namespace. A worker reaches the other only through hairpin NAT on that
IP. Each worker tests hairpin NAT on its own port before it enrolls. On an IP without it, the head refuses a second
candidate, and the provider rents no other instance there.

vast lists one offer for each machine. After a rental it lists the offer for about 80 s more (renting it fails), and then
not again while the machine is rented. So each machine gives one worker. The test takes the public IPs with offers on
two or more machines (sharedip.py). Before each IP it searches again and goes on only when two or more machines of the
IP are listed. It pins the node type to the machines of the IP (`machine_id` `in` the list) and asks for 2 workers
(`Cluster.set_shape`). Ray's autoscaler asks for one worker in each create call: the next call comes about 5 s after
the last candidate was allocated, and a failed launch is retried every 5 s. The provider rents at most one candidate on
each offer IP in one call. So the second candidate is usually rented on another machine of the IP before the first
worker enrolls. The outcome on one IP (`sharedip.outcome`):
- hairpin: both candidates enroll, and 2 workers have the same IP. Then Ray traffic both ways (jobs/shared_ip.py): an
  actor call and a 50 MB object.
- no hairpin: one worker stays, and the head refused the other candidate (`no hairpin NAT on public IP`; the usual path:
  the second candidate was in flight), or the provider logged `skipped the offers on public IP <ip>` (the second
  rental came after the first worker enrolled). A `no rentable offer` launch failure alone is no proof: a just-rented
  offer is still listed, and renting it fails. Then for 5 min no new instance is rented, the worker stays, and the
  autoscaler keeps asking (a new `no rentable offer` failure). The kept worker measured `hairpin: False` unless the head
  refused: the refused candidate can be the one that measured it.
No outcome in 30 min, or 5 min with no instance (on two listings in a row) and only `no rentable offer` failures
(`sharedip.stalled`: vast lists no other machine of the IP now): the test goes to the next IP. It tries up to 10 IPs,
until it saw both outcomes; an IP with an outcome already seen is only recorded (about 5 min). The workers use the CPU
image (smaller to pull): the test needs no CUDA.
Time and cost: an IP with a new outcome takes 10 to 30 min (two image pulls). Typical: 1 to 2 h, vast under $1 (at most
2 cheap 1-GPU instances at a time), EC2 about $0.20. Worst case: 10 IPs x 50 min (30 min wait for an outcome, 5 min
quiet, 15 min to end the workers) at the cap of $2/h: about $17 on vast, and EC2 about $0.70. The rent-loop breaker counts the
slots of the node type that ended with no enrolled worker, in a row and across IPs; an enrollment sets the count to 0. A
refused candidate counts one. Three in a row (for example a refusal and two failed pulls) pause the launches for 10
min, inside the 30 min wait.
RAYVAST_LIVE_SHARED_IPS (comma-separated public IPs) puts these IPs first, for example IPs known to have no hairpin NAT."""
import os
import secrets
import time
from dataclasses import dataclass, field

from cluster import JOBS, alive_workers, shaped_config, vast
from livejob import workdir
from liverun import wait_until
from ray_vast.config import build_offer_query
from sharedip import outcome, public_ip_groups, stalled, worker_hairpin

REGION = "us-west-2"
MAX_IPS = 10  # many shared IPs have hairpin NAT; 10 makes it likely that one without it is among them
OUTCOME_S = 1800  # the wait for one of the two outcomes on one IP (image pulls, enrollments)
STALL_S = 300  # no instance and only `no rentable offer` failures for this long: no outcome on the IP
QUIET_S = 300  # after the no-hairpin outcome: the time in which no new instance may be rented on the IP
IDLE_S = 900  # the wait for 0 instances between two IPs


@dataclass
class Look:
    """What the wait on one IP saw last."""
    result: str | None = None  # sharedip.outcome
    ips: list = field(default_factory=list)  # the IPs of the alive workers
    refused: bool = False  # a `no hairpin NAT` ending of the head
    skipped_ips: list = field(default_factory=list)  # the IPs of the provider's `skipped the offers on public IP` lines
    stalled: bool = False


def base_config():
    """The cluster: one worker type `pair` (1 GPU, CPU 4), at most 2 workers, no worker until `set_shape`."""
    return {
        "cluster_name": f"rvtest-sip-{secrets.token_hex(2)}",
        "idle_timeout_minutes": 1,
        "provider": {"image": "rayproject/ray:2.59.0-py311-cpu", "max_dollars_per_hour": 2.0, "candidate_factor": 2},
        "head_node_type": "head",
        "available_node_types": {
            "head": {"resources": {"CPU": 0}, "node_config": {"region": REGION}},
            "pair": {"resources": {"CPU": 4, "GPU": 1}, "min_workers": 0, "max_workers": 2,
                     "node_config": {"disk_gb": 30, "offer_query": {"verified": {"eq": True}, "inet_down": {"gte": 200}}}}},
    }


def hairpin_logs(cluster, out_dir):
    """sharedip.worker_hairpin of the vast instances of the cluster now; the container logs are kept in `out_dir`."""
    def fetch(instance_id):
        text = cluster.container_log(instance_id)
        if text:
            (out_dir / f"container_{instance_id}.log").write_text(text)
        return text
    return worker_hairpin([i["id"] for i in cluster.instances()], fetch)


def no_offer_failures(w):
    """The `no rentable offer` launch failures of the node type `pair` in the window."""
    return [e for e in w.events("launch_failed") if e.fields["node_type"] == "pair" and "no rentable offer" in e.reason]


def test_two_workers_on_one_public_ip_reach_each_other_or_the_second_is_refused(record, new_cluster, tmp_path):
    base = base_config()
    offers = vast().search_offers(build_offer_query(base["available_node_types"]["pair"], "pair"), limit=1000)
    groups = public_ip_groups(offers, len(offers))
    first = os.environ.get("RAYVAST_LIVE_SHARED_IPS", "").split(",")  # the IPs to try first (module docstring)
    groups = sorted(groups, key=lambda group: group[0] not in first)[:MAX_IPS]
    record.data("groups", groups)
    record.require("public IPs with offers on two or more machines", groups, f"{len(offers)} offers")
    cluster = new_cluster(base, record.dir, tmp_path / "home")
    cluster.up()
    seen = {}  # outcome (hairpin, no_hairpin) -> IP
    with cluster.tunnel() as env:
        for ip, machines in groups:
            if {"hairpin", "no_hairpin"} <= seen.keys():
                break
            pin = {"pair": {"machine_id": {"in": machines}}}
            # The node type's query on these machines, as the provider builds it from the head's YAML.
            pinned_query = build_offer_query(shaped_config(base, {"pair": 2}, pin)["available_node_types"]["pair"], "pair")
            listed = sorted({o["machine_id"] for o in vast().search_offers(pinned_query) if o.get("public_ipaddr") == ip})
            if len(listed) < 2:  # the other machines were rented since the first search, or left the IP
                record.data(f"ip_{ip}", f"skipped: {len(listed)} machine(s) of {machines} listed on the IP now: {listed}")
                continue
            phase = record.dir / f"ip_{ip}"
            phase.mkdir()
            w = cluster.window()
            cluster.set_shape({"pair": 2}, query=pin)
            counts = []  # the number of vast instances of the cluster at each look
            try:
                def look():
                    ips = alive_workers(cluster, "node_ip").get("pair", [])
                    refused = any(e.cause == "no_hairpin" for e in w.events("ending") if e.fields["machine"] in machines)
                    skipped_ips = [e.fields["ip"] for e in w.events("hairpin_skip") if e.fields["node_type"] == "pair"]
                    failures = [(e.t, e.reason) for e in w.events("launch_failed") if e.fields["node_type"] == "pair"]
                    counts.append(len(w.vast_now()))
                    now_stalled = stalled(time.time(), w.opened, counts, [e.t for e in w.events("rented")], failures,
                                          STALL_S)
                    return Look(outcome(ips, skipped_ips, refused), ips, refused, skipped_ips, now_stalled)
                waited = wait_until(look, OUTCOME_S, done=lambda now: now.result is not None or now.stalled, poll_s=20)
                (phase / "monitor_slice.log").write_text("\n".join(w.lines()))
                last = waited.seen or Look()
                if last.result is None:
                    why = f"{STALL_S} s with no instance and only `no rentable offer`" if last.stalled else f"{OUTCOME_S} s"
                    record.data(f"ip_{ip}", f"no outcome ({why}): workers {last.ips or waited.error}")
                elif last.result == "no_hairpin" and "no_hairpin" not in seen:
                    detected, asked_before = time.time(), len(no_offer_failures(w))
                    time.sleep(QUIET_S)
                    asked_after = len(no_offer_failures(w))
                    rented_after = [iid for iid, changes in w.vast_seen().items() if changes[0][0] > detected]
                    workers_after = alive_workers(cluster, "node_ip").get("pair", [])
                    measured, no_log = hairpin_logs(cluster, phase)
                    record.data(f"ip_{ip}", {"outcome": "no_hairpin", "ip": last.ips[0], "hairpin": measured,
                                             "head_refused": last.refused, "skipped_ips": sorted(set(last.skipped_ips)),
                                             "rented_after": rented_after, "workers_after": workers_after,
                                             "no_offer_failures": [asked_before, asked_after]})
                    record.check(f"IP {ip}: the container logs were fetched", not no_log, no_log)
                    if not last.refused:  # after a refusal, the refused candidate can be the one that measured "no"
                        record.check(f"IP {ip}: the worker on {last.ips[0]} measured no hairpin NAT",
                                     list(measured.values()) == [False], measured)
                    record.check(f"IP {ip}: no instance rented in the {QUIET_S} s after the outcome", not rented_after,
                                 rented_after)
                    record.check(f"IP {ip}: the one worker stays", workers_after == last.ips, workers_after)
                    record.check(f"IP {ip}: the autoscaler kept asking in the {QUIET_S} s (a new `no rentable offer` "
                                 "failure)", asked_after > asked_before, (asked_before, asked_after))
                    seen["no_hairpin"] = ip
                elif last.result == "hairpin" and "hairpin" not in seen:
                    job = cluster.job(env, ["python", "shared_ip.py", last.ips[0]], phase, name="shared_ip",
                                      prefix="JOB_RESULT", working_dir=workdir(JOBS / "shared_ip.py", JOBS / "jobcommon.py"),
                                      timeout=1200)
                    measured, no_log = hairpin_logs(cluster, phase)
                    record.data(f"ip_{ip}", {"outcome": "hairpin", "ip": last.ips[0], "job": job.data, "hairpin": measured})
                    record.check(f"IP {ip}: the container logs were fetched", not no_log, no_log)
                    record.check(f"IP {ip}: both workers on {last.ips[0]} measured hairpin NAT",
                                 list(measured.values()) == [True, True], measured)
                    record.check(f"IP {ip}: Ray traffic both ways between the two workers on {last.ips[0]} "
                                 "(actor call, 50 MB object)", job.word == "PASS", job.data)
                    seen["hairpin"] = ip
                else:  # an outcome already seen on an earlier IP
                    record.data(f"ip_{ip}", {"outcome": last.result, "workers": last.ips, "head_refused": last.refused})
            finally:
                cluster.set_shape({})  # max_workers 0: the autoscaler ends the workers
                idle = wait_until(cluster.instances, IDLE_S, done=lambda listed: not listed, poll_s=15)
                record.require(f"IP {ip}: 0 instances before the next IP (within {IDLE_S} s)", idle.ok,
                               [i["id"] for i in idle.seen or []] or idle.error)
    record.data("outcomes", seen)
    record.check("an IP with hairpin NAT was found and tested", "hairpin" in seen, seen)
    record.check("an IP without hairpin NAT was found and tested", "no_hairpin" in seen, seen)
