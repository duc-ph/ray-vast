"""The hosts of the shared-IP test (test_shared_ip_on_vast.py; unit-tested in tests/test_live_suite_kit.py). Two workers
get one public IP when two machines sit behind one NAT IP. vast lists one offer for each machine. After a rental it lists
the offer for about 80 s more (renting it fails), and then not again while the machine is rented. So each machine gives
one worker."""
import re


def public_ip_groups(offers, count):
    """Up to `count` public IPs with offers on two or more machines, as (IP, [machine IDs, cheapest offer first]). The
    IP with the cheapest two machines comes first. `offers` come from the node type's query, so each one fits a worker."""
    cheapest = {}  # IP -> {machine ID: its cheapest offer price}
    for offer in offers:
        if offer.get("public_ipaddr"):
            machines = cheapest.setdefault(offer["public_ipaddr"], {})
            machines[offer["machine_id"]] = min(offer["dph_total"], machines.get(offer["machine_id"], offer["dph_total"]))
    groups = [(ip, sorted(machines, key=machines.get)) for ip, machines in cheapest.items() if len(machines) >= 2]
    groups.sort(key=lambda group: sum(sorted(cheapest[group[0]].values())[:2]))
    return groups[:count]


def worker_hairpin(instance_ids, fetch):
    """({instance ID: the `hairpin:` value of its container log, None without the line} of the enrolled workers, [the
    instances with no log]). `fetch(instance_id)` gives a container log, "" when the fetch failed; a failed fetch is
    tried once more."""
    found, missing = {}, []
    for instance_id in instance_ids:
        text = fetch(instance_id) or fetch(instance_id)
        if not text:
            missing.append(instance_id)
        elif "bootstrap: enrolled as slot" in text:  # a worker, not a candidate
            value = re.search(r"bootstrap: hairpin: (True|False)", text)
            found[instance_id] = value.group(1) == "True" if value else None
    return found, missing


def stalled(now, since, instance_counts, rent_times, failures, stall_s):
    """True when the wait on one IP can end with no outcome: the last two vast listings of the cluster have no instance
    (one listing can miss an instance), nothing was rented in the last `stall_s` (nor since the wait began at `since`),
    and the launch failures in that time are all `no rentable offer`, at least one. `instance_counts`: the number of
    instances of each listing, in time order. `failures`: (time, reason) of the launch failures of the node type."""
    recent = [reason for t, reason in failures if t > now - stall_s]
    return (instance_counts[-2:] == [0, 0] and now - max([since, *rent_times]) >= stall_s and bool(recent)
            and all("no rentable offer" in reason for reason in recent))


def outcome(ips, skipped_ips, refused):
    """The outcome on one public IP, or None (not yet).
    `ips`: the IPs of the alive workers. `skipped_ips`: the IPs of the provider's `skipped the offers on public IP` lines
    (no hairpin NAT there, and a worker on it). `refused`: a `no hairpin NAT` ending of the head.
    - hairpin: 2 workers with one IP.
    - no_hairpin: one worker, and the head refused a second candidate on its IP, or the provider skipped the offers on
      its IP. A `no rentable offer` failure alone is no proof: vast lists a just-rented offer for a while, and renting it
      fails."""
    if len(ips) == 2 and len(set(ips)) == 1:
        return "hairpin"
    if len(ips) == 1 and (refused or ips[0] in skipped_ips):
        return "no_hairpin"
    return None
