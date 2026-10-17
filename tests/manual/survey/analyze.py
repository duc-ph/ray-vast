# Analysis of the host survey (docs/spikes.md, "S6 — host survey"). Reads OUT/rentals.jsonl and prints a text report
# (and writes OUT/analysis.txt and OUT/machines.csv). Usage: python analyze.py
import collections
import csv
import json
import math
import statistics
import sys
from pathlib import Path

OUT = Path(__file__).resolve().parents[3] / "archive/results/2026-10-17/survey"
SMALL, RAY = "python:3.11-slim", "rayproject/ray:2.59.0-py311-cpu"
BUCKET_NAMES = ["<4 (or unknown)", "4-8", "8-16", "16-32", "32-64", "64-128", "128+"]
lines = []


def say(*a):
    lines.append(" ".join(str(x) for x in a))


def pct(n, d):
    return f"{n}/{d} ({100 * n / d:.0f}%)" if d else f"{n}/0"


def ports_of(pids):
    """The product formula (bootstrap.worker_ports_and_soft_limit): worker ports from pids.max."""
    return 58 if not isinstance(pids, int) else max(4, min(58, (pids - 400) // 60))


rows = [json.loads(l) for l in open(OUT / "rentals.jsonl")]
small = [r for r in rows if r["image"] == SMALL]
# One final row per machine for the small image: a probe wins over other outcomes.
best = {}
rank = {"probed": 0, "no_probe_running": 1, "never_started": 2, "vanished": 3, "aborted": 4, "create_failed": 5}
for r in small:
    m = r["machine_id"]
    if m not in best or rank[r["outcome"]] < rank[best[m]["outcome"]]:
        best[m] = r
machines = list(best.values())
probed = [r for r in machines if r["outcome"] == "probed"]
say("== Counts (small image, distinct machines) ==")
say("machines tried:", len(machines), "hosts:", len({r["host_id"] for r in machines}))
say("outcomes:", dict(collections.Counter(r["outcome"] for r in machines)))
say("rentals (rows):", len(rows), "create_failed rows:", sum(r["outcome"] == "create_failed" for r in rows),
    "distinct probed hosts:", len({r["host_id"] for r in probed}))
cf = collections.Counter(r.get("error", "")[:90] for r in rows if r["outcome"] == "create_failed")
say("create_failed reasons:", cf.most_common(8))
for r in machines:
    if r["outcome"] in ("never_started", "no_probe_running", "vanished"):
        st = r.get("statuses") or []
        say(f"  {r['outcome']} m{r['machine_id']} b{r['bucket']} {r['offer']['gpu_name']} dc={r['offer'].get('hosting_type')}"
            f" {r['offer'].get('verification')} last={st[-1] if st else None}")

say("\n== pids.max rule ==")
cats = collections.Counter()
exceptions = []
for r in probed:
    p = r["probe"]
    pm, q = p["pids_max"], p["cpu_quota"]
    if not isinstance(pm, int):
        cat = f"no pids limit ({pm}); quota {'none' if q is None else 'set'}"
    elif q is None:
        cat = "pids limit, no CPU quota"
    elif pm == 256 * math.floor(q):
        cat = "pids.max = 256 x floor(quota)"
    elif pm == math.floor(256 * q):
        cat = "pids.max = floor(256 x quota)"
    else:
        cat = "other"
    cats[cat] += 1
    if cat != "pids.max = 256 x floor(quota)":
        exceptions.append((cat, r["machine_id"], p["cgroup"], pm, q, r["offer"].get("cpu_cores_effective"),
                           r["offer"].get("cpu_cores"), r["offer"]["num_gpus"], r["offer"]["gpu_name"],
                           r["offer"].get("hosting_type"), p["os_cpu_count"], p["affinity"]))
say("probed:", len(probed), dict(cats))
for e in sorted(exceptions, key=str)[:60]:
    say("  exception:", e)
fl = [r for r in probed if isinstance(r["probe"]["pids_max"], int) and r["probe"]["cpu_quota"]]
say("pids.max / floor(quota) values:", collections.Counter(r["probe"]["pids_max"] / max(1, math.floor(r["probe"]["cpu_quota"]))
                                                           for r in fl).most_common(6))

say("\n== CPU quota vs offer fields ==")
qrows = [r for r in probed if r["probe"]["cpu_quota"]]
eq_eff = sum(1 for r in qrows if math.floor(r["probe"]["cpu_quota"]) == math.floor(r["offer"].get("cpu_cores_effective") or 0))
say("floor(quota) == floor(cpu_cores_effective):", pct(eq_eff, len(qrows)))
ratios = [r["probe"]["cpu_quota"] / r["offer"]["cpu_cores_effective"] for r in qrows if r["offer"].get("cpu_cores_effective")]
say("quota / cpu_cores_effective: min %.3f median %.3f max %.3f" % (min(ratios), statistics.median(ratios), max(ratios))
    if ratios else "no ratios")
say("ratio buckets:", sorted(collections.Counter(round(x, 2) for x in ratios).most_common(10)))
inst_eff = [r for r in qrows if (r.get("instance") or {}).get("cpu_cores_effective")]
say("floor(quota) == floor(instance cpu_cores_effective):",
    pct(sum(1 for r in inst_eff if math.floor(r["probe"]["cpu_quota"]) == math.floor(r["instance"]["cpu_cores_effective"])), len(inst_eff)))
say("quota == instance cpu_cores_effective (2 dp):",
    pct(sum(1 for r in inst_eff if abs(r["probe"]["cpu_quota"] - r["instance"]["cpu_cores_effective"]) < 0.01), len(inst_eff)))
say("affinity == os_cpu_count:", pct(sum(1 for r in probed if r["probe"]["affinity"] == r["probe"]["os_cpu_count"]), len(probed)))
say("affinity < floor(quota) (affinity is the cap):",
    pct(sum(1 for r in qrows if r["probe"]["affinity"] < math.floor(r["probe"]["cpu_quota"])), len(qrows)))

say("\n== Per bucket (cpu_cores_effective) ==")
say("bucket | tried | probed | cgroup v1 | pids.max (median, min-max) | no limit | worker ports (median, min-max) | B=ports-4 median | Ray CPUs median | est. cost $")
for b, name in enumerate(BUCKET_NAMES):
    t = [r for r in machines if r["bucket"] == b]
    pr = [r for r in probed if r["bucket"] == b]
    lim = [r["probe"]["pids_max"] for r in pr if isinstance(r["probe"]["pids_max"], int)]
    pts = [ports_of(r["probe"]["pids_max"]) for r in pr]
    cpus = [min(r["probe"]["affinity"], max(1, math.floor(r["probe"]["cpu_quota"]))) if r["probe"]["cpu_quota"]
            else r["probe"]["affinity"] for r in pr]
    cost = sum(r.get("est_cost") or 0 for r in rows if r["bucket"] == b)
    say(f"{name} | {len(t)} | {len(pr)} | {sum(r['probe']['cgroup'] == 'v1' for r in pr)} | "
        f"{statistics.median(lim) if lim else '-'} ({min(lim) if lim else '-'}-{max(lim) if lim else '-'}) | "
        f"{len(pr) - len(lim)} | {statistics.median(pts) if pts else '-'} ({min(pts) if pts else '-'}-{max(pts) if pts else '-'}) | "
        f"{statistics.median(pts) - 4 if pts else '-'} | {statistics.median(cpus) if cpus else '-'} | {cost:.2f}")
say("worker ports distribution:", sorted(collections.Counter(ports_of(r["probe"]["pids_max"]) for r in probed).items()))
say("pids.max 1280 (14 ports) count:", sum(r["probe"]["pids_max"] == 1280 for r in probed))
say("ports 58 (pids >= 3880 or none):", sum(ports_of(r["probe"]["pids_max"]) == 58 for r in probed))


def rate(rs, key):
    vals = [key(r) for r in rs]
    vals = [v for v in vals if v is not None]
    return pct(sum(1 for v in vals if v), len(vals))


say("\n== Attributes by class ==")
classes = {"all": probed,
           "datacenter": [r for r in probed if r["offer"].get("hosting_type") == 1],
           "non-datacenter": [r for r in probed if r["offer"].get("hosting_type") != 1],
           "verified": [r for r in probed if r["offer"].get("verification") == "verified"],
           "unverified": [r for r in probed if r["offer"].get("verification") == "unverified"],
           "deverified": [r for r in probed if r["offer"].get("verification") == "deverified"],
           "dpc>=64": [r for r in probed if (r["offer"].get("direct_port_count") or 0) >= 64]}
checks = {
    "cgroup v1": lambda r: r["probe"]["cgroup"] == "v1",
    "pids limit set": lambda r: isinstance(r["probe"]["pids_max"], int),
    "CPU quota set": lambda r: r["probe"]["cpu_quota"] is not None,
    "hairpin ok": lambda r: r["probe"]["hairpin_ok"],
    "egress == PUBLIC_IPADDR": lambda r: r["probe"]["egress_ok"],
    "all ports reachable from outside": lambda r: r.get("ext_ports_ok") == r.get("ext_ports_total") if r.get("ext_ports_total") else None,
    "identity port env == requested": lambda r: r["probe"]["port_env_count"] == r["n_ports"] if r.get("n_ports") else None,
    "NET_ADMIN (CapBnd)": lambda r: "NET_ADMIN" in r["probe"]["CapBnd_names"],
    "NET_RAW (CapBnd)": lambda r: "NET_RAW" in r["probe"]["CapBnd_names"],
    "SYS_ADMIN (CapBnd)": lambda r: "SYS_ADMIN" in r["probe"]["CapBnd_names"],
    "SYS_PTRACE (CapBnd)": lambda r: "SYS_PTRACE" in r["probe"]["CapBnd_names"],
    "root": lambda r: r["probe"]["uid"] == 0,
    "seccomp filter (2)": lambda r: r["probe"]["seccomp"] == 2,
    "own netns": lambda r: r["probe"]["own_netns"],
    "/dev/shm empty": lambda r: r["probe"].get("shm_entries") == 0,
    "IPv6 global address": lambda r: r["probe"]["ipv6_global"] > 0,
    "/.dockerenv": lambda r: r["probe"]["dockerenv"],
    "nvidia devs == num_gpus": lambda r: r["probe"]["nvidia_devs"] == r["offer"]["num_gpus"],
    "memory limit set": lambda r: isinstance(r["probe"]["memory_max"], int) and r["probe"]["memory_max"] < 2 ** 60,
    "RLIMIT_NPROC unlimited": lambda r: r["probe"]["rlimit_nproc"][0] == -1,
    "user namespace (uid_map not 0 0 4294967295)": lambda r: (r["probe"].get("uid_map") or "").split() != ["0", "0", "4294967295"],
    "static_ip (offer)": lambda r: r["offer"].get("static_ip"),
}
say("check | " + " | ".join(f"{k} (n={len(v)})" for k, v in classes.items()))
for name, f in checks.items():
    say(name + " | " + " | ".join(rate(v, f) for v in classes.values()))
say("eth0 MTU:", collections.Counter((r["probe"]["ifaces"].get("eth0") or {}).get("mtu") for r in probed).most_common(6))
say("kernel:", collections.Counter(".".join(r["probe"]["kernel"].split(".")[:2]) for r in probed).most_common(8))
say("nofile soft:", collections.Counter(r["probe"]["rlimit_nofile"][0] for r in probed).most_common(5))
say("shm bytes / mem_total:", collections.Counter(round((r["probe"]["shm_bytes"] or 0) / (r["probe"]["mem_total_kb"] * 1024), 2)
                                                  for r in probed).most_common(6))
say("shm 64 MiB:", pct(sum(r["probe"]["shm_bytes"] == 64 * 2 ** 20 for r in probed), len(probed)))
say("CapBnd values:", collections.Counter(r["probe"]["CapBnd"] for r in probed).most_common(5))
say("egress services disagree:", sum(not r["probe"]["egress_services_agree"] for r in probed),
    "egress mismatch examples:", [(r["machine_id"], r["offer"].get("hosting_type")) for r in probed if not r["probe"]["egress_ok"]][:10])
say("hairpin errors:", collections.Counter(str(h) for r in probed for h in (r["probe"]["hairpin"] or [])[:1] if h is not True).most_common(5))
say("local_ip private:", collections.Counter(str(r["probe"]["local_ip"]).split(".")[0] for r in probed).most_common(5),
    "local_ip == public:", sum(r["probe"]["local_ip"] == r["probe"]["public_ipaddr"] for r in probed))
say("runtime cgroup path:", collections.Counter(("docker" if "docker" in r["probe"]["self_cgroup"] else "0::/" if
                                                 r["probe"]["self_cgroup"].startswith("0::/") else r["probe"]["self_cgroup"][:30])
                                                for r in probed).most_common(5))

say("\n== Timing (s) ==")
tr = [r["t_running"] - r["t_create"] for r in probed if r.get("t_running")]
tp = [r["probe"]["t_probe"] - r["t_create"] for r in probed if r.get("probe")]
tf = [r["t_fetch"] - r["t_create"] for r in probed if r.get("t_fetch")]
for name, v in (("create -> running (20 s list period)", tr), ("create -> probe line (host clock)", tp),
                ("create -> probe read by operator", tf)):
    if v:
        v = sorted(v)
        say(f"{name}: median {statistics.median(v):.0f}, p90 {v[int(0.9 * (len(v) - 1))]:.0f}, max {v[-1]:.0f}")
say("self-destroy after /done:", rate([r for r in probed if r.get("probe_source") == "port"], lambda r: r.get("self_destroyed")))
say("probe source:", collections.Counter(r.get("probe_source") for r in probed))

say("\n== Small image vs Ray image (same machine) ==")
rayrows = [r for r in rows if r["image"] == RAY and r["outcome"] == "probed"]
same = 0
for r in rayrows:
    s = best.get(r["machine_id"])
    if not s or s["outcome"] != "probed":
        continue
    a, b2 = s["probe"], r["probe"]
    keys = ("cgroup", "pids_max", "cpu_quota", "memory_max", "affinity", "CapBnd", "seccomp")
    eq = all(a[k] == b2[k] for k in keys)
    same += eq
    say(f"  m{r['machine_id']} gpus {s['offer']['num_gpus']}/{r['offer']['num_gpus']} same={eq} small="
        f"{[a[k] for k in keys[:5]]} ray={[b2[k] for k in keys[:5]]} ray uid={b2['uid']}")
say("ray-image rentals:", len([r for r in rows if r["image"] == RAY]), "probed:", len(rayrows), "identical limits:", same)

say("\n== Spend ==")
say("estimated (dph x lifetime) all rows: $%.2f" % sum(r.get("est_cost") or 0 for r in rows))
cr = [json.loads(l) for l in open(OUT / "credit.jsonl")] if (OUT / "credit.jsonl").exists() else []
if cr:
    drops = sum(max(0, a["credit"] - b["credit"]) for a, b in zip(cr, cr[1:]))
    ups = sum(max(0, b["credit"] - a["credit"]) for a, b in zip(cr, cr[1:]))
    say(f"credit first {cr[0]['credit']:.4f} last {cr[-1]['credit']:.4f}; drops {drops:.4f}; rises (top-ups) {ups:.4f}")

text = "\n".join(lines)
print(text)
(OUT / "analysis.txt").write_text(text + "\n")
with open(OUT / "machines.csv", "w", newline="") as f:
    cols = ["machine_id", "host_id", "outcome", "bucket", "gpu_name", "num_gpus", "cpu_name", "cpu_cores", "cpu_cores_effective",
            "cpu_ram", "gpu_ram", "disk_space", "dph_total", "hosting_type", "verification", "reliability", "geolocation",
            "direct_port_count", "inet_up", "inet_down", "driver_version", "cuda_max_good", "static_ip", "cgroup", "pids_max",
            "pids_current", "cpu_quota", "pids_per_quota", "os_cpu_count", "affinity", "memory_max", "shm_bytes", "kernel",
            "CapBnd", "seccomp", "uid", "hairpin_ok", "egress_ok", "ipv6_global", "eth0_mtu", "nvidia_devs", "ext_ports_ok",
            "ext_ports_total", "t_running_s", "t_probe_s"]
    w = csv.writer(f)
    w.writerow(cols)
    for r in machines:
        o, p = r["offer"], r.get("probe") or {}
        w.writerow([r["machine_id"], r["host_id"], r["outcome"], r["bucket"]] + [o.get(k) for k in cols[4:23]] +
                   [p.get(k) for k in cols[23:39]] + [p.get("ipv6_global"), (p.get("ifaces") or {}).get("eth0", {}).get("mtu"),
                    p.get("nvidia_devs"), r.get("ext_ports_ok"), r.get("ext_ports_total"),
                    round(r["t_running"] - r["t_create"]) if r.get("t_running") else None,
                    round(p["t_probe"] - r["t_create"]) if p.get("t_probe") else None])
