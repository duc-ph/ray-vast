# Host survey orchestrator (docs/spikes.md, "S6 — host survey"). Runs on the operator machine (the dev box).
# It rents many distinct vast machines (stratified by cpu_cores_effective), runs probe.py as the container command, reads
# the probe JSON from the identity ports (or from the vast log), checks every identity port from outside, and lets the
# probe destroy its own instance. Every instance has the label LABEL; the end (also Ctrl-C, SIGTERM, a crash) is a
# label sweep until 0. The vast key comes from ~/.config/vastai/vast_api_key and is never printed.
#
# Usage: python survey.py [--max N] [--conc 20] [--ray-checks 10] [--spend-cap 75] [--only-machine ID]
# Output (OUT): plan.json, rentals.jsonl (one row per rental), events.log, credit.jsonl, offers_<utc>.json.gz
import argparse
import base64
import concurrent.futures
import gzip
import json
import random
import signal
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
import zlib
from pathlib import Path

API = "https://console.vast.ai/api/v0"
LABEL = "ray-vast-test-2026-10-17"
HERE = Path(__file__).resolve().parent
OUT = HERE.parents[2] / "archive/results/2026-10-17/survey"
SMALL_IMAGE = "python:3.11-slim"
RAY_IMAGE = "rayproject/ray:2.59.0-py311-cpu"
BUCKETS = [(0, 4), (4, 8), (8, 16), (16, 32), (32, 64), (64, 128), (128, 10**6)]  # cpu_cores_effective [lo, hi)
TARGETS = {0: 56, 1: 60, 2: 62, 3: 62, 4: 62, 5: 70, 6: 40}  # machines per bucket (6: every rentable machine)
CAP_DPH = 1.50
CAP_DPH_BIG = 3.00  # for cpu_cores_effective >= 64 (lead's change)
DEADLINE_S = {SMALL_IMAGE: 720, RAY_IMAGE: 900}  # create -> probe line
LIST_PERIOD_S = 20
KEY = (Path.home() / ".config/vastai/vast_api_key").read_text().strip()
PROBE_ARGS = ["python3", "-c", "import zlib,base64;exec(zlib.decompress(base64.b64decode('%s')))"
              % base64.b64encode(zlib.compress((HERE / "probe.py").read_bytes(), 9)).decode()]
INSTANCE_FIELDS = ("id", "machine_id", "host_id", "actual_status", "intended_status", "cur_state", "status_msg",
                   "public_ipaddr", "cpu_cores", "cpu_cores_effective", "cpu_ram", "mem_limit", "disk_space", "gpu_name",
                   "num_gpus", "image_uuid", "start_date", "direct_port_start", "direct_port_end", "inet_down", "inet_up",
                   "geolocation", "static_ip", "hosting_type", "verification", "dph_total", "os_version",
                   "driver_version", "cuda_max_good", "local_ipaddrs", "gpu_frac")

lock = threading.RLock()
stop = threading.Event()


def utc(t=None):
    return time.strftime("%FT%TZ", time.gmtime(t))


def event(text):
    line = f"{utc()} {text}"
    print(line, flush=True)
    with open(OUT / "events.log", "a") as f:
        f.write(line + "\n")


class Bucket:
    """One token bucket for all vast API calls (rate per second, small burst)."""

    def __init__(self, rate=1.0, burst=3):
        self.rate, self.burst, self.tokens, self.t = rate, burst, burst, time.monotonic()
        self.lock = threading.Lock()

    def take(self):
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.burst, self.tokens + (now - self.t) * self.rate)
                self.t = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
            time.sleep(0.2)


bucket = Bucket()
stats = {"calls": 0, "http429": 0}


def call(method, path, body=None, retry_5xx=True, tries=8):
    """One vast API call through the token bucket. Exponential backoff with jitter on 429 (and 5xx/connection errors
    when retry_5xx). Errors carry only the method, path, status and a short body without secrets."""
    data = json.dumps(body).encode() if body is not None else None
    for attempt in range(tries):
        bucket.take()
        stats["calls"] += 1
        req = urllib.request.Request(API + path, method=method, data=data,
                                     headers={"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=40) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            text = e.read(400).decode(errors="replace").replace(KEY, "<key>")
            if e.code == 429:
                stats["http429"] += 1
            elif not (retry_5xx and e.code >= 500):
                raise RuntimeError(f"{method} {path}: HTTP {e.code}: {text[:300]}")
            why = f"HTTP {e.code}"
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            if not retry_5xx:
                raise RuntimeError(f"{method} {path}: {type(e).__name__}")
            why = type(e).__name__
        delay = min(2 ** attempt, 60) * (1 + random.random())
        event(f"api {method} {path}: {why}, retry {attempt + 1} in {delay:.0f}s")
        time.sleep(delay)
    raise RuntimeError(f"{method} {path}: gave up")


def bucket_of(offer):
    eff = offer.get("cpu_cores_effective") or 0
    return next(i for i, (lo, hi) in enumerate(BUCKETS) if lo <= eff < hi)


def search_offers():
    """Every rentable on-demand offer (verified and not), cheapest first, under the per-offer caps."""
    offers = []
    for verified in (True, False):
        body = {"rentable": {"eq": True}, "dph_total": {"lte": CAP_DPH_BIG}, "verified": {"eq": verified},
                "order": [["dph_total", "asc"]], "type": "on-demand", "limit": 10000}
        offers += call("POST", "/bundles/", body)["offers"]
    offers = [o for o in offers if o["dph_total"] <= (CAP_DPH_BIG if (o.get("cpu_cores_effective") or 0) >= 64 else CAP_DPH)]
    with gzip.open(OUT / f"offers_{utc().replace(':', '')}.json.gz", "wt") as f:
        json.dump(offers, f)
    cheapest = {}
    for o in offers:
        if o["machine_id"] not in cheapest or o["dph_total"] < cheapest[o["machine_id"]]["dph_total"]:
            cheapest[o["machine_id"]] = o
    return cheapest


def order_bucket(machines, seed):
    """Diversity order: round-robin over (verification, hosting_type) groups; inside a group, one machine per host_id
    first, then GPU types and countries not yet seen. Deterministic for a seed."""
    rnd = random.Random(seed)
    groups = {}
    for o in sorted(machines, key=lambda o: o["machine_id"]):
        groups.setdefault((o.get("verification"), o.get("hosting_type")), []).append(o)
    seen_host, seen_gpu, seen_cc, ordered = set(), set(), set(), []
    for g in groups.values():
        rnd.shuffle(g)
    keys = sorted(groups, key=str)
    while any(groups.values()):
        for k in keys:
            g = groups[k]
            if not g:
                continue
            best = max(g, key=lambda o: (o["host_id"] not in seen_host, o["gpu_name"] not in seen_gpu,
                                         (o.get("geolocation") or "").split(",")[-1] not in seen_cc))
            g.remove(best)
            seen_host.add(best["host_id"])
            seen_gpu.add(best["gpu_name"])
            seen_cc.add((best.get("geolocation") or "").split(",")[-1])
            ordered.append(best["machine_id"])
    return ordered


def write_row(rec):
    with lock, open(OUT / "rentals.jsonl", "a") as f:
        f.write(json.dumps(rec, sort_keys=True) + "\n")


def request_logs(iid):
    """The container log via vast (a signed S3 URL; it is not printed or stored). Returns text or None."""
    result = call("PUT", f"/instances/request_logs/{iid}/", {"tail": "1000"})
    url = result.get("result_url")
    if not url:
        return None
    for _ in range(15):
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                return r.read().decode(errors="replace")
        except urllib.error.HTTPError as e:
            if e.code not in (403, 404):
                return None
        except OSError:
            pass
        time.sleep(2)
    return None


def http(ip, port, path, timeout=5):
    with socket.create_connection((ip, port), timeout=timeout) as c:
        c.settimeout(timeout)
        c.sendall(f"GET {path} HTTP/1.0\r\n\r\n".encode())
        data = b""
        while True:
            chunk = c.recv(65536)
            if not chunk:
                break
            data += chunk
    return data.split(b"\r\n\r\n", 1)[1]


def fetch(rec):
    """Thread: check every host port from outside (5 s, in parallel, like the head's port check), read /result, send
    /done. Falls back to the vast log when no port answers."""
    ip, ports = rec["public_ip"], rec["host_ports"]
    ok = []
    if ports:
        def probe_port(p):
            try:
                return p if http(ip, p, "/") == b"rayvast-survey" else None
            except OSError:
                return None
        with concurrent.futures.ThreadPoolExecutor(64) as pool:
            ok = [p for p in pool.map(probe_port, ports) if p]
    with lock:
        rec["ext_ports_ok"] = len(ok)
        rec["ext_ports_total"] = len(ports)
        rec["fetch_tries"] = rec.get("fetch_tries", 0) + 1
    for p in ok:
        try:
            t = time.time()
            result = json.loads(http(ip, p, "/result", timeout=10))
            with lock:
                rec["probe"], rec["probe_source"], rec["t_fetch"] = result, "port", t
                rec["clock_skew_s"] = round(result["served_at"] - t, 2)
            try:
                http(ip, p, "/done")
                with lock:
                    rec["t_done_sent"] = time.time()
            except OSError:
                pass
            return
        except (OSError, ValueError, KeyError):
            continue
    if time.time() - rec["t_running"] > 90 and not ok:
        try:
            text = request_logs(rec["id"]) or ""
        except RuntimeError as e:
            text = ""
            with lock:
                rec["log_error"] = str(e)[:200]
        # The vast log keeps only the first 500 characters of a line: the probe also prints the JSON in parts.
        parts = {}
        for l in text.splitlines():
            if l.startswith("RAYVAST_SURVEY_PART "):
                _, k, n, part = l.rstrip("\r").split(" ", 3)
                parts[int(k)] = (int(n), part[:-1])  # drop the "|" end mark
        if parts and len(parts) == next(iter(parts.values()))[0]:
            try:
                probe = json.loads("".join(parts[k][1] for k in sorted(parts)))
            except ValueError:
                return
            with lock:
                rec["probe"], rec["probe_source"], rec["t_fetch"] = probe, "log", time.time()


def create(offer, image):
    n_ports = min(64, offer.get("direct_port_count") or 0)
    body = {"client_id": "me", "image": image, "label": LABEL, "disk": 10, "runtype": "args", "args": PROBE_ARGS,
            "env": {**{f"-p {p}:{p}": "1" for p in range(70000, 70000 + n_ports)}, "SURVEY_WAIT_S": "600"}}
    t = time.time()
    result = call("PUT", f"/asks/{offer['id']}/", body, retry_5xx=False)
    result.pop("instance_api_key", None)
    if not result.get("success", True) or "new_contract" not in result:
        raise RuntimeError(f"create: {json.dumps({k: result.get(k) for k in ('success', 'error', 'msg')})[:300]}")
    return {"id": int(result["new_contract"]), "machine_id": offer["machine_id"], "host_id": offer["host_id"],
            "offer": offer, "bucket": bucket_of(offer), "image": image, "n_ports": n_ports, "t_create": t,
            "statuses": []}


def sweep():
    for _ in range(60):
        found = [i for i in call("GET", "/instances/")["instances"] if i.get("label") == LABEL]
        event(f"sweep: {len(found)} left")
        if not found:
            return 0
        for i in found:
            try:
                call("DELETE", f"/instances/{i['id']}/")
            except RuntimeError as e:
                event(f"sweep: destroy {i['id']} failed: {e}")
        time.sleep(5)
    return len(found)


def credit_sample(credits):
    c = call("GET", "/users/current/")["credit"]
    credits.append((time.time(), c))
    with open(OUT / "credit.jsonl", "a") as f:
        f.write(json.dumps({"utc": utc(), "credit": c}) + "\n")
    # spend = sum of drops; a rise is an auto top-up
    return sum(max(0.0, a[1] - b[1]) for a, b in zip(credits, credits[1:]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=420, help="most new machines to try with the small image")
    ap.add_argument("--conc", type=int, default=20)
    ap.add_argument("--ray-checks", type=int, default=10)
    ap.add_argument("--spend-cap", type=float, default=75.0)
    ap.add_argument("--stop-early", action="store_true", help="stop after 250 probed if every bucket has >= 20")
    ap.add_argument("--only-machine", type=int, action="append")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)

    def on_signal(signum, frame):
        stop.set()
        raise KeyboardInterrupt(signal.Signals(signum).name)
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGHUP, on_signal)

    # Resume: machines with a final row are not tried again (a create failure may be retried once).
    done_rows = [json.loads(l) for l in open(OUT / "rentals.jsonl")] if (OUT / "rentals.jsonl").exists() else []
    tried = {(r["machine_id"], r["image"]) for r in done_rows if r["outcome"] not in ("create_failed", "aborted")}
    create_fail_count = {}
    for r in done_rows:
        if r["outcome"] == "create_failed":
            create_fail_count[r["machine_id"]] = create_fail_count.get(r["machine_id"], 0) + 1
    probed_small = [r for r in done_rows if r["image"] == SMALL_IMAGE and r["outcome"] == "probed"]
    ray_done = {r["machine_id"] for r in done_rows if r["image"] == RAY_IMAGE}
    credits = []
    inflight = {}
    try:
        spend = credit_sample(credits)
        machines = search_offers()
        t_search = time.time()
        per_bucket = {b: [o for o in machines.values() if bucket_of(o) == b] for b in range(len(BUCKETS))}
        queues = {b: [m for m in order_bucket(per_bucket[b], seed=b) if (m, SMALL_IMAGE) not in tried]
                  for b in per_bucket}
        if args.only_machine:
            queues = {b: [m for m in q if m in args.only_machine] for b, q in queues.items()}
        attempted = {b: sum(1 for r in done_rows if r["bucket"] == b and r["image"] == SMALL_IMAGE
                            and r["outcome"] not in ("create_failed", "aborted")) for b in per_bucket}
        with open(OUT / "plan.json", "w") as f:
            json.dump({"utc": utc(), "buckets": BUCKETS, "targets": TARGETS, "available": {b: len(v) for b, v in per_bucket.items()},
                       "queues": queues}, f)
        event(f"start: {len(machines)} machines; per bucket {[len(per_bucket[b]) for b in per_bucket]}; "
              f"done before {len(done_rows)} rows; credit {credits[-1][1]:.4f}")
        ray_queue = []
        new_small = 0
        last_list = last_credit = 0.0
        fetchers = {}

        def finish(rec, outcome, destroy):
            if destroy:
                try:
                    call("DELETE", f"/instances/{rec['id']}/")
                    rec["destroyed_by"] = "operator"
                except RuntimeError as e:
                    rec["destroy_error"] = str(e)[:200]
            rec["outcome"] = outcome
            rec["t_end"] = time.time()
            rec["est_cost"] = round(rec["offer"]["dph_total"] * (rec["t_end"] - rec["t_create"]) / 3600, 5)
            inflight.pop(rec["id"], None)
            write_row(rec)
            p = rec.get("probe") or {}
            event(f"{outcome} i{rec['id']} m{rec['machine_id']} b{rec['bucket']} {rec['image'].split(':')[0]} "
                  f"eff={rec['offer'].get('cpu_cores_effective')} pids={p.get('pids_max')} quota={p.get('cpu_quota')} "
                  f"cg={p.get('cgroup')} ext={rec.get('ext_ports_ok')}/{rec.get('ext_ports_total')} hp={p.get('hairpin_ok')} "
                  f"cost={rec['est_cost']}")
            if outcome == "probed" and rec["image"] == SMALL_IMAGE:
                probed_small.append(rec)
                b = rec["bucket"]
                if (len(ray_queue) + len(ray_done) < args.ray_checks and rec["machine_id"] not in ray_done
                        and sum(1 for o in ray_queue if bucket_of(o) == b) + sum(
                            1 for r in done_rows if r["image"] == RAY_IMAGE and r["bucket"] == b) < 2):
                    ray_queue.append(rec["offer"])

        while not stop.is_set():
            now = time.time()
            if now - last_credit > 300:
                spend = credit_sample(credits)
                last_credit = now
                event(f"credit {credits[-1][1]:.4f}, spend so far {spend:.4f}, inflight {len(inflight)}, "
                      f"probed {len(probed_small)}, api calls {stats['calls']} (429: {stats['http429']})")
            if now - t_search > 900:
                fresh = search_offers()
                machines.update(fresh)
                for m in list(machines):
                    if m not in fresh:
                        machines[m]["_gone"] = True
                t_search = now
            if now - last_list > LIST_PERIOD_S:
                last_list = now
                listing = {i["id"]: i for i in call("GET", "/instances/")["instances"]}
                for iid, i in listing.items():
                    if i.get("label") == LABEL and iid not in inflight and now - i.get("start_date", now) > 120:
                        event(f"stray i{iid}: destroying")
                        call("DELETE", f"/instances/{iid}/")
                for rec in list(inflight.values()):
                    i = listing.get(rec["id"])
                    if i is None:
                        if rec.get("probe"):
                            rec["self_destroyed"] = "t_done_sent" in rec and not rec.get("destroyed_by")
                            rec["t_gone"] = now
                            finish(rec, "probed", destroy=False)
                        elif now - rec["t_create"] > 60:
                            finish(rec, "vanished", destroy=False)
                        continue
                    st = (i.get("actual_status"), (i.get("status_msg") or "")[:240])
                    if not rec["statuses"] or tuple(rec["statuses"][-1][1:]) != st:
                        rec["statuses"].append([round(now - rec["t_create"], 1), *st])
                    if i.get("actual_status") == "running" and "t_running" not in rec:
                        rec["t_running"] = now
                        rec["public_ip"] = (i.get("public_ipaddr") or "").strip()
                        rec["host_ports"] = sorted({int(k.split("/")[0]) for k in (i.get("ports") or {}) if k.endswith("/tcp")})
                        rec["instance"] = {k: i.get(k) for k in INSTANCE_FIELDS}
            # per-instance work
            for rec in list(inflight.values()):
                age = now - rec["t_create"]
                th = fetchers.get(rec["id"])
                busy = th is not None and th.is_alive()
                if rec.get("probe"):
                    if busy or ("t_done_sent" in rec and now - rec["t_done_sent"] <= 150):
                        continue  # the probe destroys itself after /done; the listing sees it go
                    rec["self_destroyed"] = False  # no /done (log source) or no self-destroy in 150 s
                    finish(rec, "probed", destroy=True)
                    continue
                if "t_running" in rec and not busy and now - rec.get("t_fetch_try", 0) > 15:
                    rec["t_fetch_try"] = now
                    fetchers[rec["id"]] = threading.Thread(target=fetch, args=(rec,), daemon=True)
                    fetchers[rec["id"]].start()
                elif age > DEADLINE_S[rec["image"]] and not busy:
                    rec["last_status"] = rec["statuses"][-1] if rec["statuses"] else None
                    finish(rec, "no_probe_running" if "t_running" in rec else "never_started", destroy=True)
            # new rentals
            probed_b = {}
            for r in probed_small:
                probed_b[r["bucket"]] = probed_b.get(r["bucket"], 0) + 1
            early = args.stop_early and len(probed_small) >= 250 and all(probed_b.get(b, 0) >= 20 for b in per_bucket
                                                                           if len(per_bucket[b]) >= 20)
            want_more = spend < args.spend_cap and not early
            while want_more and len(inflight) < args.conc:
                if ray_queue:
                    offer, image = ray_queue.pop(0), RAY_IMAGE
                    fresh = machines.get(offer["machine_id"])
                    offer = fresh if fresh and not fresh.get("_gone") else offer
                    ray_done.add(offer["machine_id"])
                elif new_small < args.max:
                    open_b = [b for b in queues if queues[b] and attempted[b] < TARGETS[b]]
                    if not open_b:
                        break
                    b = min(open_b, key=lambda b: attempted[b] / TARGETS[b])
                    m = queues[b].pop(0)
                    offer = machines[m]
                    if offer.get("_gone"):
                        continue
                    image = SMALL_IMAGE
                else:
                    break
                try:
                    rec = create(offer, image)
                except RuntimeError as e:
                    row = {"machine_id": offer["machine_id"], "host_id": offer["host_id"], "offer": offer,
                           "bucket": bucket_of(offer), "image": image, "outcome": "create_failed",
                           "error": str(e)[:300], "t_create": time.time()}
                    write_row(row)
                    event(f"create_failed m{offer['machine_id']} offer {offer['id']}: {str(e)[:160]}")
                    if image == SMALL_IMAGE and create_fail_count.get(offer["machine_id"], 0) == 0:
                        create_fail_count[offer["machine_id"]] = 1
                        machines[offer["machine_id"]]["_gone"] = True  # retried after the next search, at the end
                        queues[bucket_of(offer)].append(offer["machine_id"])
                    continue
                inflight[rec["id"]] = rec
                if image == SMALL_IMAGE:
                    new_small += 1
                    attempted[rec["bucket"]] += 1
                event(f"created i{rec['id']} m{rec['machine_id']} b{rec['bucket']} {image} ${offer['dph_total']:.3f}/h "
                      f"eff={offer.get('cpu_cores_effective')} ports={rec['n_ports']}")
            if not want_more and not inflight:
                event(f"stop: spend {spend:.2f} (cap {args.spend_cap}), early stop {early}")
                break
            if not inflight and not ray_queue and (new_small >= args.max or not any(
                    queues[b] and attempted[b] < TARGETS[b] for b in queues)):
                event("stop: no more machines to try")
                break
            time.sleep(3)
    except KeyboardInterrupt as e:
        event(f"interrupted: {e}")
    finally:
        stop.set()
        for rec in list(inflight.values()):
            rec["outcome"] = "aborted"
            rec["t_end"] = time.time()
            write_row(rec)
        left = sweep()
        try:
            spend = credit_sample(credits)
            event(f"end: credit {credits[-1][1]:.4f}, spend (drops, top-ups removed) {spend:.4f}, sweep left {left}")
        except RuntimeError as e:
            event(f"end: credit read failed: {e}")


if __name__ == "__main__":
    main()
