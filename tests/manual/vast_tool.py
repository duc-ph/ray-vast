# vast.ai helper for the live cycles C5, C6 and C8 (docs/test-plan.md, "Scripts"). Runs on the operator machine.
# The vast key comes from VAST_API_KEY or ~/.config/vastai/vast_api_key. This script never prints a key or a token.
#
# Usage: python vast_tool.py credit
#        python vast_tool.py count                  # instances with a test label (see is_test_label)
#        python vast_tool.py sweep                  # destroy them; list again until 0
#        python vast_tool.py poll <label> <period>  # one JSON line per sample, until Ctrl-C
#        python vast_tool.py logs <id>              # the container log (fetch it before the destroy)
#        python vast_tool.py fields <label>         # the instance fields that the provider reads (C5.4)
#        python vast_tool.py env-secret <id> <file> # the instance's join token into a new mode-600 file (for scan_secrets.sh)
#        python vast_tool.py reboot <id>            # C6.3: restart the container (PUT /instances/reboot/<id>/); test labels only
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

API = "https://console.vast.ai/api/v0"
TEST_LABEL_PREFIXES = ("ray-vast-test-", "ray-vast:rvtest-")  # direct test rentals; provider labels of rvtest-* clusters
POLL_FIELDS = ("id", "actual_status", "intended_status", "status_msg", "public_ipaddr", "dph_total", "machine_id")
READ_FIELDS = ("id", "label", "actual_status", "cur_state", "intended_status", "status_msg", "public_ipaddr", "machine_id",
               "dph_total", "ports")


def is_test_label(label):
    return isinstance(label, str) and any(label.startswith(p) and len(label) > len(p) for p in TEST_LABEL_PREFIXES)


def join_token(instance):
    """The RAYVAST_JOIN_TOKEN value in the instance's extra_env (a list of [name, value] pairs, or a mapping)."""
    env = instance.get("extra_env") or []
    pairs = env.items() if isinstance(env, dict) else [p for p in env if isinstance(p, (list, tuple)) and len(p) == 2]
    return next((value for name, value in pairs if name == "RAYVAST_JOIN_TOKEN"), None)


def call(method, path, body=None):
    """One vast API call with a 30 s timeout. Retries HTTP 429, 5xx and connection errors (all calls here are
    idempotent). Errors name only the method, the path and the status: a vast error body can echo the env."""
    key = os.environ.get("VAST_API_KEY") or (Path.home() / ".config/vastai/vast_api_key").read_text()
    request = urllib.request.Request(API + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                     headers={"Authorization": f"Bearer {key.strip()}", "Content-Type": "application/json"})
    for attempt in range(8):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as e:
            if e.code != 429 and e.code < 500:
                raise SystemExit(f"{method} {path}: HTTP {e.code}")
            why = f"HTTP {e.code}"
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            why = type(e).__name__
        print(f"{method} {path}: {why}, retry {attempt + 1}", file=sys.stderr)
        time.sleep(min(2 ** attempt, 30))
    raise SystemExit(f"{method} {path}: gave up")


def instances():
    return call("GET", "/instances/")["instances"]


def utc():
    return time.strftime("%FT%TZ", time.gmtime())


if __name__ == "__main__":
    command, args = sys.argv[1], sys.argv[2:]
    if command == "credit":
        print(json.dumps({"utc": utc(), "credit": call("GET", "/users/current/")["credit"]}))
    elif command == "count":
        listed = instances()
        found = [i for i in listed if is_test_label(i.get("label"))]
        for i in found:
            print(i["id"], i.get("label"), i.get("actual_status"), i.get("dph_total"))
        print(f"{len(found)} test instance(s) (account total {len(listed)})")
    elif command == "sweep":
        for _ in range(60):
            found = [i for i in instances() if is_test_label(i.get("label"))]
            print(f"{utc()} {len(found)} left", flush=True)
            if not found:
                break
            for i in found:
                call("DELETE", f"/instances/{i['id']}/")
                print(f"destroyed {i['id']} ({i.get('label')})", flush=True)
            time.sleep(5)
        else:
            raise SystemExit("sweep: instances still listed after 60 rounds")
    elif command == "poll":
        label, period = args[0], float(args[1])
        while True:
            row = {"utc": utc()}
            try:
                row["instances"] = [{k: i.get(k) for k in POLL_FIELDS} for i in instances() if i.get("label") == label]
            except SystemExit as e:  # one bad sample must not end the timeline
                row["error"] = str(e)
            print(json.dumps(row), flush=True)
            time.sleep(period)
    elif command == "logs":
        result = call("PUT", f"/instances/request_logs/{args[0]}/", {"tail": "20000"})  # vast limit: tail <= 20000 (HTTP 400 above)
        url = result.get("result_url")  # an unauthenticated S3 URL: do not print it
        if not url:
            raise SystemExit(f"request_logs: no result_url (keys {sorted(result)})")
        for _ in range(30):  # vast uploads the log a few seconds after the request
            try:
                with urllib.request.urlopen(url, timeout=30) as response:
                    sys.stdout.write(response.read().decode(errors="replace"))
                    break
            except urllib.error.HTTPError as e:
                if e.code not in (403, 404):
                    raise SystemExit(f"log download: HTTP {e.code}")
            time.sleep(2)
        else:
            raise SystemExit("log download: not ready after 60 s")
    elif command == "fields":
        print(json.dumps([{k: i.get(k) for k in READ_FIELDS} for i in instances() if i.get("label") == args[0]], indent=1))
    elif command == "env-secret":
        instance_id, path = int(args[0]), args[1]
        instance = next((i for i in instances() if i["id"] == instance_id), None)
        token = join_token(instance) if instance else None
        if not token:
            raise SystemExit(f"instance {instance_id}: {'not listed' if not instance else 'no RAYVAST_JOIN_TOKEN in extra_env'}")
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as f:
            f.write(token)
        print(f"wrote the join token of {instance_id} to {path}")
    elif command == "reboot":
        instance = next((i for i in instances() if i["id"] == int(args[0])), None)
        if not instance or not is_test_label(instance.get("label")):
            raise SystemExit(f"instance {args[0]}: not listed or no test label; not rebooted")
        print(f"{utc()} reboot {args[0]}: {json.dumps(call('PUT', f'/instances/reboot/{args[0]}/', {}))}")
    else:
        raise SystemExit(f"unknown command {command!r}")
