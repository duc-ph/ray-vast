# C4: poll one vast instance every <period> s and print its status and cost fields (one JSON line each).
# Run on the laptop. VAST_API_KEY must be set. Stops when the instance is gone (404 or not in the listing).
# Usage: python c4_poll.py <instance id> <period s> <max s>
import json
import os
import sys
import time
import urllib.error
import urllib.request

iid, period, max_s = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
t_start = time.time()
while time.time() - t_start < max_s:
    row = {"utc": time.strftime("%FT%TZ", time.gmtime())}
    try:
        r = json.load(urllib.request.urlopen(urllib.request.Request(
            f"https://console.vast.ai/api/v0/instances/{iid}/", headers={"Authorization": f"Bearer {os.environ['VAST_API_KEY']}"}), timeout=20))
        inst = r.get("instances", r)
        if not inst:
            row["gone"] = True
        else:
            row.update({k: inst.get(k) for k in ("actual_status", "cur_state", "intended_status", "next_state", "dph_total",
                                                  "dph_base", "storage_total_cost", "status_msg")})
    except urllib.error.HTTPError as e:
        row["http"] = e.code
        row["gone"] = e.code == 404
    except Exception as e:
        row["error"] = repr(e)
    print(json.dumps(row), flush=True)
    if row.get("gone"):
        break
    time.sleep(period)
