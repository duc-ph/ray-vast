# C3.1 (T10): what can a vast key do? Never prints the key or any key value.
# Usage: python c3_key_probe.py <key source: file path or env:NAME> <own instance id> <other instance id>
# Calls: GET own instance, GET other instance, DELETE other instance (expect refused), GET /users/current,
# offer search, GET /instances/ (list), GET /auth/apikeys/ (are key values returned?), GET /ssh/.
import json
import os
import sys
import urllib.error
import urllib.request

src, own, other = sys.argv[1], sys.argv[2], sys.argv[3]
key = os.environ[src[4:]] if src.startswith("env:") else open(src).read().strip()
API = "https://console.vast.ai/api/v0"
calls = [("GET own instance", "GET", f"/instances/{own}/", None),
         ("GET other instance", "GET", f"/instances/{other}/", None),
         ("DELETE other instance", "DELETE", f"/instances/{other}/", None),
         ("GET users/current", "GET", "/users/current/", None),
         ("POST bundles (offer search)", "POST", "/bundles/", {"num_gpus": {"eq": 1}, "limit": 2}),
         ("GET instances (list)", "GET", "/instances/", None),
         ("GET auth/apikeys", "GET", "/auth/apikeys/", None),
         ("GET ssh keys", "GET", "/ssh/", None)]
for label, method, path, body in calls:
    req = urllib.request.Request(API + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    try:
        r = urllib.request.urlopen(req, timeout=30)
        code, text = r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        code, text = e.code, e.read().decode()
    info = ""
    try:
        d = json.loads(text)
        if path == "/auth/apikeys/" and isinstance(d, dict) and "apikeys" in d:
            vals = [k.get("key") for k in d["apikeys"]]
            info = f"{len(vals)} keys listed; {sum(1 for v in vals if v and len(v) >= 32)} with a full key value"
        elif path == "/instances/" and isinstance(d, dict) and "instances" in d:
            info = f"{len(d['instances'])} instances listed: {[i['id'] for i in d['instances']]}"
        elif path.startswith("/instances/") and isinstance(d, dict) and "instances" in d:
            i = d["instances"]
            info = f"instance {i.get('id') if isinstance(i, dict) else i} status {i.get('actual_status') if isinstance(i, dict) else ''}"
        elif path == "/users/current/" and isinstance(d, dict):
            info = f"fields={len(d)} has_credit={'credit' in d} has_email={'email' in d}"
        elif path == "/bundles/" and isinstance(d, dict) and "offers" in d:
            info = f"{len(d['offers'])} offers"
        elif path == "/ssh/":
            info = f"{len(d) if isinstance(d, list) else type(d).__name__} ssh keys"
        else:
            info = text[:160].replace("\n", " ")
    except ValueError:
        info = text[:160].replace("\n", " ")
    print(f"{label}: HTTP {code} {info}", flush=True)
