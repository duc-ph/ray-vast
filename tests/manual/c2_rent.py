# C2/C3 setup: rent vast candidates and time each one (T8).
# Run on the laptop. VAST_API_KEY must be set (the full key; it is never sent to an instance).
# Usage: python c2_rent.py <head ssh pubkey file> <out dir> <offers.json from the search> <offer_id> [<offer_id> ...]
# For each offer: PUT /asks/<id>/, then poll every 10 s until `running` and the sshd port answers.
# Writes <out>/create_requests.jsonl (request bodies, no secrets) and <out>/rent_<offer>.json (timeline).
import json
import os
import socket
import sys
import time
import urllib.request

API = "https://console.vast.ai/api/v0"
KEY = os.environ["VAST_API_KEY"]
LABEL = "rayvast-test-20261012"
IMAGE = "rayproject/ray:2.59.0-py311-cpu"
IMAGE_BYTES = 951297021
pubkey = open(sys.argv[1]).read().strip()
out = sys.argv[2]
offers = {o["id"]: o for o in json.load(open(sys.argv[3]))["offers"]}
offer_ids = sys.argv[4:]

# The image runs as user ray (uid 1000) with passwordless sudo and has no sshd.
# vast puts VAST_TCP_PORT_* and PUBLIC_IPADDR in the env of PID 1 only, so we copy them to a file
# that SSH sessions can read. CONTAINER_API_KEY (vast puts it in the env too) is NOT copied.
start_script = r"""
set -x
export DEBIAN_FRONTEND=noninteractive
date -u +%FT%TZ > /tmp/rayvast_boot_start
command -v /usr/sbin/sshd || (sudo -E apt-get update -qq && sudo -E apt-get install -y -qq openssh-server >/tmp/rayvast_apt.log 2>&1)
date -u +%FT%TZ > /tmp/rayvast_sshd_installed
env | grep -E '^(VAST_TCP_PORT_[0-9]+|PUBLIC_IPADDR|CONTAINER_ID)=' | sudo tee /etc/rayvast_vast.env >/dev/null
for d in /root /home/ray; do
  sudo mkdir -p $d/.ssh
  echo "$RAYVAST_HEAD_SSH_PUBKEY" | sudo tee $d/.ssh/authorized_keys >/dev/null
done
sudo chown -R ray:users /home/ray/.ssh
sudo chmod 700 /root/.ssh /home/ray/.ssh
sudo chmod 600 /root/.ssh/authorized_keys /home/ray/.ssh/authorized_keys
sudo mkdir -p /run/sshd /opt/rayvast
sudo chown ray:users /opt/rayvast
sudo /usr/sbin/sshd -p $VAST_TCP_PORT_70040 -o PasswordAuthentication=no -o PermitRootLogin=prohibit-password
date -u +%FT%TZ > /tmp/rayvast_sshd_started
sleep infinity
"""

env = {f"-p {p}:{p}": "1" for p in range(70000, 70064)}
env["RAYVAST_HEAD_SSH_PUBKEY"] = pubkey

instances = {}
for offer_id in offer_ids:
    offer = offers[int(offer_id)]
    body = {"client_id": "me", "image": IMAGE, "label": LABEL, "disk": 20, "runtype": "args",
            "args": ["bash", "-c", start_script], "env": env}
    t_create = time.time()
    resp = json.load(urllib.request.urlopen(urllib.request.Request(
        f"{API}/asks/{offer_id}/", method="PUT", data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"})))
    # The response has "instance_api_key", a per-instance vast key. It is a secret: never log it.
    resp.pop("instance_api_key", None)
    print(offer_id, "->", resp, flush=True)
    with open(f"{out}/create_requests.jsonl", "a") as f:
        f.write(json.dumps({"utc": time.strftime("%FT%TZ", time.gmtime(t_create)), "url": f"PUT {API}/asks/{offer_id}/",
                            "body": body, "response": resp}) + "\n")
    pull_limit = min(max(2 * IMAGE_BYTES * 8 / (offer["inet_down"] * 1e6), 300), 900)
    instances[resp["new_contract"]] = {
        "offer_id": int(offer_id), "machine_id": offer["machine_id"], "host_id": offer["host_id"],
        "gpu": offer["gpu_name"], "dph_total": offer["dph_total"], "inet_down": offer["inet_down"],
        "inet_up": offer["inet_up"], "geolocation": offer["geolocation"], "static_ip": offer.get("static_ip"),
        "t_create": t_create, "pull_limit_s": pull_limit, "statuses": []}

while any("result" not in r for r in instances.values()):
    time.sleep(10)
    listing = json.load(urllib.request.urlopen(urllib.request.Request(
        f"{API}/instances/", headers={"Authorization": f"Bearer {KEY}"})))["instances"]
    by_id = {i["id"]: i for i in listing}
    now = time.time()
    for iid, r in instances.items():
        if "result" in r:
            continue
        inst = by_id.get(iid, {})
        status = inst.get("actual_status")
        if not r["statuses"] or r["statuses"][-1][1] != status:
            r["statuses"].append([round(now - r["t_create"], 1), status, inst.get("status_msg")])
            print(iid, round(now - r["t_create"]), status, (inst.get("status_msg") or "")[:120], flush=True)
        if status in ("exited", "offline") or (inst.get("intended_status") == "stopped"):
            r["result"] = f"FAIL status {status}"
            continue
        if status == "running" and "t_running" not in r:
            r["t_running"] = now - r["t_create"]
            r["public_ip"] = inst.get("public_ipaddr")
            # The ports dict is keyed by the host port P (identity mapping), so it does not say which P is
            # 70040. We find sshd by its banner below.
            r["host_ports"] = sorted(int(k.split("/")[0]) for k in inst["ports"])
        if "t_running" not in r and now - r["t_create"] > r["pull_limit_s"]:
            r["result"] = f"FAIL pull limit {r['pull_limit_s']:.0f}s"
            continue
        if "t_running" in r:
            for p in r["host_ports"]:
                try:
                    s = socket.create_connection((r["public_ip"].strip(), p), timeout=1)
                    s.settimeout(1)
                    banner = s.recv(64)
                    s.close()
                except OSError:
                    continue
                if banner.startswith(b"SSH-"):
                    r["ssh_port"] = p
                    r["t_ssh"] = time.time() - r["t_create"]
                    r["result"] = "OK"
                    print(iid, "ssh up", round(r["t_ssh"]), p, banner, flush=True)
                    break
            if "result" in r:
                continue
            if now - r["t_create"] - r["t_running"] > 60:
                r["result"] = "FAIL sshd limit 60s"

for iid, r in instances.items():
    json.dump({"instance_id": iid, **r}, open(f"{out}/rent_{r['offer_id']}.json", "w"), indent=1)
    print(iid, r["offer_id"], r["result"], "running", round(r.get("t_running", -1)), "ssh", round(r.get("t_ssh", -1)),
          r.get("public_ip"), r.get("ssh_port"))
