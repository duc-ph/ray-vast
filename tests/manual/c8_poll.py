# C8 poller (docs/test-plan.md): every <period> s, one JSON line with the cluster's vast instances (ID, actual_status,
# dph_total, machine_id), the sum of their dph_total, the number of worker rules in the cluster's 5 security groups,
# and the Ray nodes (`ray list nodes` in the container ray-vast-head). Runs on the operator machine until Ctrl-C.
# AWS: profile `personal` (or AWS_PROFILE). SSH key: RAYVAST_KEY_FILE or ~/.ray-vast/<cluster>/ssh_key.pem (made by `up`).
# Usage: python c8_poll.py [<cluster, default rvtest-c8>] [<period s, default 5>]
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import boto3
from ray_vast.vast_api import label  # the cluster marker: the vast label and the SG rule description

import vast_tool

cluster = sys.argv[1] if len(sys.argv) > 1 else "rvtest-c8"
period = float(sys.argv[2]) if len(sys.argv) > 2 else 5
state_dir = Path.home() / ".ray-vast"
key_file = os.environ.get("RAYVAST_KEY_FILE", str(Path.home() / ".ray-vast" / cluster / "ssh_key.pem"))
ec2 = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "personal"), region_name="us-west-2").client("ec2")
while True:
    row = {"utc": vast_tool.utc()}
    try:
        found = [i for i in vast_tool.instances() if i.get("label") == label(cluster)]
        row["instances"] = [{k: i.get(k) for k in ("id", "actual_status", "dph_total", "machine_id")} for i in found]
        row["dph_total_sum"] = round(sum(i.get("dph_total") or 0 for i in found), 4)
    except SystemExit as e:
        row["vast_error"] = str(e)
    try:
        groups = ec2.describe_security_groups(Filters=[
            {"Name": "group-name", "Values": [f"ray-vast-{cluster}-{i}" for i in range(5)]},
            {"Name": "tag:ray-vast-cluster", "Values": [cluster]}])["SecurityGroups"]
        row["worker_rules"] = sum(1 for g in groups for p in g["IpPermissions"] for r in p.get("IpRanges", [])
                                  if r.get("Description") == label(cluster))
        row["groups"] = len(groups)
    except Exception as e:
        row["aws_error"] = type(e).__name__
    state_file = state_dir / f"{cluster}.json"
    if state_file.exists():
        head_ip = json.loads(state_file.read_text())["head_ip"]
        try:
            done = subprocess.run(
                ["ssh", "-i", key_file, "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=accept-new",
                 "-o", f"UserKnownHostsFile={state_dir / (cluster + '.known_hosts')}", f"ubuntu@{head_ip}",
                 "sudo docker exec ray-vast-head ray list nodes --format json"], capture_output=True, text=True, timeout=30)
            nodes = json.loads(done.stdout) if done.returncode == 0 else None
            row["ray_nodes"] = [{"id": n["node_id"][:8], "ip": n["node_ip"], "state": n["state"], "head": n["is_head_node"],
                                 "type": (n.get("labels") or {}).get("ray.io/node-group")} for n in nodes] if nodes is not None \
                else f"ssh exit {done.returncode}"
        except (subprocess.TimeoutExpired, ValueError, KeyError) as e:
            row["ray_nodes"] = type(e).__name__
    print(json.dumps(row), flush=True)
    time.sleep(period)
