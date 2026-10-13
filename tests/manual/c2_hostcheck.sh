#!/usr/bin/env bash
# C2 setup step 4, head side: run the host checks on one candidate and connect to its identity ports.
# Run on the test EC2 host. Usage: bash c2_hostcheck.sh <instance_id> <public_ip> <ssh_port>
set -u
ID=$1; IP=$2; SSHP=$3
OUT=/opt/rayvast-test/results/c2/hostcheck_$ID
mkdir -p /opt/rayvast-test/results/c2
SSH="ssh -i $HOME/.ssh/rayvast_head -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$HOME/.ssh/known_hosts_rayvast -o ConnectTimeout=10 -p $SSHP ray@$IP"
$SSH 'python3 - ' < /opt/rayvast-test/tests/c2_hostcheck_worker.py > $OUT.json 2>$OUT.err &
for i in $(seq 1 40); do [ -s $OUT.json ] && break; sleep 1; done
python3 - $OUT.json $IP <<'PY'
import json, socket, sys
r = json.loads(open(sys.argv[1]).readline())
ok, bad = 0, []
for req, p in r["ports"].items():
    if req == "70040":
        continue
    try:
        c = socket.create_connection((sys.argv[2], p), timeout=5)
        ok += c.recv(16) == b"rayvast-ok\n"
        c.close()
    except OSError as e:
        bad.append((req, p, repr(e)))
r["head_connect_ok"] = ok
r["head_connect_fail"] = bad
print(json.dumps({k: v for k, v in r.items() if k != "ports"}))
PY
wait
