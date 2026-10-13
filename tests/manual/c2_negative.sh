#!/usr/bin/env bash
# C2.3: negative TLS and token checks from the head HOST (outside the container: no certs, no token).
# Run on the test EC2 host. Usage: bash c2_negative.sh <public_ip> <ssh_port>
set -u
IP=$1; SSHP=$2
SSH="ssh -i $HOME/.ssh/rayvast_head -o UserKnownHostsFile=$HOME/.ssh/known_hosts_rayvast -o ConnectTimeout=10 -p $SSHP ray@$IP"
eval "$($SSH 'grep -E "^VAST_TCP_PORT_7000[0-5]=" /etc/rayvast_vast.env')"
R=/opt/rayvast-test/results/c2/rogue
mkdir -p $R
[ -f $R/rogue.crt ] || { openssl req -x509 -newkey rsa:2048 -days 1 -nodes -subj "/CN=rogue-ca" -keyout $R/rogue-ca.key -out $R/rogue-ca.crt 2>/dev/null
  openssl req -newkey rsa:2048 -nodes -subj "/CN=rogue" -keyout $R/rogue.key -out $R/rogue.csr 2>/dev/null
  openssl x509 -req -in $R/rogue.csr -CA $R/rogue-ca.crt -CAkey $R/rogue-ca.key -CAcreateserial -days 1 -out $R/rogue.crt 2>/dev/null; }
echo "== 1. plaintext HTTP/2 (gRPC) to raylet $IP:$VAST_TCP_PORT_70000"
curl -s -m 10 --http2-prior-knowledge -o /dev/null -w 'http_code=%{http_code}\n' http://$IP:$VAST_TCP_PORT_70000/ray.rpc.NodeManagerService/GetNodeStats; echo "curl rc=$?"
echo "== 2a. TLS, no client cert"
echo | timeout 15 openssl s_client -connect $IP:$VAST_TCP_PORT_70000 -alpn h2 2>&1 | grep -E "Verify return|alert|Cipher is|error|CONNECTED" | head -5
echo | timeout 15 openssl s_client -connect $IP:$VAST_TCP_PORT_70000 -alpn h2 -ign_eof 2>&1 >/dev/null | head -3
echo "== 2b. TLS with a client cert from an unknown CA"
(sleep 3; echo) | timeout 15 openssl s_client -connect $IP:$VAST_TCP_PORT_70000 -alpn h2 -cert $R/rogue.crt -key $R/rogue.key 2>&1 | grep -E "alert|Cipher is|error|CONNECTED|closed" | head -5
echo "== 3. runtime-env agent $IP:$VAST_TCP_PORT_70005"
curl -s -m 10 -o /dev/null -w 'no token: %{http_code}\n' http://$IP:$VAST_TCP_PORT_70005/ || echo "no token: curl rc=$?"
curl -s -m 10 -o /dev/null -w 'wrong token: %{http_code}\n' -H "Authorization: Bearer $(openssl rand -hex 32)" http://$IP:$VAST_TCP_PORT_70005/ || echo "wrong token: curl rc=$?"
echo "== 4. job port $IP:$VAST_TCP_PORT_70004 (expect refused)"
curl -s -m 10 -o /dev/null -w '%{http_code}\n' http://$IP:$VAST_TCP_PORT_70004/api/jobs/; echo "curl rc=$? (7 = connection refused)"
echo "== 5. job port on the worker's 127.0.0.1 (python, over SSH)"
$SSH "/home/ray/anaconda3/bin/python /opt/rayvast/battery_worker_check.py $VAST_TCP_PORT_70004"
echo "== 6. metrics $IP:$VAST_TCP_PORT_70002"
curl -s -m 10 -o /tmp/metrics.$$ -w 'metrics: %{http_code}\n' http://$IP:$VAST_TCP_PORT_70002/metrics; echo "lines=$(wc -l < /tmp/metrics.$$) ray_ lines=$(grep -c '^ray_' /tmp/metrics.$$)"; rm -f /tmp/metrics.$$
echo "== extra: dashboard-agent gRPC $IP:$VAST_TCP_PORT_70003 and object manager $IP:$VAST_TCP_PORT_70001 plaintext"
for p in $VAST_TCP_PORT_70001 $VAST_TCP_PORT_70003; do curl -s -m 10 --http2-prior-knowledge -o /dev/null -w "$p http_code=%{http_code}\n" http://$IP:$p/; echo "rc=$?"; done
