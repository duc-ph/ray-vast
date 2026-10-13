#!/usr/bin/env bash
# C2.4: plain HTTP hop (T4). Run on the test EC2 host while the battery runs with a new pip package.
# Captures the head's agent ports on all interfaces, and all non-SSH TCP on ens5, then searches both for the token.
# On each worker it samples `ss -tnp` once per second for the runtime-env agent port P(70005).
set -u
R=/opt/rayvast-test/results/c2
TOKEN=/opt/rayvast-test/secrets/token
sudo tcpdump -i any -s0 -w $R/head-hop.pcap 'tcp port 6385 or tcp port 6384' 2>$R/head-hop.err &
sudo tcpdump -i ens5 -s0 -w $R/head-ens5.pcap 'tcp and not port 22' 2>$R/head-ens5.err &
for w in "whp 143.55.45.86 42778" "wnohp 185.62.108.226 45809"; do set -- $w
  ssh -i ~/.ssh/rayvast_head -o UserKnownHostsFile=~/.ssh/known_hosts_rayvast -p $3 ray@$2 \
    '. /etc/rayvast_vast.env; for i in $(seq 1 150); do date -u +%T; ss -tnpH | grep -E ":$VAST_TCP_PORT_70005\b"; sleep 1; done' > $R/c2_4_ss_$1.txt 2>&1 &
done
sleep 3
docker exec rayvast-head python /opt/rayvast-test/tests/battery.py 143.55.45.86 185.62.108.226 humanize==4.10.0 2>&1 | grep -E "^step3|BATTERY" | cut -c1-400
wait_ss=$(jobs -p)
sleep 5
sudo pkill -INT -f "tcpdump -i any -s0 -w $R/head-hop.pcap"
sudo pkill -INT -f "tcpdump -i ens5 -s0 -w $R/head-ens5.pcap"
sleep 2
sudo chown ubuntu $R/*.pcap
echo "== head-hop.pcap: $(sudo tcpdump -r $R/head-hop.pcap -nn 2>/dev/null | wc -l) packets; by interface:"
sudo tcpdump -r $R/head-hop.pcap -nn 2>/dev/null | awk '{print $2, $3}' | sort | uniq -c
echo "== token matches by interface (tcpdump -A, per packet):"
sudo tcpdump -r $R/head-hop.pcap -nn -A 2>/dev/null | awk -v tok="$(cat $TOKEN)" '/^[0-9][0-9]:[0-9][0-9]:/{ifc=$2} index($0, tok){print ifc}' | sort | uniq -c
echo "== raw token matches: head-hop.pcap $(grep -a -c -F -f $TOKEN $R/head-hop.pcap), head-ens5.pcap $(grep -a -c -F -f $TOKEN $R/head-ens5.pcap) ($(du -h $R/head-ens5.pcap | cut -f1))"
echo "== 'Bearer' on ens5: $(grep -a -c 'Bearer' $R/head-ens5.pcap)"
