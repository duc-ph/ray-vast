#!/usr/bin/env bash
# C0.1: shim unit test with a fake public IP (203.0.113.10, TEST-NET-3).
# Run on the test EC2 host (not in a container). Output goes to stdout.
set -u
SHIM=/opt/rayvast-test/shim/librayvast_shim.so
LOG=/opt/rayvast-test/results/c0_1_shim.log
rm -f "$LOG"
export RAYVAST_PUBLIC_IP=203.0.113.10 RAYVAST_PRIVATE_IP=127.0.0.1 RAYVAST_SHIM_LOG=$LOG

echo "== step 1: bind 203.0.113.10:5000 without the shim"
python3 -c "import socket; s=socket.socket(); s.bind(('203.0.113.10', 5000))" 2>&1 | tail -1

echo "== step 2: same bind with the shim, then listen and keep it open"
LD_PRELOAD=$SHIM python3 -c "
import socket, time
s = socket.socket(); s.bind(('203.0.113.10', 5000)); s.listen()
print('bound to', s.getsockname(), flush=True); time.sleep(30)" &
P2=$!
sleep 1
ss -ltn 'sport = :5000'
echo "shim log bind lines: $(grep -c ' bind ' "$LOG")"
cat "$LOG"
kill $P2; wait $P2 2>/dev/null

echo "== step 3: http.server --bind 203.0.113.10 and curl, both with the shim"
: > "$LOG"
LD_PRELOAD=$SHIM python3 -m http.server 5000 --bind 203.0.113.10 >/dev/null 2>&1 &
P3=$!
sleep 1
echo "curl code: $(LD_PRELOAD=$SHIM curl -s -o /dev/null -w '%{http_code}' http://203.0.113.10:5000/)"
echo "shim log connect lines: $(grep -c ' connect ' "$LOG")"
cat "$LOG"
kill $P3; wait $P3 2>/dev/null

echo "== step 4: RAYVAST_LOCALHOST_PORTS=5001, bind 0.0.0.0:5001 with the shim"
: > "$LOG"
RAYVAST_LOCALHOST_PORTS=5001 LD_PRELOAD=$SHIM python3 -c "
import socket, time
s = socket.socket(); s.bind(('0.0.0.0', 5001)); s.listen()
print('bound to', s.getsockname(), flush=True); time.sleep(30)" &
P4=$!
sleep 1
ss -ltn 'sport = :5001'
cat "$LOG"
kill $P4; wait $P4 2>/dev/null

echo "== extra a: errno passes through (connect to a rewritten port with no listener)"
: > "$LOG"
LD_PRELOAD=$SHIM python3 -c "
import socket
try: socket.create_connection(('203.0.113.10', 5002), timeout=3)
except OSError as e: print(type(e).__name__, e.errno, e)"
cat "$LOG"

echo "== extra b: IPv4-mapped IPv6 (the form that gRPC dual-stack sockets use)"
: > "$LOG"
LD_PRELOAD=$SHIM python3 -c "
import socket, time
s = socket.socket(socket.AF_INET6); s.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
s.bind(('::ffff:203.0.113.10', 5003)); s.listen()
c = socket.socket(socket.AF_INET6); c.connect(('::ffff:203.0.113.10', 5003))
print('v6 server', s.getsockname()[:2], 'client peer', c.getpeername()[:2])
l = socket.socket(socket.AF_INET6); l.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
l.bind(('::', 5004)); print('v6 any', l.getsockname()[:2])"
cat "$LOG"

echo "== extra c: unrelated addresses pass through unchanged (no log lines)"
: > "$LOG"
LD_PRELOAD=$SHIM curl -s -o /dev/null -w 'checkip %{http_code}\n' https://checkip.amazonaws.com
echo "shim log lines: $(wc -l < "$LOG")"

echo "== extra d: Ray-agent pattern: bind node IP then 127.0.0.1 on a LOCALHOST_PORTS port (shim v2: SO_REUSEPORT)"
: > "$LOG"
RAYVAST_LOCALHOST_PORTS=5005 LD_PRELOAD=$SHIM python3 -c "
import socket
a = socket.socket(); a.bind(('203.0.113.10', 5005)); a.listen()
b = socket.socket(); b.bind(('127.0.0.1', 5005)); b.listen()
print('both bound:', a.getsockname(), b.getsockname())"
cat "$LOG"

echo "== extra e: log after setproctitle with a long title (ray._raylet.setproctitle, as Ray uses); in the Ray image"
: > "$LOG"
docker run --rm --network host -u 1000 -v /opt/rayvast-test:/opt/rayvast-test -e LD_PRELOAD=$SHIM -e RAYVAST_SHIM_LOG=$LOG \
  -e RAYVAST_PUBLIC_IP=203.0.113.10 -e RAYVAST_PRIVATE_IP=127.0.0.1 rayproject/ray:2.59.0-py311-cpu python -c "
import socket, ray._raylet
ray._raylet.setproctitle('ray::SomeVeryLongProcessTitle ' + 'x' * 3000)
s = socket.socket(); s.bind(('203.0.113.10', 5006)); print('bound to', s.getsockname())"
echo "shim log lines after setproctitle: $(wc -l < "$LOG")"
cat "$LOG"
