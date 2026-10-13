# C2 setup step 4, worker side: host checks inside a vast candidate. Run over SSH as user ray.
# Prints one JSON line with the local results, then keeps a listener on every identity port
# (except sshd) for 90 s, so that the head can connect to them (c2_hostcheck.sh).
import json
import os
import platform
import socket
import subprocess
import sys
import threading
import time
import urllib.request

vast = dict(line.strip().split("=", 1) for line in open("/etc/rayvast_vast.env") if "=" in line)
public_ip = vast["PUBLIC_IPADDR"].strip()
ports = {int(k.rsplit("_", 1)[1]): int(v) for k, v in vast.items() if k.startswith("VAST_TCP_PORT_")}
r = {"public_ip": public_ip, "uname_m": platform.machine(), "identity_mapping": sorted(ports) == list(range(70000, 70064))}
r["ray_python"] = subprocess.run([sys.executable, "-c", "import ray,sys; print(ray.__version__, sys.version.split()[0])"],
                                 capture_output=True, text=True).stdout.strip()
r["boot"] = {f: open(f"/tmp/rayvast_{f}").read().strip() for f in ("boot_start", "sshd_installed", "sshd_started")
             if os.path.exists(f"/tmp/rayvast_{f}")}

listeners = []
for req, p in sorted(ports.items()):
    if req == 70040:
        continue
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", p))
    s.listen(16)
    listeners.append(s)


def serve(s):
    while True:
        try:
            c, _ = s.accept()
            c.sendall(b"rayvast-ok\n")
            c.close()
        except OSError:
            return


for s in listeners:
    threading.Thread(target=serve, args=(s,), daemon=True).start()
r["listeners"] = len(listeners)

try:
    r["egress_ip"] = urllib.request.urlopen("https://api.ipify.org", timeout=10).read().decode().strip()
except Exception as e:
    r["egress_ip"] = f"error {e!r}"
r["egress_ok"] = r["egress_ip"] == public_ip

try:
    c = socket.create_connection((public_ip, ports[70006]), timeout=5)
    r["hairpin"] = c.recv(16) == b"rayvast-ok\n"
    c.close()
except OSError as e:
    r["hairpin"] = False
    r["hairpin_error"] = repr(e)

cap = int([l for l in open("/proc/self/status") if l.startswith("CapEff")][0].split()[1], 16)
r["capeff"] = hex(cap)
r["net_admin"] = bool(cap >> 12 & 1)
r["net_raw"] = bool(cap >> 13 & 1)
r["hostname_I"] = subprocess.run(["hostname", "-I"], capture_output=True, text=True).stdout.split()
r["own_network"] = any(ip.startswith(("10.", "192.168.", "172.")) and ip != public_ip for ip in r["hostname_I"])
r["dev_shm"] = os.listdir("/dev/shm")
r["nproc"] = os.cpu_count()
r["ports"] = {str(k): v for k, v in sorted(ports.items())}
print(json.dumps(r), flush=True)
time.sleep(90)
