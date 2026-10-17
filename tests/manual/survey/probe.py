# Host survey probe (docs/spikes.md, "S6 — host survey"). Runs as the container command of a vast instance. Stdlib only.
# It reads the container limits and the network facts, prints ONE line "RAYVAST_SURVEY_PROBE <json>" to stdout, and serves
# that JSON on every identity port (GET /result) for the operator. GET /done (or SURVEY_WAIT_S seconds) makes it destroy
# its own instance with CONTAINER_API_KEY (as the bootstrap does). It never prints an env value except PUBLIC_IPADDR,
# CONTAINER_ID and the VAST_TCP_PORT_* numbers. CONTAINER_API_KEY stays in memory only.
import json
import os
import resource
import selectors
import socket
import threading
import time
import urllib.error
import urllib.request

T0 = time.time()
KEY = os.environ.pop("CONTAINER_API_KEY", "")
CAPS = ["CHOWN", "DAC_OVERRIDE", "DAC_READ_SEARCH", "FOWNER", "FSETID", "KILL", "SETGID", "SETUID", "SETPCAP",
        "LINUX_IMMUTABLE", "NET_BIND_SERVICE", "NET_BROADCAST", "NET_ADMIN", "NET_RAW", "IPC_LOCK", "IPC_OWNER",
        "SYS_MODULE", "SYS_RAWIO", "SYS_CHROOT", "SYS_PTRACE", "SYS_PACCT", "SYS_ADMIN", "SYS_BOOT", "SYS_NICE",
        "SYS_RESOURCE", "SYS_TIME", "SYS_TTY_CONFIG", "MKNOD", "LEASE", "AUDIT_WRITE", "AUDIT_CONTROL", "SETFCAP",
        "MAC_OVERRIDE", "MAC_ADMIN", "SYSLOG", "WAKE_ALARM", "BLOCK_SUSPEND", "AUDIT_READ", "PERFMON", "BPF",
        "CHECKPOINT_RESTORE"]


def read(path, limit=4000):
    try:
        with open(path) as f:
            return f.read(limit).strip()
    except (OSError, UnicodeDecodeError):
        return None


def num(text):
    try:
        return int(text)
    except (TypeError, ValueError):
        return text  # "max" or None


def http_get(url, timeout=5):
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(url, timeout=timeout) as r:
            return r.read(200).decode(errors="replace").strip()
    except Exception as e:
        return f"ERR {type(e).__name__}"


env = os.environ
r = {"t_start": T0, "container_id": env.get("CONTAINER_ID"), "public_ipaddr": (env.get("PUBLIC_IPADDR") or "").strip(),
     "uid": os.getuid(), "euid": os.geteuid(), "kernel": os.uname().release, "env_names": sorted(env)}

# cgroup: version, pids, CPU quota, memory, cpuset.
cg = "/sys/fs/cgroup"
v2 = os.path.exists(f"{cg}/cgroup.controllers")
r["cgroup"] = "v2" if v2 else "v1"
pids = cg if v2 else f"{cg}/pids"
r["pids_max"] = num(read(f"{pids}/pids.max"))
r["pids_current"] = num(read(f"{pids}/pids.current"))
r["pids_peak"] = num(read(f"{pids}/pids.peak"))
r["pids_events"] = read(f"{pids}/pids.events")
if v2:
    cpu_max = read(f"{cg}/cpu.max")
    r["cpu_max_raw"] = cpu_max
    quota = None
    if cpu_max and cpu_max.split()[0] != "max":
        q, p = cpu_max.split()
        quota = int(q) / int(p)
    r["memory_max"] = num(read(f"{cg}/memory.max"))
    r["cpuset"] = read(f"{cg}/cpuset.cpus.effective")
    r["cpu_weight"] = num(read(f"{cg}/cpu.weight"))
else:
    cpu_dir = next((d for d in (f"{cg}/cpu", f"{cg}/cpu,cpuacct", f"{cg}/cpuacct,cpu") if os.path.exists(d)), None)
    q, p = (num(read(f"{cpu_dir}/cpu.cfs_quota_us")), num(read(f"{cpu_dir}/cpu.cfs_period_us"))) if cpu_dir else (None, None)
    r["cpu_max_raw"] = f"{q} {p}"
    quota = q / p if isinstance(q, int) and isinstance(p, int) and q > 0 and p > 0 else None
    r["memory_max"] = num(read(f"{cg}/memory/memory.limit_in_bytes"))
    r["cpuset"] = read(f"{cg}/cpuset/cpuset.cpus")
    r["cpu_weight"] = num(read(f"{cpu_dir}/cpu.shares")) if cpu_dir else None
r["cpu_quota"] = quota
r["pids_per_quota"] = r["pids_max"] / quota if quota and isinstance(r["pids_max"], int) else None
r["os_cpu_count"] = os.cpu_count()
r["affinity"] = len(os.sched_getaffinity(0))
r["self_cgroup"] = (read("/proc/self/cgroup") or "")[:400]
r["threads_max"] = num(read("/proc/sys/kernel/threads-max"))
r["pid_max"] = num(read("/proc/sys/kernel/pid_max"))
meminfo = read("/proc/meminfo") or ""
r["mem_total_kb"] = next((int(l.split()[1]) for l in meminfo.splitlines() if l.startswith("MemTotal:")), None)
cpuinfo = read("/proc/cpuinfo", 20000) or ""
r["cpu_model"] = next((l.split(":", 1)[1].strip() for l in cpuinfo.splitlines() if l.startswith("model name")), None)
try:
    st = os.statvfs("/dev/shm")
    r["shm_bytes"] = st.f_blocks * st.f_frsize
    r["shm_entries"] = len(os.listdir("/dev/shm"))
except OSError:
    r["shm_bytes"] = None
r["rlimit_nproc"] = resource.getrlimit(resource.RLIMIT_NPROC)
r["rlimit_nofile"] = resource.getrlimit(resource.RLIMIT_NOFILE)

# Capabilities, seccomp, namespaces, runtime hints.
status = dict(l.split(":\t", 1) for l in (read("/proc/self/status", 20000) or "").splitlines() if ":\t" in l)
for field in ("CapEff", "CapBnd", "CapPrm"):
    value = int(status.get(field, "0").strip(), 16)
    r[field] = f"{value:x}"
    r[field + "_names"] = [c for i, c in enumerate(CAPS) if value >> i & 1]
r["seccomp"] = num(status.get("Seccomp", "").strip())
r["no_new_privs"] = num(status.get("NoNewPrivs", "").strip())
r["uid_map"] = read("/proc/self/uid_map")
r["netns_inode"] = os.stat("/proc/self/ns/net").st_ino
r["own_netns"] = r["netns_inode"] not in (0xEFFFFFF9, 0xF0000000)
r["pidns_inode"] = os.stat("/proc/self/ns/pid").st_ino
r["dockerenv"] = os.path.exists("/.dockerenv")
r["containerenv"] = os.path.exists("/run/.containerenv")
mounts = read("/proc/self/mountinfo", 200000) or ""
r["root_fstype"] = next((l.split(" - ")[1].split()[0] for l in mounts.splitlines() if l.split()[4] == "/" and " - " in l), None)
devs = os.listdir("/dev")
r["nvidia_devs"] = sum(1 for n in devs if n.startswith("nvidia") and n[6:].isdigit())
r["fuse"] = "fuse" in devs
r["kvm"] = "kvm" in devs

# Network: interfaces, MTU, IPv6, egress IP.
ifaces = {}
for name in os.listdir("/sys/class/net"):
    ifaces[name] = {"mtu": num(read(f"/sys/class/net/{name}/mtu")), "state": read(f"/sys/class/net/{name}/operstate")}
r["ifaces"] = ifaces
v6 = [l.split() for l in (read("/proc/net/if_inet6") or "").splitlines()]
r["ipv6_global"] = sum(1 for l in v6 if len(l) > 3 and l[3] == "00")
r["ipv6_any"] = len(v6)
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("1.1.1.1", 53))
    r["local_ip"] = s.getsockname()[0]
    s.close()
except OSError as e:
    r["local_ip"] = f"ERR {e.errno}"
r["egress_ipify"] = http_get("https://api.ipify.org")
r["egress_ifconfig"] = http_get("https://ifconfig.me/ip")
r["egress_ipv6"] = http_get("https://api6.ipify.org", timeout=4) if r["ipv6_global"] else None
r["egress_ok"] = r["egress_ipify"] == r["public_ipaddr"] or r["egress_ifconfig"] == r["public_ipaddr"]
r["egress_services_agree"] = r["egress_ipify"] == r["egress_ifconfig"]

# Identity ports: VAST_TCP_PORT_<internal> = host port. Listen on each host port P (identity mapping P->P).
mapping = {k[len("VAST_TCP_PORT_"):]: num(v) for k, v in env.items() if k.startswith("VAST_TCP_PORT_")}
r["port_env_count"] = len(mapping)
r["ports"] = mapping
r["udp_port_env_count"] = sum(1 for k in env if k.startswith("VAST_UDP_PORT_"))
listeners, bind_errors = [], 0
for internal, port in sorted(mapping.items()):
    if not isinstance(port, int):
        continue
    try:
        ls = socket.socket()
        ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ls.bind(("0.0.0.0", port))
        ls.listen(64)
        ls.setblocking(False)
        listeners.append(ls)
    except OSError:
        bind_errors += 1
r["listen_ok"] = len(listeners)
r["listen_errors"] = bind_errors

done = threading.Event()
result_bytes = [b"{}"]


def serve():
    sel = selectors.DefaultSelector()
    for ls in listeners:
        sel.register(ls, selectors.EVENT_READ)
    while True:
        for key, _ in sel.select(timeout=1):
            try:
                conn, _ = key.fileobj.accept()
            except OSError:
                continue
            try:
                conn.settimeout(3)
                line = conn.recv(1024).split(b"\r\n", 1)[0].decode(errors="replace").split()
                path = line[1] if len(line) > 1 else ""
                if path.startswith("/h/"):
                    body = path[3:].encode()
                elif path == "/result":
                    body = result_bytes[0][:-1] + b', "served_at": %.3f}' % time.time()
                elif path == "/done":
                    body = b"ok"
                    done.set()
                else:
                    body = b"rayvast-survey"
                conn.sendall(b"HTTP/1.0 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n" % len(body) + body)
            except OSError:
                pass
            finally:
                conn.close()


if listeners:
    threading.Thread(target=serve, daemon=True).start()

# Hairpin: connect from inside to PUBLIC_IPADDR:P (our own listener) and to local_ip:P (control).
def echo(ip, port, nonce):
    try:
        with socket.create_connection((ip, port), timeout=5) as c:
            c.settimeout(5)
            c.sendall(f"GET /h/{nonce} HTTP/1.0\r\n\r\n".encode())
            data = b""
            while True:
                chunk = c.recv(4096)
                if not chunk:
                    break
                data += chunk
        return data.endswith(nonce.encode())
    except OSError as e:
        return f"ERR {type(e).__name__} {e.errno}"


ports = [ls.getsockname()[1] for ls in listeners]
if ports:
    nonce = os.urandom(6).hex()
    r["hairpin"] = [echo(r["public_ipaddr"], p, nonce) for p in (ports[0], ports[-1])]
    r["hairpin_ok"] = any(x is True for x in r["hairpin"])
    r["local_echo_ok"] = echo(r["local_ip"], ports[0], nonce) is True if not str(r["local_ip"]).startswith("ERR") else None
else:
    r["hairpin"], r["hairpin_ok"], r["local_echo_ok"] = None, None, None

r["t_probe"] = time.time()
r["probe_s"] = round(r["t_probe"] - T0, 2)
r["has_api_key"] = bool(KEY)
line = json.dumps(r, sort_keys=True)
result_bytes[0] = line.encode()
print("RAYVAST_SURVEY_PROBE " + line, flush=True)
# The vast log keeps only the first 500 characters of a line, so the same JSON also goes out in parts of 400,
# each ending in "|".
parts = [line[i:i + 400] for i in range(0, len(line), 400)]
for k, part in enumerate(parts):
    print(f"RAYVAST_SURVEY_PART {k} {len(parts)} {part}|", flush=True)  # "|" keeps a trailing space

# Wait for the operator (GET /done) or the deadline, then destroy this instance.
done.wait(float(env.get("SURVEY_WAIT_S", "600")))
time.sleep(1)
url = f"https://console.vast.ai/api/v0/instances/{env.get('CONTAINER_ID')}/"
for attempt in range(60):
    try:
        req = urllib.request.Request(url, method="DELETE", headers={"Authorization": f"Bearer {KEY}"})
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req, timeout=30) as resp:
            print(f"RAYVAST_SURVEY_DESTROY HTTP {resp.status}", flush=True)
            break
    except urllib.error.HTTPError as e:
        print(f"RAYVAST_SURVEY_DESTROY HTTP {e.code}", flush=True)
        if e.code == 404:
            break
    except Exception as e:
        print(f"RAYVAST_SURVEY_DESTROY {type(e).__name__}", flush=True)
    time.sleep(5)
while True:
    time.sleep(3600)
