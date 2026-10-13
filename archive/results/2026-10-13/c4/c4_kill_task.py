# C4.1.3: from a Ray task pinned to W-kill, try to kill the watchdog and PID 1. Run on the head as a driver.
import json, sys
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

ray.init()
node_id, wd_pid = sys.argv[1], int(sys.argv[2])


@ray.remote(num_cpus=1)
def try_kills(wd_pid):
    import os, subprocess
    out = {"uid": os.getuid(), "euid": os.geteuid(), "pid": os.getpid(), "node_ip": ray.util.get_node_ip_address()}
    for name, pid in (("watchdog", wd_pid), ("pid1", 1)):
        try:
            os.kill(pid, 9)
            out[name] = "NO ERROR (signal sent)"
        except Exception as e:
            out[name] = repr(e)
    r = subprocess.run(["sudo", "-n", "kill", "-9", str(wd_pid)], capture_output=True, text=True)
    out["sudo_kill"] = f"rc={r.returncode} {r.stderr.strip()}"
    try:
        open("/proc/1/environ").read()
        out["proc1_environ"] = "READABLE"
    except Exception as e:
        out["proc1_environ"] = repr(e)
    out["watchdog_alive"] = os.path.exists(f"/proc/{wd_pid}")
    out["ps"] = subprocess.run(["ps", "-o", "pid,ppid,user,args", "-p", f"1,{wd_pid}"], capture_output=True, text=True).stdout
    return out


print(json.dumps(ray.get(try_kills.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False)).remote(wd_pid)), indent=1))
