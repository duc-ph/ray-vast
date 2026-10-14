# C8 re-run extra check (after C8.2): PID 1 and the process tree on one -gpu worker, behind the NVIDIA ENTRYPOINT.
import glob, json, os, subprocess
import ray

@ray.remote(num_gpus=1, num_cpus=1)
def probe():
    def cmd(pid):
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return f.read().replace(b"\0", b" ").decode(errors="replace").strip()
    def ppid(pid):
        with open(f"/proc/{pid}/stat") as f:
            return int(f.read().rsplit(")", 1)[1].split()[1])
    pid1 = cmd(1)
    children = [{"pid": int(p), "cmd": cmd(p)[:120]} for p in filter(str.isdigit, os.listdir("/proc")) if os.path.exists(f"/proc/{p}/stat") and ppid(p) == 1]
    smi = subprocess.run(["nvidia-smi", "-L"], capture_output=True, text=True).stdout.strip().splitlines()
    return {"pid1_cmdline": pid1, "pid1_is_bootstrap": "bootstrap" in pid1, "pid1_children": children,
            "dev_nvidia": sorted(glob.glob("/dev/nvidia[0-9]*")), "nvidia_smi_L": smi,
            "ray_get_gpu_ids": ray.get_gpu_ids(), "CUDA_VISIBLE_DEVICES": os.environ.get("CUDA_VISIBLE_DEVICES"),
            "container_id": os.environ.get("CONTAINER_ID"), "node_type": os.environ.get("RAY_NODE_TYPE_NAME"), "uid": os.getuid()}

ray.init()
print(json.dumps(ray.get(probe.remote(), timeout=900)), flush=True)
