# C5.6 evidence: how many GPUs does the container have, and how many does Ray advertise?
import glob, json, os, subprocess
import ray

@ray.remote(num_gpus=4, num_cpus=1)
def probe():
    smi = subprocess.run(["nvidia-smi", "-L"], capture_output=True, text=True) if any(
        os.access(os.path.join(p, "nvidia-smi"), os.X_OK) for p in os.environ.get("PATH", "").split(":")) else None
    return {"proc_driver_nvidia_gpus": sorted(glob.glob("/proc/driver/nvidia/gpus/*")),
            "dev_nvidia": sorted(glob.glob("/dev/nvidia*")),
            "nvidia_smi_L": smi.stdout.strip().splitlines() if smi else "no nvidia-smi in PATH",
            "CUDA_VISIBLE_DEVICES": os.environ.get("CUDA_VISIBLE_DEVICES"),
            "NVIDIA_VISIBLE_DEVICES": os.environ.get("NVIDIA_VISIBLE_DEVICES"),
            "ray_get_gpu_ids": ray.get_gpu_ids(), "container_id": os.environ.get("CONTAINER_ID")}

ray.init()
print(json.dumps({"worker_nodes": [{"resources": n["Resources"]} for n in ray.nodes() if n["Alive"] and n["Resources"].get("CPU")]}))
print(json.dumps(ray.get(probe.remote(), timeout=120)))
