# C9.4.4: a Serve deployment with ray_actor_options={"num_gpus": 1}: one replica on each GPU worker, a torch matmul on CUDA.
# Needs torch (submit with run_c9.sh TORCH_ENV). At the end it shuts Serve down (also the app "c9" of C9.4.1).
# PASS: 20 calls answer, from replicas on >= C9_MIN_NODES nodes, each with 1 GPU id and CUDA.
import collections

from ray import serve

from c9common import GPU, MIN_NODES, error_info, node_info, result, short, start, wait_for_nodes

ID = "C9.4.4"
start(ID, 2400)
wait_for_nodes(ID, gpus_per_node=GPU)


@serve.deployment(num_replicas=MIN_NODES, max_replicas_per_node=1 if MIN_NODES >= 2 else None,
                  ray_actor_options={"num_gpus": GPU, "num_cpus": 1})
class GpuModel:
    def __init__(self):
        import torch
        self.torch = torch
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.weights = torch.randn(1024, 1024, device=self.device)

    def __call__(self, request=None):
        import ray
        value = float((self.weights @ self.weights).sum().item())
        return dict(node_info(), device=self.device, gpu_ids=[str(g) for g in ray.get_gpu_ids()], finite=value == value)


try:
    handle = serve.run(GpuModel.bind(), name="c9gpu", route_prefix="/gpu")
    answers = [handle.remote().result(timeout_s=120) for _ in range(20)]
except Exception as e:
    serve.shutdown()
    result(ID, "FAIL", error_info(e))
serve.shutdown()
data = {"answers": len(answers), "per_node": dict(collections.Counter(short(a["node_id"]) for a in answers)),
        "devices": sorted({a["device"] for a in answers}), "gpu_ids": sorted({",".join(a["gpu_ids"]) for a in answers})}
ok = len(answers) == 20 and len(data["per_node"]) >= MIN_NODES and data["devices"] == ["cuda"] and all(
    len(a["gpu_ids"]) == 1 for a in answers)
result(ID, "PASS" if ok else "FAIL", data)
