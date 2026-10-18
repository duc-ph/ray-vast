# runtime_env pip installs torch on the workers; CUDA works on each GPU worker.
# Submit with --runtime-env-json (pairs.TORCH_ENV). One task per GPU node (placement group STRICT_SPREAD).
# PASS: on each node torch.cuda.is_available() is True, device_count() == 1, and a matmul on the GPU works.
import ray
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from jobcommon import GPU, MIN_NODES, error_info, node_info, result, start, wait_for_nodes

ID = "torch_cuda"
start(ID, 2400)
wait_for_nodes(ID, gpus_per_node=GPU)


@ray.remote(num_gpus=GPU, num_cpus=1)
def cuda_probe():
    import time
    import torch
    out = dict(node_info(), torch=torch.__version__, torch_cuda=torch.version.cuda, available=torch.cuda.is_available(),
               count=torch.cuda.device_count(), gpu_ids=ray.get_gpu_ids())
    if out["available"]:
        out["name"] = torch.cuda.get_device_name(0)
        t = time.time()
        a = torch.randn(4096, 4096, device="cuda")
        out["matmul_sum_finite"] = bool(torch.isfinite((a @ a).sum()).item())
        torch.cuda.synchronize()
        out["matmul_s"] = round(time.time() - t, 3)
    return out


try:
    group = placement_group([{"CPU": 1, **({"GPU": GPU} if GPU else {})}] * MIN_NODES, strategy="STRICT_SPREAD")
    ray.get(group.ready(), timeout=600)
    rows = ray.get([cuda_probe.options(scheduling_strategy=PlacementGroupSchedulingStrategy(group, placement_group_bundle_index=i))
                    .remote() for i in range(MIN_NODES)], timeout=1800)
except Exception as e:
    result(ID, "FAIL", error_info(e))
for row in rows:
    print(row, flush=True)
ok = len({r["node_id"] for r in rows}) == MIN_NODES and all(r["available"] and r["count"] == 1 and r.get("matmul_sum_finite") for r in rows)
result(ID, "PASS" if ok else "FAIL", {"nodes": [{k: r.get(k) for k in ("ip", "name", "torch", "torch_cuda", "available", "count",
                                                                       "gpu_ids", "matmul_s")} for r in rows]})
