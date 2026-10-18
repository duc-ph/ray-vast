# A Serve deployment with ray_actor_options={"num_gpus": 1}: one replica on each GPU worker, a torch matmul on CUDA.
# Needs torch (pairs.TORCH_ENV). At the end it shuts Serve down.
# PASS: 40 concurrent calls answer, and the replica actors are on >= JOB_MIN_NODES nodes (20 sequential
# calls all went to one replica, so the old check on the answering nodes failed although both replicas ran), each with 1 GPU id and CUDA.
import collections

from ray import serve

from jobcommon import GPU, MIN_NODES, error_info, node_info, result, short, start, wait_for_nodes

ID = "serve_gpu"
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



def replica_actor_nodes(app, deployment):
    """The nodes of the ALIVE replica actors of the deployment (Ray state API)."""
    from ray.util.state import list_actors
    import ray
    actors = list_actors(address=ray.get_runtime_context().gcs_address, filters=[("state", "=", "ALIVE")], limit=10000, raise_on_missing_output=False)
    return sorted(short(a.node_id) for a in actors if a.class_name.startswith(f"ServeReplica:{app}:{deployment}"))


try:
    handle = serve.run(GpuModel.bind(), name="gpu", route_prefix="/gpu")
    responses = [handle.remote() for _ in range(40)]  # concurrent calls: the router spreads them by queue length
    answers = [r.result(timeout_s=120) for r in responses]
    replica_nodes = replica_actor_nodes("gpu", "GpuModel")
except Exception as e:
    serve.shutdown()
    result(ID, "FAIL", error_info(e))
serve.shutdown()
data = {"replica_nodes": replica_nodes, "answers": len(answers), "per_node": dict(collections.Counter(short(a["node_id"]) for a in answers)),
        "devices": sorted({a["device"] for a in answers}), "gpu_ids": sorted({",".join(a["gpu_ids"]) for a in answers})}
ok = len(answers) == 40 and len(set(replica_nodes)) >= MIN_NODES and data["devices"] == ["cuda"] and all(
    len(a["gpu_ids"]) == 1 for a in answers)
result(ID, "PASS" if ok else "FAIL", data)
