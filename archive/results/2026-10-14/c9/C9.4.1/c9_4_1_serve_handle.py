# C9.4.1: Ray Serve: serve.run of a deployment with num_replicas=2 (max_replicas_per_node=1: one replica on each worker),
# called through a DeploymentHandle. The app "c9" (route "/") stays up after the job for the HTTP checks C9.4.2 and C9.4.3;
# C9.4.4 shuts Serve down. Set C9_SERVE_SHUTDOWN=1 to shut it down at the end of this job.
# PASS: 40 concurrent calls answer, and the replica actors are on >= C9_MIN_NODES nodes (phase 2 change: 20 sequential
# calls all went to one replica, so the old check on the answering nodes failed although both replicas ran).
import collections
import os

from ray import serve

from c9common import MIN_NODES, error_info, node_info, result, short, start, wait_for_nodes

ID = "C9.4.1"
start(ID, 900)
wait_for_nodes(ID)


@serve.deployment(num_replicas=MIN_NODES, max_replicas_per_node=1 if MIN_NODES >= 2 else None, ray_actor_options={"num_cpus": 1})
class Hello:
    def __call__(self, request=None):
        info = node_info()
        return {"hello": "c9", "node_id": info["node_id"], "ip": info["ip"], "pid": info["pid"]}



def replica_actor_nodes(app, deployment):
    """The nodes of the ALIVE replica actors of the deployment (Ray state API)."""
    from ray.util.state import list_actors
    import ray
    actors = list_actors(address=ray.get_runtime_context().gcs_address, filters=[("state", "=", "ALIVE")], limit=10000, raise_on_missing_output=False)
    return sorted(short(a.node_id) for a in actors if a.class_name.startswith(f"ServeReplica:{app}:{deployment}"))


try:
    handle = serve.run(Hello.bind(), name="c9", route_prefix="/")
    responses = [handle.remote() for _ in range(40)]  # concurrent calls: the router spreads them by queue length
    answers = [r.result(timeout_s=60) for r in responses]
    replica_nodes = replica_actor_nodes("c9", "Hello")
    status = serve.status()
    proxies = {short(k): str(v) for k, v in status.proxies.items()}
except Exception as e:
    result(ID, "FAIL", error_info(e))
data = {"replica_nodes": replica_nodes, "answers": len(answers), "per_node": dict(collections.Counter(short(a["node_id"]) for a in answers)),
        "replica_ips": sorted({a["ip"] for a in answers}), "proxies": proxies,
        "app_status": str(status.applications["c9"].status)}
if os.environ.get("C9_SERVE_SHUTDOWN") == "1":
    serve.shutdown()
result(ID, "PASS" if len(answers) == 40 and len(set(replica_nodes)) >= MIN_NODES else "FAIL", data)
