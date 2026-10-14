# C9.4.1: Ray Serve: serve.run of a deployment with num_replicas=2 (max_replicas_per_node=1: one replica on each worker),
# called through a DeploymentHandle. The app "c9" (route "/") stays up after the job for the HTTP checks C9.4.2 and C9.4.3;
# C9.4.4 shuts Serve down. Set C9_SERVE_SHUTDOWN=1 to shut it down at the end of this job.
# PASS: 20 calls answer, from replicas on >= C9_MIN_NODES nodes.
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


try:
    handle = serve.run(Hello.bind(), name="c9", route_prefix="/")
    answers = [handle.remote().result(timeout_s=60) for _ in range(20)]
    status = serve.status()
    proxies = {short(k): str(v) for k, v in status.proxies.items()}
except Exception as e:
    result(ID, "FAIL", error_info(e))
data = {"answers": len(answers), "per_node": dict(collections.Counter(short(a["node_id"]) for a in answers)),
        "replica_ips": sorted({a["ip"] for a in answers}), "proxies": proxies,
        "app_status": str(status.applications["c9"].status)}
if os.environ.get("C9_SERVE_SHUTDOWN") == "1":
    serve.shutdown()
result(ID, "PASS" if len(answers) == 20 and len(data["per_node"]) >= MIN_NODES else "FAIL", data)
