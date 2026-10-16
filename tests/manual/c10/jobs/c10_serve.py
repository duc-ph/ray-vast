# C10.serve: a Ray Serve deployment on one node, called 40 times through a DeploymentHandle.
#   many:  REPLICAS replicas with num_cpus=0.1 (they fit the CPUs). Each replica is one worker process, and the node also
#          runs a Serve proxy: more processes than worker ports. Some replicas never start, so serve.run never returns:
#          the job hangs.
#   tuned: num_replicas = budget // 2 (budget = ports - 4), ray_actor_options={"num_cpus": 1}.
# Ends with serve.shutdown() (run_c10.py also runs c10_cleanup.py after a hang).
# Usage: python c10_serve.py many|tuned
import time

from ray import serve

from c10common import PROGRESS, SCALE, error_info, finish, start, target

ID = "C10.serve"
mode = start(ID, 600)
cpus, ports, budget = target()
replicas, replica_cpu = (max(8, int(60 * SCALE)), 0.1) if mode == "many" else (max(1, budget // 2), 1)


@serve.deployment(num_replicas=replicas, ray_actor_options={"num_cpus": replica_cpu})
class Echo:
    def __call__(self, i):
        return i


params = {"num_replicas": replicas, "replica_cpu": replica_cpu}
print(f"C10_PARAMS {params}", flush=True)
PROGRESS.update(params, stage="serve.run")
try:
    t = time.time()
    handle = serve.run(Echo.bind(), name="c10", route_prefix=None)
    PROGRESS["stage"] = "calls"
    answers = [r.result(timeout_s=120) for r in [handle.remote(i) for i in range(40)]]
    seconds = time.time() - t
    serve.shutdown()
except Exception as e:
    finish(ID, mode, "ERROR", dict(error_info(e), progress=dict(PROGRESS)))
kind = "COMPLETED" if answers == list(range(40)) else "ERROR"
finish(ID, mode, kind, dict(params, answers=len(answers), seconds=round(seconds, 1)))
