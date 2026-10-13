# Step 11 scenario 1: the subset of tests/manual/battery.py that matters, run inside the head container.
# It differs from battery.py in one way: it keys nodes by node ID, not by IP, because every node here has the box IP.
# Usage: python battery_subset.py <number of worker nodes>. Prints one line: BATTERY_JSON {...}
import json
import subprocess
import sys

import numpy as np
import ray
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

n_workers = int(sys.argv[1])
ray.init(address="auto")
# A job that needs n nodes: this placement group makes the autoscaler start them.
pg = placement_group([{"CPU": 1}] * n_workers, strategy="STRICT_SPREAD")
ray.get(pg.ready(), timeout=900)
workers = [n["NodeID"] for n in ray.nodes() if n["Alive"] and n["Resources"].get("CPU", 0) > 0]
result = {"workers": len(workers), "cpus": sorted(n["Resources"].get("CPU", 0) for n in ray.nodes() if n["Alive"])}


@ray.remote(num_cpus=1)
def where():
    return ray.get_runtime_context().get_node_id()


ran_on = ray.get([where.options(scheduling_strategy="SPREAD").remote() for _ in range(40)], timeout=300)
result["tasks_on_nodes"] = len(set(ran_on) & set(workers))


@ray.remote(num_cpus=1)
class Probe:
    def ping(self):
        return 1

    def make(self, nbytes):
        return [ray.put(np.random.bytes(nbytes))]

    def fetch(self, refs):
        return len(ray.get(refs[0]))


actors = [Probe.options(scheduling_strategy=NodeAffinitySchedulingStrategy(w, soft=False)).remote() for w in workers]
result["actors_ping"] = ray.get([a.ping.remote() for a in actors], timeout=300)

nbytes = 50 * 1024 * 1024
result["head_to_worker"] = [ray.get(a.fetch.remote([ray.put(np.random.bytes(nbytes))]), timeout=300) == nbytes for a in actors]
result["worker_to_head"] = [len(ray.get(ray.get(a.make.remote(nbytes), timeout=300)[0], timeout=300)) == nbytes for a in actors]
if len(actors) == 2:
    result["worker_to_worker"] = ray.get(actors[1].fetch.remote(ray.get(actors[0].make.remote(nbytes), timeout=300)), timeout=300) == nbytes

job = subprocess.run(
    ["ray", "job", "submit", "--address", "http://127.0.0.1:8265", "--", "python", "-c",
     "import ray; ray.init(); print(ray.cluster_resources())"],
    capture_output=True, text=True, timeout=600,
)
result["job_succeeded"] = job.returncode == 0 and "succeeded" in (job.stdout + job.stderr).lower()
result["job_tail"] = (job.stdout + job.stderr)[-300:]
print("BATTERY_JSON " + json.dumps(result), flush=True)
