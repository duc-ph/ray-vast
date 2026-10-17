# LB16: a small job. One task for each Ray CPU of the cluster's workers, all at the same time (each task holds one worker
# process). Prints `LB_RESULT LB16 PASS|FAIL {"tasks": N, "cpus": N, "nodes": [...]}`.
import json
import time

import ray

ray.init()
cpus = int(sum(n["Resources"].get("CPU", 0) for n in ray.nodes() if n["Alive"]))


@ray.remote
def work(i):
    time.sleep(2)
    return ray.get_runtime_context().get_node_id()


nodes = ray.get([work.remote(i) for i in range(cpus)], timeout=600)
word = "PASS" if cpus > 0 and len(nodes) == cpus else "FAIL"
print(f"LB_RESULT LB16 {word} " + json.dumps({"tasks": len(nodes), "cpus": cpus, "nodes": sorted(set(nodes))}), flush=True)
