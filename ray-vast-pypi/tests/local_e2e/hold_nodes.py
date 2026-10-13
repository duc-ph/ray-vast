# Step 11 scenarios 2 to 7: make the autoscaler start N worker nodes, then print what Ray reports about them.
# Usage: python hold_nodes.py <N> [busy]. With "busy", a detached actor keeps one node busy after this script ends and
# the output has the actor's CONTAINER_ID. The placement group goes away when this script ends.
import json
import os
import sys

import ray
from ray.util.placement_group import placement_group

n = int(sys.argv[1])
ray.init(address="auto", namespace="st11")
pg = placement_group([{"CPU": 1}] * n, strategy="STRICT_SPREAD")
ray.get(pg.ready(), timeout=900)
result = {"nodes": [{"id": x["NodeID"], "cpu": x["Resources"].get("CPU", 0)} for x in ray.nodes()
                    if x["Alive"] and x["Resources"].get("CPU", 0) > 0]}
if len(sys.argv) > 2 and sys.argv[2] == "busy":
    @ray.remote(num_cpus=1)
    class Busy:
        def container(self):
            return os.environ["CONTAINER_ID"]

    actor = Busy.options(name="busy", lifetime="detached").remote()
    result["busy_container"] = ray.get(actor.container.remote(), timeout=300)
print("HOLD_JSON " + json.dumps(result), flush=True)
