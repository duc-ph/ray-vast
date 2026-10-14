# C8.1 job (docs/test-plan.md): a placement group of 4 bundles {GPU: 1, CPU: 1} with STRICT_SPREAD makes the autoscaler
# start 4 GPU workers. Prints `C8_ALL_PLACED <epoch>` when the group is placed, then one JSON line per bundle task
# (nvidia-smi -L, ray.get_gpu_ids(), CONTAINER_ID, RAY_NODE_TYPE_NAME). Each task sleeps 30 s.
# Exits 1 if the 4 tasks did not run in 4 different containers.
# Submit: ray job submit --address http://127.0.0.1:8265 --working-dir tests/manual --no-wait -- python c8_job.py
import json
import os
import subprocess
import sys
import time

import ray
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

BUNDLES = 4


@ray.remote(num_gpus=1, num_cpus=1)
def on_gpu(index):
    try:
        smi = subprocess.run(["nvidia-smi", "-L"], capture_output=True, text=True, timeout=60).stdout.strip()
    except (OSError, subprocess.TimeoutExpired) as e:
        smi = f"nvidia-smi failed: {type(e).__name__}"
    result = {"bundle": index, "nvidia_smi": smi, "gpu_ids": ray.get_gpu_ids(), "container_id": os.environ.get("CONTAINER_ID"),
              "node_type": os.environ.get("RAY_NODE_TYPE_NAME"), "node_id": ray.get_runtime_context().get_node_id()}
    time.sleep(30)
    return result


ray.init()
print(f"C8_SUBMITTED {time.time():.1f}", flush=True)
group = placement_group([{"GPU": 1, "CPU": 1}] * BUNDLES, strategy="STRICT_SPREAD")
ray.get(group.ready())
print(f"C8_ALL_PLACED {time.time():.1f}", flush=True)
results = ray.get([on_gpu.options(scheduling_strategy=PlacementGroupSchedulingStrategy(group, placement_group_bundle_index=i))
                   .remote(i) for i in range(BUNDLES)])
for result in results:
    print(json.dumps(result), flush=True)
containers = {r["container_id"] for r in results}
print(f"C8_DONE {time.time():.1f} containers={len(containers)} node_types={sorted({str(r['node_type']) for r in results})}", flush=True)
sys.exit(0 if len(containers) == BUNDLES and None not in containers else 1)
