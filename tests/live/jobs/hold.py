# Live suite: hold N worker nodes. A placement group of N bundles {GPU: 1, CPU: 1} (STRICT_SPREAD) makes the autoscaler
# start N GPU workers and keeps them busy (not idle) until the job is stopped. Prints `HOLD_READY <epoch>` when placed.
# Usage: python hold.py N
import sys
import time

import ray
from ray.util.placement_group import placement_group

ray.init()
group = placement_group([{"GPU": 1, "CPU": 1}] * int(sys.argv[1]), strategy="STRICT_SPREAD")
ray.get(group.ready())
print(f"HOLD_READY {time.time():.1f}", flush=True)
while True:
    time.sleep(60)
