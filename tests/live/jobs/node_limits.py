# On each alive worker node, read pids.max, pids.current, pids.peak, pids.events (cgroup), the worker ports of
# the raylet, the threads of all processes and the number of Ray worker processes. Prints one `NODE_LIMITS <json>` line.
# The live tests run it before and after each process-limit job; the change of `pids.events` ("max N") counts the refused forks.
import json
import os

import ray

from paircommon import probe_nodes

ray.init(address=os.environ.get("RAY_ADDRESS", "auto"))
print("NODE_LIMITS " + json.dumps(probe_nodes(), sort_keys=True), flush=True)
