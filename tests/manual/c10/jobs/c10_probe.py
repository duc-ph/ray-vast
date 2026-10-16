# C10 probe: on each alive worker node, read pids.max, pids.current, pids.peak, pids.events (cgroup), the worker ports of
# the raylet, the threads of all processes and the number of Ray worker processes. Prints one `C10_PROBE <json>` line.
# run_c10.py runs it before and after each C10 test; the change of `pids.events` ("max N") counts the refused forks.
import json
import os

import ray

from c10common import probe_nodes

ray.init(address=os.environ.get("RAY_ADDRESS", "auto"))
print("C10_PROBE " + json.dumps(probe_nodes(), sort_keys=True), flush=True)
