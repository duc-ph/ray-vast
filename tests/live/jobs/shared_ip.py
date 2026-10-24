# Live suite: Ray traffic between two workers that share one public IP (each container has its own network namespace).
# An actor on each worker is called from a task on the other, and a 50 MB object made on each worker is fetched by a task
# on the other. Prints one line: `JOB_RESULT shared_ip PASS|FAIL <json>`.
# Usage: python shared_ip.py <public ip>
import hashlib
import sys
import time

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from jobcommon import error_info, result, start, worker_nodes

ID = "shared_ip"
# The 50 MB object: PATTERN x REPEAT. Small constants, so the remote function does not carry the 50 MB itself.
PATTERN, REPEAT = bytes(range(250)), 200_000
ip = sys.argv[1]
start(ID, 900)
deadline = time.time() + 600
while len(nodes := sorted(n for n, v in worker_nodes().items() if v["ip"] == ip)) < 2:
    if time.time() > deadline:
        result(ID, "FAIL", {"error": f"no 2 worker nodes with the IP {ip}", "worker_nodes": worker_nodes()})
    time.sleep(5)
a, b = nodes[:2]


def on(node):
    return {"scheduling_strategy": NodeAffinitySchedulingStrategy(node, soft=False)}


@ray.remote(num_cpus=1)
class Echo:
    def node(self):
        return ray.get_runtime_context().get_node_id()


@ray.remote(num_cpus=1)
def call(actor):
    return ray.get(actor.node.remote(), timeout=120)


@ray.remote(num_cpus=1)
def make():
    return PATTERN * REPEAT


@ray.remote(num_cpus=1)
def fetch(refs):
    started = time.time()
    blob = ray.get(refs[0], timeout=300)
    return len(blob), hashlib.sha256(blob).hexdigest(), round(time.time() - started, 2), ray.get_runtime_context().get_node_id()


expected = hashlib.sha256(PATTERN * REPEAT).hexdigest()
data = {"ip": ip, "node_a": a[:8], "node_b": b[:8]}
try:
    for name, here, there in (("a_to_b", a, b), ("b_to_a", b, a)):
        actor = Echo.options(**on(there)).remote()
        data[f"actor_call_{name}"] = ray.get(call.options(**on(here)).remote(actor), timeout=300) == there
        nbytes, checksum, seconds, fetched_on = ray.get(fetch.options(**on(there)).remote([make.options(**on(here)).remote()]),
                                                        timeout=600)
        data[f"object_{name}"] = {"bytes": nbytes, "checksum_ok": checksum == expected, "get_s": seconds,
                                  "fetched_on_other_node": fetched_on == there}
except Exception as e:
    result(ID, "FAIL", dict(data, **error_info(e)))
ok = all(data[f"actor_call_{n}"] and data[f"object_{n}"]["checksum_ok"] and data[f"object_{n}"]["bytes"] == len(PATTERN) * REPEAT
         and data[f"object_{n}"]["fetched_on_other_node"] for n in ("a_to_b", "b_to_a"))
result(ID, "PASS" if ok else "FAIL", data)
