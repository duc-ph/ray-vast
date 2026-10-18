# A large object between two vast hosts: an actor on worker A does ray.put of 2 GB (JOB_OBJECT_GB) and returns the ref
# (the actor stays alive as the owner); a task on the other worker does ray.get of it. Records the transfer time and the rate.
# PASS: the other worker gets all bytes with the right checksum, on a different node than A.
import os
import time

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from jobcommon import MIN_NODES, SMALL, error_info, result, short, start, wait_for_nodes

ID = "large_object"
GB = 0.02 if SMALL else float(os.environ.get("JOB_OBJECT_GB", "2"))
start(ID, 1800)
found = wait_for_nodes(ID)
nodes = sorted(found)
node_a, node_b = nodes[0], nodes[min(1, len(nodes) - 1)]


@ray.remote(num_cpus=1)
class Owner:
    def make(self):
        import numpy as np
        t = time.time()
        array = np.arange(int(GB * 1e9) // 8, dtype=np.int64)
        self.ref = ray.put(array)
        checksum = int(array[::1_000_000].sum())
        del array
        return [self.ref], checksum, round(time.time() - t, 1)


@ray.remote(num_cpus=1)
def fetch(refs):
    t = time.time()
    array = ray.get(refs[0])
    seconds = time.time() - t
    return array.nbytes, int(array[::1_000_000].sum()), seconds


try:
    owner = Owner.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_a, soft=False)).remote()
    refs, checksum, put_s = ray.get(owner.make.remote(), timeout=600)
    nbytes, got_checksum, get_s = ray.get(fetch.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_b, soft=False))
                                          .remote(refs), timeout=1200)
except Exception as e:
    result(ID, "FAIL", error_info(e))
data = {"gb": GB, "bytes": nbytes, "node_a": short(node_a), "node_b": short(node_b), "ip_a": found[node_a]["ip"], "ip_b": found[node_b]["ip"],
        "put_s": put_s, "get_s": round(get_s, 1), "mb_per_s": round(nbytes / 1e6 / max(get_s, 1e-3), 1), "checksum_ok": checksum == got_checksum}
ok = data["checksum_ok"] and nbytes == int(GB * 1e9) // 8 * 8 and (node_a != node_b or MIN_NODES < 2)
result(ID, "PASS" if ok else "FAIL", data)
