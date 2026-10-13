# Check 3: type w2r4 declares CPU 2, raylet starts with 4 CPUs.
# Phase A: one short task brings up one node. Phase B: 4 x num_cpus=1 x 120 s.
import time, ray
ray.init(address="auto")
t0 = time.time()

@ray.remote(num_cpus=1, resources={"r4": 0.01})
def work(i, s):
    start = time.time()
    time.sleep(s)
    return i, ray.get_runtime_context().get_node_id()[:12], round(start - t0, 1), round(time.time() - t0, 1)

print("A", ray.get(work.remote(-1, 1)), flush=True)
r4 = [n for n in ray.nodes() if n["Alive"] and "r4" in n["Resources"]]
print("r4 nodes", [(n["NodeID"][:12], n["Resources"]["CPU"]) for n in r4], flush=True)
print("B", ray.get([work.remote(i, 120) for i in range(4)]), flush=True)
print("r4 nodes after", [(n["NodeID"][:12], n["Resources"]["CPU"]) for n in ray.nodes()
                         if n["Alive"] and "r4" in n["Resources"]])
print("total_s", round(time.time() - t0, 1))
