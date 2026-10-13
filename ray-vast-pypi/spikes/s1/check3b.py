# Check 3b: cold start. 4 x num_cpus=1 on w2r4 with no w2r4 node running.
import time, ray
ray.init(address="auto")
t0 = time.time()

@ray.remote(num_cpus=1, resources={"r4": 0.01})
def work(i):
    start = time.time()
    time.sleep(60)
    return i, ray.get_runtime_context().get_node_id()[:12], round(start - t0, 1)

print("B", ray.get([work.remote(i) for i in range(4)]), flush=True)
print("total_s", round(time.time() - t0, 1))
