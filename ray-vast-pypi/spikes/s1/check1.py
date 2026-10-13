# Check 1/2: 2 tasks x num_cpus=2 on worker type w2 (CPU 2) -> 2 nodes.
import socket, time, ray
ray.init(address="auto")
t0 = time.time()

@ray.remote(num_cpus=2, resources={"w2": 0.01})
def work(i):
    start = time.time()
    time.sleep(60)
    return i, ray.get_runtime_context().get_node_id(), socket.gethostname(), start, time.time()

for r in ray.get([work.remote(i) for i in range(2)]):
    print(r, flush=True)
print("total_s", round(time.time() - t0, 1))
