# Check 4: type slow has start_delay_s 900. One task that needs it.
import time, ray
ray.init(address="auto")
t0 = time.time()

@ray.remote(num_cpus=1, resources={"slow": 0.01})
def work():
    return ray.get_runtime_context().get_node_id()[:12], time.time()

r = ray.get(work.remote())
print("done", r, "wait_s", round(r[1] - t0, 1), flush=True)
