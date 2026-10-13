# C0.2 step 3 / C1 step 5 driver: 20 tasks, 1 actor with 10 calls, 1 pip runtime_env task.
# Usage: python ray_driver.py [num_cpus]   (C1 runs it with 0, because the head has CPU: 0)
import sys
import time

import ray

num_cpus = float(sys.argv[1]) if len(sys.argv) > 1 else 1
ray.init(address="auto")
print("cluster resources:", ray.cluster_resources(), flush=True)


@ray.remote(num_cpus=num_cpus)
def square(x):
    return x * x


@ray.remote(num_cpus=num_cpus)
class Counter:
    def __init__(self):
        self.n = 0

    def incr(self):
        self.n += 1
        return self.n


@ray.remote(num_cpus=num_cpus, runtime_env={"pip": ["emoji==2.12.1"]})
def emoji_version():
    import emoji

    return emoji.__version__


t0 = time.time()
results = ray.get([square.remote(i) for i in range(20)], timeout=300)
print(f"tasks: {len(results)} done, sum={sum(results)} (expect 2470), {time.time() - t0:.1f}s", flush=True)

t0 = time.time()
counter = Counter.remote()
calls = ray.get([counter.incr.remote() for _ in range(10)], timeout=300)
print(f"actor: 10 calls, last={calls[-1]} (expect 10), {time.time() - t0:.1f}s", flush=True)

t0 = time.time()
version = ray.get(emoji_version.remote(), timeout=900)
print(f"pip task: emoji {version} (expect 2.12.1), {time.time() - t0:.1f}s", flush=True)
print("DRIVER OK")
