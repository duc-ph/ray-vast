# C2.6: worker port exhaustion (T11). Run on the head as a driver.
# Usage: python c2_ports.py <tasks|actors> <W-hp public ip> [hold seconds]
# tasks:  8 tasks, num_cpus=1, sleep(60), pinned to W-hp.
# actors: 8 actors, num_cpus=0, pinned to W-hp; keeps them alive for <hold> s (for the driver check).
import sys
import time

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

mode, ip = sys.argv[1], sys.argv[2]
hold = int(sys.argv[3]) if len(sys.argv) > 3 else 0
ray.init(address="auto")
node = [n["NodeID"] for n in ray.nodes() if n["Alive"] and n["NodeManagerAddress"] == ip][0]
pin = NodeAffinitySchedulingStrategy(node, soft=False)
t0 = time.time()


@ray.remote(num_cpus=1)
def sleeper(i):
    start = time.time()
    time.sleep(60)
    return i, start


@ray.remote(num_cpus=0)
class Holder:
    def ready(self):
        import os

        return os.getpid(), time.time()


if mode == "tasks":
    refs = {sleeper.options(scheduling_strategy=pin).remote(i): i for i in range(8)}
else:
    actors = [Holder.options(scheduling_strategy=pin).remote() for _ in range(8)]
    refs = {a.ready.remote(): i for i, a in enumerate(actors)}

pending = list(refs)
while pending:
    done, pending = ray.wait(pending, num_returns=1, timeout=600)
    if not done:
        print(f"{time.time() - t0:7.1f}s TIMEOUT: {len(pending)} still pending", flush=True)
        break
    i = refs[done[0]]
    try:
        v = ray.get(done[0])
        if mode == "tasks":
            print(f"{time.time() - t0:7.1f}s #{i} OK started at +{v[1] - t0:.1f}s", flush=True)
        else:
            print(f"{time.time() - t0:7.1f}s #{i} OK pid {v[0]} ready at +{v[1] - t0:.1f}s", flush=True)
    except Exception as e:
        print(f"{time.time() - t0:7.1f}s #{i} FAIL {type(e).__name__}: {str(e)[:600]!r}", flush=True)
if hold:
    print(f"holding actors for {hold}s", flush=True)
    time.sleep(hold)
print("DONE", round(time.time() - t0, 1))
