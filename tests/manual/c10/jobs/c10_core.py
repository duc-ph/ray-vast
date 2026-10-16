# C10.core: actors that wait on tasks. Each work item is one actor call that submits a child task and waits for it.
#   many:  ITEMS actors (num_cpus=0.1; they fit the node's CPUs), one item each. Every actor holds one worker process, so
#          the actors take all worker ports, and the child tasks never get a process: the job hangs.
#   tuned: the same ITEMS work items on an actor pool of budget // 2 actors (budget = ports - 4), num_cpus = cpus / budget
#          for the actors and the child tasks. At most budget processes: the job completes.
# Usage: python c10_core.py many|tuned
import time

import ray

from c10common import PROGRESS, SCALE, error_info, finish, start, target

ID = "C10.core"
mode = start(ID, 600)
cpus, ports, budget = target()
ITEMS = max(8, int(200 * SCALE))


@ray.remote
def child(i):
    time.sleep(0.2)
    return i


@ray.remote
class Worker:
    def __init__(self, child_cpus):
        self.child_cpus = child_cpus

    def work(self, i):
        return ray.get(child.options(num_cpus=self.child_cpus).remote(i))


if mode == "many":
    actors = [Worker.options(num_cpus=0.1).remote(0.1) for _ in range(min(ITEMS, int(cpus / 0.1)))]
    params = {"actors": len(actors), "actor_cpus": 0.1, "child_cpus": 0.1}
else:
    share = cpus / budget
    actors = [Worker.options(num_cpus=share).remote(share) for _ in range(max(1, budget // 2))]
    params = {"actors": len(actors), "actor_cpus": round(share, 3), "child_cpus": round(share, 3)}
print(f"C10_PARAMS {params}", flush=True)
PROGRESS.update(params, items=ITEMS, done=0)
try:
    t = time.time()
    pending = [actors[i % len(actors)].work.remote(i) for i in range(ITEMS)]
    while pending:
        ready, pending = ray.wait(pending, num_returns=len(pending), timeout=5)
        ray.get(ready)
        PROGRESS["done"] += len(ready)
    seconds = time.time() - t
except Exception as e:
    finish(ID, mode, "ERROR", dict(error_info(e), progress=dict(PROGRESS)))
finish(ID, mode, "COMPLETED", dict(params, items=ITEMS, done=PROGRESS["done"], seconds=round(seconds, 1)))
