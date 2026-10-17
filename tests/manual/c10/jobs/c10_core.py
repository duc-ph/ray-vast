# C10.core: actors that wait on tasks. Each work item is one actor call that submits a child task and waits for it.
#   many:  ITEMS actors (num_cpus = min(0.1, cpus / (2 * ITEMS)): they and their tasks fit the node's CPUs), one item each. Every actor holds one worker process, so
#          the actors take all worker ports, and the child tasks never get a process: the job hangs.
#   tuned: the same ITEMS work items on an actor pool of budget // 2 actors (budget = ports - 4), num_cpus = cpus / budget
#          for the actors and the child tasks. At most budget processes: the job completes.
#   default: Ray's defaults at one process for each CPU: cpus actors with the default options (an actor holds no CPU
#          while it runs), each item an actor call that waits for a child task with the default num_cpus=1. At most
#          2 * cpus processes: on a worker with cpus <= budget, some ports stay free for the child tasks (rule 1).
#   full:  budget actors with num_cpus = cpus / budget (the rule of the library pages: at most budget processes), each
#          with ITEMS / budget work items that need no child task. The actors fill the CPUs and use budget ports.
# Usage: python c10_core.py many|tuned|full|default
import time

import ray

from c10common import PROGRESS, SCALE, error_info, finish, start, target

ID = "C10.core"
mode = start(ID, 600)
cpus, ports, budget = target()
ITEMS = max(ports + 4, int(200 * SCALE))


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

    def solo(self, i):
        time.sleep(0.2)
        return i


if mode == "many":
    share = min(0.1, cpus / (2 * ITEMS))
    actors = [Worker.options(num_cpus=share).remote(share) for _ in range(ITEMS)]
    params = {"actors": len(actors), "actor_cpus": round(share, 4), "child_cpus": round(share, 4)}
elif mode == "default":
    actors = [Worker.remote(1) for _ in range(int(cpus))]
    params = {"actors": len(actors), "actor_cpus": "default", "child_cpus": 1}
elif mode == "full":
    share = cpus / budget
    actors = [Worker.options(num_cpus=share).remote(share) for _ in range(budget)]
    params = {"actors": len(actors), "actor_cpus": round(share, 3), "child_cpus": None}
else:
    share = cpus / budget
    actors = [Worker.options(num_cpus=share).remote(share) for _ in range(max(1, budget // 2))]
    params = {"actors": len(actors), "actor_cpus": round(share, 3), "child_cpus": round(share, 3)}
print(f"C10_PARAMS {params}", flush=True)
PROGRESS.update(params, items=ITEMS, done=0)
try:
    t = time.time()
    call = (lambda a: a.solo) if mode == "full" else (lambda a: a.work)
    pending = [call(actors[i % len(actors)]).remote(i) for i in range(ITEMS)]
    while pending:
        ready, pending = ray.wait(pending, num_returns=len(pending), timeout=5)
        ray.get(ready)
        PROGRESS["done"] += len(ready)
    seconds = time.time() - t
except Exception as e:
    finish(ID, mode, "ERROR", dict(error_info(e), progress=dict(PROGRESS)))
finish(ID, mode, "COMPLETED", dict(params, items=ITEMS, done=PROGRESS["done"], seconds=round(seconds, 1)))
