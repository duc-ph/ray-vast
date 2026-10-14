# C9.5.2: actor faults on a worker. Actor A ends its own process (os._exit); actor B is killed with ray.kill(no_restart=False).
# Both have max_restarts=1. PASS: each comes back in a new process (new pid) and answers again; the restart time is recorded.
import time

import ray
from ray.exceptions import RayActorError

from c9common import error_info, node_info, result, short, start, wait_for_nodes

ID = "C9.5.2"
start(ID, 900)
wait_for_nodes(ID, nodes=1)


@ray.remote(num_cpus=1, max_restarts=1, max_task_retries=0)
class Victim:
    def info(self):
        return node_info()

    def crash(self):
        import os
        os._exit(1)


def wait_restart(actor, old_pid):
    t = time.time()
    while time.time() - t < 300:
        try:
            info = ray.get(actor.info.remote(), timeout=30)
            if info["pid"] != old_pid:
                return info, round(time.time() - t, 1)
        except (RayActorError, ray.exceptions.GetTimeoutError):
            pass
        time.sleep(1)
    raise TimeoutError(f"actor did not restart within 300 s (old pid {old_pid})")


data = {}
try:
    a, b = Victim.remote(), Victim.remote()
    before_a, before_b = ray.get([a.info.remote(), b.info.remote()], timeout=300)
    try:
        ray.get(a.crash.remote(), timeout=60)
    except RayActorError as e:
        data["crash_error"] = type(e).__name__
    after_a, data["a_restart_s"] = wait_restart(a, before_a["pid"])
    ray.kill(b, no_restart=False)
    after_b, data["b_restart_s"] = wait_restart(b, before_b["pid"])
except Exception as e:
    result(ID, "FAIL", dict(data, **error_info(e)))
data.update(a_nodes=[short(before_a["node_id"]), short(after_a["node_id"])], b_nodes=[short(before_b["node_id"]), short(after_b["node_id"])],
            a_pids=[before_a["pid"], after_a["pid"]], b_pids=[before_b["pid"], after_b["pid"]])
result(ID, "PASS", data)
