# Shared parts of the C9 job scripts (docs/test-plan.md, "Cycle C9"). Stdlib and Ray only.
# Each job prints exactly one final line: `C9_RESULT <id> PASS|FAIL|LIMIT <json>`, then ends the process.
# Environment:
#   C9_SMALL=1        tiny sizes, for a local check of the scripts
#   C9_MIN_NODES=N    the number of worker nodes that a multi-node check needs (default 2; 1 for a local check)
#   C9_WAIT_S=S       how long a job waits for its worker nodes or GPUs before it gives up (default 900)
#   C9_LOCAL_CPUS=N   a local check only: ray.init(num_cpus=N, _temp_dir=C9_LOCAL_TMP) starts a small Ray in the process
#   C9_GUARD_S=S      overrides the wall-clock guard of the job (each script has its own default)
#   C9_GPU=0          a local dry run of the GPU scripts on CPU: they ask for 0 GPUs (the CUDA checks then FAIL)
import json
import os
import sys
import threading
import time
import traceback

import ray

SMALL = os.environ.get("C9_SMALL") == "1"
MIN_NODES = int(os.environ.get("C9_MIN_NODES", "2"))
WAIT_S = float(os.environ.get("C9_WAIT_S", "900"))
GPU = float(os.environ.get("C9_GPU", "1"))  # GPUs per GPU task, actor or bundle
T0 = time.time()
_lock = threading.Lock()
_done = False


def start(test_id, timeout_s):
    """Connect to Ray and start the wall-clock guard: after timeout_s, print a FAIL line and end the process."""
    timeout_s = float(os.environ.get("C9_GUARD_S", timeout_s))
    def guard():
        time.sleep(timeout_s)
        result(test_id, "FAIL", {"timeout_s": timeout_s, "error": "wall-clock guard fired; the job hung"})
    threading.Thread(target=guard, daemon=True).start()
    print(f"C9_START {test_id} {T0:.1f} small={SMALL} min_nodes={MIN_NODES}", flush=True)
    if os.environ.get("C9_LOCAL_CPUS"):
        context = ray.init(num_cpus=int(os.environ["C9_LOCAL_CPUS"]), _temp_dir=os.environ.get("C9_LOCAL_TMP"))
        os.environ["RAY_API_SERVER_ADDRESS"] = f"http://{context.dashboard_url}"  # the state API ignores RAY_ADDRESS=local
    else:
        ray.init()
    return time.time()


def result(test_id, status, data):
    """Print the one result line and end the process at once (no hang in a Ray shutdown). Exit 0 PASS, 1 FAIL, 2 LIMIT."""
    global _done
    with _lock:
        if _done:
            return
        _done = True
        data = dict(data, elapsed_s=round(time.time() - T0, 1))
        print(f"C9_RESULT {test_id} {status} {json.dumps(data, default=str, sort_keys=True)}", flush=True)
        sys.stderr.flush()
        os._exit({"PASS": 0, "FAIL": 1, "LIMIT": 2}[status])


def error_info(exc):
    """The error class and the first lines of the error text, for a FAIL or LIMIT line."""
    lines = [line for line in str(exc).splitlines() if line.strip()]
    return {"error_class": type(exc).__name__, "error_head": lines[:12],
            "traceback_tail": traceback.format_exception(exc)[-3:]}


def node_info():
    """Called inside a task or actor: where it runs."""
    return {"node_id": ray.get_runtime_context().get_node_id(), "ip": ray.util.get_node_ip_address(),
            "container_id": os.environ.get("CONTAINER_ID"), "node_type": os.environ.get("RAY_NODE_TYPE_NAME"),
            "pid": os.getpid()}


def worker_nodes():
    """Alive nodes that can run tasks (the head has CPU 0). Returns {node_id: {"ip", "CPU", "GPU", "object_store_gb"}}."""
    nodes = {}
    for n in ray.nodes():
        res = n.get("Resources", {})
        if n.get("Alive") and res.get("CPU", 0) > 0:
            nodes[n["NodeID"]] = {"ip": n.get("NodeManagerAddress"), "CPU": res.get("CPU", 0), "GPU": res.get("GPU", 0),
                                  "object_store_gb": round(res.get("object_store_memory", 0) / 1e9, 2)}
    return nodes


def wait_for_nodes(test_id, nodes=MIN_NODES, gpus_per_node=0):
    """Wait up to C9_WAIT_S for `nodes` worker nodes with at least `gpus_per_node` GPUs each. Print FAIL and end if not."""
    deadline = time.time() + WAIT_S
    while True:
        found = {k: v for k, v in worker_nodes().items() if v["GPU"] >= gpus_per_node}
        if len(found) >= nodes:
            return found
        if time.time() > deadline:
            result(test_id, "FAIL", {"error": f"needs {nodes} worker node(s) with GPU >= {gpus_per_node}; found {len(found)}",
                                     "worker_nodes": worker_nodes(), "waited_s": WAIT_S})
        time.sleep(5)


def short(node_id):
    return node_id[:8] if node_id else node_id


def job_task_nodes():
    """The nodes where the tasks and actors of this job ran (Ray state API, through the head's dashboard).
    Returns {short node id: task count}, or {"error": ...} if the state API fails."""
    import collections
    from ray.util.state import list_tasks
    try:
        job_id = ray.get_runtime_context().get_job_id()
        tasks = list_tasks(address=ray.get_runtime_context().gcs_address, filters=[("job_id", "=", job_id)], limit=10000,
                           raise_on_missing_output=False)
        return dict(collections.Counter(short(t.node_id) for t in tasks if t.node_id))
    except Exception as e:
        return {"error": f"{type(e).__name__}: {str(e)[:200]}"}
