# C10 cleanup, after each test: shut Ray Serve down (its controller and replicas outlive the job that started them).
# The other libraries end with the job's driver. Prints `C10_CLEANUP done`.
import ray
from ray import serve

ray.init()
try:
    serve.shutdown()
except Exception as e:
    print(f"serve.shutdown: {type(e).__name__}: {e}", flush=True)
print("C10_CLEANUP done", flush=True)
