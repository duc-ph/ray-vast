# Cleanup after each process-limit job: shut Ray Serve down (its controller and replicas outlive the job that started them).
# The other libraries end with the job's driver. Prints `JOB_CLEANUP done`.
import os

import ray
from ray import serve

ray.init(address=os.environ.get("RAY_ADDRESS", "auto"))
try:
    serve.shutdown()
except Exception as e:
    print(f"serve.shutdown: {type(e).__name__}: {e}", flush=True)
print("JOB_CLEANUP done", flush=True)
