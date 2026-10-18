# Write the input of the data.md snippet, about 3 GB of parquet, to S3.
# Each row: `key` (0 to 999, for groupby("key")) and `payload` (1000 random bytes: parquet cannot compress them, so the
# size on S3 is about rows x 1 kB). Ray Data defaults: only a read and a map, no shuffle (data.md: "the defaults worked").
# Prints `JOB_RESULT make_parquet PASS|FAIL <json>`.
# Usage: python make_parquet.py <s3 path> <rows>   (credentials: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env_vars)
import json
import os
import sys
import time

import numpy as np
import pyarrow as pa
import ray

PATH, ROWS = sys.argv[1], int(sys.argv[2])
BLOCKS = max(1, ROWS // 50_000)  # about 50 MB in each file
t = time.time()
ray.init(address=os.environ.get("RAY_ADDRESS", "auto"))


def make_rows(batch):
    ids = batch["id"]
    payload = np.frombuffer(os.urandom(len(ids) * 1000), dtype=np.uint8).reshape(len(ids), 1000)
    return pa.table({"key": pa.array(ids % 1000), "payload": pa.array([row.tobytes() for row in payload], type=pa.binary())})


try:
    ray.data.range(ROWS, override_num_blocks=BLOCKS).map_batches(make_rows, batch_format="numpy").write_parquet(PATH)
    status, data = "PASS", {}
except Exception as e:
    status, data = "FAIL", {"error_class": type(e).__name__, "error_head": str(e).splitlines()[:8]}
print("JOB_RESULT make_parquet " + status + " " + json.dumps(dict(data, rows=ROWS, blocks=BLOCKS, seconds=round(time.time() - t, 1))),
      flush=True)
sys.exit(0 if status == "PASS" else 1)
