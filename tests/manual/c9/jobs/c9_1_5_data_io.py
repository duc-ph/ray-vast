# C9.1.5: write and read. write_parquet to /tmp/c9_out (a local path: each worker writes to its own disk), then:
#  - list /tmp/c9_out on each worker node (where the files land);
#  - read /tmp/c9_out back from the driver (it runs on the head; the files are not there);
#  - read_parquet of a public file (C9_PUBLIC_PARQUET, default Ray's example bucket) and read_csv of a working-dir file.
# PASS: the write and both reads of shared sources work. The local read-back result is recorded (it shows the need for
# a cloud bucket for shared output).
import os
import time

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from c9common import SMALL, error_info, result, short, start, wait_for_nodes

ID = "C9.1.5"
OUT = "/tmp/c9_out"
PUBLIC = os.environ.get("C9_PUBLIC_PARQUET", "s3://anonymous@ray-example-data/iris.parquet")
start(ID, 1200)
nodes = wait_for_nodes(ID)


@ray.remote(num_cpus=0)
def list_out():
    files = sorted(os.listdir(OUT)) if os.path.isdir(OUT) else []
    return {"files": len(files), "bytes": sum(os.path.getsize(os.path.join(OUT, f)) for f in files)}


data = {}
try:
    t = time.time()
    ray.data.range(10_000 if SMALL else 1_000_000, override_num_blocks=16).write_parquet(OUT)
    data["write_s"] = round(time.time() - t, 1)
    data["files_per_node"] = {short(n): ray.get(list_out.options(scheduling_strategy=NodeAffinitySchedulingStrategy(n, soft=False))
                                                .remote(), timeout=120) for n in nodes}
except Exception as e:
    result(ID, "FAIL", error_info(e))
try:
    data["readback_rows"] = ray.data.read_parquet(OUT).count()
except Exception as e:
    data["readback_error"] = error_info(e)["error_head"][:3]
try:
    t = time.time()
    data["public_rows"] = ray.data.read_parquet(PUBLIC).count()
    data["public_s"] = round(time.time() - t, 1)
    data["csv_rows"] = ray.data.read_csv("data/numbers.csv").count()
except Exception as e:
    result(ID, "FAIL", dict(data, **error_info(e)))
ok = data["public_rows"] > 0 and data["csv_rows"] == 100 and sum(v["files"] for v in data["files_per_node"].values()) > 0
result(ID, "PASS" if ok else "FAIL", data)
