# C9.1.4: GPU batch inference: map_batches with a callable class, concurrency=2, num_gpus=1, a small PyTorch MLP on the GPU.
# Needs torch (submit with run_c9.sh TORCH_ENV). PASS: all rows are scored, on >= C9_MIN_NODES nodes, on CUDA.
import time

import numpy as np
import ray

from c9common import GPU, MIN_NODES, SMALL, error_info, result, short, start, wait_for_nodes

ID = "C9.1.4"
ROWS = 2_000 if SMALL else 200_000
start(ID, 2400)
wait_for_nodes(ID, gpus_per_node=GPU)


class Scorer:
    def __init__(self):
        import torch
        self.torch = torch
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        torch.manual_seed(0)
        self.model = torch.nn.Sequential(torch.nn.Linear(256, 1024), torch.nn.ReLU(), torch.nn.Linear(1024, 10)).to(self.device).eval()
        self.node = ray.get_runtime_context().get_node_id()

    def __call__(self, batch):
        with self.torch.inference_mode():
            x = self.torch.as_tensor(batch["data"], dtype=self.torch.float32, device=self.device)
            pred = self.model(x).argmax(dim=1).cpu().numpy()
        n = len(pred)
        return {"pred": pred, "node": np.array([self.node] * n), "device": np.array([self.device] * n)}


try:
    ds = ray.data.range_tensor(ROWS, shape=(256,))
    t = time.time()
    out = ds.map_batches(Scorer, concurrency=MIN_NODES, num_gpus=GPU, batch_size=4096).select_columns(["node", "device"]).materialize()
    seconds = time.time() - t
    scored = out.count()
    nodes = out.groupby("node").count().take_all()
    devices = sorted(out.unique("device"))
except Exception as e:
    result(ID, "FAIL", error_info(e))
data = {"rows": ROWS, "scored": scored, "seconds": round(seconds, 1), "rows_per_s": round(ROWS / seconds),
        "rows_per_node": {short(r["node"]): r["count()"] for r in nodes}, "devices": devices}
ok = scored == ROWS and len(nodes) >= MIN_NODES and devices == ["cuda"]
result(ID, "PASS" if ok else "FAIL", data)
