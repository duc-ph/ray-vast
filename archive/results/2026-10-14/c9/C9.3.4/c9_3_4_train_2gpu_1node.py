# C9.3.4: Ray Train with 2 GPU workers on ONE node (STRICT_PACK), NCCL inside one host. PASS expected.
# The `gpu` workers have 1 GPU each, so STRICT_PACK makes the autoscaler start the `gpu2` node type (GPU: 2).
# The job waits for that node (pull of the GPU image and the torch install): the guard allows 45 min.
from c9common import GPU, start
import c9train

ID = "C9.3.4"
start(ID, 2700)
c9train.run(ID, num_workers=2, use_gpu=GPU > 0, backend="nccl" if GPU else "gloo", strategy="STRICT_PACK", checkpoint=False,
            limit_errors=None)
