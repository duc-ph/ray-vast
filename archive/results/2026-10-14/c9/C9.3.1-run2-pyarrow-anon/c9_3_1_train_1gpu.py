# C9.3.1: Ray Train, one worker, one GPU, 2 epochs, a checkpoint (default storage_path). PASS expected.
# A storage error (checkpoint on a worker, storage_path on the head) is a LIMIT; the JSON keeps the error.
from c9common import GPU, start, wait_for_nodes
import c9train

ID = "C9.3.1"
start(ID, 2400)
wait_for_nodes(ID, nodes=1, gpus_per_node=GPU)
c9train.run(ID, num_workers=1, use_gpu=GPU > 0, backend=None, strategy="PACK", checkpoint=True, limit_errors=c9train.STORAGE_ERRORS)
