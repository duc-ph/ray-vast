# Ray Train, one worker, one GPU, 2 epochs, a checkpoint (default storage_path). PASS expected.
# A storage error (checkpoint on a worker, storage_path on the head) is a LIMIT; the JSON keeps the error.
from jobcommon import GPU, start, wait_for_nodes
import trainjob

ID = "train_one_gpu"
start(ID, 2400)
wait_for_nodes(ID, nodes=1, gpus_per_node=GPU)
trainjob.run(ID, num_workers=1, use_gpu=GPU > 0, backend=None, strategy="PACK", checkpoint=True, limit_errors=trainjob.STORAGE_ERRORS)
