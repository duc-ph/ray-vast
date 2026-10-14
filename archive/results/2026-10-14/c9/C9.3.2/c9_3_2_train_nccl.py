# C9.3.2: Ray Train DDP over 2 nodes with NCCL (one GPU worker on each node, STRICT_SPREAD). Expected: LIMIT.
# PyTorch opens its own TCPStore and NCCL connections between the nodes, on ports that Ray does not manage; vast maps
# only the 64 identity ports. Ray Train sets MASTER_PORT to a free port of rank 0 (no setting), so no retry with other ports.
from c9common import GPU, MIN_NODES, start, wait_for_nodes
import c9train

ID = "C9.3.2"
start(ID, 1500)
wait_for_nodes(ID, gpus_per_node=GPU)
c9train.run(ID, num_workers=2, use_gpu=GPU > 0, backend="nccl" if GPU else "gloo",
            strategy="STRICT_SPREAD" if MIN_NODES >= 2 else "PACK", checkpoint=False, limit_errors=c9train.NETWORK_ERRORS)
