# C9.3.3: Ray Train DDP over 2 nodes on CPU with gloo (STRICT_SPREAD). Expected: LIMIT, for the same reason as C9.3.2.
from c9common import MIN_NODES, start, wait_for_nodes
import c9train

ID = "C9.3.3"
start(ID, 1500)
wait_for_nodes(ID)
c9train.run(ID, num_workers=2, use_gpu=False, backend="gloo", strategy="STRICT_SPREAD" if MIN_NODES >= 2 else "PACK",
            checkpoint=False, limit_errors=c9train.NETWORK_ERRORS)
