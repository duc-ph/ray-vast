# The local cluster tests: the real provider, with the rent-loop breaker pause divided by RAYVAST_TIME_SCALE (from the head env).
# The breaker scenario loads it with `provider.module: scaled_provider.VastNodeProvider` (the head has /e2e on PYTHONPATH).
# Only the pause is scaled: the other provider limits stay real, so a slow enrollment cannot end a good candidate.
import os

import ray_vast.state
from ray_vast.provider import VastNodeProvider  # noqa: F401  (the class that Ray loads)

ray_vast.state.BREAKER_PAUSE_S = ray_vast.state.BREAKER_PAUSE_S / float(os.environ.get("RAYVAST_TIME_SCALE", "1"))
