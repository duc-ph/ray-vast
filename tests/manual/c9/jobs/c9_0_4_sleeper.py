# C9.0.4: a long job for `ray job list`, `ray job logs --follow` and `ray job stop` (run_c9.sh runs the commands).
# It prints one line every 2 s for 300 s. run_c9.sh stops it; a SUCCEEDED end means that `ray job stop` failed.
import time

for i in range(150):
    print(f"C9_SLEEPER tick {i} {time.time():.1f}", flush=True)
    time.sleep(2)
