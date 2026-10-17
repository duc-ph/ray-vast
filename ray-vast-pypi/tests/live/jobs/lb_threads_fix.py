# Live suite, LB15: the fixes of rule 2 (threads), for each Ray worker process (the worker_process_setup_hook of
# lb_threads.py in mode `fixed`). The thread pools of Arrow and torch get LB_FIX_THREADS threads (default 2), not one
# for each visible CPU. torch is set only when it is installed (the data workload runs without torch).
import importlib.util
import os


def setup():
    n = int(os.environ.get("LB_FIX_THREADS", "2"))
    import pyarrow
    pyarrow.set_cpu_count(n)
    pyarrow.set_io_thread_count(n)
    if importlib.util.find_spec("torch") is not None:
        import torch
        torch.set_num_threads(n)
        try:
            torch.set_num_interop_threads(n)
        except RuntimeError:  # the inter-op pool already started in this process
            pass
