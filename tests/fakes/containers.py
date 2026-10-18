"""The cleanup of the test containers on this box.

A test container name starts with `<family>-<pid>-`: `rvworker` (tests/fakes/worker_world.py) or `rvlocal`
(tests/local_cluster), and the pid of the pytest process that made it. A killed run (SIGKILL,
a timeout of its parent) cannot remove its containers, and a later run on the same ports and names then fails.
Rule: a process removes the containers of its own pid and of every pid that is no longer alive, never those of a live
pid (a concurrent run). `install()` does this at the start, at exit (also after a KeyboardInterrupt) and on SIGTERM.
The pids are the pids of this box: a pytest that runs in a container (another pid namespace) must use other families.
"""
import atexit
import os
import re
import shutil
import signal
import subprocess

FAMILIES = ("rvworker", "rvlocal")
NAME = re.compile(rf"^(?:{'|'.join(FAMILIES)})-(\d+)-")
_installed = False


def pid_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # another user's process
        return True
    return True


def stale(names, alive=pid_alive, me=None):
    """The names of `names` that belong to this process (`me`) or to a pid that is not alive."""
    me = os.getpid() if me is None else me
    return [n for n in names if (m := NAME.match(n)) and (int(m[1]) == me or not alive(int(m[1])))]


def sweep():
    """Remove the stale test containers (`docker rm -f`). Without docker, or if docker fails, do nothing."""
    if not shutil.which("docker"):
        return
    try:
        listed = subprocess.run(["docker", "ps", "-a", "--format", "{{.Names}}"], capture_output=True, text=True, timeout=60)
        names = stale(listed.stdout.split())
        if names:
            subprocess.run(["docker", "rm", "-f", *names], capture_output=True, timeout=120)
    except (OSError, subprocess.SubprocessError):
        pass


def install():
    """Sweep now, at exit and on SIGTERM (then end as SIGTERM ends the process). Once for each process."""
    global _installed
    if _installed:
        return
    _installed = True
    sweep()
    atexit.register(sweep)
    previous = signal.getsignal(signal.SIGTERM)

    def on_sigterm(signum, frame):
        sweep()
        if callable(previous):
            previous(signum, frame)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        os.kill(os.getpid(), signal.SIGTERM)
    signal.signal(signal.SIGTERM, on_sigterm)
