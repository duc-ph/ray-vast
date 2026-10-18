"""The cleanup of test containers (tests/fakes/containers.py): a run removes its own and those of dead runs, never those of
a run that is still alive."""
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from fakes import containers

TESTS = Path(__file__).resolve().parent


def test_stale_names_are_this_process_and_dead_processes_of_the_test_families():
    alive = {1000, 2000}.__contains__  # 1000 is a concurrent live run; 2000 is this process
    names = ["rvlocal-1000-head", "rvworker-1000-w-1", "rvlocal-2000-ns", "rvworker-3000-worker", "rvworker-3001-head", "rvworker-core-many",
             "pl-net", "other-1", "rvlocal-x-head", "xrvlocal-3000-head"]
    assert containers.stale(names, alive, me=2000) == ["rvlocal-2000-ns", "rvworker-3000-worker", "rvworker-3001-head"]


def fake_docker(tmp_path, names):
    """A `docker` on PATH: `ps` prints `names`, every call is written to calls.txt."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (tmp_path / "names.txt").write_text("\n".join(names) + "\n")
    script = bin_dir / "docker"
    script.write_text(f'#!/bin/sh\necho "$@" >> {tmp_path}/calls.txt\n'
                      f'[ "$1" = ps ] && cat {tmp_path}/names.txt\nexit 0\n')
    script.chmod(0o755)
    return {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "PYTHONPATH": str(TESTS)}


def child(tmp_path, env, body):
    """A Python process that installs the cleanup, makes two containers with its pid in the name, and runs `body`."""
    code = ("import os, sys, time; from fakes import containers\n"
            "containers.install()\n"
            f"open({str(tmp_path / 'names.txt')!r}, 'a').write(f'rvlocal-{{os.getpid()}}-head\\nrvworker-{{os.getpid()}}-w-1\\n')\n"
            f"print('ready', flush=True)\n{body}\n")
    return subprocess.Popen([sys.executable, "-c", code], env=env, stdout=subprocess.PIPE, text=True,
                            start_new_session=True)


def removed(tmp_path):
    return [line.split()[2:] for line in (tmp_path / "calls.txt").read_text().splitlines() if line.startswith("rm -f")]


def test_a_sigterm_removes_the_containers_of_the_process(tmp_path):
    env = fake_docker(tmp_path, ["rvlocal-1-head"])  # pid 1 (init) is alive: a concurrent run, never removed
    proc = child(tmp_path, env, "time.sleep(60)")
    try:
        assert proc.stdout.readline() == "ready\n"
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=20) == -signal.SIGTERM  # the process still ends as a SIGTERM ends it
    finally:
        proc.kill()
    assert removed(tmp_path)[-1] == [f"rvlocal-{proc.pid}-head", f"rvworker-{proc.pid}-w-1"]


def test_a_keyboard_interrupt_removes_the_containers_of_the_process(tmp_path):
    env = fake_docker(tmp_path, ["rvlocal-1-head"])
    proc = child(tmp_path, env, "raise KeyboardInterrupt")
    try:
        proc.wait(timeout=20)
    finally:
        proc.kill()
    assert removed(tmp_path)[-1] == [f"rvlocal-{proc.pid}-head", f"rvworker-{proc.pid}-w-1"]


def test_the_start_removes_the_containers_of_a_dead_run_only(tmp_path):
    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    time.sleep(0.1)
    env = fake_docker(tmp_path, [f"rvworker-{dead.pid}-w", "rvlocal-1-head"])
    proc = child(tmp_path, env, "pass")
    proc.wait(timeout=20)
    assert removed(tmp_path)[0] == [f"rvworker-{dead.pid}-w"]  # install(): before this process made any container
