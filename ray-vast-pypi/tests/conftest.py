"""Test hygiene for the whole suite: a test must not leave a process or a container behind."""
import os
import shutil
import signal
import subprocess

import pytest

from fakes import containers
from fakes.worker_world import CONTAINER_PREFIX

containers.install()  # remove the containers of dead runs now, and this run's at exit and on SIGTERM

# moto loads about 1300 default AMIs on every `mock_aws()` start (1.5 s). No test needs them: the CLI test makes its own image.
os.environ.setdefault("MOTO_EC2_LOAD_DEFAULT_AMIS", "false")


@pytest.fixture(scope="session", autouse=True)
def one_botocore_loader():
    """moto's `mock_aws()` drops boto3's default session, so every test made a new botocore loader that parsed the large
    service JSON files again (0.3 s per test). All sessions of the run share one loader instead."""
    import botocore.session

    loaders = {}
    real_create_loader = botocore.session.create_loader
    botocore.session.create_loader = lambda *args: loaders.setdefault(args, real_create_loader(*args))
    yield
    botocore.session.create_loader = real_create_loader


@pytest.fixture(scope="session", autouse=True)
def no_leaked_processes():
    """Fail the run if a test left a process (or a process group) that it started, or a test container.

    Tests must start long-lived processes with `start_new_session=True` and kill the group in a finalizer. This check
    kills what it finds, so a leak cannot outlive the run; the failing teardown makes the run exit non-zero.
    """
    started = []
    real_init = subprocess.Popen.__init__

    def recording_init(self, *args, **kwargs):
        real_init(self, *args, **kwargs)
        started.append((self, bool(kwargs.get("start_new_session"))))

    subprocess.Popen.__init__ = recording_init
    yield
    subprocess.Popen.__init__ = real_init

    leaked = []
    for proc, own_session in started:
        alive = proc.poll() is None
        if own_session:  # the leader is its group id; children may outlive it
            try:
                os.killpg(proc.pid, 0)
                alive = True
            except ProcessLookupError:
                pass
            except PermissionError:  # the pid was reused by another user's process
                pass
        if alive:
            leaked.append(proc.args)
            try:
                os.killpg(proc.pid, signal.SIGKILL) if own_session else proc.kill()
            except ProcessLookupError:
                pass
    if shutil.which("docker"):
        names = subprocess.run(
            ["docker", "ps", "-aq", "--filter", f"name=^{CONTAINER_PREFIX}"], capture_output=True, text=True
        ).stdout.split()
        if names:
            subprocess.run(["docker", "rm", "-f", *names], capture_output=True)
            leaked.append(f"containers {names}")
    assert not leaked, f"tests leaked processes or containers: {leaked}"
