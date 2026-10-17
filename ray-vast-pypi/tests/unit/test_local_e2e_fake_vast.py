"""The fake vast server of the local end-to-end tests (tests/local_e2e/fake_vast_server.py), without Docker."""
import subprocess
import threading
import time

from local_e2e import fake_vast_server
from local_e2e.fake_vast_server import FakeVast


def test_two_deletes_of_one_instance_at_the_same_time_give_one_destroy_and_one_404(monkeypatch):
    """F08 (2026-10-17): the worker destroyed itself (its instance key) while the provider destroyed it (the account key).
    The second DELETE raised KeyError in the fake, so the provider saw a connection error. vast answers 404 for an
    instance that is gone, and the provider counts that as done."""
    def slow_docker(args, **kwargs):  # `docker logs` and `docker rm -f` take some time
        time.sleep(0.2)
        return subprocess.CompletedProcess(args, 0, "", "")
    monkeypatch.setattr(fake_vast_server.subprocess, "run", slow_docker)
    fake = FakeVast("192.0.2.7", [], "st11-test-", "127.0.0.1", "none")
    fake.start()
    try:
        offer = {"id": 500, "machine_id": 900, "dph_total": 0.1}
        fake.instances[1003] = dict(key="instance-key", offer=offer, name="st11-test-w-1003", block=20000, label="ray-vast:c1")
        answers = []
        calls = [threading.Thread(target=lambda key=key: answers.append(fake.handle("DELETE", "/instances/1003/", key, None)[0]))
                 for key in ("instance-key", fake.account_key)]
        for call in calls:
            call.start()
        for call in calls:
            call.join()
        assert sorted(answers) == [200, 404] and [d[0] for d in fake.destroys] == [1003] and fake.live_ids() == []
    finally:
        fake.stop()


def test_the_destroy_log_has_the_last_line_that_the_container_wrote_before_its_delete(monkeypatch):
    """F14 (2026-10-17): under load, dockerd puts a container's output into its log some time after the container
    writes it. The fake read `docker logs` at the DELETE, so the bootstrap's last lines ("destroying this instance: ...",
    "destroy report failed") were sometimes not in the log yet. Docker has the full log when the container has stopped.
    This fake Docker gives the last line only after a `docker kill` of the container."""
    stopped = set()

    def lagging_docker(args, **kwargs):
        name = args[-1]
        if args[1] == "kill":
            stopped.add(name)
        out = "bootstrap: started ray\n"
        if name in stopped:
            out += "bootstrap: destroying this instance: ray start exited with code 1\n"
        return subprocess.CompletedProcess(args, 0, out if args[1] == "logs" else "", "")
    monkeypatch.setattr(fake_vast_server.subprocess, "run", lagging_docker)
    fake = FakeVast("192.0.2.7", [], "st11-test-", "127.0.0.1", "none")
    fake.start()
    try:
        offer = {"id": 500, "machine_id": 900, "dph_total": 0.1}
        fake.instances[1003] = dict(key="instance-key", offer=offer, name="st11-test-w-1003", block=20000, label="ray-vast:c1")
        assert fake.handle("DELETE", "/instances/1003/", "instance-key", None)[0] == 200
        ((_, who, log),) = fake.destroys
        assert who == "instance" and "destroying this instance: ray start exited with code 1" in log, log
    finally:
        fake.stop()
