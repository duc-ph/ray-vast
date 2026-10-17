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
