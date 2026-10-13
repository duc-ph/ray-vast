"""Ray 2.59 autoscaler v2: our provider behind the real NodeProviderAdapter (signature and tag compatibility, $0).
The full autoscaler end-to-end test is step 11."""
import time

import pytest
import yaml
from ray.autoscaler.v2.instance_manager.config import FileConfigReader
from ray.autoscaler.v2.instance_manager.node_provider import NodeProviderAdapter

from ray_vast.config import bootstrap_config
from conftest import offer

pytestmark = pytest.mark.slow  # imports Ray and runs the provider threads

CLUSTER = {
    "cluster_name": "c1",
    "max_workers": 4,
    "provider": {"region": "us-west-2", "image": "img:1", "max_dollars_per_hour": 10},
    "head_node_type": "head",
    "available_node_types": {
        "head": {"resources": {"CPU": 0}, "node_config": {}},
        "gpu": {"resources": {"CPU": 4, "GPU": 1}, "min_workers": 0, "max_workers": 4,
                "node_config": {"offer_query": {"gpu_ram": {"gte": 24000}}, "disk_gb": 40}},
    },
}


def wait_for(condition, timeout=10):
    deadline = time.time() + timeout
    while not condition():
        assert time.time() < deadline, "timed out"
        time.sleep(0.05)


def test_adapter_launch_list_and_terminate(env, tmp_path):
    config_path = tmp_path / "cluster.yaml"
    config_path.write_text(yaml.safe_dump(bootstrap_config(CLUSTER)[0]))
    env.vast.offers = [offer(1, 11), offer(2, 12)]
    provider = env.make()
    adapter = NodeProviderAdapter(provider, FileConfigReader(str(config_path)))

    nodes = adapter.get_non_terminated()
    assert list(nodes) == ["head"] and nodes["head"].is_running

    adapter.launch({"gpu": 1}, "req-1")
    wait_for(lambda: len(adapter.get_non_terminated()) == 2)
    assert adapter.poll_errors() == []
    (slot_id,) = [i for i in adapter.get_non_terminated() if i != "head"]
    slot = adapter.get_non_terminated()[slot_id]
    assert slot.node_type == "gpu" and slot.request_id == "req-1" and not slot.is_running
    assert len(env.vast.instances) == 2

    adapter.terminate([slot_id], "req-2")
    wait_for(lambda: list(adapter.get_non_terminated()) == ["head"])
    assert env.vast.instances == {}

    env.vast.offers = []
    adapter.launch({"gpu": 1}, "req-3")
    wait_for(lambda: adapter.poll_errors())
    assert list(adapter.get_non_terminated()) == ["head"]
