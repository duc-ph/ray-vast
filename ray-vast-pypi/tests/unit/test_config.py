import copy
import pathlib

import pytest
import requests
import yaml

from ray_vast.config import bootstrap_config, build_offer_query, load_and_bootstrap

EXAMPLES = sorted(pathlib.Path(__file__).parents[2].joinpath("src/ray_vast/examples").glob("*.yaml"))

BASE = {
    "cluster_name": "t",
    "provider": {"region": "us-west-2", "image": "img", "max_dollars_per_hour": 2},
    "head_node_type": "head",
    "available_node_types": {
        "head": {"resources": {"CPU": 0}, "node_config": {}},
        "w": {
            "resources": {"CPU": 4, "GPU": 2},
            "node_config": {"offer_query": {"gpu_ram": {"gte": 24000}}},
        },
    },
}


def make(**edits):
    """BASE with dotted-path edits, for example make(**{"provider.region": None}) deletes the key."""
    config = copy.deepcopy(BASE)
    for path, value in edits.items():
        *parents, key = path.split(".")
        target = config
        for parent in parents:
            target = target[parent]
        if value is None:
            del target[key]
        else:
            target[key] = value
    return config


def query(allow_non_datacenter=False, **node_config):
    node_type = {"resources": {"CPU": 4, "GPU": 2}, "node_config": node_config}
    return build_offer_query(node_type, allow_non_datacenter, "w")


def test_offer_query_adds_gpu_count_filter():
    assert query(offer_query={})["num_gpus"] == {"eq": 2}


def test_offer_query_adds_cpu_floor_filter():
    assert query(offer_query={})["cpu_cores_effective"] == {"gte": 4}


def test_offer_query_adds_port_count_filter():
    assert query(offer_query={})["direct_port_count"] == {"gte": 64}


def test_offer_query_adds_datacenter_filter():
    assert query(offer_query={})["datacenter"] == {"eq": True}


def test_offer_query_leaves_datacenter_out_when_non_datacenter_allowed():
    assert "datacenter" not in query(True, offer_query={})


def test_offer_query_adds_verified_and_rentable_filters():
    result = query(offer_query={})
    assert result["verified"] == {"eq": True}
    assert result["rentable"] == {"eq": True}


def test_offer_query_keeps_user_filters():
    result = query(offer_query={"gpu_ram": {"gte": 24000, "lte": 32000}, "dlperf": {"gte": 40}})
    assert result["gpu_ram"] == {"gte": 24000, "lte": 32000}
    assert result["dlperf"] == {"gte": 40}


def test_offer_query_accepts_user_filter_equal_to_added_filter():
    assert query(offer_query={"num_gpus": {"eq": 2}})["num_gpus"] == {"eq": 2}


def test_offer_query_keeps_stricter_cpu_filter_with_upper_bound():
    result = query(offer_query={"cpu_cores_effective": {"gte": 8, "lt": 16}})
    assert result["cpu_cores_effective"] == {"gte": 8, "lt": 16}


def test_offer_query_keeps_higher_cpu_floor():
    node_type = {"resources": {"CPU": 8, "GPU": 1}, "node_config": {"offer_query": {"cpu_cores_effective": {"gte": 16}}}}
    assert build_offer_query(node_type, False, "w")["cpu_cores_effective"] == {"gte": 16}


def test_offer_query_accepts_cpu_gt_at_or_above_floor():
    assert query(offer_query={"cpu_cores_effective": {"gt": 4}})["cpu_cores_effective"] == {"gt": 4}


def test_offer_query_rejects_looser_cpu_filter():
    node_type = {"resources": {"CPU": 8, "GPU": 1}, "node_config": {"offer_query": {"cpu_cores_effective": {"gte": 4}}}}
    with pytest.raises(ValueError, match=r"'w'.*'cpu_cores_effective'.*looser"):
        build_offer_query(node_type, False, "w")


def test_offer_query_accepts_higher_port_floor():
    assert query(offer_query={"direct_port_count": {"gte": 100}})["direct_port_count"] == {"gte": 100}


def test_offer_query_rejects_looser_port_filter():
    with pytest.raises(ValueError, match=r"'w'.*'direct_port_count'"):
        query(offer_query={"direct_port_count": {"gte": 32}})


def test_offer_query_accepts_gpu_count_as_in_list():
    assert query(offer_query={"num_gpus": {"in": [2]}})["num_gpus"] == {"eq": 2}


def test_offer_query_rejects_gpu_count_in_wider_list():
    with pytest.raises(ValueError, match=r"'w'.*'num_gpus'"):
        query(offer_query={"num_gpus": {"in": [2, 4]}})


def test_offer_query_rejects_non_numeric_ram():
    with pytest.raises(ValueError, match=r"'w'.*'gpu_ram'"):
        query(offer_query={"gpu_ram": {"gte": "24GB"}})


def test_offer_query_rejects_non_list_in_value():
    with pytest.raises(ValueError, match=r"'w'.*'geolocation'.*list"):
        query(offer_query={"geolocation": {"in": "US"}})


def test_offer_query_rejects_list_for_scalar_operator():
    with pytest.raises(ValueError, match=r"'w'.*'dlperf'.*scalar"):
        query(offer_query={"dlperf": {"gte": [40]}})


def test_offer_query_accepts_cpu_eq_at_or_above_floor():
    assert query(offer_query={"cpu_cores_effective": {"eq": 4}})["cpu_cores_effective"] == {"eq": 4}


def test_offer_query_accepts_cpu_in_list_above_floor():
    assert query(offer_query={"cpu_cores_effective": {"in": [4, 8]}})["cpu_cores_effective"] == {"in": [4, 8]}


def test_offer_query_rejects_cpu_in_list_with_value_below_floor():
    with pytest.raises(ValueError, match=r"'w'.*'cpu_cores_effective'"):
        query(offer_query={"cpu_cores_effective": {"in": [2, 8]}})


def test_offer_query_rejects_cpu_eq_below_floor():
    with pytest.raises(ValueError, match=r"'w'.*'cpu_cores_effective'"):
        query(offer_query={"cpu_cores_effective": {"eq": 2}})


@pytest.mark.parametrize(
    "condition",
    [{"gte": 8, "lt": 4}, {"gt": 8, "lte": 8}, {"gte": 8, "lt": 8}, {"gt": 5, "lt": 5}],
)
def test_offer_query_rejects_empty_range(condition):
    with pytest.raises(ValueError, match=r"'w'.*'dlperf'.*never match"):
        query(offer_query={"dlperf": condition})


def test_offer_query_accepts_single_point_range():
    assert query(offer_query={"dlperf": {"gte": 8, "lte": 8}})["dlperf"] == {"gte": 8, "lte": 8}


def test_offer_query_rejects_empty_range_on_added_field():
    with pytest.raises(ValueError, match=r"'w'.*'cpu_cores_effective'.*never match"):
        query(offer_query={"cpu_cores_effective": {"gte": 8, "lt": 4}})


def test_offer_query_rejects_contradicting_gpu_count():
    with pytest.raises(ValueError, match=r"'w'.*'num_gpus'"):
        query(offer_query={"num_gpus": {"eq": 1}})


def test_offer_query_rejects_contradicting_datacenter():
    with pytest.raises(ValueError, match=r"'w'.*'datacenter'"):
        query(offer_query={"datacenter": {"eq": False}})


def test_offer_query_rejects_unknown_operator():
    with pytest.raises(ValueError, match=r"'w'.*'dlperf'.*'min'"):
        query(offer_query={"dlperf": {"min": 40}})


@pytest.mark.parametrize("field", ["gpu_ram", "cpu_ram", "gpu_total_ram"])
def test_offer_query_rejects_values_in_gb(field):
    with pytest.raises(ValueError, match=rf"'w'.*'{field}'.*unit is MB"):
        query(offer_query={field: {"gte": 24}})


def test_offer_query_rejects_gb_values_inside_in_list():
    with pytest.raises(ValueError, match="unit is MB"):
        query(offer_query={"gpu_ram": {"in": [24, 48000]}})


def test_bootstrap_accepts_two_cpu_tiers_with_own_core_bands():
    config = make()
    types = config["available_node_types"]
    del types["w"]
    for type_name, cpu, band in [("gpu24_c8", 8, {"gte": 8, "lt": 16}), ("gpu24_c16", 16, {"gte": 16})]:
        types[type_name] = {
            "resources": {"CPU": cpu, "GPU": 1},
            "node_config": {"offer_query": {"gpu_ram": {"gte": 24000}, "cpu_cores_effective": band}},
        }
    out, warnings = bootstrap_config(config)
    assert out["available_node_types"]["gpu24_c8"]["node_config"]["offer_query"]["cpu_cores_effective"] == {"gte": 8, "lt": 16}
    assert warnings == []


def test_bootstrap_sets_external_provider():
    config, warnings = bootstrap_config(make())
    assert config["provider"]["type"] == "external"
    assert config["provider"]["module"] == "ray_vast.provider.VastNodeProvider"
    assert config["provider"]["disable_node_updaters"] is True
    assert "use_node_id_as_ip" not in config["provider"]
    assert "disable_launch_config_check" not in config["provider"]
    assert warnings == []


def test_bootstrap_fills_defaults():
    config, _ = bootstrap_config(make())
    provider = config["provider"]
    assert (provider["candidate_factor"], provider["cert_max_age_days"]) == (2, 365)
    assert (provider["allow_non_datacenter"], provider["order_by"]) == (False, "dph_total")
    assert provider["head_instance_type"] == "t3.large"
    assert config["available_node_types"]["w"]["node_config"]["disk_gb"] == 50
    assert config["auth"]["ssh_user"] == "ray"


def test_bootstrap_keeps_user_disk_size():
    config = make()
    config["available_node_types"]["w"]["node_config"]["disk_gb"] = 120
    assert bootstrap_config(config)[0]["available_node_types"]["w"]["node_config"]["disk_gb"] == 120


def test_bootstrap_does_not_change_its_input():
    config = make()
    bootstrap_config(config)
    assert config == BASE


@pytest.mark.parametrize("field", ["region", "image", "max_dollars_per_hour"])
def test_bootstrap_requires_provider_field(field):
    with pytest.raises(ValueError, match=f"provider.{field}"):
        bootstrap_config(make(**{f"provider.{field}": None}))


@pytest.mark.parametrize("budget", [0, -1, "5", True])
def test_bootstrap_rejects_bad_budget(budget):
    with pytest.raises(ValueError, match="max_dollars_per_hour"):
        bootstrap_config(make(**{"provider.max_dollars_per_hour": budget}))


def test_bootstrap_requires_head_resources():
    config = make()
    del config["available_node_types"]["head"]["resources"]
    with pytest.raises(ValueError, match=r"'head'.*resources"):
        bootstrap_config(config)


def test_bootstrap_rejects_head_with_cpu():
    config = make()
    config["available_node_types"]["head"]["resources"] = {"CPU": 2}
    with pytest.raises(ValueError, match=r"'head'.*CPU must be 0"):
        bootstrap_config(config)


def test_bootstrap_requires_offer_query_on_workers():
    config = make()
    config["available_node_types"]["w"]["node_config"] = {}
    with pytest.raises(ValueError, match=r"'w'.*offer_query"):
        bootstrap_config(config)


@pytest.mark.parametrize("missing", ["CPU", "GPU"])
def test_bootstrap_requires_worker_resources(missing):
    config = make()
    del config["available_node_types"]["w"]["resources"][missing]
    with pytest.raises(ValueError, match=rf"'w'.*{missing}"):
        bootstrap_config(config)


def test_bootstrap_rejects_resources_that_contradict_the_query():
    config = make()
    config["available_node_types"]["w"]["node_config"]["offer_query"]["num_gpus"] = {"eq": 1}
    with pytest.raises(ValueError, match=r"'w'.*'num_gpus'"):
        bootstrap_config(config)


def test_bootstrap_warns_when_cpu_is_above_worker_ports():
    config = make()
    config["available_node_types"]["w"]["resources"]["CPU"] = 59
    _, warnings = bootstrap_config(config)
    assert len(warnings) == 1 and "'w'" in warnings[0] and "58" in warnings[0]


def test_bootstrap_warns_on_non_datacenter():
    _, warnings = bootstrap_config(make(**{"provider.allow_non_datacenter": True}))
    assert len(warnings) == 1 and "allow_non_datacenter" in warnings[0]


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.name)
def test_example_passes_validation_and_ray_validate_config(path):
    from ray.autoscaler._private.util import validate_config

    config, warnings = load_and_bootstrap(path)
    validate_config(config)
    assert warnings == []


def test_examples_exist():
    assert len(EXAMPLES) >= 3


@pytest.mark.live
@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.name)
def test_live_example_query_returns_offers(path):
    config, _ = load_and_bootstrap(path)
    for name, node_type in config["available_node_types"].items():
        if name == config["head_node_type"]:
            continue
        body = build_offer_query(node_type, config["provider"]["allow_non_datacenter"], name)
        response = requests.post(
            "https://console.vast.ai/api/v0/bundles/",
            json={**body, "limit": 5, "type": "on-demand"},
            timeout=30,
        )
        response.raise_for_status()
        assert len(response.json()["offers"]) >= 1, name
