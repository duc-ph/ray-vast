import copy
import yaml

QUERY_OPS = {"eq", "neq", "gt", "gte", "lt", "lte", "in", "notin"}
MB_FIELDS = ("gpu_ram", "cpu_ram", "gpu_total_ram")
WORKER_PORTS = 58
# Ray's validate_config() needs these; ray-vast does not use them (no node updaters).
RAY_EMPTY_FIELDS = (
    "file_mounts",
    "cluster_synced_files",
    "initialization_commands",
    "setup_commands",
    "head_setup_commands",
    "worker_setup_commands",
    "head_start_ray_commands",
    "worker_start_ray_commands",
)
PROVIDER_DEFAULTS = {
    "candidate_factor": 2,
    "cert_max_age_days": 365,
    "allow_non_datacenter": False,
    "order_by": "dph_total",
    "head_instance_type": "t3.large",
}


def build_offer_query(node_type_config, allow_non_datacenter, node_type="?"):
    """Return the vast API query: the user's offer_query plus the filters that ray-vast adds."""
    resources = node_type_config["resources"]
    user_query = node_type_config.get("node_config", {}).get("offer_query") or {}
    for field, condition in user_query.items():
        if not isinstance(condition, dict) or not condition:
            raise ValueError(f"node type {node_type!r}: offer_query field {field!r} must be a mapping of operator to value")
        for op, value in condition.items():
            if op not in QUERY_OPS:
                raise ValueError(f"node type {node_type!r}: offer_query field {field!r} has unknown operator {op!r}")
            if field in MB_FIELDS and any(v < 1000 for v in (value if isinstance(value, list) else [value])):
                raise ValueError(f"node type {node_type!r}: offer_query field {field!r} value {value} is below 1000; the unit is MB")

    added = {
        "num_gpus": {"eq": resources["GPU"]},
        "cpu_cores_effective": {"gte": resources["CPU"]},
        "direct_port_count": {"gte": 64},
        "verified": {"eq": True},
        "rentable": {"eq": True},
    }
    if not allow_non_datacenter:
        added["datacenter"] = {"eq": True}
    merged = dict(user_query)
    for field, condition in added.items():
        user = user_query.get(field)
        if user is None:
            merged[field] = condition
        elif field in ("cpu_cores_effective", "direct_port_count"):
            floor = condition["gte"]
            if not (user.get("gte", 0) >= floor or user.get("gt", 0) >= floor):
                raise ValueError(
                    f"node type {node_type!r}: offer_query field {field!r} is {user}, "
                    f"which is looser than the added filter {condition}"
                )
        elif user != condition and not (field == "num_gpus" and user == {"in": [condition["eq"]]}):
            raise ValueError(
                f"node type {node_type!r}: offer_query field {field!r} is {user}, "
                f"but ray-vast sets it to {condition} from the node type"
            )
        else:
            merged[field] = condition
    return merged


def bootstrap_config(config):
    """Check a cluster config and return (new config, warnings). Raises ValueError on the first error."""
    config = copy.deepcopy(config)
    warnings = []

    if not config.get("cluster_name"):
        raise ValueError("cluster_name is required")
    provider = config.get("provider") or {}
    for field in ("region", "image", "max_dollars_per_hour"):
        if field not in provider:
            raise ValueError(f"provider.{field} is required")
    budget = provider["max_dollars_per_hour"]
    if isinstance(budget, bool) or not isinstance(budget, (int, float)) or budget <= 0:
        raise ValueError(f"provider.max_dollars_per_hour must be a number above 0, got {budget!r}")
    for field, default in PROVIDER_DEFAULTS.items():
        provider.setdefault(field, default)
    provider.update(
        type="external",
        module="ray_vast.provider.VastNodeProvider",
        disable_node_updaters=True,
    )
    config["provider"] = provider
    for field in RAY_EMPTY_FIELDS:
        config.setdefault(field, [] if field.endswith(("commands", "files")) else {})
    config.setdefault("auth", {}).setdefault("ssh_user", "ray")
    if provider["allow_non_datacenter"]:
        warnings.append("provider.allow_non_datacenter is true: non-datacenter hosts are allowed")

    node_types = config.get("available_node_types") or {}
    head_type = config.get("head_node_type")
    if head_type not in node_types:
        raise ValueError(f"head_node_type {head_type!r} is not in available_node_types")
    head_resources = node_types[head_type].get("resources")
    if head_resources is None:
        raise ValueError(f"node type {head_type!r} (head): resources is required, with CPU: 0")
    if head_resources.get("CPU") != 0:
        raise ValueError(f"node type {head_type!r} (head): resources.CPU must be 0, got {head_resources.get('CPU')!r}")

    for name, node_type in node_types.items():
        if name == head_type:
            continue
        resources = node_type.get("resources") or {}
        for field in ("CPU", "GPU"):
            if not isinstance(resources.get(field), int) or isinstance(resources.get(field), bool) or resources[field] < 0:
                raise ValueError(f"node type {name!r}: resources.{field} is required and must be a whole number")
        node_config = node_type.setdefault("node_config", {})
        if not node_config.get("offer_query"):
            raise ValueError(f"node type {name!r}: node_config.offer_query is required")
        node_config.setdefault("disk_gb", 50)
        build_offer_query(node_type, provider["allow_non_datacenter"], name)
        if resources["CPU"] > WORKER_PORTS:
            warnings.append(f"node type {name!r}: resources.CPU {resources['CPU']} is above the {WORKER_PORTS} worker ports")

    return config, warnings


def load_and_bootstrap(path):
    with open(path) as f:
        return bootstrap_config(yaml.safe_load(f))
