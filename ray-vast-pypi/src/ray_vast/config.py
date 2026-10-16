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
    "allow_non_datacenter": False,
    "order_by": "dph_total",
}
HEAD_INSTANCE_TYPE = "t3.large"
PORT_A, PORT_B = 7443, 7444  # head server ports: download and enrollment (TLS); heartbeat and reports (mTLS)
# Provider fields of older versions. A YAML that sets one gets an error that says what to do instead.
OLD_PROVIDER_FIELDS = {
    "region": "moved: set node_config.region of the head node type",
    "head_instance_type": "moved: set node_config.instance_type of the head node type",
    "port_a": f"removed: the head server always uses port {PORT_A}",
    "port_b": f"removed: the head server always uses port {PORT_B}",
    "cert_max_age_days": "removed: the CA and the certificates always live 365 days",
    "image_size_gb": "removed: `ray-vast up` measures the image",
    "key_name": "removed: `ray-vast up` makes an EC2 key pair for each cluster",
    "key_file": "removed: `ray-vast up` makes an EC2 key pair for each cluster",
}


def build_offer_query(node_type_config, allow_non_datacenter, name):
    """Return the vast API query: the user's offer_query plus the filters that ray-vast adds."""
    resources = node_type_config["resources"]
    user_query = node_type_config.get("node_config", {}).get("offer_query") or {}
    for field, condition in user_query.items():
        where = f"node type {name!r}: offer_query field {field!r}"
        if not isinstance(condition, dict) or not condition:
            raise ValueError(f"{where} must be a mapping of operator to value")
        for op, value in condition.items():
            if op not in QUERY_OPS:
                raise ValueError(f"{where} has unknown operator {op!r}")
            if op in ("in", "notin") and not isinstance(value, list):
                raise ValueError(f"{where}: operator {op!r} needs a list, got {value!r}")
            if op not in ("in", "notin") and isinstance(value, (list, dict)):
                raise ValueError(f"{where}: operator {op!r} needs a scalar, got {value!r}")
            if field in MB_FIELDS:
                for v in value if isinstance(value, list) else [value]:
                    if isinstance(v, bool) or not isinstance(v, (int, float)):
                        raise ValueError(f"{where}: value {v!r} is not a number")
                    if v < 1000:
                        raise ValueError(f"{where}: value {v} is below 1000; the unit is MB")
        for lower_op in ("gte", "gt"):
            for upper_op in ("lte", "lt"):
                if lower_op in condition and upper_op in condition:
                    lower, upper = condition[lower_op], condition[upper_op]
                    strict = lower_op == "gt" or upper_op == "lt"
                    if isinstance(lower, (int, float)) and isinstance(upper, (int, float)) and (lower > upper or (strict and lower == upper)):
                        raise ValueError(f"{where}: {condition} can never match")

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
        where = f"node type {name!r}: offer_query field {field!r}"
        if user is None:
            merged[field] = condition
        elif field in ("cpu_cores_effective", "direct_port_count"):
            floor = condition["gte"]
            eq_or_in = [user["eq"]] if "eq" in user else user.get("in")
            at_or_above = lambda v: isinstance(v, (int, float)) and v >= floor
            if not (at_or_above(user.get("gte")) or at_or_above(user.get("gt")) or (eq_or_in and all(at_or_above(v) for v in eq_or_in))):
                raise ValueError(f"{where} is {user}, which is looser than the added filter {condition}")
        elif user != condition and not (field == "num_gpus" and user == {"in": [condition["eq"]]}):
            raise ValueError(f"{where} is {user}, but ray-vast sets it to {condition} from the node type")
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
    check_old_provider_fields(provider)
    for field in ("image", "max_dollars_per_hour"):
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
    head_node_config = node_types[head_type].setdefault("node_config", {})
    if not head_node_config.get("region"):
        raise ValueError(f"node type {head_type!r} (head): node_config.region is required (the AWS region of the head)")
    head_node_config.setdefault("instance_type", HEAD_INSTANCE_TYPE)
    # Ray's external provider has no default for the top-level max_workers. Ray counts a missing per-type value as 0.
    config.setdefault("max_workers", sum(t.get("max_workers", 0) for name, t in node_types.items() if name != head_type))

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


def check_old_provider_fields(provider):
    """Raise ValueError for a provider field that an older version had (`OLD_PROVIDER_FIELDS`)."""
    for field, what_to_do in OLD_PROVIDER_FIELDS.items():
        if field in provider:
            raise ValueError(f"provider.{field} is no longer a field ({what_to_do})")


def load_and_bootstrap(path):
    with open(path) as f:
        return bootstrap_config(yaml.safe_load(f))
