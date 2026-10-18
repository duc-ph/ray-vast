# Placement groups: PACK of 4 CPU bundles, then STRICT_SPREAD of 2 GPU bundles.
# PASS: PACK puts the 4 bundles on one node, and STRICT_SPREAD puts the 2 GPU bundles on 2 different nodes.
import ray
from ray.util.placement_group import placement_group, placement_group_table, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from jobcommon import GPU, MIN_NODES, SMALL, error_info, node_info, result, short, start, wait_for_nodes

ID = "placement"
start(ID, 900)
wait_for_nodes(ID, gpus_per_node=GPU)


@ray.remote(num_cpus=0)
def where():
    return node_info()


def run_group(bundles, strategy):
    group = placement_group(bundles, strategy=strategy)
    ray.get(group.ready(), timeout=300)
    rows = ray.get([where.options(scheduling_strategy=PlacementGroupSchedulingStrategy(group, placement_group_bundle_index=i)).remote()
                    for i in range(len(bundles))], timeout=120)
    table_nodes = placement_group_table(group)["bundles_to_node_id"]
    remove_placement_group(group)
    return [short(r["node_id"]) for r in rows], {k: short(v) for k, v in table_nodes.items()}


try:
    pack_nodes, pack_table = run_group([{"CPU": 1}] * (2 if SMALL else 4), "PACK")
    spread_nodes, spread_table = run_group([{"CPU": 1, **({"GPU": GPU} if GPU else {})}] * MIN_NODES, "STRICT_SPREAD")
except Exception as e:
    result(ID, "FAIL", error_info(e))
data = {"pack_task_nodes": pack_nodes, "pack_bundles": pack_table, "spread_task_nodes": spread_nodes, "spread_bundles": spread_table}
ok = len(set(pack_nodes)) == 1 and len(set(spread_nodes)) == MIN_NODES
result(ID, "PASS" if ok else "FAIL", data)
