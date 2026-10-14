# The offer search of the provider for one node type of a cluster YAML (docs/test-plan.md, "Scripts").
# It uses the same query (ray_vast.config.build_offer_query) and the same search call (VastClient.search_offers:
# type on-demand, limit 100, sorted by provider.order_by) as the provider. Runs on the operator machine.
# Prints one JSON document: the query, the number of offers and of distinct machine_ids, and the cheapest offers.
# Usage: python offers.py <cluster.yaml> <node type> [<how many, default 12>]
import json
import os
import sys
from pathlib import Path

from ray_vast.config import build_offer_query, load_and_bootstrap
from ray_vast.vast_api import VastClient

FIELDS = ("id", "machine_id", "gpu_name", "num_gpus", "gpu_ram", "cpu_cores_effective", "cpu_ram", "dph_total",
          "inet_down", "inet_up", "geolocation", "static_ip", "direct_port_count", "reliability2")

config, _ = load_and_bootstrap(sys.argv[1])
name, how_many = sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 12
provider = config["provider"]
query = build_offer_query(config["available_node_types"][name], provider["allow_non_datacenter"], name)
key = os.environ.get("VAST_API_KEY") or (Path.home() / ".config/vastai/vast_api_key").read_text()
offers = VastClient(key.strip()).search_offers(query, provider["order_by"])
print(json.dumps({
    "node_type": name, "query": query, "order_by": provider["order_by"], "max_dollars_per_hour": provider["max_dollars_per_hour"],
    "count": len(offers), "machines": len({o["machine_id"] for o in offers}),
    "offers": [{k: o.get(k) for k in FIELDS} for o in offers[:how_many]]}, indent=1))
