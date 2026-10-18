"""The offer queries of the example cluster YAMLs and of docs/how-to/node-types.md against the public vast offer search.
It needs a vast key but rents nothing: no cost. About 1 min."""
import json
import re
from pathlib import Path

import yaml
from ray_vast.config import bootstrap_config, build_offer_query

from cluster import vast

PKG = Path(__file__).resolve().parents[2]


def doc_queries():
    """Every YAML block with an `offer_query` in node-types.md."""
    text = (PKG / "docs/how-to/node-types.md").read_text()
    found = []
    for n, block in enumerate(re.findall(r"```yaml\n(.*?)```", text, re.S)):
        try:
            data = yaml.safe_load(block)
        except yaml.YAMLError:
            continue
        stack = [data]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                if "offer_query" in item and "resources" in item:
                    found.append((f"node-types block {n}", item))
                stack.extend(item.values())
    return found


def test_the_example_and_doc_offer_queries_find_offers_within_the_budget(record):
    client, rows = vast(), []
    for path in sorted((PKG / "src/ray_vast/examples").glob("*.yaml")):
        config, _ = bootstrap_config(yaml.safe_load(path.read_text()))
        provider = config["provider"]
        for name, node_type in config["available_node_types"].items():
            if name == config["head_node_type"]:
                continue
            offers = client.search_offers(build_offer_query(node_type, name))
            machines = {o["machine_id"] for o in offers}
            first = sorted(o["dph_total"] for o in offers)[:provider["candidate_factor"]]
            rows.append({"source": path.name, "type": name, "offers": len(offers), "machines": len(machines),
                         "cheapest": [round(x, 3) for x in first], "budget": provider["max_dollars_per_hour"]})
            record.check(f"{path.name} {name}: >= 4 offers on >= 3 machines", len(offers) >= 4 and len(machines) >= 3, rows[-1])
            record.check(f"{path.name} {name}: the first candidates fit the budget", sum(first) <= provider["max_dollars_per_hour"], rows[-1])
    for source, node_type in doc_queries():
        offers = client.search_offers(build_offer_query(node_type, source))
        rows.append({"source": source, "query": node_type["node_config"]["offer_query"], "offers": len(offers)})
        record.check(f"{source}: vast accepts the query", isinstance(offers, list), len(offers))
    record.evidence("offers.json", json.dumps(rows, indent=1))
