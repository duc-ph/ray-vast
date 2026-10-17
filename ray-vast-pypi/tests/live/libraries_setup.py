"""The `libraries` clusters of the live suite (clusters/libraries.yaml, one cluster for each host H1, H2, H3, gpu2, so
the hosts run in parallel) and their controls: the cluster shape (which workers run), the host pins of H1 and H2, and the
node probe.

How a scenario gets its hosts. The head's autoscaler reads `/home/ubuntu/ray-vast-<cluster>/cluster.yaml` again in each
loop (Ray 2.59: autoscaler/v2/autoscaler.py calls `FileConfigReader.refresh_cached_autoscaling_config()` before each
reconcile, and node_provider.py takes the `node_config` of a launch from that config). `set_shape()` writes
min_workers = max_workers for every worker type there (`livekit.shaped_config`), and the offer query of a pinned type.
So the autoscaler starts exactly the workers of a scenario and ends the others; the library code needs no node pinning,
and no demand can start a worker of another type. A host is blocked by a `machine_id` filter in that query (the provider
itself does not block a destroyed host: C10, docs/test-results.md)."""
import json
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import yaml

from ray_vast.config import build_offer_query

import livekit
from cluster import MANUAL, Cluster, vast

HERE = Path(__file__).parent
JOBS = HERE / "jobs"
# The named hosts (archive/results/2026-10-14/c9, 2026-10-16/c10) and the query to use when none of them is rentable.
PINS = {
    # H1: m32816 (C9: 48 Ray CPUs) has a cgroup v1 CPU quota of 5.76 cores: since the quota fix it gives 5 Ray CPUs. A host
    # with many Ray CPUs needs many effective cores (the quota follows `cpu_cores_effective`, 2026-10-17).
    "h1": {"named": [], "fallback": {"cpu_cores_effective": {"gte": 40}}, "want": {"pids_max": 1280, "cgroup": "v1", "cpus": {"gte": 36}}},
    "h2": {"named": [51706, 55704], "fallback": {"gpu_ram": {"gte": 8000}}, "want": {"cgroup": "v2"}},
}
SHAPE_TIMEOUT_S = 2400  # a GPU image pull can take 10 min; a failed candidate is replaced


HOST_TYPE = {"H1": "h1", "H2": "h2", "H3": "gpu", "gpu2": "gpu2"}  # the worker type of each libraries host


def make_libraries_cluster(group_dir, home, host, suffix):
    """The Cluster of one libraries host (not up yet): libraries.yaml with only the head and the worker type of `host`,
    named `rvtest-lib-<host>-<suffix>`. For H1 and H2: search the vast offers of the named hosts first and record them in
    `<group_dir>/host_offers.json` (a host that is not rentable now is replaced by the fallback query at rent time)."""
    config = yaml.safe_load((HERE / "clusters" / "libraries.yaml").read_text())
    kind = HOST_TYPE[host]
    config["cluster_name"] = f"{config['cluster_name']}-{host.lower()}-{suffix}"
    config["available_node_types"] = {name: t for name, t in config["available_node_types"].items() if name in ("head", kind)}
    if kind in PINS:
        found = vast().search_offers(build_offer_query(config["available_node_types"][kind], False, kind))
        offers = [{k: o.get(k) for k in ("machine_id", "id", "gpu_name", "num_gpus", "cpu_cores", "cpu_cores_effective",
                                          "dph_total", "geolocation", "compute_cap")} for o in found]
        Path(group_dir).mkdir(parents=True, exist_ok=True)
        (Path(group_dir) / "host_offers.json").write_text(json.dumps({kind: offers}, indent=1))
    cluster = Cluster("libraries", config, group_dir, home)
    cluster.queries = {}   # the offer query of each type now in the head's YAML, when it differs from libraries.yaml
    cluster.matched = {}   # node type -> the host facts of the last host that matched (rented first next time)
    cluster.shape = {}
    return cluster


def head_yaml(cluster):
    return f"/home/ubuntu/ray-vast-{cluster.name}/cluster.yaml"


def set_shape(cluster, shape):
    """Write the shape into the head's YAML (atomic: a new file, then mv). The autoscaler acts on its next loop."""
    config = yaml.safe_load(cluster.ssh(f"cat {head_yaml(cluster)}"))
    text = yaml.safe_dump(livekit.shaped_config(config, shape, cluster.queries), sort_keys=False)
    subprocess.run([*cluster.ssh_args(), f"cat > {head_yaml(cluster)}.new && mv {head_yaml(cluster)}.new {head_yaml(cluster)}"],
                   input=text, text=True, capture_output=True, timeout=120, check=True)
    cluster.shape = dict(shape)
    with open(cluster.dir / "shapes.log", "a") as f:
        f.write(json.dumps({"t": round(time.time(), 1), "shape": shape, "queries": cluster.queries}) + "\n")


def alive_workers(cluster):
    """{node type: [Ray node id, ...]} of the ALIVE worker nodes (the node label `ray.io/node-group` is the node type)."""
    out = cluster.head_exec("ray list nodes --detail --format json --filter state=ALIVE --limit 1000", timeout=120)
    found = {}
    for node in json.loads(out[out.index("["):]):
        if not node.get("is_head_node"):
            found.setdefault((node.get("labels") or {}).get("ray.io/node-group"), []).append(node["node_id"])
    return found


def wait_shape(cluster, shape, timeout=SHAPE_TIMEOUT_S):
    """Wait until Ray has exactly the workers of `shape` and vast has no other instance of the cluster (the losing
    candidates are destroyed). Returns the seconds it took; raises TimeoutError."""
    want = {k: v for k, v in shape.items() if v}
    started, seen = time.time(), None
    while time.time() < started + timeout:
        try:
            nodes = {k: len(v) for k, v in alive_workers(cluster).items()}
            instances = len(cluster.instances())
            seen = (nodes, instances)
            if nodes == want and instances == sum(want.values()):
                return round(time.time() - started, 1)
        except Exception as e:  # noqa: BLE001 - one failed sample must not end the wait
            seen = f"{type(e).__name__}: {str(e)[:200]}"
        time.sleep(15)
    raise TimeoutError(f"shape {shape} not reached in {timeout} s; last seen {seen}")


def ensure_shape(cluster, shape):
    set_shape(cluster, shape)
    return wait_shape(cluster, shape)


def workdir(*files):
    """A temp working directory for `ray job submit` with these files."""
    d = Path(tempfile.mkdtemp(prefix="rvjob-"))
    for f in files:
        shutil.copy(f, d / Path(f).name)
    return d


def probe(cluster, env, out_dir, name="probe"):
    """jobs/node_probe.py on every worker: {node type: [NODE_PROBE row, ...]}, each row with its `machine_id`."""
    status, log = cluster.job(env, workdir(JOBS / "node_probe.py", MANUAL / "c10" / "jobs" / "c10common.py"), ["python", "node_probe.py"],
                              out_dir=out_dir, name=name, timeout=900)
    machines = {str(i["id"]): i.get("machine_id") for i in cluster.instances()}
    nodes = {}
    for line in log.splitlines():
        if line.startswith("NODE_PROBE "):
            row = json.loads(line.split(" ", 1)[1])
            row["machine_id"] = machines.get(str(row.get("container_id")))
            nodes.setdefault(row.get("node_type"), []).append(row)
    if status != "SUCCEEDED":
        raise RuntimeError(f"node probe {name}: {status}")
    return nodes


def pin_query(cluster, kind, blocked):
    """The offer query of `kind` for the next rental: its named hosts (the last match first) that are rentable now and not
    blocked; else the fallback query (for other types: the query of libraries.yaml) without the blocked machines."""
    node_type = cluster.config["available_node_types"][kind]
    pin = PINS.get(kind)
    if pin:
        last = cluster.matched.get(kind, {}).get("machine_id")
        named = [m for m in dict.fromkeys([last, *pin["named"]]) if m is not None and m not in blocked]
        if named:
            query = {"machine_id": {"in": named}}
            if vast().search_offers(build_offer_query({**node_type, "node_config": {**node_type["node_config"], "offer_query": query}},
                                                      False, kind)):
                return query
        query = dict(pin["fallback"])
    else:
        query = dict(node_type["node_config"]["offer_query"])
    if blocked:
        query["machine_id"] = {"notin": sorted(set(query.get("machine_id", {}).get("notin", [])) | set(blocked))}
    return query


def find(cluster, env, kind, want, keep, out_dir):
    """Rent one worker of `kind` next to the shape `keep` until its probe matches `want` (livekit.find_host: at most 3
    rentals; each mismatch is ended and its machine blocked). Leaves the shape keep + {kind: 1}. Returns (facts, tries);
    facts has the full NODE_PROBE row as `probe`."""
    def rent(blocked):
        cluster.queries[kind] = pin_query(cluster, kind, blocked)
        ensure_shape(cluster, {**keep, kind: 1})
        row = probe(cluster, env, out_dir, name=f"probe_{kind}_{len(blocked)}")[kind][0]
        return dict(livekit.host_facts(row, row["machine_id"]), query=cluster.queries[kind], probe=row)

    def end(facts):
        ensure_shape(cluster, keep)

    facts, tries = livekit.find_host(want, rent, end)
    cluster.matched[kind] = facts
    return facts, tries


def b_and_cpus(rows):
    """(B, CPUS) for the workers of one scenario: B = the smallest worker ports - 4; CPUS = the largest Ray CPUs (core.md)."""
    return min(r["ports"] for r in rows) - 4, max(r["ray_cpus"] for r in rows)

