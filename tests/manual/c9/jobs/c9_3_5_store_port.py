# C9.3.5 (extra, for the C9.3.2/C9.3.3 LIMIT): can a user make the PyTorch rendezvous use a reachable port?
# Ray Train has no setting for MASTER_PORT, so this uses Ray Core: 2 actors on 2 nodes (STRICT_SPREAD) set up
# torch.distributed by hand. Rank 0 starts a TCPStore on a free port of the worker-port-list (an identity port that vast
# maps; read from VAST_TCP_PORT_700xx). Rank 1 connects to the public IP of rank 0 on that port. Then both call
# init_process_group("gloo", store=...) and run an all_reduce. Each stage is recorded: store, gloo init, all_reduce.
# LIMIT: the store works but gloo fails (gloo opens its own connections on random ports and advertises the interface IP).
import os
import socket
import time

import ray
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from c9common import error_info, result, short, start, wait_for_nodes

ID = "C9.3.5"
start(ID, 900)
wait_for_nodes(ID)


@ray.remote(num_cpus=1)
class Rank:
    def info(self):
        return {"node": ray.get_runtime_context().get_node_id(), "ip": ray.util.get_node_ip_address(),
                "private_ip": os.environ.get("RAYVAST_PRIVATE_IP"), "hostname": socket.gethostname(),
                "has_vast_ports": "VAST_TCP_PORT_70063" in os.environ}

    def free_mapped_port(self):
        """The highest worker-port-list port (identity port P) that is free now."""
        for i in range(63, 5, -1):
            port = int(os.environ[f"VAST_TCP_PORT_{70000 + i}"])
            with socket.socket() as s:
                try:
                    s.bind(("0.0.0.0", port))
                    return port
                except OSError:
                    continue
        return None

    def store(self, host, port, rank):
        import torch.distributed as dist
        t = time.time()
        try:
            self._store = dist.TCPStore(host, port, 2, rank == 0, timeout=__import__("datetime").timedelta(seconds=60),
                                        wait_for_workers=False)
            self._store.set(f"hello{rank}", str(rank))
            self._store.wait([f"hello{1 - rank}"])
            return {"ok": True, "s": round(time.time() - t, 1)}
        except Exception as e:
            return {"ok": False, "s": round(time.time() - t, 1), **error_info(e)}

    def gloo(self, rank):
        import datetime
        import torch
        import torch.distributed as dist
        t = time.time()
        try:
            dist.init_process_group("gloo", store=self._store, rank=rank, world_size=2, timeout=datetime.timedelta(seconds=90))
            x = torch.ones(4) * (rank + 1)
            dist.all_reduce(x)
            return {"ok": True, "s": round(time.time() - t, 1), "sum": x.tolist()}
        except Exception as e:
            return {"ok": False, "s": round(time.time() - t, 1), **error_info(e)}


pg = placement_group([{"CPU": 1}, {"CPU": 1}], strategy="STRICT_SPREAD")
ray.get(pg.ready(), timeout=300)
ranks = [Rank.options(scheduling_strategy=PlacementGroupSchedulingStrategy(pg, placement_group_bundle_index=i)).remote() for i in range(2)]
infos = ray.get([r.info.remote() for r in ranks])
data = {"ranks": [dict(i, node=short(i["node"])) for i in infos]}
port = ray.get(ranks[0].free_mapped_port.remote())
data["port"] = port
if port is None:
    result(ID, "FAIL", dict(data, error="no free identity port on rank 0"))
stores = ray.get([ranks[0].store.remote("0.0.0.0", port, 0), ranks[1].store.remote(infos[0]["ip"], port, 1)])
data["store"] = stores
if not all(s["ok"] for s in stores):
    result(ID, "LIMIT", dict(data, stage="store"))
print("C9_STORE", data, flush=True)
refs = [r.gloo.remote(i) for i, r in enumerate(ranks)]
ready, _ = ray.wait(refs, num_returns=2, timeout=240)  # a rank whose peer failed can hang in gloo past its own timeout
glo = [ray.get(ref) if ref in ready else {"ok": False, "error": "no answer within 240 s (hangs in init_process_group)"} for ref in refs]
data["gloo"] = glo
result(ID, "PASS" if all(g["ok"] for g in glo) else "LIMIT", dict(data, stage="gloo"))
