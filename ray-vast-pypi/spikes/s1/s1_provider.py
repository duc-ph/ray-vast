"""S1 spike: a minimal external NodeProvider for autoscaler v2.

Slots are returned at once; a thread fills each slot after start_delay_s by
starting a raylet on this host with RAY_CLOUD_INSTANCE_ID=<slot id>.
"""
import json
import os
import signal
import subprocess
import threading
import time
import uuid

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import NODE_KIND_HEAD, TAG_RAY_NODE_KIND, TAG_RAY_USER_NODE_TYPE

LOG_DIR = "/home/ray/s1/logs"
RAY = "/home/ray/anaconda3/bin/ray"


def log(msg, **kw):
    line = json.dumps({"t": round(time.time(), 3), "pid": os.getpid(),
                       "thread": threading.current_thread().name, "msg": msg, **kw},
                      default=str)
    with open(f"{LOG_DIR}/provider.log", "a") as f:
        f.write(line + "\n")


class S1Provider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        super().__init__(provider_config, cluster_name)
        self.lock = threading.RLock()
        self.host_ip = provider_config["host_ip"]
        self.head_address = provider_config["head_address"]
        # The head must be listed: v2 marks an IM instance TERMINATED when its
        # cloud instance id ("head") is missing from non_terminated_nodes, and then
        # never scales (reconciler._is_head_node_running).
        self.slots = {"head": {"tags": {TAG_RAY_NODE_KIND: NODE_KIND_HEAD, TAG_RAY_USER_NODE_TYPE: "head"},
                               "proc": None, "state": "running", "n": -1}}
        self.counter = 0
        log("__init__", provider_config=provider_config, cluster_name=cluster_name,
            obj=id(self))

    def non_terminated_nodes(self, tag_filters):
        with self.lock:
            ids = [i for i, s in self.slots.items() if s["state"] != "terminated"
                   and all(s["tags"].get(k) == v for k, v in tag_filters.items())]
        log("non_terminated_nodes", tag_filters=tag_filters, result=ids)
        return ids

    def is_running(self, node_id):
        with self.lock:
            r = self.slots[node_id]["state"] == "running"
        log("is_running", node_id=node_id, result=r)
        return r

    def is_terminated(self, node_id):
        with self.lock:
            r = node_id not in self.slots or self.slots[node_id]["state"] == "terminated"
        log("is_terminated", node_id=node_id, result=r)
        return r

    def node_tags(self, node_id):
        with self.lock:
            tags = dict(self.slots[node_id]["tags"])
        log("node_tags", node_id=node_id, result=tags)
        return tags

    def set_node_tags(self, node_id, tags):
        log("set_node_tags", node_id=node_id, tags=tags)
        with self.lock:
            self.slots[node_id]["tags"].update(tags)

    def internal_ip(self, node_id):
        log("internal_ip", node_id=node_id)
        return self.host_ip

    def external_ip(self, node_id):
        log("external_ip", node_id=node_id)
        return self.host_ip

    def create_node(self, node_config, tags, count):
        raise NotImplementedError("S1 expects create_node_with_resources_and_labels")

    def create_node_with_resources_and_labels(self, node_config, tags, count,
                                              resources, labels):
        log("create_node_with_resources_and_labels", node_config=node_config, tags=tags,
            count=count, resources=resources, labels=labels)
        created = {}
        with self.lock:
            for _ in range(count):
                slot_id = f"slot-{self.counter}-{uuid.uuid4().hex[:6]}"
                self.slots[slot_id] = {"tags": dict(tags), "proc": None,
                                       "state": "pending", "n": self.counter}
                self.counter += 1
                threading.Thread(target=self._fill, args=(slot_id, node_config, resources),
                                 name=f"fill-{slot_id}", daemon=True).start()
                created[slot_id] = slot_id
        log("created", slots=list(created))
        return created

    def _fill(self, slot_id, node_config, resources):
        time.sleep(node_config.get("start_delay_s", 5))
        with self.lock:
            slot = self.slots[slot_id]
            if slot["state"] != "pending":
                log("fill_skipped", slot_id=slot_id, state=slot["state"])
                return
            # Ports 20000+100n .. +99: below the Linux ephemeral range, above the
            # head's default worker ports (10002-19999).
            base = 20000 + 100 * slot["n"]
            cpus = node_config.get("real_cpus", resources["CPU"])
            cmd = [RAY, "start", "--block", f"--address={self.head_address}",
                   f"--node-ip-address={self.host_ip}", f"--num-cpus={cpus}",
                   f"--node-manager-port={base}", f"--object-manager-port={base + 1}",
                   f"--metrics-export-port={base + 2}",
                   f"--dashboard-agent-listen-port={base + 3}",
                   f"--dashboard-agent-grpc-port={base + 4}",
                   f"--runtime-env-agent-port={base + 5}",
                   f"--min-worker-port={base + 10}", f"--max-worker-port={base + 99}",
                   "--object-store-memory=200000000",
                   "--resources=" + json.dumps({k: v for k, v in resources.items()
                                                if k not in ("CPU", "GPU", "memory")})]
            env = dict(os.environ, RAY_CLOUD_INSTANCE_ID=slot_id,
                       RAY_NODE_TYPE_NAME=slot["tags"][TAG_RAY_USER_NODE_TYPE])
            out = open(f"{LOG_DIR}/raylet-{slot_id}.log", "w")
            # New session so that terminate_node can kill this raylet's whole group.
            slot["proc"] = subprocess.Popen(cmd, env=env, stdout=out, stderr=subprocess.STDOUT,
                                            start_new_session=True)
            slot["state"] = "running"
        log("filled", slot_id=slot_id, pgid=slot["proc"].pid, cmd=cmd)

    def terminate_node(self, node_id):
        log("terminate_node", node_id=node_id)
        assert node_id != "head", "S1 never terminates the head"
        with self.lock:
            slot = self.slots[node_id]
            proc, slot["state"] = slot["proc"], "terminated"
        if proc is None:
            return
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(15)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
        left = subprocess.run(["pgrep", "-g", str(proc.pid)], capture_output=True,
                              text=True).stdout.split()
        log("terminated", node_id=node_id, pgid=proc.pid, rc=proc.returncode,
            procs_left_in_group=left)

    def terminate_nodes(self, node_ids):
        log("terminate_nodes", node_ids=node_ids)
        for node_id in node_ids:
            self.terminate_node(node_id)
