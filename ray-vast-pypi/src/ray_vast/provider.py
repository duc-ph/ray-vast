"""VastNodeProvider: the Ray autoscaler node provider for vast.ai workers (plan step 9).

Ray loads it from `provider: {type: external, module: ray_vast.provider.VastNodeProvider}` and calls
`VastNodeProvider(provider_config, cluster_name)` once per monitor process (F9).

`provider_config` keys (`ray-vast up` writes them, step 10):
- `head_ip`: the head public IP (required).
- `secrets_dir`: directory with `ca.pem`, `ca.key`, `head.pem`, `head.key`, `token` and `vast_api_key` (required).
- `security_group_id`, `region`, `image`, `max_dollars_per_hour`: required.
- `candidate_factor`, `order_by`, `port_a`, `port_b`, `allow_non_datacenter`: defaults from `config.PROVIDER_DEFAULTS`.
- `image_size_gb` (optional): the pull limit is `2 x image_size / inet_down`, clamped to 5-15 min. Without it, 15 min.
Per node type, Ray passes `node_config` (`offer_query`, `disk_gb`) and `resources` to the create call.

Slots are the nodes that Ray sees. A create call makes `count` slots and rents up to `candidate_factor x count`
candidates (vast instances); the first candidates that enroll fill the slots, the rest are destroyed. The read methods
answer from memory (F10). A reconcile thread (every 10 s) lists the vast instances once and applies the rules in
`reconcile_once`. All vast and AWS calls run outside `state.lock` (rule 8).
Every path that ends a candidate or worker goes through `_end` or the head server's `_destroy`, then `_revoke_unused`
(rule 9).

Unknown node IDs: `node_tags` returns `{}` (v2 then skips the node as unmanaged), `is_running` returns False.
"""
import logging
import threading
import time
import uuid
from pathlib import Path

import boto3
import botocore.exceptions
from ray.autoscaler.node_launch_exception import NodeLaunchException
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    STATUS_UP_TO_DATE,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)

from . import pki
from .aws_sg import SecurityGroupRules, SgQuotaError, ec2_client_config
from .config import PROVIDER_DEFAULTS, build_offer_query
from .head_server import HeadServer
from .state import Candidate, ClusterState, Slot
from .vast_api import VastClient, VastError

log = logging.getLogger(__name__)

HEAD_ID = "head"
RECONCILE_INTERVAL_S = 10
ENROLL_TIMEOUT_S = 60
HEARTBEAT_TIMEOUT_S = 300
PULL_LIMIT_MIN_S, PULL_LIMIT_MAX_S = 300.0, 900.0
MISSING_GRACE_S = 60  # vast may list a new instance late; a missing instance counts only after this
# vast `actual_status` values that mean the instance is dead or unusable. Unproven until C5 (live test): the only
# value seen in real data is "exited". Other values seen in the wild: "running", "loading", "created" (all pending).
FAILED_STATUSES = {"exited", "error", "failed", "offline"}
PENDING_STATUSES = {None, "", "created", "loading", "running", "scheduling"}


class VastNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name, vast=None, ec2=None, clock=time.time, start_threads=True):
        """`vast`, `ec2`, `clock` and `start_threads` are for tests: a fake vast client, an ec2 client, a clock, and
        False to skip the head server and the reconcile thread (tests then call `reconcile_once` themselves)."""
        super().__init__(provider_config, cluster_name)
        cfg = {**PROVIDER_DEFAULTS, **provider_config}
        self._cfg, self._clock = cfg, clock
        self._label = f"ray-vast:{cluster_name}"
        self._head_ip = cfg["head_ip"]
        self._max_dph = cfg["max_dollars_per_hour"]
        self.state = ClusterState()
        self._creating = set()  # slot IDs of create calls in flight: reconcile leaves them and their instances alone
        self._reserved_dph = 0.0  # dollars per hour of rentals in flight (budget check)
        self._request_of_slot = {}  # slot id -> the id of the create call that made it
        self._sg_lock = threading.Lock()  # one SG change at a time; never held with state.lock
        self._stop = threading.Event()
        self._seen_statuses = set()  # unknown vast statuses are logged once each

        secrets_dir = Path(cfg["secrets_dir"])
        read = lambda name: (secrets_dir / name).read_bytes()
        ca_cert, ca_key = read("ca.pem"), read("ca.key")
        self._ca_fingerprint = pki.ca_fingerprint(ca_cert)
        self._vast = vast or VastClient(read("vast_api_key").decode().strip())
        self._sg = SecurityGroupRules(
            ec2 or boto3.client("ec2", region_name=cfg["region"], config=ec2_client_config()),
            cfg["security_group_id"], cluster_name)
        package = Path(__file__).parent
        self._bootloader = (package / "worker" / "bootloader.sh").read_text()

        # Start-up sweep (D3): a new provider means a new GCS, so every old instance and rule is useless.
        for instance in self._vast.list_instances():
            if instance.get("label") == self._label:
                log.warning("start-up sweep: destroying instance %s", instance["id"])
                try:
                    self._vast.destroy_instance(instance["id"])
                except VastError as e:
                    log.error("start-up sweep: destroy of instance %s failed (reconcile retries): %s", instance["id"], e)
        self._sg.revoke_all()

        self.head_server = HeadServer(
            self.state, self._vast, ca_cert, ca_key, read("head.pem"), read("head.key"), read("token").decode().strip(),
            (package / "shim" / "librayvast_shim.so").read_bytes(), (package / "worker" / "bootstrap.py").read_bytes(),
            self._head_ip, cfg["port_a"], cfg["port_b"], cluster_name, clock=clock,
            on_destroy=lambda ip, remaining: self._revoke_unused(ip) if remaining == 0 else None)
        if start_threads:
            self.head_server.start()
            self._thread = threading.Thread(target=self._reconcile_loop, name="ray-vast-reconcile", daemon=True)
            self._thread.start()

    def close(self):
        """Stop the reconcile thread and the head server. Instances stay; the next provider sweeps them."""
        self._stop.set()
        self.head_server.stop()

    # ---- read methods: memory only, no network ----

    def non_terminated_nodes(self, tag_filters):
        with self.state.lock:
            nodes = {HEAD_ID: self._head_tags()}
            nodes.update({slot.id: self._slot_tags(slot) for slot in self.state.slots.values()})
        return [i for i, tags in nodes.items() if all(tags.get(k) == v for k, v in tag_filters.items())]

    def is_running(self, node_id):
        with self.state.lock:
            slot = self.state.slots.get(node_id)
            return node_id == HEAD_ID or (slot is not None and slot.instance_id in self.state.candidates)

    def is_terminated(self, node_id):
        with self.state.lock:
            return node_id != HEAD_ID and node_id not in self.state.slots

    def node_tags(self, node_id):
        with self.state.lock:
            if node_id == HEAD_ID:
                return self._head_tags()
            slot = self.state.slots.get(node_id)
            return self._slot_tags(slot) if slot else {}

    def set_node_tags(self, node_id, tags):
        with self.state.lock:
            slot = self.state.slots.get(node_id)
            if slot is not None:
                slot.tags.update(tags)

    def internal_ip(self, node_id):
        return self.external_ip(node_id)

    def external_ip(self, node_id):
        with self.state.lock:
            slot = self.state.slots.get(node_id)
            cand = self.state.candidates.get(slot.instance_id) if slot else None
        return self._head_ip if node_id == HEAD_ID else (cand.public_ip if cand else None)

    def _head_tags(self):
        return {TAG_RAY_NODE_KIND: NODE_KIND_HEAD, TAG_RAY_USER_NODE_TYPE: "head", TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE}

    def _slot_tags(self, slot):
        return {**slot.tags, TAG_RAY_NODE_STATUS: slot.status}

    # ---- create and terminate ----

    def create_node_with_resources_and_labels(self, node_config, tags, count, resources, labels):
        node_type = tags[TAG_RAY_USER_NODE_TYPE]
        query = build_offer_query({"resources": {"GPU": 0, **resources}, "node_config": node_config},
                                  self._cfg["allow_non_datacenter"], node_type)
        request_id = uuid.uuid4().hex
        with self.state.lock:
            slots = [Slot(self.state.new_slot_id(), node_type, dict(tags)) for _ in range(count)]
            for slot in slots:
                self.state.add_slot(slot)
                self._request_of_slot[slot.id] = request_id
                self._creating.add(slot.id)

        env = {
            "RAYVAST_HEAD_IP": self._head_ip,
            "RAYVAST_PORT_A": str(self._cfg["port_a"]),
            "RAYVAST_PORT_B": str(self._cfg["port_b"]),
            "RAYVAST_BOOTSTRAP_SHA256": self.head_server.bootstrap_sha256,
            "RAYVAST_CA_SHA256": self._ca_fingerprint,
            "RAYVAST_CLUSTER": self.cluster_name,
        }
        rented, over_budget = 0, False
        try:
            offers = self._vast.search_offers(query, self._cfg["order_by"])
            for offer in offers:
                if rented >= self._cfg["candidate_factor"] * count:
                    break
                dph = offer["dph_total"]
                with self.state.lock:
                    if self.state.is_blocked(offer["machine_id"], self._clock()):
                        continue
                    if self.state.total_dph() + self._reserved_dph + dph > self._max_dph:
                        over_budget = True
                        continue
                    self._reserved_dph += dph
                size_gb, inet_down = self._cfg.get("image_size_gb"), offer.get("inet_down")
                pull_limit = PULL_LIMIT_MAX_S
                if size_gb and inet_down:  # inet_down is in Mb/s, the image in GB
                    pull_limit = min(max(2 * size_gb * 8000 / inet_down, PULL_LIMIT_MIN_S), PULL_LIMIT_MAX_S)
                instance_id = None
                try:
                    instance_id = self._vast.create_instance(
                        offer["id"], self._cfg["image"], env, ["sh", "-c", self._bootloader], self._label,
                        node_config.get("disk_gb", 50))
                except VastError as e:
                    log.warning("renting offer %s failed: %s", offer["id"], e)
                finally:
                    with self.state.lock:  # the reservation ends in the same section that records the rental
                        self._reserved_dph -= dph
                        if instance_id is not None:
                            self.state.add_candidate(Candidate(
                                instance_id, slots[rented % count].id, offer["id"], offer["machine_id"],
                                status="rented", rented_at=self._clock(), dph_total=dph, node_type=node_type,
                                pull_limit_s=pull_limit))
                if instance_id is None:
                    continue
                rented += 1
        finally:
            with self.state.lock:
                self._creating.difference_update(s.id for s in slots)
                if rented == 0:
                    for slot in slots:
                        self.state.remove_slot(slot.id)
        if rented == 0:
            category, why = ("budget", f"every matching offer would pass max_dollars_per_hour {self._max_dph}") if over_budget \
                else ("no offers", "no rentable offer matches the node type query")
            raise NodeLaunchException(category, f"node type {node_type!r}: {why}", None)
        log.info("create %d x %s: rented %d candidates", count, node_type, rented)
        return {slot.id: slot.id for slot in slots}

    def terminate_node(self, node_id):
        if node_id == HEAD_ID:
            log.warning("ignoring terminate for the head node")
            return
        with self.state.lock:
            slot = self.state.remove_slot(node_id)
            if slot is None:
                return
            ending = [c.instance_id for c in self.state.candidates.values() if c.instance_id == slot.instance_id
                      or (c.slot_id == slot.id and c.instance_id not in self.state.enrolled)]
        for instance_id in ending:
            self._end(instance_id, "terminated by the autoscaler")

    # ---- ending instances ----

    def _end(self, instance_id, reason, block_kind=None):
        """End a candidate or worker: drop it and its slot from state, destroy the instance, lower the IP ref count
        and remove the SG rule at 0. Does nothing if the head server ended it first."""
        with self.state.lock:
            cand = self.state.remove_candidate(instance_id)
            if cand is None:
                return
            for slot in [s for s in self.state.slots.values() if s.instance_id == instance_id]:
                self.state.remove_slot(slot.id)
            remaining = self.state.ip_ref_remove(cand.public_ip) if cand.public_ip else None
            if block_kind:
                self.state.block(cand.machine_id, reason, block_kind, self._clock())
        log.warning("ending instance %s (machine %s, ip %s): %s", instance_id, cand.machine_id, cand.public_ip, reason)
        try:
            self._vast.destroy_instance(instance_id)
        except VastError as e:
            log.error("destroy of instance %s failed (the next reconcile pass retries): %s", instance_id, e)
        if remaining == 0:
            self._revoke_unused(cand.public_ip)

    def _revoke_unused(self, ip):
        """Remove the SG rule if no candidate or worker uses the IP any more (checked again under the SG lock)."""
        with self._sg_lock:
            with self.state.lock:
                in_use = self.state.ip_refs.get(ip, 0) > 0
            if in_use:
                return
            try:
                self._sg.revoke_ip(ip)
            except botocore.exceptions.ClientError as e:
                log.error("revoke of the SG rule for %s failed: %s", ip, e)

    # ---- reconcile ----

    def _reconcile_loop(self):
        while not self._stop.wait(RECONCILE_INTERVAL_S):
            try:
                self.reconcile_once()
            except Exception:
                log.exception("reconcile pass failed")

    def reconcile_once(self):
        now = self._clock()
        try:
            instances = self._vast.list_instances()
        except VastError as e:
            log.warning("reconcile: list of instances failed, skipping this pass: %s", e)
            return
        by_id = {i["id"]: i for i in instances if i.get("label") == self._label}
        ended_ids = set()

        # 1. Judge each candidate and worker against what vast says.
        to_end, newly_running = [], []
        with self.state.lock:
            for cand in list(self.state.candidates.values()):
                instance = by_id.get(cand.instance_id)
                enrolled = cand.instance_id in self.state.enrolled
                status = instance.get("actual_status") if instance else None
                vast_ip = instance.get("public_ipaddr") if instance else None
                reason, block = None, None
                if instance is None:
                    if now - cand.rented_at > MISSING_GRACE_S:
                        reason = "instance is gone from vast"
                elif status in FAILED_STATUSES or str(instance.get("status_msg") or "").lower().startswith("error"):
                    reason, block = f"instance status {status!r}: {str(instance.get('status_msg'))[:100]}", \
                        (None if enrolled else "network")
                elif cand.public_ip and vast_ip and vast_ip != cand.public_ip:
                    reason = "public IP changed"
                elif enrolled:
                    if now - (cand.last_heartbeat or cand.running_at or cand.rented_at) > HEARTBEAT_TIMEOUT_S:
                        reason = "no heartbeat for 5 min"
                elif status == "running" and vast_ip and cand.running_at is None:
                    # first time running: record the IP (never refreshed) and its ref count
                    cand.public_ip, cand.status, cand.running_at = vast_ip, "running", now
                    self.state.ip_ref_add(vast_ip)
                    newly_running.append(cand)
                elif cand.running_at is not None:
                    if now - cand.running_at > ENROLL_TIMEOUT_S:
                        reason, block = f"not enrolled {ENROLL_TIMEOUT_S} s after running", "timeout"
                elif now - cand.rented_at > cand.pull_limit_s:
                    reason, block = f"not running with an IP after {cand.pull_limit_s:.0f} s", "timeout"
                if status not in PENDING_STATUSES | FAILED_STATUSES and status not in self._seen_statuses:
                    self._seen_statuses.add(status)
                    log.warning("reconcile: unknown vast actual_status %r (treated as pending)", status)
                if reason:
                    to_end.append((cand.instance_id, reason, block))

        # 2. A rule for each new candidate IP. A full security group ends the candidate.
        for cand in newly_running:
            try:
                with self._sg_lock:
                    self._sg.allow_ip(cand.public_ip)
            except SgQuotaError as e:
                to_end.append((cand.instance_id, str(e), None))
            except botocore.exceptions.ClientError as e:
                log.error("SG rule for %s failed (the candidate will time out): %s", cand.public_ip, e)
        for instance_id, reason, block in to_end:
            self._end(instance_id, reason, block)
            ended_ids.add(instance_id)

        # 3. Candidates of a finished request; instances that are not ours to keep (outside check).
        request_ends, strays = [], []
        with self.state.lock:
            open_requests = {self._request_of_slot.get(s.id) for s in self.state.slots.values() if s.instance_id is None}
            for cand in self.state.candidates.values():
                if cand.instance_id not in self.state.enrolled and self._request_of_slot.get(cand.slot_id) not in open_requests:
                    request_ends.append(cand.instance_id)
            if not self._creating:  # a create call may hold an instance that is not recorded yet
                strays = [i for i in by_id if i not in self.state.candidates and i not in ended_ids]
        for instance_id in request_ends:
            self._end(instance_id, "its request has no free slot left")
        for instance_id in strays:
            log.warning("outside check: destroying unknown instance %s", instance_id)
            try:
                self._vast.destroy_instance(instance_id)
            except VastError as e:
                log.error("destroy of instance %s failed (the next pass retries): %s", instance_id, e)

        # 4. Slots: drop a slot whose worker is gone. Drop a free slot only when no pending candidate of its node type is
        # left (the head server fills any free slot of the type). Forget request entries that nothing points at.
        with self.state.lock:
            for slot in list(self.state.slots.values()):
                if slot.id in self._creating:
                    continue
                worker = self.state.candidates.get(slot.instance_id)
                if slot.instance_id is not None and worker is None:
                    self.state.remove_slot(slot.id)
                elif worker is not None:
                    if worker.last_heartbeat and slot.status == "setting-up":
                        slot.status = STATUS_UP_TO_DATE
                elif not any(c.node_type == slot.node_type and c.instance_id not in self.state.enrolled
                             for c in self.state.candidates.values()):
                    self.state.remove_slot(slot.id)
            in_use = set(self.state.slots) | {c.slot_id for c in self.state.candidates.values()}
            for slot_id in [i for i in self._request_of_slot if i not in in_use and i not in self._creating]:
                del self._request_of_slot[slot_id]
