"""In-memory cluster state shared by the head server (step 8) and the provider (step 9). No persistence (D3).

Callers hold `state.lock` (an RLock) around compound changes. Each method below also takes the lock, so a single call is atomic on its own.
The head node is not stored here: the provider adds a fixed `head` entry when it lists nodes.
Times are epoch seconds and always passed in by the caller.

Rules for callers:
- `Candidate.public_ip` is set once, when vast first reports the instance running, and is never refreshed. The provider compares vast's current `public_ipaddr` with it to detect an IP change (plan step 9).
- Candidate status: `rented` -> `running` -> `enrolled`. Slot status: `uninitialized` -> `setting-up` -> `up-to-date` (written into the slot's `ray-node-status` tag).
- Compound changes (claim a free slot and mark enrolled; add a candidate and its IP ref) are done by the caller under `state.lock`.
"""
import secrets
import threading
from dataclasses import dataclass, field
from typing import Optional

# Blocklist lifetime by failure kind (D4), in seconds.
BLOCK_TTL = {"security": 30 * 86400, "network": 86400, "timeout": 86400}
BREAKER_FAILED_SLOTS, BREAKER_PAUSE_S = 3, 600  # rent-loop breaker (design 4)


@dataclass
class Slot:
    id: str
    node_type: str
    tags: dict = field(default_factory=dict)
    status: str = "uninitialized"
    instance_id: Optional[int] = None  # vast instance of the enrolled worker, once filled


@dataclass
class Candidate:
    instance_id: int
    slot_id: str
    offer_id: int
    machine_id: int
    public_ip: Optional[str] = None
    status: str = "created"
    rented_at: float = 0.0
    running_at: Optional[float] = None
    dph_total: float = 0.0
    node_type: Optional[str] = None  # set by the provider at rent time; used when the candidate's own slot is gone
    gpus: int = 0  # the declared GPU of the node type, set by the provider at rent time (design F4: GPU is exact)
    last_heartbeat: Optional[float] = None  # set by the head server on each worker heartbeat
    pull_limit_s: float = 900.0  # the provider's limit for "rented, not yet running" (image pull)
    join_token: Optional[str] = field(default=None, repr=False)  # secret; valid until the candidate enrolls or ends


@dataclass
class BlockEntry:
    reason: str
    kind: str
    expires_at: float


class ClusterState:
    def __init__(self):
        self.lock = threading.RLock()
        self.slots = {}  # slot id -> Slot
        self.candidates = {}  # vast instance id -> Candidate
        self.enrolled = set()  # instance IDs; never removed while the provider lives
        self.ip_refs = {}  # public ip -> number of candidates and workers on it
        self.blocklist = {}  # machine id -> BlockEntry
        self._failed_slots = {}  # node type -> slots in a row that ended with no enrolled worker (rent-loop breaker)
        self._paused_until = {}  # node type -> time until which the rent-loop breaker refuses launches
        self._slot_count = 0

    def new_slot_id(self):
        """`slot-<n>-<random6>`; it becomes RAY_CLOUD_INSTANCE_ID, so it is unique for the life of the provider."""
        with self.lock:
            self._slot_count += 1
            return "slot-%d-%s" % (self._slot_count, secrets.token_hex(3))

    def add_slot(self, slot):
        with self.lock:
            self.slots[slot.id] = slot

    def remove_slot(self, slot_id):
        with self.lock:
            return self.slots.pop(slot_id, None)

    def add_candidate(self, candidate):
        with self.lock:
            self.candidates[candidate.instance_id] = candidate

    def remove_candidate(self, instance_id):
        with self.lock:
            return self.candidates.pop(instance_id, None)

    def mark_enrolled(self, instance_id):
        with self.lock:
            self.enrolled.add(instance_id)

    def ip_ref_add(self, ip):
        with self.lock:
            self.ip_refs[ip] = self.ip_refs.get(ip, 0) + 1
            return self.ip_refs[ip]

    def ip_ref_remove(self, ip):
        """Returns the new count. At 0 the caller removes the security group rule. Never goes below 0."""
        with self.lock:
            count = max(self.ip_refs.get(ip, 0) - 1, 0)
            if count:
                self.ip_refs[ip] = count
            else:
                self.ip_refs.pop(ip, None)
            return count

    def block(self, machine_id, reason, kind, now):
        if kind not in BLOCK_TTL:
            raise ValueError("unknown blocklist kind %r; expected one of %s" % (kind, sorted(BLOCK_TTL)))
        with self.lock:
            self.blocklist[machine_id] = BlockEntry(reason, kind, now + BLOCK_TTL[kind])

    def is_blocked(self, machine_id, now):
        with self.lock:
            entry = self.blocklist.get(machine_id)
            if entry is None:
                return False
            if now >= entry.expires_at:
                del self.blocklist[machine_id]
                return False
            return True

    def slot_failed(self, node_type, now):
        """Count a slot of `node_type` that ended with no enrolled worker. Returns True when the breaker trips: the
        launches of the type pause for BREAKER_PAUSE_S, and the count starts again at 0."""
        with self.lock:
            failed = self._failed_slots.get(node_type, 0) + 1
            tripped = failed >= BREAKER_FAILED_SLOTS
            self._failed_slots[node_type] = 0 if tripped else failed
            if tripped:
                self._paused_until[node_type] = now + BREAKER_PAUSE_S
            return tripped

    def slot_enrolled(self, node_type):
        """A worker of `node_type` enrolled: the failures are no longer in a row."""
        with self.lock:
            self._failed_slots.pop(node_type, None)

    def launch_pause_left(self, node_type, now):
        """Seconds until the breaker allows launches of `node_type` again; 0 when it allows them now."""
        with self.lock:
            return max(self._paused_until.get(node_type, 0) - now, 0)

    def total_dph(self):
        """Dollars per hour of all candidates and workers (a worker stays in `candidates` until its instance is destroyed)."""
        with self.lock:
            return sum(c.dph_total for c in self.candidates.values())
