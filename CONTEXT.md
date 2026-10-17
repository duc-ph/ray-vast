# ray-vast

Runs a Ray cluster with the head on AWS EC2 and the workers on vast.ai containers, which talk to each other across NAT over the public internet.

## Language

### Vast resources

**Offer**:
A vast listing of rentable capacity on one host, with a price and specs, before rental.
_Avoid_: machine, listing

**Host**:
The physical vast machine (identified by `machine_id`) that runs one or more containers.
_Avoid_: machine, server

**Instance**:
One rented vast container on a host.
_Avoid_: machine, VM, box

**Co-tenant**:
Another party's instance on the same host as ours.

**Offer query**:
The `offer_query` of a node type: a mapping in the vast API JSON form that defines which offers the node type may rent.
_Avoid_: filters, spec

### Cluster shape

**Node type**:
A named class of workers with fixed declared Ray resources and one offer query that guarantees those resources on every rented host.
_Avoid_: GPU class, worker group, pool

### Cluster lifecycle

**Slot**:
A node ID that the provider returns to the autoscaler at once, before any instance is chosen for it; a candidate that enrolls fills it.
_Avoid_: placeholder, pending node

**Request**:
The slots of one create call of the autoscaler, with the candidates rented for them.

**Candidate**:
An instance that is rented but has not enrolled yet, so it has no cluster secrets.

**Host checks**:
The tests that a candidate must pass before it can enroll: the local checks of the bootstrap (egress IP, no NET_RAW/NET_ADMIN, own network namespace, empty /dev/shm) and the head's port check.
_Avoid_: probe, preflight

**Port check**:
The host check that the head does from outside: a TCP connect to each identity port of the candidate (10 to 64 ports).
_Avoid_: outside check

**Blocklist**:
The set of hosts (by `machine_id`) that failed, each with a kind and an expiry, kept in the provider's memory for one cluster.
_Avoid_: denylist, bad hosts

**Bootloader**:
The short script in the container command that downloads the bootstrap from the head, checks its SHA-256, and starts it.
_Avoid_: stub, loader, bootstrap loader

**Bootstrap**:
The program from the head that runs as PID 1 on a candidate: it does the local host checks, enrolls, starts Ray, and then acts as the watchdog.

**Join token**:
A random secret that the provider makes for one candidate and puts in its env; the candidate shows it once to enroll.
_Avoid_: nonce, enroll key

**Enrollment**:
The step where a candidate proves its instance ID to the head with its join token and receives its cert, key, cluster token and shim.
_Avoid_: secret push, provisioning

**Watchdog**:
The role of the bootstrap after enrollment: it destroys its own instance when the heartbeat to the head fails for too long.

**Worker**:
A candidate that has enrolled and runs Ray as a cluster member.
_Avoid_: node (when it means only vast workers)

**Head**:
The EC2 instance that runs the Ray head, the autoscaler and the provider, and holds the CA key and the cloud credentials.

**Head server**:
The HTTPS server thread in the provider that serves the bootstrap, the enrollment, the heartbeat and the reports (port A: TLS; port B: mTLS).

**Orphan sweep**:
The provider's check, in each reconcile pass, that destroys every instance with the cluster label that it does not know.
_Avoid_: outside check

**Start-up sweep**:
The provider's first action: it destroys every instance with the cluster label and removes the cluster's security group rules.
