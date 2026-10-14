# Future work

Items that we agreed not to build in the first version.

- **Short-lived job credentials.** `ray-vast` adds an optional IAM role for workers. The head gets STS credentials for 1 hour, scoped to one bucket or prefix, and pushes them to the workers. It renews them before they expire. A stolen credential then works for at most 1 hour and only on that prefix. (First version: the user puts credentials in `runtime_env` `env_vars`.)
- **Label selectors.** Scale-up for `label_selector` on `ray.io/accelerator-type` (autoscaler v2 is already the target).
- **Separate the autoscaler from Ray workers on the head.** The autoscaler runs as a different Linux user, and IMDS is blocked for the Ray user.
- **More CLI commands.** `exec`, `rsync-up`/`rsync-down`, `status`, `submit`.
- **Rolling cert renewal.** The CA file holds the old and the new CA together (gRPC accepts a CA bundle). The autoscaler replaces the workers one at a time with workers that have new certs, and the head restarts last.
- **Total budget.** `max_spend`: the head stops all workers when the cluster spends this amount.
- **Blocklist across clusters.** Keep the blocklist in a file (on the laptop or in S3), so that a new cluster does not rent known bad hosts again. (First version: one blocklist in memory for each cluster.)
- **Autoscaler restart without losing the cluster.** Run the autoscaler monitor in its own container with a restart policy, as KubeRay does, and make the provider rebuild its state from vast (instance labels) instead of the start-up sweep. Then a monitor crash does not end the running jobs. (First version: no self-healing; a dead monitor stops the head.)
- **`ClusterState` owns the candidate lifecycle.** The state takes plain data (the vast instance list, the clock, an enrollment claim) and returns decisions. The provider's reconcile pass and the head server's `/enroll` then only carry out those decisions, with every network call outside the lock. The tests then use the state's methods, not its dicts. The heartbeat limit is defined once (today the provider and the head server each have a `HEARTBEAT_TIMEOUT_S`). (Design review of 2026-10-14. First version: the lifecycle rules are in the provider and the head server; one function ends an instance.)
- **Enrollment decision apart from the TLS server.** `HeadServer` takes 15 constructor arguments, and the port check opens real sockets inside the enrollment logic. Move the decision into the state (item above), inject the port check, and keep the server as a thin HTTPS layer. The one ending function (done on 2026-10-14) already removes the `vast` and `on_destroy` arguments. (Design review of 2026-10-14.)
