# Future work

Items that we agreed not to build in the first version.

- **Short-lived job credentials.** `ray-vast` adds an optional IAM role for workers. The head gets STS credentials for 1 hour, scoped to one bucket or prefix, and pushes them to the workers. It renews them before they expire. A stolen credential then works for at most 1 hour and only on that prefix. (First version: the user puts credentials in `runtime_env` `env_vars`.)
- **Label selectors.** Scale-up for `label_selector` on `ray.io/accelerator-type` (autoscaler v2 is already the target).
- **Separate the autoscaler from Ray workers on the head.** The autoscaler runs as a different Linux user, and IMDS is blocked for the Ray user.
- **More CLI commands.** `exec`, `rsync-up`/`rsync-down`, `status`, `submit`.
- **Rolling cert renewal.** The CA file holds the old and the new CA together (gRPC accepts a CA bundle). The autoscaler replaces the workers one at a time with workers that have new certs, and the head restarts last.
- **Total budget.** `max_spend`: the head stops all workers when the cluster spends this amount.
- **Blocklist across clusters.** Keep the blocklist in a file (on the laptop or in S3), so that a new cluster does not rent known bad hosts again. (First version: one blocklist in memory for each cluster.)
