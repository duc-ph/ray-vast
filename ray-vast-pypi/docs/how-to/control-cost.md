# Control cost

A ray-vast cluster costs money in two places: the vast workers and the EC2 head.
This page tells you how to limit both.

## Set a budget

`provider.max_dollars_per_hour` is required. It is a hard limit on the sum of `dph_total` over all workers, candidates and rentals in progress.
The provider does not rent an offer that would take the sum above the limit.

```yaml
provider:
  max_dollars_per_hour: 3.0
```

When the budget stops a rental, the autoscaler tries again later. Jobs wait.
`ray status` on the head then shows the node type under "Recent failures" as `LaunchFailed`.
The reason (`every matching offer would pass max_dollars_per_hour`) is in the head log. Read [Find logs and troubleshoot](troubleshoot.md).

The limit is for one cluster, per hour. It is not a total spend limit.

## Remove idle workers

`idle_timeout_minutes` is a Ray field. Ray removes a worker after it is idle for this time.

```yaml
idle_timeout_minutes: 5
```

A short timeout saves money. A long timeout keeps warm workers for the next job.

## Limit the cluster size

- `max_workers` at the top level limits the number of workers in the cluster.
- `max_workers` in each node type limits the workers of that type.
- `min_workers` in a node type keeps that many workers running at all times. They cost money while they are idle. Use `min_workers: 0` to pay only when jobs run.

## Candidates

For each worker, the provider rents up to `candidate_factor` instances (default 2). These are candidates.
The first candidate that enrolls becomes the worker. The provider destroys the other candidates as soon as every slot of the request has a worker.
A candidate that is not needed costs a few cents at most. In a test, the provider destroyed the extra candidate 8.6 seconds after the first one enrolled.

Candidates make a worker arrive sooner, and one bad host does not delay it.
Set `candidate_factor: 1` to rent no extra candidates. Then one bad host costs a retry.

## The watchdog

A worker that loses the head destroys its own instance after 5 minutes.
It destroys the instance, and does not only stop it. A stopped vast container still costs money.
Read [Worker life cycle and the watchdog](../explanation/worker-lifecycle.md).

## The EC2 head

The head runs until you run `ray-vast down`. The default `t3.large` costs about $0.08 per hour in `us-west-2`, plus the 60 GB disk.
Change the type with `provider.head_instance_type`.

## Watch the spend

- The vast console shows each instance, its price and your credit. The instances of a cluster have the label `ray-vast:<cluster_name>`.
- The vast CLI lists them too: `vastai show instances`.
- `ray status` on the head shows the running workers and the pending demand.

## Always run `down`

```bash
ray-vast down cluster.yaml
```

`down` removes the head and every vast instance with the cluster label. It prints `left: nothing` when nothing is left.
If `up` failed, run `down` too. Read [Clean up after a failure](clean-up.md).
