# Worker life cycle and the watchdog

This page tells how a worker starts, runs and ends. It also tells why a worker destroys itself.

## From demand to worker

1. **Demand.** A task, an actor or a placement group needs resources that no node has. The autoscaler chooses a node type and asks the provider for n nodes.
2. **Slots.** The provider makes n slots at once. The n slots of one call are one request.
3. **Rent.** The provider rents up to `candidate_factor` × n candidates (default 2 × n). Read [How the provider picks an offer](#how-the-provider-picks-an-offer).
4. **Pull.** vast pulls the image and starts the container.
5. **Enroll.** The first candidates that enroll fill the slots. Read [Enrollment and the join token](enrollment.md).
6. **Extra candidates.** When every slot of the request has a worker, the provider destroys the other candidates of the request.
7. **Ray.** The worker starts Ray with its real core count. The autoscaler matches the Ray node to the slot, and schedules the work.

A live test on 2026-10-14 used the CPU image, on a host that had the image in its cache. The results:

- The candidate was `running` within 24 seconds of the rental.
- The candidate enrolled 26.7 seconds after the rental.
- Its Ray node was running 7.9 seconds after the enrollment.
- The provider destroyed the extra candidate 8.6 seconds after the enrollment.

A host without the image in its cache must pull the image first. For the 18 GB GPU image, this can take several minutes.

## How the provider picks an offer

- The provider searches with your offer query plus the filters that it adds. It asks vast for up to 100 offers.
- It sorts the offers by `order_by` (default `dph_total`), in ascending order. So the cheapest offers come first.
- It skips the hosts on the blocklist. A host that fails a check goes on the blocklist for this cluster.
- It skips an offer that would take the total price above `max_dollars_per_hour`.

## Time limits for a candidate

| Limit | Value | Then |
|---|---|---|
| Pull | The pull limit. Read `image_size_gb` in [Configuration](../reference/configuration.md#provider). | The provider destroys the candidate. Blocklist `timeout`, 1 day. |
| Enrollment | 1 minute after the provider first sees the candidate `running` | The provider destroys the candidate. Blocklist `timeout`, 1 day. |
| Missing from vast's list | 60 seconds | The provider forgets the candidate. |
| vast status `exited` or an error | at once | The provider destroys the candidate. Blocklist `network`, 1 day. |

**Rent-loop breaker.** The provider counts the slots of each node type that end with no worker. After 3 such slots in a row, it stops new launches of that node type for 10 minutes. A successful enrollment sets the count to 0.
Some faults affect every host, for example a broken image. The breaker stops the provider from renting instance after instance for such a fault.

## The watchdog

After it starts Ray, the bootstrap stays as PID 1 and becomes the watchdog.

- It sends a heartbeat to the head every 30 seconds, on port B (mTLS).
- The provider serves the heartbeat. So the heartbeat fails when the head is down, and also when only the provider is down.
- **Destroy, never exit.** When PID 1 of a vast container exits, vast starts the container again, and the instance keeps its full cost. So the watchdog always destroys its own instance through the vast API. It retries every 5 seconds until vast confirms.
- **Report first.** Before each destroy, the worker sends the reason to the head, and waits up to 10 seconds. vast keeps no container log after a destroy, so the head log is the only record.

## All the ways a candidate or a worker ends

| Trigger | Who acts | What happens |
|---|---|---|
| Idle for `idle_timeout_minutes` | Ray autoscaler | It drains the node. The provider destroys the instance. |
| No heartbeat success for 300 seconds | Worker | Report, then destroy. |
| A 403 answer to a heartbeat | Worker | Report, then destroy. |
| A 403 answer to `/enroll` | Candidate | Report, then destroy. |
| No enrollment within 10 minutes | Candidate | Report, then destroy. |
| Ray on the worker exits (for example, the head is lost for 60 seconds) | Worker | Report, then destroy. |
| `TERM`, `INT` or `HUP` to PID 1, or an error in the bootstrap | Candidate or worker | Report, then destroy. |
| A container restart | Worker | The head refuses the second enrollment. The worker destroys itself. |
| A failed token check | Head | The head destroys the worker. |
| No heartbeat for 5 minutes, seen by the head | Provider | Destroy. |
| The public IP of the instance changes | Provider | Destroy. A Ray node cannot change its address. |
| vast status `exited` or an error | Provider | Destroy. |
| An instance with the cluster label that the provider does not know | Provider (orphan sweep) | Destroy. |
| `ray-vast down` | CLI | It terminates the head, then destroys every instance with the label. |

The bootloader also never exits. After a hash mismatch, or after 10 minutes of download failures, it destroys its own instance.

## Measured in a live test

A live test on 2026-10-14 tested the self-destroy paths on real vast hosts:

| Test | Result |
|---|---|
| `SIGTERM` to PID 1 | The worker sent its report 0.5 seconds after the signal. vast listed the instance as gone 3.3 seconds after the signal at most. The Ray node was DEAD 31.6 seconds after the signal. |
| `SIGKILL`, `SIGQUIT`, `SIGUSR1`, `SIGUSR2`, `SIGCONT` to PID 1 | No effect after 120 seconds. The kernel drops `SIGKILL` to PID 1 from inside the container. |
| Heartbeats blocked on the head | The provider destroyed the worker 308.9 seconds after the last heartbeat (limit 270 to 330 seconds). The worker's own watchdog decided to destroy about 2 seconds later. |
| Reboot of a worker container | The head refused the second enrollment 8 seconds after the reboot. The instance was gone 0.7 seconds after the refusal at most. |

In every case, the head log had the reason. No instance was left after `ray-vast down`.

## When the head dies

ray-vast does not restart the head. If Ray's monitor dies, Ray stops the head container.
Then the heartbeats stop, and every worker destroys itself within 5 minutes.
The EC2 instance stays until you run `ray-vast down`. This is the same result as with a standard `ray up` cluster.

To recover, run `ray-vast down` and `ray-vast up`. The head keeps all state in memory, so the new head starts from zero.
