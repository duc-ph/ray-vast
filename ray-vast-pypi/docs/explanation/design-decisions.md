# Design decisions

The rule for every decision: the simplest option that works, with no loss of reliability and no fragility.
This page gives a short summary. The full list, with the rejected options, is in [decisions.md](https://github.com/duc-ph/ray-vast/blob/main/docs/decisions.md) on GitHub.

| Question | Decision | Reason |
|---|---|---|
| Where does the head run? | On AWS EC2, not on vast. | A vast instance can stop, and its IP can change. If the head stops, the cluster is lost. |
| How do the nodes connect? | Over the public internet, with TLS, a token and a small networking shim. | vast containers have no `NET_ADMIN` and no TUN device (0 of 146 hosts in a test), so a VPN or an overlay network is not possible. |
| How does a worker get its secrets? | It enrolls itself with a one-time join token. | vast shows the rental request in its metadata. SSH would need sshd, apt and `sudo` in the image. |
| Where does the enrollment server run? | In a thread of the provider, in the Ray monitor process. | One process. If the monitor dies, the heartbeats stop, and the workers destroy themselves. |
| Where is the state? | In memory only. A new provider destroys the old instances (start-up sweep). | A new head has a new Ray GCS, so old workers cannot join again. A database would not save the cluster. |
| What does a worker do when it loses the head? | It destroys its own instance after 5 minutes. It never only exits. | vast restarts a stopped container and keeps billing it. |
| Does the head restart itself? | No. | Ray never restarts a dead monitor. The result is the same as with a standard `ray up` cluster. |
| Why rent extra candidates? | `candidate_factor` 2 by default. | About 8% of hosts in a test had a port that we could not reach, and about 8% had a wrong egress IP. Extra candidates hide one bad host. |
| How do you choose GPUs? | An offer query in the vast API JSON form, with MB units. `CPU` is a minimum, `GPU` is exact. | No own copy of the vast CLI parser. Ray reports the real cores of a running worker. |
| Is there a budget? | `max_dollars_per_hour` is required. | A missing budget must not mean "no limit". |
| Does the head get an Elastic IP? | No. | A reboot keeps the IP. After a stop and start, run `down` and `up`. |
| How many worker IPs? | Up to 5 security groups, about 299 worker IPs with the default quota. | 5 is the AWS default limit of groups for one network interface. An open head would let a DDoS stop the cluster. |
| Do certificates renew? | No. They live 365 days. Run `down` and `up` to renew. | Ray loads certificate files only at process start. Short certificates make outages more likely. |
| Which Ray versions? | `ray[default]>=2.59`. `up` warns for an untested version. | A hard pin would make pip change the image's Ray. |
| Which autoscaler? | Ray autoscaler v2. | v2 matches a node to an instance by ID, not by IP. Two workers can share one public IP. |
