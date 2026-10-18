# Clean up after a failure

Do these steps after a failed `up`, after a lost head, or when `down` reports that something is left.

## 1. Run `down`

```bash
ray-vast down cluster.yaml
```

`down` finds the resources of the cluster by tag, name and label. So it works in these cases:

- `up` failed half way;
- the head is gone;
- the local state file `~/.ray-vast/<cluster_name>.json` is missing.

`down` does not need an SSH key. You can run `down` again with no risk.

The steps of `down` are in [CLI](../reference/cli.md#ray-vast-down).

## 2. Run `down` again if something is left

`down` lists what is left and exits with code 1. Most leftovers are slow AWS or vast deletes. Run `down` again.

## 3. Remove the leftovers by hand

If something is still left after the second `down`, remove it by hand. Check each resource in this table:

| Resource | Name or marker | How to check |
|---|---|---|
| vast instances | label `ray-vast:<cluster_name>` | `vastai show instances`, or the vast console |
| EC2 head | tag `ray-vast-cluster=<cluster_name>` | EC2 console, or `aws ec2 describe-instances` |
| Security groups | `ray-vast-<cluster_name>-0` … `-4` | EC2 console |
| EC2 key pair | `ray-vast-<cluster_name>` | EC2 console, **Key Pairs** |
| IAM instance profile and role | `ray-vast-<cluster_name>` | IAM console |
| Local state | `~/.ray-vast/<cluster_name>.json`, `~/.ray-vast/<cluster_name>/` (with the private SSH key) | Your laptop |

Do not delete the policy `ray-vast-head-boundary`. All clusters use it.

To destroy a vast instance, run `vastai destroy instance <instance id>`.

## `down` without a vast key

Without a vast key, `down` cleans AWS only. It prints a warning that it did not sweep the vast instances. It exits with code 0.
Then do one of these steps:

- Set `VAST_API_KEY`, and run `down` again.
- Run `vastai show instances`. Destroy the instances with the label `ray-vast:<cluster_name>`.

The instances also destroy themselves after the head is gone: a worker within 5 minutes, a candidate within 10 minutes.
Read [Worker life cycle and the watchdog](../explanation/architecture.md#worker-life-cycle-and-the-watchdog).
