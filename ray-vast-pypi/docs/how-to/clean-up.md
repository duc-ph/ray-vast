# Clean up after a failure

`ray-vast down` removes everything of a cluster. Use it after a failed `up`, after a lost head, and when you finish.

## Run `down`

```bash
ray-vast down cluster.yaml
```

`down` needs only the cluster YAML (`cluster_name` and `provider.region`), your AWS credentials and the vast key. It does not need an SSH key.

`down` finds everything by tag, name and label. So it works:

- when `up` failed half way;
- when the head is gone;
- when the local state file `~/.ray-vast/<cluster_name>.json` is missing.

It is safe to run `down` again.

## What `down` does

1. It terminates the EC2 head first. So the autoscaler cannot rent new workers.
2. It destroys every vast instance with the label `ray-vast:<cluster_name>`. It lists the instances again until none is left.
3. It deletes the 5 security groups `ray-vast-<cluster_name>-0` … `-4`.
4. It deletes the instance profile and the role `ray-vast-<cluster_name>`.
5. It deletes the local state files in `~/.ray-vast/`.
6. It prints `left: nothing`, or it lists what is left and exits with code 1.

## Something is left

Run `down` again. Most leftovers are slow AWS or vast deletes.

If something is still left, remove it by hand. After `down`, nothing of the cluster must exist:

| Resource | Name or marker | How to check |
|---|---|---|
| vast instances | label `ray-vast:<cluster_name>` | `vastai show instances`, or the vast console |
| EC2 head | tag `ray-vast-cluster=<cluster_name>` | EC2 console, or `aws ec2 describe-instances` |
| Security groups | `ray-vast-<cluster_name>-0` … `-4` | EC2 console |
| IAM instance profile and role | `ray-vast-<cluster_name>` | IAM console |

Destroy a vast instance with `vastai destroy instance <instance id>`.

## `down` without a vast key

Without a vast key, `down` cleans AWS and prints a warning: the vast instances were not swept. It exits with code 0.
Then do one of these:

- Set `VAST_API_KEY`, and run `down` again.
- List the instances with `vastai show instances`. Destroy those with the label `ray-vast:<cluster_name>`.

The instances also destroy themselves after the head is gone: a worker within 5 minutes, a candidate within 10 minutes.
