"""The cloud side of the live suite, for conftest.py: the one factory of its cluster kinds and its restricted IAM user,
and the cloud guards. conftest.py imports no other cloud module, so tests/unit/test_live_parallel.py replaces this module
alone with a fake (no cloud call).

The cluster interface (cluster.Cluster, cluster.ReleaseCluster, libraries_setup.LibrariesCluster, and the fake):
- `name`, `region`, `yaml`, `home`: what Run.register writes when the cluster joins the run (in its constructor);
- `kept`: the --keep-cluster flag (conftest decides it; the end of the cluster reads it: no `down`, sweep or AWS check);
- `head_instance_type`: the EC2 type of the head (the EC2 spend estimate of a group);
- `up()` (returns the cluster), `down()` (returns a CompletedProcess; `last_down` keeps the last one);
- `up_output`: the output of this run's `up` (None when the run reused a kept cluster);
- `state`: the CLI state file ({} without one), `head_runs()`, `collect_token()`: a kept cluster is reused only while its
  head runs;
- `records`: the directories that the end of the cluster scans (its own directory, and each record of a test that uses
  it: Run.record);
- `secrets`: its secret values now (the cluster token, the join tokens), for the scans;
- `poller` (livepoller.Poller: `start()`, `stop()`) and `timeline`: its vast samples."""
from cluster import (REPO, Cluster, ReleaseCluster, aws_account_leftovers, aws_leftovers, credit_sample, invoices,  # noqa: F401
                     sweep_label, vast_key)


def suite_cluster(run, config, out_dir, home, kept=False, user=None):
    """A suite cluster of `config` (a YAML of tests/live/clusters/), as the admin, or as the restricted `user`."""
    if user is None:
        return Cluster(run, config, out_dir, home, kept)
    return Cluster(run, config, out_dir, home, kept, profile=user.profile, credentials_file=user.credentials_file)


def libraries_cluster(run, group_dir, home, host, suffix, kept):
    """The cluster of one libraries host (H1, H1b, H2, H2b, H3, gpu2, H4, H5)."""
    from libraries_setup import LibrariesCluster
    return LibrariesCluster(run, group_dir, home, host, suffix, kept)


def release_cluster(run, out_dir, home, region, version):
    """The getting-started tutorial's cluster, with ray-vast `version` from PyPI."""
    return ReleaseCluster(run, out_dir, home, region, version)


def restricted_user(out_dir, tmp_dir, date):
    """The IAM user `ray-vast-livetest-<date>` (restricted.RestrictedUser): `create()`, `reuse()`, `delete()`, `profile`,
    `credentials_file`, `secret`."""
    from restricted import RestrictedUser
    return RestrictedUser(out_dir, tmp_dir, date)
