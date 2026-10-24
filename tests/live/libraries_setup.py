"""The `libraries` clusters of the live suite (clusters/libraries.yaml, one cluster for each host, so the hosts run in
parallel): gpu (2 GPU workers), gpu2 (one worker with 2 GPUs), big-a and big-b (one worker with CPU 64: more CPUs than
the processes at once), few-ports (one worker on a host with 14 to 63 direct ports). `LibrariesCluster` owns what the
tests of its host change: the cluster shape (which workers run) and the S3 access.

How a test gets its hosts. The head's autoscaler reads `/home/ubuntu/ray-vast-<cluster>/cluster.yaml` again in each
loop (Ray 2.59: autoscaler/v2/autoscaler.py calls `FileConfigReader.refresh_cached_autoscaling_config()` before each
reconcile, and node_provider.py takes the `node_config` of a launch from that config). `LibrariesCluster.set_shape()` writes
min_workers = max_workers for every worker type there (`cluster.shaped_config`). So the autoscaler starts exactly the workers of a
test and ends the others; the library code needs no node pinning, and no demand can start a worker of another type."""
import json
import re
import secrets
import shutil
import time
from pathlib import Path

import boto3
import yaml

from cluster import ADMIN_PROFILE, Cluster, alive_workers
from liverun import assert_ours, wait_until

HERE = Path(__file__).parent
SHAPE_TIMEOUT_S = 2400  # a GPU image pull can take 10 min; a failed candidate is replaced
DATE8 = time.strftime("%Y%m%d", time.gmtime())


def host_facts(row, machine_id):
    """The facts of one host, from one NODE_FACTS row (jobs/node_facts.py): pids.max (None for "max" or no file),
    the cgroup version, the Ray CPUs, whether the container has a process limit, the CPU quota in cores (cgroup v2
    `cpu.max`, or v1 `cpu.cfs_quota_us` / `cpu.cfs_period_us`; None for no quota), the visible CPUs (the smaller of
    `os.cpu_count()` and the affinity: the thread pools of Arrow, torch and gRPC size themselves by these) and the visible
    CPUs per quota core (2 decimals), and the cores: the smaller of the visible CPUs and the quota, as the head counts them
    for its 90% CPU rule."""
    value = row.get("pids.max")
    pids_max = int(value) if value and value.isdigit() else None
    raw = row.get("cpu_quota") or {}
    if "cpu.max" in raw:
        limit, period = raw["cpu.max"].split()
    else:
        limit, period = raw.get("cpu.cfs_quota_us"), raw.get("cpu.cfs_period_us")
    quota = round(int(limit) / int(period), 2) if limit and period and limit.lstrip("-").isdigit() and int(limit) > 0 and int(period) > 0 else None
    counts = [n for n in (row.get("cpu_count"), row.get("affinity")) if n]
    visible = min(counts) if counts else None
    return {"machine_id": machine_id, "pids_max": pids_max, "cgroup": row.get("cgroup"), "cpus": int(row["ray_cpus"]),
            "limited": pids_max is not None, "quota": quota, "visible_cpus": visible,
            "visible_per_quota": round(visible / quota, 2) if visible and quota else None,
            "cores": min(visible, quota) if visible and quota else visible}


def expected_limits(row):
    """(worker ports, RAY_num_workers_soft_limit) that the bootstrap must set on the node of one NODE_FACTS row: the
    product's own formula (ray_vast.worker.bootstrap.worker_ports_and_soft_limit) on the Ray CPUs, pids.max and
    mapped identity ports."""
    from ray_vast.worker.bootstrap import worker_ports_and_soft_limit
    facts = host_facts(row, row.get("machine_id"))
    return worker_ports_and_soft_limit(facts["cpus"], facts["pids_max"], row["identity_ports"])


def doc_snippet(markdown, heading, index, subs):
    """The `index`-th ```python block in the section `heading` of a user doc, with each (old, new) substitution applied.
    Each `old` must occur exactly once in the block, so a doc change makes the test fail, not test other code."""
    section = markdown.split("\n" + heading + "\n", 1)[1].split("\n## ", 1)[0]
    code = re.findall(r"```python\n(.*?)```", section, re.S)[index]
    for old, new in subs:
        if code.count(old) != 1:
            raise ValueError(f"{old!r} is in the snippet {code.count(old)} times, not exactly once")
        code = code.replace(old, new)
    return code


class S3Access:
    """The test bucket and the least-privilege IAM user of one host (admin calls; the log has no secret)."""

    def __init__(self, out_dir, host):
        self.bucket = f"ray-vast-rvtest-{DATE8}-{secrets.token_hex(2)}"
        self.user = f"ray-vast-livetest-{DATE8}-s3-{host}"
        assert_ours(self.bucket)
        assert_ours(self.user)
        session = boto3.Session(profile_name=ADMIN_PROFILE, region_name="us-west-2")
        self.s3, self.iam = session.client("s3"), session.client("iam")
        self.log = Path(out_dir) / "s3_iam.log"
        self.secret = None

    def note(self, text):
        with open(self.log, "a") as f:
            f.write(f"{time.strftime('%FT%TZ', time.gmtime())} {text}\n")

    def create(self):
        self.s3.create_bucket(Bucket=self.bucket, CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
        self.note(f"created bucket {self.bucket} (us-west-2, private)")
        self.delete_user()  # a user of the same date left by a killed run
        policy = {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"],
             "Resource": f"arn:aws:s3:::{self.bucket}"},
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload",
                                           "s3:ListMultipartUploadParts"], "Resource": f"arn:aws:s3:::{self.bucket}/*"}]}
        self.iam.create_user(UserName=self.user)
        self.iam.put_user_policy(UserName=self.user, PolicyName="rvtest-bucket", PolicyDocument=json.dumps(policy))
        key = self.iam.create_access_key(UserName=self.user)["AccessKey"]
        self.key_id, self.secret = key["AccessKeyId"], key["SecretAccessKey"]
        self.note(f"created user {self.user} with the inline policy {json.dumps(policy)} and one access key (not logged)")
        user_s3 = boto3.Session(aws_access_key_id=self.key_id, aws_secret_access_key=self.secret, region_name="us-west-2").client("s3")
        # A new access key takes some seconds to work.
        works = wait_until(lambda: user_s3.list_objects_v2(Bucket=self.bucket, MaxKeys=1), 60, done=lambda _: True, poll_s=2)
        if not works:
            raise RuntimeError(f"the new access key of {self.user} did not work in 60 s ({works.error})")
        time.sleep(10)
        return self

    @property
    def env_vars(self):
        return {"AWS_ACCESS_KEY_ID": self.key_id, "AWS_SECRET_ACCESS_KEY": self.secret, "AWS_DEFAULT_REGION": "us-west-2"}

    def objects(self, prefix):
        pages = self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix)
        return [o for page in pages for o in page.get("Contents", [])]

    def delete_bucket(self):
        assert_ours(self.bucket)
        try:
            keys = [{"Key": o["Key"]} for o in self.objects("")]
            for i in range(0, len(keys), 1000):
                self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": keys[i:i + 1000]})
            for upload in self.s3.list_multipart_uploads(Bucket=self.bucket).get("Uploads", []):
                self.s3.abort_multipart_upload(Bucket=self.bucket, Key=upload["Key"], UploadId=upload["UploadId"])
            self.s3.delete_bucket(Bucket=self.bucket)
            self.note(f"deleted {len(keys)} objects and the bucket {self.bucket}")
        except self.s3.exceptions.NoSuchBucket:
            pass

    def delete_user(self):
        assert_ours(self.user)
        try:
            for key in self.iam.list_access_keys(UserName=self.user)["AccessKeyMetadata"]:
                self.iam.delete_access_key(UserName=self.user, AccessKeyId=key["AccessKeyId"])
            for name in self.iam.list_user_policies(UserName=self.user)["PolicyNames"]:
                self.iam.delete_user_policy(UserName=self.user, PolicyName=name)
            self.iam.delete_user(UserName=self.user)
            self.note(f"deleted the user {self.user}, its access keys and its inline policy")
        except self.iam.exceptions.NoSuchEntityException:
            pass

    def delete(self):
        self.delete_bucket()
        self.delete_user()


class LibrariesCluster(Cluster):
    """The cluster of one libraries host (not up yet): libraries.yaml with only the head and the worker type of `host`
    (the node type of the same name), named `rvtest-lib-<host>-<suffix>`. The cluster owns `shape` (the workers that run
    now) and its S3 access (`s3()`)."""

    def __init__(self, run, group_dir, home, host, suffix, kept):
        config = yaml.safe_load((HERE / "clusters" / "libraries.yaml").read_text())
        config["cluster_name"] = f"{config['cluster_name']}-{host}-{suffix}"
        config["available_node_types"] = {name: t for name, t in config["available_node_types"].items() if name in ("head", host)}
        super().__init__(run, config, group_dir, home, kept)
        self.host = self.kind = host
        self._s3 = None

    def wait_shape(self, shape, timeout=SHAPE_TIMEOUT_S):
        """Wait until Ray has exactly the workers of `shape` and vast has no other instance of the cluster (the losing
        candidates are destroyed). Returns the seconds it took; raises TimeoutError."""
        want = {k: v for k, v in shape.items() if v}
        waited = wait_until(lambda: ({k: len(v) for k, v in alive_workers(self).items()}, len(self.instances())), timeout,
                            done=lambda seen: seen == (want, sum(want.values())), poll_s=15)
        if not waited:
            raise TimeoutError(f"shape {shape} not reached in {timeout} s; last seen {waited.seen}, last error {waited.error}")
        return waited.elapsed

    def ensure_shape(self, shape):
        self.set_shape(shape)
        return self.wait_shape(shape)

    def s3(self):
        """The S3 access of this host, made at its first use. Its secret key joins the cluster's secrets (the scan)."""
        if self._s3 is None:
            self._s3 = S3Access(self.dir, self.host).create()
            self.secrets.append(self._s3.secret)
        return self._s3

    def keep_one_worker(self, record):
        """Before the final `down`: one worker runs, so `down` must destroy it (min_workers 1 replaces a worker that ended)."""
        shape = {self.kind: 1}
        try:
            record.data("shape_wait_s", self.wait_shape(shape, timeout=1200) if self.shape == shape else self.ensure_shape(shape))
        except TimeoutError as e:
            record.data("shape_error", str(e))

    def end_checks(self, record, done):
        """After the final `down` (`done`; None for a kept cluster): it destroyed the running worker; the S3 test bucket and
        IAM user are deleted (also for a kept cluster: each run makes new ones). Each check goes to `record`; returns the
        names of the false ones (problems for liverun.end_cluster)."""
        checks = []
        if done is not None:
            destroyed = re.search(r"destroyed (\d+) vast instance\(s\)", done.stdout)
            checks.append(("down while a worker runs: `destroyed N vast instance(s)` with N >= 1 (the CLI reference)",
                           destroyed is not None and int(destroyed.group(1)) >= 1, destroyed.group(0) if destroyed else None))
        if self._s3 is not None:
            s3 = self._s3
            s3.delete()
            shutil.copy(s3.log, record.dir / "s3_iam.log")
            try:
                s3.s3.head_bucket(Bucket=s3.bucket)
                bucket_gone = False
            except s3.s3.exceptions.ClientError:
                bucket_gone = True
            try:
                s3.iam.get_user(UserName=s3.user)
                user_gone = False
            except s3.iam.exceptions.NoSuchEntityException:
                user_gone = True
            checks += [("the S3 test bucket is deleted", bucket_gone, s3.bucket), ("the IAM test user is deleted", user_gone, s3.user)]
        return [name for name, ok, detail in checks if not record.check(name, ok, detail)]


def node_facts(cluster, env, out_dir, name="node_facts"):
    """Cluster.node_facts, by node type: {node type: [NODE_FACTS row, ...]}."""
    nodes = {}
    for row in cluster.node_facts(env, out_dir, name):
        nodes.setdefault(row.get("node_type"), []).append(row)
    return nodes


def n_and_cpu(cluster, rows):
    """(N, CPU) for the workers of one test: N = the smallest `processes at once` of their start lines; CPU = the largest
    Ray CPUs (the rule of the library pages: num_cpus = CPU / N)."""
    counts = [cluster.processes_at_once(int(r["container_id"])) for r in rows]
    if None in counts:
        raise RuntimeError(f"a worker start line has no `processes at once`: {counts}")
    return min(counts), max(r["ray_cpus"] for r in rows)
