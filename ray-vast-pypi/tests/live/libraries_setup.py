"""The `libraries` clusters of the live suite (clusters/libraries.yaml, one cluster for each host H1, H2, H3, gpu2, so
the hosts run in parallel). `LibrariesCluster` owns what the scenarios of its host change: the cluster shape (which workers
run), the host pins of H1 and H2 (the offer query of each pinned type and the last host that matched), and the S3 access.

How a scenario gets its hosts. The head's autoscaler reads `/home/ubuntu/ray-vast-<cluster>/cluster.yaml` again in each
loop (Ray 2.59: autoscaler/v2/autoscaler.py calls `FileConfigReader.refresh_cached_autoscaling_config()` before each
reconcile, and node_provider.py takes the `node_config` of a launch from that config). `LibrariesCluster.set_shape()` writes
min_workers = max_workers for every worker type there (`livekit.shaped_config`), and the offer query of a pinned type.
So the autoscaler starts exactly the workers of a scenario and ends the others; the library code needs no node pinning,
and no demand can start a worker of another type. A host is blocked by a `machine_id` filter in that query (the provider
itself does not block a destroyed host: C10, docs/test-results.md)."""
import json
import re
import secrets
import shutil
import subprocess
import time
from pathlib import Path

import boto3
import yaml

from ray_vast.config import build_offer_query

import livekit
from cluster import ADMIN_PROFILE, Cluster, vast
from window import wait_until

HERE = Path(__file__).parent
# The named hosts (archive/results/2026-10-14/c9, 2026-10-16/c10, 2026-10-17) and the query to use when none of them is
# rentable. vast sets pids.max = 256 x the CPU quota in whole cores (2026-10-17: 1280 at 5.76 cores, 9728 at 38.4, 11776 at
# 46.08, 15616 at 61.44), so no host has many Ray CPUs and pids.max 1280. H1 is the large-host case: 32 or more Ray CPUs,
# any pids limit (m36376: 38 Ray CPUs, 9728, v1; m58013: 46, 11776, v2; m148655: 61, 15616, v1).
PINS = {
    "h1": {"named": [36376, 58013, 148655], "fallback": {"cpu_cores_effective": {"gte": 32}},
           "want": {"cpus": {"gte": 32}, "limited": True}},
    "h2": {"named": [51706, 55704], "fallback": {"gpu_ram": {"gte": 8000}}, "want": {"cgroup": "v2"}},
}
SHAPE_TIMEOUT_S = 2400  # a GPU image pull can take 10 min; a failed candidate is replaced
HOST_TYPE = {"H1": "h1", "H2": "h2", "H3": "gpu", "gpu2": "gpu2"}  # the worker type of each libraries host
DATE8 = time.strftime("%Y%m%d", time.gmtime())


class S3Access:
    """The test bucket and the least-privilege IAM user of one host (admin calls; the log has no secret)."""

    def __init__(self, out_dir, host):
        self.bucket = f"ray-vast-rvtest-{DATE8}-{secrets.token_hex(2)}"
        self.user = f"ray-vast-livetest-{DATE8}-s3-{host.lower()}"
        livekit.assert_ours(self.bucket)
        livekit.assert_ours(self.user)
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
        livekit.assert_ours(self.bucket)
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
        livekit.assert_ours(self.user)
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
    """The cluster of one libraries host (not up yet): libraries.yaml with only the head and the worker type of `host`,
    named `rvtest-lib-<host>-<suffix>`. For H1 and H2: the vast offers of the named hosts are searched first and recorded
    in `<group_dir>/host_offers.json` (a host that is not rentable now is replaced by the fallback query at rent time).
    The cluster owns `shape` (the workers that run now), `queries` (the offer query of each type in the head's YAML, when
    it differs from libraries.yaml), `matched` (node type -> the host facts of the last host that matched, rented first
    next time) and its S3 access (`s3()`)."""

    def __init__(self, run, group_dir, home, host, suffix, kept):
        config = yaml.safe_load((HERE / "clusters" / "libraries.yaml").read_text())
        kind = HOST_TYPE[host]
        config["cluster_name"] = f"{config['cluster_name']}-{host.lower()}-{suffix}"
        config["available_node_types"] = {name: t for name, t in config["available_node_types"].items() if name in ("head", kind)}
        if kind in PINS:
            found = vast().search_offers(build_offer_query(config["available_node_types"][kind], False, kind))
            offers = [{k: o.get(k) for k in ("machine_id", "id", "gpu_name", "num_gpus", "cpu_cores", "cpu_cores_effective",
                                              "dph_total", "geolocation", "compute_cap")} for o in found]
            Path(group_dir).mkdir(parents=True, exist_ok=True)
            (Path(group_dir) / "host_offers.json").write_text(json.dumps({kind: offers}, indent=1))
        super().__init__(run, config, group_dir, home, kept)
        self.host, self.kind = host, kind
        self.shape, self.queries, self.matched = {}, {}, {}
        self._s3 = None

    @property
    def head_yaml(self):
        return f"/home/ubuntu/ray-vast-{self.name}/cluster.yaml"

    def set_shape(self, shape):
        """Write the shape into the head's YAML (atomic: a new file, then mv). The autoscaler acts on its next loop."""
        config = yaml.safe_load(self.ssh(f"cat {self.head_yaml}"))
        text = yaml.safe_dump(livekit.shaped_config(config, shape, self.queries), sort_keys=False)
        subprocess.run([*self.ssh_args(), f"cat > {self.head_yaml}.new && mv {self.head_yaml}.new {self.head_yaml}"],
                       input=text, text=True, capture_output=True, timeout=120, check=True)
        self.shape = dict(shape)
        with open(self.dir / "shapes.log", "a") as f:
            f.write(json.dumps({"t": round(time.time(), 1), "shape": shape, "queries": self.queries}) + "\n")

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

    def pin_query(self, kind, blocked):
        """The offer query of `kind` for the next rental: its named hosts (the last match first) that are rentable now and
        not blocked; else the fallback query (for other types: the query of libraries.yaml) without the blocked machines."""
        node_type = self.config["available_node_types"][kind]
        pin = PINS.get(kind)
        if pin:
            last = self.matched.get(kind, {}).get("machine_id")
            named = [m for m in dict.fromkeys([last, *pin["named"]]) if m is not None and m not in blocked]
            if named:
                query = {"machine_id": {"in": named}}
                if vast().search_offers(build_offer_query({**node_type, "node_config": {**node_type["node_config"], "offer_query": query}},
                                                          False, kind)):
                    return query
            query = dict(pin["fallback"])
        else:
            query = dict(node_type["node_config"]["offer_query"])
        if blocked:
            query["machine_id"] = {"notin": sorted(set(query.get("machine_id", {}).get("notin", [])) | set(blocked))}
        return query

    def find(self, env, kind, want, keep, out_dir):
        """Rent one worker of `kind` next to the shape `keep` until its probe matches `want` (livekit.find_host: at most 3
        rentals; each mismatch is ended and its machine blocked). Leaves the shape keep + {kind: 1}. Returns (facts, tries);
        facts has the full NODE_PROBE row as `probe`."""
        def rent(blocked):
            self.queries[kind] = self.pin_query(kind, blocked)
            self.ensure_shape({**keep, kind: 1})
            row = probe(self, env, out_dir, name=f"probe_{kind}_{len(blocked)}")[kind][0]
            return dict(livekit.host_facts(row, row["machine_id"]), query=self.queries[kind], probe=row)

        def end(facts):
            self.ensure_shape(keep)

        facts, tries = livekit.find_host(want, rent, end)
        self.matched[kind] = facts
        return facts, tries

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
        names of the false ones (problems for livekit.end_cluster)."""
        checks = []
        if done is not None:
            destroyed = re.search(r"destroyed (\d+) vast instance\(s\)", done.stdout)
            checks.append(("down while a worker runs: `destroyed N vast instance(s)` with N >= 1 (cli.md:125)",
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


def alive_workers(cluster):
    """{node type: [Ray node id, ...]} of the ALIVE worker nodes (the node label `ray.io/node-group` is the node type)."""
    out = cluster.head_exec("ray list nodes --detail --format json --filter state=ALIVE --limit 1000", timeout=120)
    found = {}
    for node in json.loads(out[out.index("["):]):
        if not node.get("is_head_node"):
            found.setdefault((node.get("labels") or {}).get("ray.io/node-group"), []).append(node["node_id"])
    return found


def probe(cluster, env, out_dir, name="probe"):
    """Cluster.probe, by node type: {node type: [NODE_PROBE row, ...]}."""
    nodes = {}
    for row in cluster.probe(env, out_dir, name):
        nodes.setdefault(row.get("node_type"), []).append(row)
    return nodes


def b_and_cpus(rows):
    """(B, CPUS) for the workers of one scenario: B = the smallest worker ports - 4; CPUS = the largest Ray CPUs (core.md)."""
    return min(r["ports"] for r in rows) - 4, max(r["ray_cpus"] for r in rows)
