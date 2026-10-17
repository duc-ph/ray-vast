"""The `libraries` clusters of the live suite (clusters/libraries.yaml, one cluster for each host H1, H2, H3, gpu2, so
the hosts run in parallel; H4 is the host of the thread measurement). `LibrariesCluster` owns what the scenarios of its host change: the cluster shape (which workers
run), the host pins of H1 and H2 (the offer query of each pinned type and the last host that matched), and the S3 access.

How a scenario gets its hosts. The head's autoscaler reads `/home/ubuntu/ray-vast-<cluster>/cluster.yaml` again in each
loop (Ray 2.59: autoscaler/v2/autoscaler.py calls `FileConfigReader.refresh_cached_autoscaling_config()` before each
reconcile, and node_provider.py takes the `node_config` of a launch from that config). `LibrariesCluster.set_shape()` writes
min_workers = max_workers for every worker type there (`shaped_config`), and the offer query of a pinned type.
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

from cluster import ADMIN_PROFILE, Cluster, vast
from liverun import assert_ours, wait_until

HERE = Path(__file__).parent
# The named hosts (the S6 survey, archive/results/2026-10-17/survey/machines.csv) and the query to use when none of them is
# rentable. vast sets pids.max = 256 x the CPU quota in whole cores, so a worker has 58 worker ports (B = 54) from 17
# effective cores up. H1 and H2 are the hosts where the process settings are needed: the h1 and h2 types declare CPU 64,
# so Ray has 64 CPUs, more than B (decisions.md 2026-10-17, "on hosts with 64 or more effective cores"). The provider adds
# cpu_cores_effective >= 64 to the offer query. The probe checks the container's cores: at least 0.9 x 64, as the head does. H1: any pids limit (m148655 61.44 cores, 15616, v1; m45547 v1; m151137 v1). H2: cgroup v2
# (m149678 v2; m153955, m154115, m154116 A40 v2, verified, not datacenter). H4: many visible CPUs for each quota core, the
# thread measurement: the h4 type declares CPU 16, and the query asks for 128 or more host cores and at most 32 effective
# cores, so 4 or more visible CPUs for each core (m45592 128 CPUs at 15.36 cores, v2; m43571, m45777, m45775; m153046).
PINS = {
    "h1": {"named": [148655, 45547, 151137], "fallback": {"verified": {"eq": True}, "datacenter": {"eq": True}},
           "want": {"cores": {"gte": 0.9 * 64}, "limited": True}},
    "h2": {"named": [149678, 153955, 154115, 154116], "fallback": {"verified": {"eq": True}},
           "want": {"cgroup": "v2", "cores": {"gte": 0.9 * 64}}},
    "h4": {"named": [45592, 43571, 45777, 45775, 153046],
           "fallback": {"verified": {"eq": True}, "datacenter": {"eq": True}, "cpu_cores": {"gte": 128}, "cpu_cores_effective": {"gte": 16, "lte": 32}},
           "want": {"visible_per_quota": {"gte": 4}, "limited": True}},
}
SHAPE_TIMEOUT_S = 2400  # a GPU image pull can take 10 min; a failed candidate is replaced
HOST_TYPE = {"H1": "h1", "H2": "h2", "H3": "gpu", "gpu2": "gpu2", "H4": "h4"}  # the worker type of each libraries host
DATE8 = time.strftime("%Y%m%d", time.gmtime())


class HostNotFound(Exception):
    pass


HOST_RENTALS_MAX = 3


def find_host(want, rent, end):
    """Rent a host until its probe matches `want` (for example {"cores": {"gte": 57.6}, "cgroup": "v1"}: a value is the exact
    value of that fact, or {"gte": n} for a number of at least n).
    `rent(blocked)` rents one host, never on a blocked machine, and returns its probe (with `machine_id`). `end(probe)`
    ends a host that does not match. vast can give the same machine again, so each tried machine is blocked.
    Returns (the matching probe, the list of tries); raises HostNotFound after HOST_RENTALS_MAX rentals."""
    blocked, tries = [], []
    for _ in range(HOST_RENTALS_MAX):
        probe = rent(list(blocked))
        match = all(probe.get(key) is not None and probe[key] >= value["gte"] if isinstance(value, dict) else probe.get(key) == value
                    for key, value in want.items())
        tries.append({**probe, "match": match})
        if match:
            return probe, tries
        end(probe)
        blocked.append(probe["machine_id"])
    raise HostNotFound(f"no host matched {want} in {HOST_RENTALS_MAX} rentals (machines {', '.join(map(str, blocked))})")


def shaped_config(config, shape, queries=None):
    """The head's cluster YAML for one cluster shape ({node type: workers}): every worker type gets min_workers =
    max_workers = its count (0 if not named), so the autoscaler starts exactly these workers and no other. `queries`
    ({node type: offer_query}) replaces the offer query of a type (a host pin or a block). Returns a copy."""
    import copy
    config = copy.deepcopy(config)
    for name, node_type in config["available_node_types"].items():
        if name == config["head_node_type"]:
            continue
        node_type["min_workers"] = node_type["max_workers"] = shape.get(name, 0)
        if name in (queries or {}):
            node_type["node_config"]["offer_query"] = queries[name]
    config["max_workers"] = sum(shape.values())
    return config


def host_facts(probe, machine_id):
    """What `find_host` compares, from one NODE_PROBE row (jobs/node_probe.py): pids.max (None for "max" or no file),
    the cgroup version, the Ray CPUs, whether the container has a process limit, the CPU quota in cores (cgroup v2
    `cpu.max`, or v1 `cpu.cfs_quota_us` / `cpu.cfs_period_us`; None for no quota), the visible CPUs (the smaller of
    `os.cpu_count()` and the affinity: the thread pools of Arrow, torch and gRPC size themselves by these) and the visible
    CPUs per quota core (2 decimals), and the cores: the smaller of the visible CPUs and the quota, as the head counts them
    for its 90% CPU rule."""
    value = probe.get("pids.max")
    pids_max = int(value) if value and value.isdigit() else None
    raw = probe.get("cpu_quota") or {}
    if "cpu.max" in raw:
        limit, period = raw["cpu.max"].split()
    else:
        limit, period = raw.get("cpu.cfs_quota_us"), raw.get("cpu.cfs_period_us")
    quota = round(int(limit) / int(period), 2) if limit and period and limit.lstrip("-").isdigit() and int(limit) > 0 and int(period) > 0 else None
    counts = [n for n in (probe.get("cpu_count"), probe.get("affinity")) if n]
    visible = min(counts) if counts else None
    return {"machine_id": machine_id, "pids_max": pids_max, "cgroup": probe.get("cgroup"), "cpus": int(probe["ray_cpus"]),
            "limited": pids_max is not None, "quota": quota, "visible_cpus": visible,
            "visible_per_quota": round(visible / quota, 2) if visible and quota else None,
            "cores": min(visible, quota) if visible and quota else visible}


def expected_limits(probe):
    """(worker ports, RAY_num_workers_soft_limit) that the bootstrap must set on the node of one NODE_PROBE row: the
    product's own formula (ray_vast.worker.bootstrap.worker_ports_and_soft_limit) on the probed Ray CPUs, pids.max and
    mapped identity ports."""
    from ray_vast.worker.bootstrap import worker_ports_and_soft_limit
    facts = host_facts(probe, probe.get("machine_id"))
    return worker_ports_and_soft_limit(facts["cpus"], facts["pids_max"], probe["identity_ports"])


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
        self.user = f"ray-vast-livetest-{DATE8}-s3-{host.lower()}"
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
    """The cluster of one libraries host (not up yet): libraries.yaml with only the head and the worker type of `host`,
    named `rvtest-lib-<host>-<suffix>`. For H1, H2 and H4: the vast offers of the named hosts are searched first and recorded
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
            found = vast().search_offers(build_offer_query(config["available_node_types"][kind], kind))
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
        text = yaml.safe_dump(shaped_config(config, shape, self.queries), sort_keys=False)
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
                                                          kind)):
                    return query
            query = dict(pin["fallback"])
        else:
            query = dict(node_type["node_config"]["offer_query"])
        if blocked:
            query["machine_id"] = {"notin": sorted(set(query.get("machine_id", {}).get("notin", [])) | set(blocked))}
        return query

    def find(self, env, kind, want, keep, out_dir):
        """Rent one worker of `kind` next to the shape `keep` until its probe matches `want` (find_host: at most 3
        rentals; each mismatch is ended and its machine blocked). Leaves the shape keep + {kind: 1}. Returns (facts, tries);
        facts has the full NODE_PROBE row as `probe`."""
        def rent(blocked):
            self.queries[kind] = self.pin_query(kind, blocked)
            self.ensure_shape({**keep, kind: 1})
            row = probe(self, env, out_dir, name=f"probe_{kind}_{len(blocked)}")[kind][0]
            return dict(host_facts(row, row["machine_id"]), query=self.queries[kind], probe=row)

        def end(facts):
            self.ensure_shape(keep)

        facts, tries = find_host(want, rent, end)
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
        names of the false ones (problems for liverun.end_cluster)."""
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
