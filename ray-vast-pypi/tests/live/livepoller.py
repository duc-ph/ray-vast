"""The vast poller of one cluster: every cluster kind (cluster.Cluster, cluster.ReleaseCluster) owns one. In a thread, every
`period` s: the vast instances with the cluster label go to `timeline` (in memory) and to a JSONL file (no env), and
each join token that vast shows joins `secrets` (the cluster's list, for the secret scan; never written).
The timeline feeds the observation window (window.py), the spend estimate (liverecords.timeline_spend) and the sweep of a
cluster with a foreign name (liverun.end_cluster)."""
import json
import threading
import time

FIELDS = ("id", "actual_status", "dph_total", "machine_id", "public_ipaddr", "start_date", "cpu_cores_effective", "gpu_name")


class Poller:
    """`vast()` gives a vast client (list_instances); `label` is the cluster label `ray-vast:<name>`; `path` the JSONL file."""

    def __init__(self, vast, label, path, secrets):
        self.vast, self.label, self.path, self.secrets = vast, label, path, secrets
        self.timeline = []
        self._thread = None

    def instances(self):
        """The vast instances of the cluster now."""
        return [i for i in self.vast().list_instances() if i.get("label") == self.label]

    def sample(self):
        """One sample: {"t", "instances"} ({"t", "error"} when vast does not answer). It joins the timeline."""
        row = {"t": round(time.time(), 1)}
        try:
            found = self.instances()
            for i in found:
                env = i.get("extra_env") or []
                pairs = env.items() if isinstance(env, dict) else [p for p in env if isinstance(p, (list, tuple)) and len(p) == 2]
                token = next((v for k, v in pairs if k == "RAYVAST_JOIN_TOKEN"), None)
                if token and token not in self.secrets:
                    self.secrets.append(token)
            row["instances"] = [{k: i.get(k) for k in FIELDS} for i in found]
        except Exception as e:  # one bad sample must not end the timeline
            row["error"] = type(e).__name__
        self.timeline.append(row)
        return row

    def start(self, period=10):
        stop = threading.Event()

        def run():
            with open(self.path, "a") as f:
                while not stop.is_set():
                    f.write(json.dumps(self.sample()) + "\n")
                    f.flush()
                    stop.wait(period)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        self._thread = (stop, thread)

    def stop(self):
        if self._thread:
            self._thread[0].set()
            self._thread[1].join(timeout=60)
            self._thread = None
