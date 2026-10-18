"""A release check: the getting-started tutorial, verbatim, from a clean venv with ray-vast from PyPI. It runs only
after the release is on PyPI: set RAYVAST_LIVE_RELEASE=<version> (for example 0.3.0). The cluster YAML is the packaged `gpu-24gb.yaml`; only `region` may change (RAYVAST_LIVE_RELEASE_REGION, default
us-west-2). The `release` fixture (conftest) is a cluster.ReleaseCluster: the cluster joins the run and
liverun.end_cluster ends it (no second `down` after the tutorial's clean one; the sweep, the AWS check and the scan).
About 25 min; vast under $1."""
import os
import re
import signal
import subprocess
import time


from cluster import wait_port

HELLO = '''import ray

ray.init()

@ray.remote(num_gpus=1)
def where():
    import socket
    return socket.gethostname(), ray.get_gpu_ids()

print(ray.get(where.remote()))
print(ray.cluster_resources())
'''


def test_install_from_pypi_and_up(record, release):
    """The `up` of the fixture ran the install, the `cp` command and `ray-vast up cluster.yaml --profile`."""
    up = release.up_output
    record.evidence("up.txt", up["stdout"] + up["stderr"])
    record.data("up_s", up["wall_s"])
    lines = up["stdout"].splitlines()
    record.check("up exits 0", up["returncode"] == 0, (up["stderr"] or up["stdout"])[-300:])
    record.check(f"first line: installing ray-vast=={release.version} from PyPI",
         lines[:1] == [f"installing ray-vast=={release.version} from PyPI"], lines[:1])
    version = release.shell("source .venv/bin/activate && echo 'sudo docker exec ray-vast-head python -c \"import ray_vast; "
                               "print(\\\"HEADVERSION\\\", ray_vast.__version__)\"' | ray-vast attach cluster.yaml", timeout=300)
    record.check(f"the head has {release.version}", f"HEADVERSION {release.version}" in version.stdout, version.stdout[-200:])


def test_the_getting_started_tutorial_verbatim(record, release):
    work, env = release.work, release.env
    proc = subprocess.Popen(["bash", "-c", f"cd {work} && source .venv/bin/activate && exec ray-vast dashboard cluster.yaml"], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)
    try:
        printed = [proc.stdout.readline() for _ in range(3)]
        record.require("the dashboard tunnel opens", wait_port(8265, 60), printed)
        export = next(line.strip() for line in printed if line.strip().startswith("export "))
        (work / "hello.py").write_text(HELLO)
        done = release.shell(f"source .venv/bin/activate && {export} && ray job submit --address http://127.0.0.1:8265 "
                                "--working-dir . -- python hello.py", timeout=2400)
        record.evidence("job.txt", done.stdout + done.stderr)
        found = re.search(r"\('([^']+)', \[0\]\)", done.stdout)
        record.check("hello.py prints a host name and [0]", done.returncode == 0 and found is not None, done.stdout[-300:])
        status = release.shell("source .venv/bin/activate && echo 'sudo docker exec ray-vast-head ray status' | ray-vast attach cluster.yaml",
                                  timeout=300)
        record.evidence("ray_status.txt", status.stdout)
        record.check("ray status through attach", "Node status" in status.stdout, "")
        os.killpg(proc.pid, signal.SIGINT)  # Ctrl-C in a terminal goes to the process group
        rest = proc.communicate(timeout=60)[0]
        record.check("Ctrl-C prints `tunnel closed`", "tunnel closed" in rest, rest[-100:])
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
    done = release.down()  # the end of the run checks AWS and vast after it (liverun.end_cluster)
    record.evidence("down.txt", done.stdout + done.stderr)
    out = done.stdout.splitlines()
    record.check("the down output matches the docs", out and out[0].startswith("terminating head ") and out[-1] == "left: nothing" and
         "deleted key pair ray-vast-gpu-24gb" in out and "deleted role ray-vast-gpu-24gb" in out, out[:1] + out[-4:])
    minutes = (time.time() - release.started) / 60
    record.data("tutorial_minutes", round(minutes, 1))
    # Getting started: "It takes about 20 minutes"; 30 min is the bound for "about 20 minutes".
    record.check("the tutorial takes about 20 minutes (at most 30)", minutes <= 30, round(minutes, 1))
