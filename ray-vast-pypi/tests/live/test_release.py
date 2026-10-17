"""Group `release` (docs/test-plan.md, claim matrix R01-R02): the getting-started tutorial, verbatim, from a clean venv
with ray-vast from PyPI. It runs only after the release is on PyPI: set RAYVAST_LIVE_RELEASE=<version> (for example
0.2.0). The cluster YAML is the packaged `gpu-24gb.yaml`; only `region` may change (RAYVAST_LIVE_RELEASE_REGION, default
us-west-2). About 25 min; vast under $1."""
import os
import re
import signal
import subprocess
import time
from pathlib import Path

import pytest

from livekit import check
from cluster import ADMIN_PROFILE, aws_leftovers, vast_key, wait_port

pytestmark = pytest.mark.release
VERSION = os.environ.get("RAYVAST_LIVE_RELEASE")
REGION = os.environ.get("RAYVAST_LIVE_RELEASE_REGION", "us-west-2")
HELLO = '''import ray

ray.init()

@ray.remote(num_gpus=1)
def where():
    import socket
    return socket.gethostname(), ray.get_gpu_ids()

print(ray.get(where.remote()))
print(ray.cluster_resources())
'''


@pytest.fixture(scope="module")
def release(tmp_path_factory, results_dir):
    """A clean venv, the getting-started install and `cp` commands, a private HOME. Yields a dict with the shell env."""
    if not VERSION:
        pytest.skip("set RAYVAST_LIVE_RELEASE=<version> after the release is on PyPI")
    work = tmp_path_factory.mktemp("release")
    home = work / "home"
    home.mkdir()
    env = {**os.environ, "HOME": str(home), "VAST_API_KEY": vast_key(), "AWS_PROFILE": ADMIN_PROFILE,
           "AWS_CONFIG_FILE": str(Path("~/.aws/config").expanduser()),
           "AWS_SHARED_CREDENTIALS_FILE": str(Path("~/.aws/credentials").expanduser())}
    env.pop("VIRTUAL_ENV", None)
    out = results_dir / "_session" / "release"
    out.mkdir(parents=True, exist_ok=True)
    started = time.time()
    shell = lambda script, timeout=1800: subprocess.run(["bash", "-c", f"cd {work} && {script}"], env=env, capture_output=True,
                                                        text=True, timeout=timeout)
    done = shell('python3 -m venv .venv && source .venv/bin/activate && pip install -q ray-vast "ray[default]==2.59.*" && '
                 'ray-vast --help >/dev/null && python -c "import ray_vast; print(ray_vast.__version__)"')
    (out / "install.txt").write_text(done.stdout + done.stderr)
    assert done.returncode == 0 and done.stdout.strip().endswith(VERSION), done.stderr[-500:]
    done = shell('source .venv/bin/activate && cp "$(python -c \'import ray_vast, os; print(os.path.dirname(ray_vast.__file__))\')/'
                 f'examples/gpu-24gb.yaml" cluster.yaml && sed -i "s/region: us-west-2/region: {REGION}/" cluster.yaml')
    assert done.returncode == 0, done.stderr
    state = {"work": work, "env": env, "shell": shell, "out": out, "started": started}
    yield state
    done = shell("source .venv/bin/activate && ray-vast down cluster.yaml")
    (out / "down_final.txt").write_text(done.stdout + done.stderr)


def test_R01_install_from_pypi_and_up(record, release):
    started = time.time()
    done = release["shell"]("source .venv/bin/activate && ray-vast up cluster.yaml --profile " + ADMIN_PROFILE, timeout=2400)
    record.evidence("up.txt", done.stdout + done.stderr)
    record.data("up_s", round(time.time() - started))
    lines = done.stdout.splitlines()
    check(record, "up exits 0", done.returncode == 0, (done.stderr or done.stdout)[-300:])
    check(record, f"first line: installing ray-vast=={VERSION} from PyPI", lines[:1] == [f"installing ray-vast=={VERSION} from PyPI"], lines[:1])
    version = release["shell"]("source .venv/bin/activate && echo 'sudo docker exec ray-vast-head python -c \"import ray_vast; "
                               "print(\\\"HEADVERSION\\\", ray_vast.__version__)\"' | ray-vast attach cluster.yaml", timeout=300)
    check(record, f"the head has {VERSION}", f"HEADVERSION {VERSION}" in version.stdout, version.stdout[-200:])


def test_R02_the_getting_started_tutorial_verbatim(record, release):
    work, env = release["work"], release["env"]
    proc = subprocess.Popen(["bash", "-c", f"cd {work} && source .venv/bin/activate && exec ray-vast dashboard cluster.yaml"], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)
    try:
        printed = [proc.stdout.readline() for _ in range(3)]
        check(record, "the dashboard tunnel opens", wait_port(8265, 60), printed)
        export = next(line.strip() for line in printed if line.strip().startswith("export "))
        (work / "hello.py").write_text(HELLO)
        done = release["shell"](f"source .venv/bin/activate && {export} && ray job submit --address http://127.0.0.1:8265 "
                                "--working-dir . -- python hello.py", timeout=2400)
        record.evidence("job.txt", done.stdout + done.stderr)
        found = re.search(r"\('([^']+)', \[0\]\)", done.stdout)
        check(record, "hello.py prints a host name and [0]", done.returncode == 0 and found is not None, done.stdout[-300:])
        status = release["shell"]("source .venv/bin/activate && echo 'sudo docker exec ray-vast-head ray status' | ray-vast attach cluster.yaml",
                                  timeout=300)
        record.evidence("ray_status.txt", status.stdout)
        check(record, "ray status through attach", "Node status" in status.stdout, "")
        os.killpg(proc.pid, signal.SIGINT)  # Ctrl-C in a terminal goes to the process group
        rest = proc.communicate(timeout=60)[0]
        check(record, "Ctrl-C prints `tunnel closed`", "tunnel closed" in rest, rest[-100:])
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
    done = release["shell"]("source .venv/bin/activate && ray-vast down cluster.yaml")
    record.evidence("down.txt", done.stdout + done.stderr)
    out = done.stdout.splitlines()
    check(record, "the down output matches the docs", out and out[0].startswith("terminating head ") and out[-1] == "left: nothing" and
          "deleted key pair ray-vast-gpu-24gb" in out and "deleted role ray-vast-gpu-24gb" in out, out[:1] + out[-4:])
    check(record, "AWS check", aws_leftovers("gpu-24gb", REGION) == [], "")
    minutes = (time.time() - release["started"]) / 60
    record.data("tutorial_minutes", round(minutes, 1))
    # getting-started.md:4 ("It takes about 20 minutes"); 30 min is the bound for "about 20 minutes".
    check(record, "the tutorial takes about 20 minutes (at most 30)", minutes <= 30, round(minutes, 1))
