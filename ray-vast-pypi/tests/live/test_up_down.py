"""Live `ray-vast up`, `dashboard`, `ray job submit` and `down` with no vast rental (plan step 10). Costs about $0.10 of EC2.

Run from the laptop, in a venv with `ray[default]==2.59.0` (for the `ray job` CLI) and this package:
`pytest -m live tests/live/test_up_down.py -s`
Everything goes through the `ray-vast` CLI, with a temporary HOME so `~/.ray-vast` is not touched.
Environment: RAY_VAST_LIVE_PROFILE (default `personal`); RAY_VAST_LIVE_CHECK_PROFILE (default: the same) for the final
check that nothing is left (it runs in this process, with the normal credentials file); RAY_VAST_LIVE_CREDENTIALS_FILE (default
~/.aws/credentials): the credentials file of the `ray-vast` commands.
The account needs the boundary policy `ray-vast-head-boundary` (getting started). `up` makes the SSH key pair. `down` runs
without `--profile`, so it proves that `down` uses the profile that `up` saved.
The vast key comes from VAST_API_KEY or ~/.config/vastai/vast_api_key.
"""
import os
import secrets
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import boto3
import pytest
import yaml

from ray_vast import commands

REGION = "us-west-2"
CLI = [sys.executable, "-c", "import sys; from ray_vast.cli import main; sys.exit(main(sys.argv[1:]))"]
BIN = Path(sys.executable).parent


@pytest.mark.live
def test_up_dashboard_job_submit_down_leaves_nothing(tmp_path):
    cluster = f"rvtest-{secrets.token_hex(2)}"
    config_path = tmp_path / "cluster.yaml"
    config_path.write_text(yaml.safe_dump({
        "cluster_name": cluster,
        "provider": {"image": "rayproject/ray:2.59.0-py311-cpu", "max_dollars_per_hour": 1.0},
        "head_node_type": "head",
        "available_node_types": {
            "head": {"resources": {"CPU": 0}, "node_config": {"region": REGION}},
            "gpu": {"resources": {"CPU": 4, "GPU": 1}, "min_workers": 0, "max_workers": 0,
                    "node_config": {"offer_query": {"gpu_ram": {"gte": 20000}}}},
        },
    }))
    profile = os.environ.get("RAY_VAST_LIVE_PROFILE", "personal")
    home = tmp_path / "home"
    home.mkdir()
    env = {**os.environ, "HOME": str(home), "VAST_API_KEY": commands._read_vast_key(),
           "AWS_CONFIG_FILE": os.environ.get("AWS_CONFIG_FILE", str(Path("~/.aws/config").expanduser())),
           "AWS_SHARED_CREDENTIALS_FILE": os.environ.get("RAY_VAST_LIVE_CREDENTIALS_FILE", str(Path("~/.aws/credentials").expanduser()))}
    env.pop("AWS_PROFILE", None)
    base = [str(config_path), "--profile", profile]
    # The final check lists volumes and IAM: a restricted user (ray-vast-user) may not, so it can use another profile.
    session = boto3.Session(profile_name=os.environ.get("RAY_VAST_LIVE_CHECK_PROFILE", profile), region_name=REGION)
    tunnel = None

    try:
        up = subprocess.run([*CLI, "up", *base], env=env, text=True, timeout=1800)
        assert up.returncode == 0
        key_file = home / ".ray-vast" / cluster / "ssh_key.pem"
        assert oct(key_file.stat().st_mode & 0o777) == "0o600"

        # `ray-vast dashboard` blocks (it is the tunnel), so it runs in the background. It copies the token first.
        tunnel = subprocess.Popen([*CLI, "dashboard", *base], env={**env, "PYTHONUNBUFFERED": "1"},
                                  stdout=subprocess.PIPE, start_new_session=True)
        for _ in range(60):
            try:
                socket.create_connection(("127.0.0.1", 8265), timeout=1).close()
                break
            except OSError:
                time.sleep(1)
        token_file = home / ".ray-vast" / cluster / "token"
        assert oct(token_file.stat().st_mode & 0o777) == "0o600" and oct(token_file.parent.stat().st_mode & 0o777) == "0o700"

        # Exactly the env line that `dashboard` printed: the job CLI of Ray 2.59.0 reads the token from RAY_AUTH_TOKEN_PATH.
        env_line = f"RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH={token_file}"
        printed = os.read(tunnel.stdout.fileno(), 4096).decode()
        assert env_line in printed
        job_env = {**os.environ, **dict(item.split("=", 1) for item in env_line.split())}
        submit = subprocess.run([str(BIN / "ray"), "job", "submit", "--address", "http://127.0.0.1:8265", "--", "python", "-c", "print(12345)"],
                                env=job_env, capture_output=True, text=True, timeout=300)
        print(submit.stdout, submit.stderr)
        assert submit.returncode == 0 and "12345" in submit.stdout
    finally:
        if tunnel:
            os.killpg(tunnel.pid, signal.SIGTERM)  # the python process and its ssh child
        down = subprocess.run([*CLI, "down", str(config_path)], env=env, text=True, capture_output=True, timeout=1800)
        print(down.stdout, down.stderr)
    assert down.returncode == 0 and "left: nothing" in down.stdout

    # Nothing tagged with the cluster name is left in the region (a terminated instance may stay listed for a while).
    ec2 = session.client("ec2", region_name=REGION)
    tag_filter = [{"Name": f"tag:{commands.TAG_KEY}", "Values": [cluster]}]
    assert commands._head_instances(ec2, cluster) == []
    assert ec2.describe_security_groups(Filters=tag_filter)["SecurityGroups"] == []
    assert ec2.describe_volumes(Filters=tag_filter)["Volumes"] == []
    assert ec2.describe_key_pairs(Filters=tag_filter)["KeyPairs"] == []
    iam = session.client("iam")
    for get, kwargs in ((iam.get_role, {"RoleName": f"ray-vast-{cluster}"}),
                        (iam.get_instance_profile, {"InstanceProfileName": f"ray-vast-{cluster}"})):
        with pytest.raises(iam.exceptions.NoSuchEntityException):
            get(**kwargs)
    assert not (home / ".ray-vast" / cluster).exists() and not (home / ".ray-vast" / f"{cluster}.json").exists()
