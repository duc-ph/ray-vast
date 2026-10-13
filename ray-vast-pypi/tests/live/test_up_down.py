"""Live `up` then `down` with no vast rental (plan step 10). Costs about $0.10 of EC2.

Run from the laptop: `pytest -m live tests/live/test_up_down.py -s`
Environment: RAY_VAST_LIVE_PROFILE (default `personal`), RAY_VAST_LIVE_KEY_FILE (default ~/phduc-personal.pem),
RAY_VAST_LIVE_KEY_NAME (default phduc-personal). The vast key comes from VAST_API_KEY or ~/.config/vastai/vast_api_key.
"""
import os
import secrets
import subprocess
import sys
from pathlib import Path

import boto3
import pytest
import yaml

from ray_vast import commands
from ray_vast.vast_api import VastClient

REGION = "us-west-2"
ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.live
def test_up_status_down_leaves_nothing(tmp_path):
    cluster = f"rvtest-{secrets.token_hex(2)}"
    config_path = tmp_path / "cluster.yaml"
    config_path.write_text(yaml.safe_dump({
        "cluster_name": cluster,
        "max_workers": 0,
        "provider": {"region": REGION, "image": "rayproject/ray:2.59.0-py311-cpu", "max_dollars_per_hour": 1.0},
        "head_node_type": "head",
        "available_node_types": {
            "head": {"resources": {"CPU": 0}, "node_config": {}, "max_workers": 0},
            "gpu": {"resources": {"CPU": 4, "GPU": 1}, "max_workers": 0, "node_config": {"offer_query": {"gpu_ram": {"gte": 20000}}}},
        },
    }))
    session = boto3.Session(profile_name=os.environ.get("RAY_VAST_LIVE_PROFILE", "personal"), region_name=REGION)
    key_file = Path(os.environ.get("RAY_VAST_LIVE_KEY_FILE", "~/phduc-personal.pem")).expanduser()
    state_dir = tmp_path / "state"
    ssh = commands.Ssh(key_file, tmp_path / "known_hosts")
    subprocess.run([sys.executable, "-m", "build", "--wheel", "--outdir", str(tmp_path / "dist"), str(ROOT)], check=True, capture_output=True)
    [wheel] = (tmp_path / "dist").glob("ray_vast-*.whl")
    vast = VastClient(commands._read_vast_key())

    try:
        head_ip = commands.up(str(config_path), session, ssh, key_name=os.environ.get("RAY_VAST_LIVE_KEY_NAME", "phduc-personal"), wheel=wheel,
                         vast_key=commands._read_vast_key(), operator_ip=commands._public_ip(), state_dir=state_dir)
        status = ssh.run(head_ip, f"sudo docker exec {commands.CONTAINER} ray status")
        print(status)
        assert "1 head" in status
        monitor_log = ssh.run(head_ip, f"sudo docker exec {commands.CONTAINER} bash -c 'cat /tmp/ray/session_latest/logs/monitor.log'")
        # v2 marks the head TERMINATED within seconds if the provider does not list it (finding A); it must reach RAY_RUNNING.
        assert "type=head, cloud_instance_id=head" in monitor_log and "ALLOCATED->RAY_RUNNING" in monitor_log
        assert "TERMINATED" not in monitor_log
    finally:
        left = commands.down(cluster, REGION, session, vast, state_dir)
    assert left == []

    # Nothing tagged with the cluster name is left in the region (a terminated instance may stay listed for a while).
    ec2 = session.client("ec2", region_name=REGION)
    tag_filter = [{"Name": f"tag:{commands.TAG_KEY}", "Values": [cluster]}]
    assert commands._head_instances(ec2, cluster) == []
    assert ec2.describe_security_groups(Filters=tag_filter)["SecurityGroups"] == []
    assert ec2.describe_volumes(Filters=tag_filter)["Volumes"] == []
