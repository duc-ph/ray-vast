"""Shared fixtures for the provider tests: an in-memory vast, a fake clock, a moto security group."""
import boto3
import pytest
from moto import mock_aws

from ray_vast import aws_sg, pki
from ray_vast.provider import VastNodeProvider
from ray_vast.vast_api import VastError

class FakeVast:
    """The vast boundary, in memory. `instances` maps id -> the dict that list_instances returns."""

    def __init__(self):
        self.offers, self.instances, self.calls, self.fail_create = [], {}, [], False
        self.fail_list = self.fail_destroy = False
        self.next_id, self.requests = 100, []
        self.on_create = None  # one-shot hook that runs inside the next create_instance call, before the rental
        self.state = None  # set by the env fixture: every call checks that the provider lock is not held

    def _call(self, name):
        assert self.state is None or not self.state.lock._is_owned(), f"{name} called with state.lock held"
        self.calls.append(name)

    def search_offers(self, query, order_by="dph_total", limit=100):
        self._call("search_offers")
        self.last_query = query
        return list(self.offers)

    def create_instance(self, offer_id, image, env, args, label, disk_gb):
        self._call("create_instance")
        hook, self.on_create = self.on_create, None
        if hook:
            hook()
        if self.fail_create:
            raise VastError("PUT: HTTP 500")
        self.next_id += 1
        self.requests.append(dict(offer_id=offer_id, image=image, env=env, args=args, label=label, disk_gb=disk_gb))
        self.instances[self.next_id] = {"id": self.next_id, "label": label, "actual_status": "loading",
                                        "public_ipaddr": None, "status_msg": ""}
        return self.next_id

    def list_instances(self):
        self._call("list_instances")
        if self.fail_list:
            raise VastError("GET /instances/: HTTP 500")
        return [dict(i) for i in self.instances.values()]

    def destroy_instance(self, instance_id, key=None):
        self._call("destroy_instance")
        if self.fail_destroy:
            raise VastError("DELETE: HTTP 500")
        self.instances.pop(instance_id, None)

    def get_instance(self, instance_id, key=None):
        return self.instances.get(instance_id)

    def run(self, instance_id, ip):
        self.instances[instance_id].update(actual_status="running", public_ipaddr=ip)


class Clock:
    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now


def offer(offer_id, machine_id, dph=0.5, inet_down=1000):
    return {"id": offer_id, "machine_id": machine_id, "dph_total": dph, "inet_down": inet_down}


@pytest.fixture
def env(tmp_path):
    secrets = tmp_path / "secrets"
    pki.write_head_files(secrets, "9.9.9.9")
    (secrets / "vast_api_key").write_text("vast-key\n")
    with mock_aws():
        ec2 = boto3.client("ec2", region_name="us-west-2", config=aws_sg.ec2_client_config())
        group_id = ec2.create_security_group(GroupName="head", Description="t")["GroupId"]
        vast, clock = FakeVast(), Clock()
        config = {"head_ip": "9.9.9.9", "secrets_dir": str(secrets), "security_group_id": group_id,
                  "region": "us-west-2", "image": "img:1", "max_dollars_per_hour": 10, "candidate_factor": 2}
        providers = []

        def make(start_threads=False, **overrides):
            p = VastNodeProvider({**config, **overrides}, "c1", vast=vast, ec2=ec2, clock=clock, start_threads=start_threads)
            providers.append(p)
            vast.state = p.state
            return p

        def rules():
            perms = ec2.describe_security_groups(GroupIds=[group_id])["SecurityGroups"][0]["IpPermissions"]
            return sorted(r["CidrIp"] for p in perms for r in p["IpRanges"])

        yield type("Env", (), dict(vast=vast, clock=clock, make=staticmethod(make), rules=staticmethod(rules),
                                   ec2=ec2, group_id=group_id))
        for p in providers:
            p.close()


