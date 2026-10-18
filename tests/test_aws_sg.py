import os
import boto3
import botocore.exceptions
import pytest
from moto import mock_aws

from ray_vast import aws_sg
from ray_vast.aws_sg import SecurityGroupRules, SgQuotaError


@pytest.fixture
def ec2():
    with mock_aws():
        yield boto3.client("ec2", region_name="us-west-2", config=aws_sg.ec2_client_config())


@pytest.fixture
def group_ids(ec2):
    """The 5 groups of a cluster, as `up` makes them."""
    return [ec2.create_security_group(GroupName=f"head-{i}", Description="test")["GroupId"] for i in range(5)]


@pytest.fixture
def group_id(group_ids):
    return group_ids[0]


def rules(ec2, group_id):
    perms = ec2.describe_security_groups(GroupIds=[group_id])["SecurityGroups"][0]["IpPermissions"]
    return sorted((r["CidrIp"], r.get("Description")) for p in perms for r in p["IpRanges"])


def test_allow_twice_makes_one_rule(ec2, group_id, group_ids):
    sg = SecurityGroupRules(ec2, group_ids, "c1")
    sg.allow_ip("192.0.2.4")
    sg.allow_ip("192.0.2.4")
    assert rules(ec2, group_id) == [("192.0.2.4/32", "ray-vast:c1")]


def test_revoke_missing_rule_is_ok(ec2, group_id, group_ids):
    sg = SecurityGroupRules(ec2, group_ids, "c1")
    sg.revoke_ip("192.0.2.4")
    sg.allow_ip("192.0.2.4")
    sg.revoke_ip("192.0.2.4")
    sg.revoke_ip("192.0.2.4")
    assert rules(ec2, group_id) == []


def test_revoke_all_keeps_other_rules(ec2, group_id, group_ids):
    ec2.authorize_security_group_ingress(GroupId=group_id, IpPermissions=[
        {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": "198.51.100.9/32", "Description": "ssh"}]},
        {"IpProtocol": "tcp", "FromPort": 0, "ToPort": 65535,
         "IpRanges": [{"CidrIp": "198.51.100.55/32", "Description": "ray-vast:other"}]}])
    sg = SecurityGroupRules(ec2, group_ids, "c1")
    sg.allow_ip("192.0.2.4")
    sg.allow_ip("192.0.2.5")
    sg.revoke_all()
    assert rules(ec2, group_id) == [("198.51.100.55/32", "ray-vast:other"), ("198.51.100.9/32", "ssh")]
    sg.revoke_all()  # nothing left: still ok


def test_revoke_ip_keeps_rule_of_other_cluster(ec2, group_id, group_ids):
    SecurityGroupRules(ec2, group_ids, "other").allow_ip("192.0.2.4")
    SecurityGroupRules(ec2, group_ids, "c1").revoke_ip("192.0.2.4")
    assert rules(ec2, group_id) == [("192.0.2.4/32", "ray-vast:other")]


def failing_with(code):
    """A stand-in for a boto3 call that fails with the AWS error `code`."""
    def fail(**kwargs):
        raise botocore.exceptions.ClientError({"Error": {"Code": code, "Message": "x"}}, "op")
    return fail


def test_quota_error_when_aws_rule_limit_is_reached(ec2, group_id, monkeypatch, group_ids):
    monkeypatch.setattr(ec2, "authorize_security_group_ingress", failing_with("RulesPerSecurityGroupLimitExceeded"))
    with pytest.raises(SgQuotaError):
        SecurityGroupRules(ec2, group_ids, "c1").allow_ip("192.0.2.4")


def test_allow_ip_ignores_duplicate_error_from_aws(ec2, group_id, monkeypatch, group_ids):
    monkeypatch.setattr(ec2, "authorize_security_group_ingress", failing_with("InvalidPermission.Duplicate"))
    SecurityGroupRules(ec2, group_ids, "c1").allow_ip("192.0.2.4")


def test_revoke_ip_ignores_not_found_error_from_aws(ec2, group_id, monkeypatch, group_ids):
    sg = SecurityGroupRules(ec2, group_ids, "c1")
    sg.allow_ip("192.0.2.4")  # the rule exists when we look, then vanishes before the revoke
    monkeypatch.setattr(ec2, "revoke_security_group_ingress", failing_with("InvalidPermission.NotFound"))
    sg.revoke_ip("192.0.2.4")


def test_other_aws_errors_are_raised(ec2, group_id, monkeypatch, group_ids):
    monkeypatch.setattr(ec2, "authorize_security_group_ingress", failing_with("UnauthorizedOperation"))
    with pytest.raises(botocore.exceptions.ClientError):
        SecurityGroupRules(ec2, group_ids, "c1").allow_ip("192.0.2.4")


@pytest.mark.live
def test_live_allow_revoke_on_temporary_group():
    ec2 = boto3.Session(profile_name=os.environ.get("RAYVAST_LIVE_PROFILE", "default"), region_name="us-west-2").client("ec2", config=aws_sg.ec2_client_config())
    vpc = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"][0]["VpcId"]
    group_id = ec2.create_security_group(GroupName="ray-vast-sg-rules-test", Description="ray-vast security group rules test", VpcId=vpc)["GroupId"]
    try:
        sg = SecurityGroupRules(ec2, [group_id], "sgtest")
        sg.allow_ip("192.0.2.1")
        sg.allow_ip("192.0.2.1")
        sg.allow_ip("192.0.2.2")
        assert [c for c, _ in rules(ec2, group_id)] == ["192.0.2.1/32", "192.0.2.2/32"]
        sg.revoke_ip("192.0.2.1")
        sg.revoke_ip("192.0.2.1")
        assert [c for c, _ in rules(ec2, group_id)] == ["192.0.2.2/32"]
        sg.revoke_all()
        assert rules(ec2, group_id) == []
    finally:
        ec2.delete_security_group(GroupId=group_id)


def test_cluster_names_that_share_a_prefix_do_not_share_rules(ec2, group_id, group_ids):
    SecurityGroupRules(ec2, group_ids, "demo").allow_ip("192.0.2.4")
    sg = SecurityGroupRules(ec2, group_ids, "c1")
    sg.revoke_all()
    assert rules(ec2, group_id) == [("192.0.2.4/32", "ray-vast:demo")]


def test_revoke_all_with_no_rules_sends_no_revoke_call(ec2, group_id, monkeypatch, group_ids):
    monkeypatch.setattr(ec2, "revoke_security_group_ingress", failing_with("InvalidParameterValue"))
    SecurityGroupRules(ec2, group_ids, "c1").revoke_all()


def test_ec2_client_config_sets_timeouts_and_retries():
    config = aws_sg.ec2_client_config()
    assert (config.connect_timeout, config.read_timeout) == (5, 30)
    assert config.retries == {"max_attempts": 5, "mode": "standard"}


def all_rules(ec2, group_ids):
    """(group index, cidr) for every rule in the 5 groups."""
    groups = {g["GroupId"]: g for g in ec2.describe_security_groups(GroupIds=group_ids)["SecurityGroups"]}
    return sorted((i, r["CidrIp"]) for i, gid in enumerate(group_ids) for p in groups[gid]["IpPermissions"]
                  for r in p["IpRanges"])


def test_a_rule_goes_into_the_next_group_when_the_first_is_full(ec2, group_ids):
    sg = SecurityGroupRules(ec2, group_ids, "c1")
    ips = [f"10.0.{n // 250}.{n % 250 + 1}" for n in range(61)]
    for ip in ips:
        sg.allow_ip(ip)
    rules_now = all_rules(ec2, group_ids)
    assert [i for i, _ in rules_now].count(0) == 60 and (1, f"{ips[60]}/32") in rules_now
    sg.allow_ip(ips[60])  # a rule that exists in group 1 is not added again to any group
    assert len(all_rules(ec2, group_ids)) == 61


def test_revoke_ip_removes_the_rule_from_the_group_that_holds_it(ec2, group_ids):
    for gid in (group_ids[2], group_ids[4]):
        ec2.authorize_security_group_ingress(GroupId=gid, IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 0, "ToPort": 65535,
            "IpRanges": [{"CidrIp": "192.0.2.4/32" if gid == group_ids[2] else "192.0.2.5/32", "Description": "ray-vast:c1"}]}])
    sg = SecurityGroupRules(ec2, group_ids, "c1")
    sg.revoke_ip("192.0.2.4")
    assert all_rules(ec2, group_ids) == [(4, "192.0.2.5/32")]
    sg.revoke_all()
    assert all_rules(ec2, group_ids) == []


def test_all_five_groups_full_raises_the_quota_error_that_names_the_quota(ec2, group_ids, monkeypatch):
    tried = []

    def full(**kwargs):
        tried.append(kwargs["GroupId"])
        raise botocore.exceptions.ClientError({"Error": {"Code": "RulesPerSecurityGroupLimitExceeded", "Message": "x"}}, "op")
    monkeypatch.setattr(ec2, "authorize_security_group_ingress", full)
    with pytest.raises(SgQuotaError, match="Service Quotas"):
        SecurityGroupRules(ec2, group_ids, "c1").allow_ip("192.0.2.4")
    assert tried == group_ids


def test_revoke_all_removes_the_rules_of_groups_0_and_1(ec2, group_ids):
    for gid, cidr in ((group_ids[0], "192.0.2.4/32"), (group_ids[1], "192.0.2.5/32")):
        ec2.authorize_security_group_ingress(GroupId=gid, IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 0, "ToPort": 65535, "IpRanges": [{"CidrIp": cidr, "Description": "ray-vast:c1"}]}])
    SecurityGroupRules(ec2, group_ids, "c1").revoke_all()
    assert all_rules(ec2, group_ids) == []
