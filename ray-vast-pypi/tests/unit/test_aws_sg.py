import boto3
import botocore.exceptions
import pytest
from moto import mock_aws

from ray_vast import aws_sg
from ray_vast.aws_sg import SecurityGroupRules, SgQuotaError


@pytest.fixture
def ec2():
    with mock_aws():
        yield boto3.client("ec2", region_name="us-west-2", config=aws_sg.client_config())


@pytest.fixture
def group_id(ec2):
    return ec2.create_security_group(GroupName="head", Description="test")["GroupId"]


def rules(ec2, group_id):
    perms = ec2.describe_security_groups(GroupIds=[group_id])["SecurityGroups"][0]["IpPermissions"]
    return sorted((r["CidrIp"], r.get("Description")) for p in perms for r in p["IpRanges"])


def test_allow_twice_makes_one_rule(ec2, group_id):
    sg = SecurityGroupRules(ec2, group_id, "c1")
    sg.allow_ip("1.2.3.4")
    sg.allow_ip("1.2.3.4")
    assert rules(ec2, group_id) == [("1.2.3.4/32", "ray-vast:c1")]


def test_revoke_missing_rule_is_ok(ec2, group_id):
    sg = SecurityGroupRules(ec2, group_id, "c1")
    sg.revoke_ip("1.2.3.4")
    sg.allow_ip("1.2.3.4")
    sg.revoke_ip("1.2.3.4")
    sg.revoke_ip("1.2.3.4")
    assert rules(ec2, group_id) == []


def test_revoke_all_keeps_other_rules(ec2, group_id):
    ec2.authorize_security_group_ingress(GroupId=group_id, IpPermissions=[
        {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": "9.9.9.9/32", "Description": "ssh"}]},
        {"IpProtocol": "tcp", "FromPort": 0, "ToPort": 65535,
         "IpRanges": [{"CidrIp": "5.5.5.5/32", "Description": "ray-vast:other"}]}])
    sg = SecurityGroupRules(ec2, group_id, "c1")
    sg.allow_ip("1.2.3.4")
    sg.allow_ip("1.2.3.5")
    sg.revoke_all()
    assert rules(ec2, group_id) == [("5.5.5.5/32", "ray-vast:other"), ("9.9.9.9/32", "ssh")]
    sg.revoke_all()  # nothing left: still ok


def test_revoke_ip_keeps_rule_of_other_cluster(ec2, group_id):
    SecurityGroupRules(ec2, group_id, "other").allow_ip("1.2.3.4")
    SecurityGroupRules(ec2, group_id, "c1").revoke_ip("1.2.3.4")
    assert rules(ec2, group_id) == [("1.2.3.4/32", "ray-vast:other")]


def test_quota_error_when_aws_rule_limit_is_reached(ec2, group_id, monkeypatch):
    err = botocore.exceptions.ClientError({"Error": {"Code": "RulesPerSecurityGroupLimitExceeded", "Message": "x"}}, "op")
    monkeypatch.setattr(ec2, "authorize_security_group_ingress", lambda **kw: (_ for _ in ()).throw(err))
    with pytest.raises(SgQuotaError):
        SecurityGroupRules(ec2, group_id, "c1").allow_ip("1.2.3.4")


@pytest.mark.live
def test_live_allow_revoke_on_temporary_group():
    ec2 = boto3.Session(profile_name="personal", region_name="us-west-2").client("ec2", config=aws_sg.client_config())
    vpc = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"][0]["VpcId"]
    group_id = ec2.create_security_group(GroupName="ray-vast-st6-test", Description="ray-vast step 6 test", VpcId=vpc)["GroupId"]
    try:
        sg = SecurityGroupRules(ec2, group_id, "st6")
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
