"""Ingress rules on the cluster's security groups (up to 5, design 8.3): TCP all ports from one worker IP, with the
description `ray-vast:<cluster>`. A rule goes into the first group with room."""
import botocore.config
import botocore.exceptions

class SgQuotaError(Exception):
    """No security group of the cluster has room for another worker IP."""


def ec2_client_config():
    """botocore Config for the ec2 client that the caller passes to SecurityGroupRules."""
    return botocore.config.Config(connect_timeout=5, read_timeout=30, retries={"max_attempts": 5, "mode": "standard"})


class SecurityGroupRules:
    def __init__(self, ec2, group_ids, cluster):
        self._ec2 = ec2
        self._group_ids = list(group_ids)
        self._description = f"ray-vast:{cluster}"

    def _our_rules(self):
        """Return {cidr: group id} for every rule of this cluster in its groups."""
        groups = self._ec2.describe_security_groups(GroupIds=self._group_ids)["SecurityGroups"]
        return {r["CidrIp"]: g["GroupId"] for g in groups for p in g["IpPermissions"] for r in p.get("IpRanges", [])
                if r.get("Description") == self._description}

    def allow_ip(self, ip):
        """Add the rule to the first group with room. A rule that exists already (in any group) is success."""
        cidr = f"{ip}/32"
        if cidr in self._our_rules():
            return
        for group_id in self._group_ids:
            try:
                self._ec2.authorize_security_group_ingress(GroupId=group_id, IpPermissions=[{
                    "IpProtocol": "tcp", "FromPort": 0, "ToPort": 65535,
                    "IpRanges": [{"CidrIp": cidr, "Description": self._description}]}])
                return
            except botocore.exceptions.ClientError as e:
                code = e.response["Error"]["Code"]
                if code == "InvalidPermission.Duplicate":  # success: the groups belong to this cluster only
                    return
                if code != "RulesPerSecurityGroupLimitExceeded":
                    raise
        raise SgQuotaError(f"all {len(self._group_ids)} security groups of the cluster are full; raise the quota in AWS "
                           "Service Quotas (VPC: inbound or outbound rules per security group)")

    def _revoke(self, group_id, cidrs):
        try:
            self._ec2.revoke_security_group_ingress(GroupId=group_id, IpPermissions=[{
                "IpProtocol": "tcp", "FromPort": 0, "ToPort": 65535, "IpRanges": [{"CidrIp": c} for c in cidrs]}])
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidPermission.NotFound":
                raise

    def revoke_ip(self, ip):
        """Remove the rule from the group that holds it. A missing rule is success. Only a rule with our description."""
        cidr = f"{ip}/32"
        group_id = self._our_rules().get(cidr)
        if group_id:
            self._revoke(group_id, [cidr])

    def revoke_all(self):
        """Remove every rule of this cluster from all its groups; other rules (SSH, other clusters) stay."""
        by_group = {}
        for cidr, group_id in self._our_rules().items():
            by_group.setdefault(group_id, []).append(cidr)
        for group_id, cidrs in by_group.items():
            self._revoke(group_id, cidrs)
