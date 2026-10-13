"""Ingress rules on the head security group: TCP all ports from one worker IP, tagged `ray-vast:<cluster>`."""
import botocore.config
import botocore.exceptions

class SgQuotaError(Exception):
    """The security group has no room for another worker IP."""


def ec2_client_config():
    """botocore Config for the ec2 client that the caller passes to SecurityGroupRules."""
    return botocore.config.Config(connect_timeout=5, read_timeout=30, retries={"max_attempts": 5, "mode": "standard"})


class SecurityGroupRules:
    def __init__(self, ec2, group_id, cluster):
        self._ec2 = ec2
        self._group_id = group_id
        self._description = f"ray-vast:{cluster}"

    def _our_cidrs(self):
        group = self._ec2.describe_security_groups(GroupIds=[self._group_id])["SecurityGroups"][0]
        return [r["CidrIp"] for p in group["IpPermissions"] for r in p.get("IpRanges", [])
                if r.get("Description") == self._description]

    def allow_ip(self, ip):
        """Add the rule. A rule that exists already is success."""
        cidr = f"{ip}/32"
        if cidr in self._our_cidrs():
            return
        try:
            self._ec2.authorize_security_group_ingress(GroupId=self._group_id, IpPermissions=[{
                "IpProtocol": "tcp", "FromPort": 0, "ToPort": 65535,
                "IpRanges": [{"CidrIp": cidr, "Description": self._description}]}])
        except botocore.exceptions.ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "InvalidPermission.Duplicate":  # success: each cluster has its own security group
                return
            if code == "RulesPerSecurityGroupLimitExceeded":
                raise SgQuotaError(f"security group {self._group_id}: the per-group rule quota is reached; "
                                   "raise it in AWS Service Quotas (VPC: inbound or outbound rules per security group)") from e
            raise

    def _revoke(self, cidrs):
        try:
            self._ec2.revoke_security_group_ingress(GroupId=self._group_id, IpPermissions=[{
                "IpProtocol": "tcp", "FromPort": 0, "ToPort": 65535, "IpRanges": [{"CidrIp": c} for c in cidrs]}])
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "InvalidPermission.NotFound":
                raise

    def revoke_ip(self, ip):
        """Remove the rule. A missing rule is success. Only a rule with our description is removed."""
        cidr = f"{ip}/32"
        if cidr in self._our_cidrs():
            self._revoke([cidr])

    def revoke_all(self):
        """Remove every rule of this cluster; other rules (SSH, other clusters) stay."""
        if cidrs := self._our_cidrs():
            self._revoke(cidrs)
