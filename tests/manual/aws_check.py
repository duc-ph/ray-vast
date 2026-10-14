# Teardown step 5 of docs/test-plan.md: the independent AWS check after `ray-vast down` (the operator machine has no
# aws CLI). Read-only. Looks for: EC2 instances with the tag ray-vast-cluster=<cluster> in a live state, security
# groups named ray-vast-<cluster>-*, volumes with the tag, the IAM role and the instance profile ray-vast-<cluster>.
# Prints each object that is left, then `aws left: nothing` (exit 0) or `aws left: <n>` (exit 1).
# Usage: python aws_check.py <cluster>       (AWS profile `personal` or AWS_PROFILE, region us-west-2)
import os
import sys

import boto3

cluster = sys.argv[1]
name = f"ray-vast-{cluster}"
session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "personal"), region_name="us-west-2")
ec2, iam = session.client("ec2"), session.client("iam")
tag = {"Name": "tag:ray-vast-cluster", "Values": [cluster]}
left = []
for r in ec2.describe_instances(Filters=[tag, {"Name": "instance-state-name",
                                               "Values": ["pending", "running", "stopping", "stopped", "shutting-down"]}])["Reservations"]:
    left += [f"EC2 instance {i['InstanceId']} ({i['State']['Name']})" for i in r["Instances"]]
left += [f"security group {g['GroupId']} ({g['GroupName']})" for g in ec2.describe_security_groups(
    Filters=[{"Name": "group-name", "Values": [f"{name}-*"]}])["SecurityGroups"]]
left += [f"volume {v['VolumeId']} ({v['State']})" for v in ec2.describe_volumes(Filters=[tag])["Volumes"]]
for kind, get, kwargs in (("role", iam.get_role, {"RoleName": name}),
                          ("instance profile", iam.get_instance_profile, {"InstanceProfileName": name})):
    try:
        get(**kwargs)
        left.append(f"{kind} {name}")
    except iam.exceptions.NoSuchEntityException:
        print(f"{kind} {name}: NoSuchEntity")
for item in left:
    print(item)
print(f"aws left: {len(left) if left else 'nothing'}")
sys.exit(1 if left else 0)
