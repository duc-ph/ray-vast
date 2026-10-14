# IAM permissions

ray-vast uses two AWS identities:

- **Your AWS user** runs `ray-vast up` and `ray-vast down` on your laptop.
- **The head role** is an IAM role that `up` makes for the EC2 head.

## Your AWS user

The user (or the profile in `--profile`) needs these actions:

| Area | Actions | Used by |
|---|---|---|
| Account | `sts:GetCallerIdentity` | `up` |
| IAM role | `iam:CreateRole`, `iam:PutRolePolicy`, `iam:ListRolePolicies`, `iam:DeleteRolePolicy`, `iam:GetRole`, `iam:DeleteRole`, `iam:PassRole` | `up`, `down` |
| Instance profile | `iam:CreateInstanceProfile`, `iam:AddRoleToInstanceProfile`, `iam:GetInstanceProfile`, `iam:RemoveRoleFromInstanceProfile`, `iam:DeleteInstanceProfile` | `up`, `down` |
| Tags | `iam:TagRole`, `iam:TagInstanceProfile`, `ec2:CreateTags` | `up` (tags at creation) |
| Security groups | `ec2:CreateSecurityGroup`, `ec2:AuthorizeSecurityGroupIngress`, `ec2:DescribeSecurityGroups`, `ec2:DeleteSecurityGroup` | `up`, `down` |
| Instances | `ec2:DescribeImages`, `ec2:RunInstances`, `ec2:DescribeInstances`, `ec2:TerminateInstances` | `up`, `down`, `attach`, `dashboard` |

`up` uses the default VPC of the region.

All resources have the name `ray-vast-<cluster_name>` (the groups add `-0` … `-4`) and the tag `ray-vast-cluster=<cluster_name>`.
You can limit the `iam:` actions to the role and instance profile names `ray-vast-*`.

## The head role

`up` makes the role `ray-vast-<cluster_name>`. Only EC2 can assume it:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}
  ]
}
```

It has one inline policy, `head`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["ec2:DescribeSecurityGroups", "ec2:DescribeInstances"],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["ec2:AuthorizeSecurityGroupIngress", "ec2:RevokeSecurityGroupIngress"],
      "Resource": [
        "arn:aws:ec2:<region>:<account>:security-group/<group 0 id>",
        "arn:aws:ec2:<region>:<account>:security-group/<group 1 id>",
        "arn:aws:ec2:<region>:<account>:security-group/<group 2 id>",
        "arn:aws:ec2:<region>:<account>:security-group/<group 3 id>",
        "arn:aws:ec2:<region>:<account>:security-group/<group 4 id>"
      ]
    }
  ]
}
```

So the head can only add and remove rules in the 5 groups of its cluster, and describe EC2.
The provider on the head uses it to allow each worker IP.

The instance profile `ray-vast-<cluster_name>` holds the role. The head requires IMDSv2.

`down` deletes the policy, the role and the instance profile.
