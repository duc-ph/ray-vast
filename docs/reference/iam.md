# IAM permissions

ray-vast uses two AWS identities:

- **Your AWS user** runs `ray-vast up` and `ray-vast down` on your laptop.
- **The head role** is an IAM role that `up` makes for the EC2 head.

## Your AWS user

You can use your own AWS credentials, for example an admin profile. Then the setup is a default VPC and the boundary policy. Read [Getting started](../getting-started.md#aws-setup).

For a restricted user, attach the policy `ray-vast-user`. It is in the package, in `ray_vast/iam/ray-vast-user.json`:

```json
--8<-- "src/ray_vast/iam/ray-vast-user.json"
```

The user (or the profile in `--profile`) needs these actions:

| Area | Actions | Used by | Limit in `ray-vast-user` |
|---|---|---|---|
| Account | `sts:GetCallerIdentity` | `up` | none |
| Read EC2 | `ec2:DescribeInstances`, `ec2:DescribeSecurityGroups`, `ec2:DescribeImages`, `ec2:DescribeKeyPairs` | all commands | none (these actions have no resource limit) |
| Security groups | `ec2:CreateSecurityGroup`, `ec2:AuthorizeSecurityGroupIngress`, `ec2:DeleteSecurityGroup` | `up`, `down` | tag `ray-vast-cluster` |
| Key pair | `ec2:CreateKeyPair`, `ec2:DeleteKeyPair` | `up`, `down` | name `ray-vast-*` and tag `ray-vast-cluster` |
| Instances | `ec2:RunInstances`, `ec2:TerminateInstances` | `up`, `down` | tag `ray-vast-cluster` on the instance, its disk and its security groups |
| Tags | `ec2:CreateTags` | `up` (tags at creation) | only in `CreateSecurityGroup`, `CreateKeyPair` and `RunInstances` |
| IAM role | `iam:CreateRole`, `iam:PutRolePolicy`, `iam:DeleteRolePolicy`, `iam:DeleteRole` | `up`, `down` | name `ray-vast-*` and the boundary `ray-vast-head-boundary` |
| IAM role, other | `iam:GetRole`, `iam:ListRolePolicies`, `iam:TagRole` | `up`, `down` | name `ray-vast-*` |
| Pass the role | `iam:PassRole` | `up` | name `ray-vast-*`, only to EC2 |
| Instance profile | `iam:CreateInstanceProfile`, `iam:TagInstanceProfile`, `iam:GetInstanceProfile`, `iam:AddRoleToInstanceProfile`, `iam:RemoveRoleFromInstanceProfile`, `iam:DeleteInstanceProfile` | `up`, `down` | name `ray-vast-*` |

`up` uses the default VPC of the region.

All resources have the name `ray-vast-<cluster_name>` (the groups add `-0` … `-4`) and the tag `ray-vast-cluster=<cluster_name>`.

`ray-vast-user` has no `iam:CreatePolicy`. So a restricted user cannot make a role with more permissions than the boundary. A live test proved it: `iam:CreateRole` without the boundary, or with another boundary, was denied.

## The head role

`up` makes the role `ray-vast-<cluster_name>`, with the permissions boundary `ray-vast-head-boundary`. Only EC2 can assume it:

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

## The boundary

The policy `ray-vast-head-boundary` is the permissions boundary of every head role. Create it once in your account. Read [Getting started](../getting-started.md#aws-setup). It is in the package, in `ray_vast/iam/ray-vast-head-boundary.json`:

```json
--8<-- "src/ray_vast/iam/ray-vast-head-boundary.json"
```

The head role can never do more than this policy allows: read EC2, and add and remove rules in security groups with the tag `ray-vast-cluster`.

`down` deletes the role policy, the role and the instance profile. It does not delete the boundary policy: all clusters use it.
