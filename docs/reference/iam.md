# AWS permissions

ray-vast uses two AWS identities:

- **Your AWS user** runs `ray-vast up` and `ray-vast down` on your laptop.
- **The head role** is an IAM role that `up` makes for the EC2 head.

With an admin profile, you need nothing from this page. Read it to use a restricted user, or to see what ray-vast does in your account.

## What `up` makes in AWS

`up` makes these resources in the region of the head. `down` deletes them.

| Resource | What it is for |
|---|---|
| An EC2 instance, with its disk | The head. It runs the Ray head and the ray-vast provider. |
| 5 security groups | The firewall of the head. They let in SSH from your laptop's IP and the IPs of the workers. |
| An EC2 key pair | The SSH key of the head. `up` keeps the private key in `~/.ray-vast/<cluster_name>/ssh_key.pem`. |
| An IAM role and an instance profile | They let the provider on the head add and remove the worker IPs in the security groups. |

All of them have the name `ray-vast-<cluster_name>` (the groups add `-0` … `-4`) and the tag `ray-vast-cluster=<cluster_name>`.
`up` puts the head in the default VPC of the region.

One more resource is for all clusters of the account: the IAM policy `ray-vast-head-boundary`. The first `up` in the account creates it. `down` does not delete it.

## The boundary policy { #the-boundary-policy }

The policy `ray-vast-head-boundary` is the permissions boundary of every head role.
The head role can never do more than this policy allows: read EC2, and add and remove rules in security groups with the tag `ray-vast-cluster`.
This is true also if somebody changes the role's own policy later.

```json
--8<-- "src/ray_vast/iam/ray-vast-head-boundary.json"
```

`up` creates the policy from this file when it is missing. Your user needs `iam:CreatePolicy` for this. An admin profile has it. A restricted user does not have it.
Then an admin creates the policy once in the account:

```bash
IAM_DIR="$(python -c 'import ray_vast, os; print(os.path.dirname(ray_vast.__file__))')/iam"
aws iam create-policy --policy-name ray-vast-head-boundary \
    --policy-document "file://$IAM_DIR/ray-vast-head-boundary.json" --profile my-admin-profile
```

The first command finds the policy file in the installed package. If the output says `EntityAlreadyExists`, the policy exists already.

If the policy is missing and your user cannot create it, `up` stops before it makes anything.

## A restricted user { #a-restricted-user }

A restricted user can do only what ray-vast needs. It has the policy `ray-vast-user`:

- EC2: make, change and delete only resources with the tag `ray-vast-cluster`, and key pairs with names `ray-vast-*`.
- IAM: make and delete roles and instance profiles with names `ray-vast-*`.
- IAM: make a role or change its policy only with the boundary `ray-vast-head-boundary`.
- IAM: pass the roles `ray-vast-*` only to EC2.
- No `iam:CreatePolicy`. A user that can create policies can also create an admin policy.

So the user cannot make a role with more permissions than the boundary. A live test proved it: `iam:CreateRole` without the boundary, or with another boundary, was denied.

```json
--8<-- "src/ray_vast/iam/ray-vast-user.json"
```

To make the user:

1. As an admin, create [the boundary policy](#the-boundary-policy) (above).
2. As an admin, create the policy `ray-vast-user`, the user, and an access key:

    ```bash
    IAM_DIR="$(python -c 'import ray_vast, os; print(os.path.dirname(ray_vast.__file__))')/iam"
    ACCOUNT=$(aws sts get-caller-identity --query Account --output text --profile my-admin-profile)
    aws iam create-policy --policy-name ray-vast-user \
        --policy-document "file://$IAM_DIR/ray-vast-user.json" --profile my-admin-profile
    aws iam create-user --user-name ray-vast --profile my-admin-profile
    aws iam attach-user-policy --user-name ray-vast \
        --policy-arn "arn:aws:iam::$ACCOUNT:policy/ray-vast-user" --profile my-admin-profile
    aws iam create-access-key --user-name ray-vast --profile my-admin-profile
    ```

    The last command prints the access key ID and the secret access key. Keep the secret safe.

3. Make the profile `ray-vast` with this key, and check it:

    ```bash
    aws configure --profile ray-vast
    aws sts get-caller-identity --profile ray-vast
    ```

4. Use `--profile ray-vast` on `ray-vast up`, or `export AWS_PROFILE=ray-vast`.

In the AWS console, step 2 is: **IAM**, **Policies**, **Create policy**, **JSON**, paste `ray-vast-user`. Then **Users**, **Create user** `ray-vast` without console access, **Attach policies directly**, `ray-vast-user`. Then open the user, **Security credentials**, **Create access key**, **Command Line Interface (CLI)**.

## The actions of your AWS user

| Area | Actions | Used by | Limit in `ray-vast-user` |
|---|---|---|---|
| Account | `sts:GetCallerIdentity` | `up` | none |
| Read EC2 | `ec2:DescribeInstances`, `ec2:DescribeSecurityGroups`, `ec2:DescribeImages`, `ec2:DescribeKeyPairs`, `ec2:DescribeVpcs` | all commands | none (these actions have no resource limit) |
| Security groups | `ec2:CreateSecurityGroup`, `ec2:AuthorizeSecurityGroupIngress`, `ec2:DeleteSecurityGroup` | `up`, `down` | tag `ray-vast-cluster` |
| Key pair | `ec2:CreateKeyPair`, `ec2:DeleteKeyPair` | `up`, `down` | name `ray-vast-*` and tag `ray-vast-cluster` |
| Instances | `ec2:RunInstances`, `ec2:TerminateInstances` | `up`, `down` | tag `ray-vast-cluster` on the instance, its disk and its security groups |
| Tags | `ec2:CreateTags` | `up` (tags at creation) | only in `CreateSecurityGroup`, `CreateKeyPair` and `RunInstances` |
| IAM role | `iam:CreateRole`, `iam:PutRolePolicy`, `iam:DeleteRolePolicy`, `iam:DeleteRole` | `up`, `down` | name `ray-vast-*` and the boundary `ray-vast-head-boundary` |
| IAM role, other | `iam:GetRole`, `iam:ListRolePolicies`, `iam:TagRole` | `up`, `down` | name `ray-vast-*` |
| Pass the role | `iam:PassRole` | `up` | name `ray-vast-*`, only to EC2 |
| Boundary policy | `iam:CreatePolicy` | `up`, only when the boundary policy is missing | not in `ray-vast-user` |
| Instance profile | `iam:CreateInstanceProfile`, `iam:TagInstanceProfile`, `iam:GetInstanceProfile`, `iam:AddRoleToInstanceProfile`, `iam:RemoveRoleFromInstanceProfile`, `iam:DeleteInstanceProfile` | `up`, `down` | name `ray-vast-*` |

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

`down` deletes the role policy, the role and the instance profile.
