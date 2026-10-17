"""The restricted IAM user of the suite: the getting-started commands ("Optional: use a restricted IAM user"), run as the
admin with the AWS CLI. Two changes from the docs: the user name is `ray-vast-livetest-<date>` (not `ray-vast`), and
`create-policy` of `ray-vast-user` is skipped when the policy exists (the account keeps it). The secret access key goes
only into a mode-600 credentials file in a temp directory, never into a log."""
import json
import os
import subprocess
import time
from pathlib import Path

import boto3

import livekit
from cluster import ADMIN_PROFILE, BIN
from window import wait_until

POLICY = "ray-vast-user"


class RestrictedUser:
    def __init__(self, out_dir, tmp_dir, date):
        self.dir = Path(out_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.name = f"ray-vast-livetest-{date.replace('-', '')}"
        livekit.assert_ours(self.name)
        self.profile = "ray-vast-livetest"
        self.credentials_file = Path(tmp_dir) / "credentials"
        self.iam = boto3.Session(profile_name=ADMIN_PROFILE).client("iam")
        self.log = self.dir / "iam_commands.log"

    def aws(self, *args, secret_output=False):
        """One AWS CLI command as the admin. The output of a command that prints a secret is not logged."""
        done = subprocess.run([str(BIN / "aws"), *args, "--profile", ADMIN_PROFILE], capture_output=True, text=True, timeout=120)
        with open(self.log, "a") as f:
            f.write(f"$ aws {' '.join(args)} --profile {ADMIN_PROFILE}\nexit {done.returncode}\n")
            f.write("(output holds the secret access key: not logged)\n" if secret_output else done.stdout + done.stderr)
        return done

    def reuse(self):
        """--keep-cluster: use the user and the credentials file of an earlier run, if both still work. Returns True then."""
        if not self.credentials_file.exists():
            return False
        import configparser
        section = configparser.ConfigParser()
        section.read(self.credentials_file)
        key_id, secret = section[self.profile]["aws_access_key_id"], section[self.profile]["aws_secret_access_key"]
        try:
            keys = [k["AccessKeyId"] for k in self.iam.list_access_keys(UserName=self.name)["AccessKeyMetadata"]]
        except self.iam.exceptions.NoSuchEntityException:
            return False
        if key_id not in keys:
            return False
        self.secret = secret
        self.session = boto3.Session(aws_access_key_id=key_id, aws_secret_access_key=secret)
        with open(self.log, "a") as f:
            f.write(f"# --keep-cluster: reused the user {self.name} and its credentials file\n")
        return True

    def create(self):
        self.delete()  # a user of the same date left by a killed or kept run
        import ray_vast
        iam_dir = Path(ray_vast.__file__).parent / "iam"
        account = self.aws("sts", "get-caller-identity", "--query", "Account", "--output", "text").stdout.strip()
        policy_arn = f"arn:aws:iam::{account}:policy/{POLICY}"
        try:
            self.iam.get_policy(PolicyArn=policy_arn)
            with open(self.log, "a") as f:
                f.write(f"# the policy {POLICY} exists: create-policy skipped\n")
        except self.iam.exceptions.NoSuchEntityException:
            assert self.aws("iam", "create-policy", "--policy-name", POLICY, "--policy-document", f"file://{iam_dir}/{POLICY}.json").returncode == 0
        assert self.aws("iam", "create-user", "--user-name", self.name).returncode == 0
        assert self.aws("iam", "attach-user-policy", "--user-name", self.name, "--policy-arn", policy_arn).returncode == 0
        done = self.aws("iam", "create-access-key", "--user-name", self.name, secret_output=True)
        assert done.returncode == 0
        key = json.loads(done.stdout)["AccessKey"]
        self.credentials_file.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with os.fdopen(os.open(self.credentials_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as f:
            f.write(f"[{self.profile}]\naws_access_key_id = {key['AccessKeyId']}\naws_secret_access_key = {key['SecretAccessKey']}\n")
        self.secret = key["SecretAccessKey"]
        self.session = boto3.Session(aws_access_key_id=key["AccessKeyId"], aws_secret_access_key=key["SecretAccessKey"])
        # A new access key takes some seconds to work.
        wait_until(self.session.client("sts").get_caller_identity, 60, done=lambda _: True, poll_s=2)
        time.sleep(10)
        return self

    def delete(self):
        livekit.assert_ours(self.name)
        try:
            for key in self.iam.list_access_keys(UserName=self.name)["AccessKeyMetadata"]:
                self.iam.delete_access_key(UserName=self.name, AccessKeyId=key["AccessKeyId"])
            for policy in self.iam.list_attached_user_policies(UserName=self.name)["AttachedPolicies"]:
                self.iam.detach_user_policy(UserName=self.name, PolicyArn=policy["PolicyArn"])
            self.iam.delete_user(UserName=self.name)
        except self.iam.exceptions.NoSuchEntityException:
            pass
        self.credentials_file.unlink(missing_ok=True)
        with open(self.log, "a") as f:
            f.write(f"# deleted the user {self.name} and its access keys\n")
