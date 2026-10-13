import argparse
import os


def main(argv=None):
    parser = argparse.ArgumentParser(prog="ray-vast", description="Ray clusters on vast.ai workers with an AWS EC2 head.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name, help_text in [
        ("up", "create the head and start the cluster"),
        ("down", "destroy all workers and the head"),
        ("attach", "open a shell on the head"),
        ("dashboard", "open an SSH tunnel to the Ray dashboard"),
    ]:
        sub = subparsers.add_parser(name, help=help_text)
        sub.add_argument("cluster_yaml")
        sub.add_argument("--profile", default=os.environ.get("AWS_PROFILE"), help="AWS profile (default: AWS_PROFILE or the default credentials)")
        if name in ("up", "attach", "dashboard"):
            sub.add_argument("--key-file", help="SSH private key of the head (or provider.key_file)")
        if name == "up":
            sub.add_argument("--key-name", help="EC2 key pair name (or provider.key_name)")
            sub.add_argument("--wheel", help="ray-vast wheel to install on the head (default: build it from the source tree)")
    args = parser.parse_args(argv)

    from .commands import run

    return run(args)
