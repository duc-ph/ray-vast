import argparse
import os
import sys


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
        if name == "up":
            sub.add_argument("--wheel", help="ray-vast wheel to install on the head, for development (default: in a source tree, build "
                                             "the wheel; else install this CLI's own version from PyPI)")
    args = parser.parse_args(argv)
    # Progress lines show at once also when stdout is a pipe or a file (there Python buffers whole blocks).
    sys.stdout.reconfigure(line_buffering=True)

    from .commands import run

    return run(args)
