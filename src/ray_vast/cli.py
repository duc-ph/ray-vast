import argparse
import os
import sys


def main(argv=None):
    # `.env` in the current directory sets the variables that are not set yet: the AWS keys and VAST_API_KEY.
    if os.path.isfile(".env"):
        with open(".env") as f:
            for line in f:
                name, sep, value = line.strip().removeprefix("export ").partition("=")
                if sep and not name.startswith("#"):
                    os.environ.setdefault(name.strip(), value.strip().strip("'\""))
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
        if name == "up":
            sub.add_argument("--wheel", help="ray-vast wheel to install on the head, for development (default: in a source tree, build "
                                             "the wheel; else install this CLI's own version from PyPI)")
    args = parser.parse_args(argv)
    # Progress lines show at once also when stdout is a pipe or a file (there Python buffers whole blocks).
    sys.stdout.reconfigure(line_buffering=True)

    from .commands import run

    return run(args)
