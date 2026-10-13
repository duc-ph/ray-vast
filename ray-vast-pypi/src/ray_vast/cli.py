import argparse


def main(argv=None):
    parser = argparse.ArgumentParser(prog="ray-vast", description="Ray clusters on vast.ai workers with an AWS EC2 head.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name, help_text in [
        ("up", "create the head and start the cluster"),
        ("down", "destroy all workers and the head"),
        ("attach", "open a shell on the head"),
        ("dashboard", "open an SSH tunnel to the Ray dashboard"),
    ]:
        subparsers.add_parser(name, help=help_text).add_argument("cluster_yaml", nargs="?")
    args = parser.parse_args(argv)
    print(f"ray-vast {args.command}: not implemented yet")
    return 1
