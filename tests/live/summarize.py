"""Print the summary table of one live-suite run and write it to `<results dir>/<date>/summary.md`.

The rows come only from `<results dir>/<date>/<test name>/result.json`: a record with a false check is FAIL. The spend
tables (liverecords.spend_tables) follow the table.
Usage (from the repository root): python tests/live/summarize.py [--results-dir DIR] [<date, default today UTC>]
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import liverecords  # noqa: E402

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", default=str(Path(__file__).resolve().parents[2] / "test-results"))
    parser.add_argument("date", nargs="?", default=time.strftime("%Y-%m-%d", time.gmtime()))
    args = parser.parse_args()
    root = Path(args.results_dir) / args.date
    table = liverecords.summary_table(root) + liverecords.spend_tables(root)
    (root / "summary.md").write_text(f"# Live suite {args.date}\n\n{table}")
    print(table)
