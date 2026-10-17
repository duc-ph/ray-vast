"""Write the summary table of one live-suite run into docs/test-results.md (block "Live suite <date>").

The rows come only from `archive/results/<date>/<ID>/result.json`: a scenario without a record is NOT RUN, and a record
with a false check is FAIL. The scenario list and its order come from the claim matrix in docs/test-plan.md. The spend
tables (liverecords.spend_tables) follow the table.
Usage (from ray-vast-pypi/): .venv/bin/python tests/live/summarize.py [<date, default today UTC>]
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import liverecords  # noqa: E402
import liveschedule  # noqa: E402

REPO = Path(__file__).resolve().parents[3]

if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else time.strftime("%Y-%m-%d", time.gmtime())
    root = REPO / "archive" / "results" / date
    expected = [r["id"] for r in liveschedule.matrix_rows((REPO / "docs" / "test-plan.md").read_text())]
    table = liverecords.summary_table(root, expected) + liverecords.spend_tables(root)
    liverecords.write_summary(REPO / "docs" / "test-results.md", date, table)
    print(table)
