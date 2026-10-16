"""Write the summary table of one live-suite run into docs/test-results.md (block "Live suite <date>").

The rows come only from `archive/results/<date>/<ID>/result.json`: a scenario without a record is NOT RUN, and a record
with a false check is FAIL. The scenario list and its order come from the claim matrix in docs/test-plan.md.
Usage (from ray-vast-pypi/): .venv/bin/python tests/live/summarize.py [<date, default today UTC>]
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import livekit  # noqa: E402

REPO = Path(__file__).resolve().parents[3]

if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else time.strftime("%Y-%m-%d", time.gmtime())
    root = REPO / "archive" / "results" / date
    expected = [r["id"] for r in livekit.matrix_rows((REPO / "docs" / "test-plan.md").read_text())]
    table = livekit.summary_table(root, expected)
    spend = []
    for path in sorted((root / "_session").glob("*/spend.json")):
        s = json.loads(path.read_text())
        spend.append(f"| {s['group']} | {s['cluster']} | ${s['spend']:.4f} | ${s['top_ups']:.2f} | {s['hours']} h | ${s['ec2_estimate']:.2f} |")
    if spend:
        table += ("\nSpend for each cluster group (vast credit before and after, plus the auto top-ups in between; EC2 is an "
                  "estimate from the hours):\n\n| Group | Cluster | vast | Top-ups | Hours | EC2 |\n|---|---|---|---|---|---|\n"
                  + "\n".join(spend) + "\n")
    livekit.write_summary(REPO / "docs" / "test-results.md", date, table)
    print(table)
