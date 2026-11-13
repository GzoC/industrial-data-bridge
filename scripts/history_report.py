"""Summarize or export a bounded API query, paging without accumulating events."""

import argparse
import csv
import io
import json
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


def report(args):
    params = {"tag": args.tag, "start": args.start, "end": args.end}
    if args.lot:
        params["lot"] = args.lot
    count = good = gaps = numeric = 0
    low = high = previous = None
    total = 0.0
    groups = set()
    query = urlencode(params)
    if args.csv:
        with Path(args.csv).open("x", encoding="utf-8", newline="") as output:
            writer = csv.writer(output)
            offset = 0
            while offset is not None:
                with urlopen(
                    f"{args.base}/api/history.csv?{query}&offset={offset}", timeout=15
                ) as response:
                    rows = csv.reader(
                        io.TextIOWrapper(response, encoding="utf-8", newline="")
                    )
                    header = next(rows)
                    if offset == 0:
                        writer.writerow(header)
                    writer.writerows(rows)
                    offset = response.headers.get("X-Next-Offset")
        return
    offset = 0
    while offset is not None:
        with urlopen(
            f"{args.base}/api/history?{query}&offset={offset}", timeout=15
        ) as response:
            page = json.load(response)
        offset = page["next_offset"]
        for event in page["events"]:
            count += 1
            stamp = datetime.fromisoformat(event["event_time"])
            if event["quality"] != "GOOD" or event["value"] is None:
                if previous is not None:
                    gaps += 1
                previous = None
                continue
            if (
                previous is not None
                and (stamp - previous).total_seconds() > args.gap_seconds
            ):
                gaps += 1
            previous = stamp
            good += 1
            groups.add((event["unit"], event.get("config_hash")))
            value = event["value"]
            if type(value) in (float, int):
                numeric += 1
                total += value
                low = value if low is None else min(low, value)
                high = value if high is None else max(high, value)
    print(
        json.dumps(
            {
                "variable": args.tag,
                "count": count,
                "good": good,
                "gaps_between_samples": gaps,
                "numeric_statistics": {"min": low, "max": high, "mean": total / numeric}
                if numeric and len(groups) == 1
                else None,
            },
            ensure_ascii=False,
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:8000")
    parser.add_argument("--tag", required=True)
    parser.add_argument("--start", required=True, help="ISO 8601 with timezone")
    parser.add_argument("--end", required=True, help="ISO 8601 with timezone")
    parser.add_argument("--lot")
    parser.add_argument("--gap-seconds", type=float, default=1.8)
    parser.add_argument("--csv", help="Write paginated CSV to a new file")
    args = parser.parse_args()
    try:
        report(args)
    except (OSError, ValueError) as exc:
        parser.exit(1, f"API or output error: {exc}\n")


if __name__ == "__main__":
    main()
