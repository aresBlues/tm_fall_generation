"""Generate TM Fallbearbeitung alerts and write JSON output."""
import argparse
import json
import os

from src.generators import generate_alert


OUTPUT_DIR = "output"
DEFAULT_NUM_ALERTS = 20


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic TM Fallbearbeitung alerts.")
    parser.add_argument(
        "-n",
        "--count",
        type=int,
        default=DEFAULT_NUM_ALERTS,
        metavar="N",
        help=f"Number of alerts to generate (default: {DEFAULT_NUM_ALERTS}).",
    )
    parser.add_argument(
        "--per-alert",
        action="store_true",
        help="Also write one JSON file per alert (alert_001.json, alert_002.json, …).",
    )
    args = parser.parse_args()

    if args.count < 1:
        parser.error("--count must be at least 1")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    n = args.count
    alerts = []
    for i in range(n):
        alert = generate_alert(i)
        d = alert.to_dict()
        alerts.append(d)
        if args.per_alert:
            path = os.path.join(OUTPUT_DIR, f"alert_{i + 1:03d}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(d, f, indent=2, ensure_ascii=False)

    combined_path = os.path.join(OUTPUT_DIR, "alerts.json")
    with open(combined_path, "w", encoding="utf-8") as f:
        json.dump(alerts, f, indent=2, ensure_ascii=False)

    msg = f"Written {n} alerts to {combined_path}"
    if args.per_alert:
        msg += f" (+ {n} per-alert files)"
    msg += "."
    print(msg)


if __name__ == "__main__":
    main()
