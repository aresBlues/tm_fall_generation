"""Generate 10 TM Fallbearbeitung alerts and write JSON output."""
import argparse
import json
import os

from src.generators import generate_alert


OUTPUT_DIR = "output"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic TM Fallbearbeitung alerts.")
    parser.add_argument(
        "--per-alert",
        action="store_true",
        help="Also write one JSON file per alert (alert_001.json … alert_010.json).",
    )
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    alerts = []
    for i in range(10):
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

    msg = f"Written 10 alerts to {combined_path}"
    if args.per_alert:
        msg += " (+ 10 per-alert files)"
    msg += "."
    print(msg)


if __name__ == "__main__":
    main()
