# TM Fallbearbeitung – Synthetic Data Generator

Generates synthetic Transaction Monitoring (TM) alerts for Fallbearbeitung (case processing). Each alert includes KYC records, alerted transactions, transaction history, and account summaries.

## Install

```bash
pip install -r requirements.txt
```

## Run

Recommended (wrapper around `src.main`):

```bash
python run.py
```

Optional flags:

```bash
python run.py -n 50              # number of alerts (default: 20)
python run.py --per-alert        # also write output/alert_001.json, …
```

Same behavior via the module:

```bash
python -m src.main
python -m src.main -n 50 --per-alert
```

## Output

- **`output/alerts.json`** – List of alerts (default 20; set with `-n`). Each alert contains:
  - `alert_id`, `status`, `created_at`, customer profile, rules, transactions, etc.
  - `kyc` – customer KYC record
  - `alerted_transactions` – transactions that triggered the alert
  - `transaction_history` – full history for the account (includes alerted transactions)
  - `account_summaries` – account(s) linked to the alert

- **`output/alert_001.json` …** – One file per alert (only with `--per-alert`; count matches `-n`).

Output is written under the `output/` directory (created automatically if it does not exist).
