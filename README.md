# TM Fallbearbeitung – Synthetic Data Generator

Generates synthetic Transaction Monitoring (TM) alerts for Fallbearbeitung (case processing). Each alert includes KYC records, alerted transactions, transaction history, and account summaries.

## Install

```bash
pip install -r requirements.txt
```

## Run

Generate 10 alerts (combined file only):

```bash
python -m src.main
```

Generate 10 alerts and also write one JSON file per alert:

```bash
python -m src.main --per-alert
```

Alternatively, using the run script:

```bash
python run.py
python run.py --per-alert
```

## Output

- **`output/alerts.json`** – List of 10 alerts (always created). Each alert contains:
  - `alert_id`, `type`, `status`, `created_at`, `risk_score`, `customer_id`, `account_id`, `requires_sar`
  - `kyc` – customer KYC record
  - `alerted_transactions` – transactions that triggered the alert
  - `transaction_history` – full history for the account (includes alerted transactions)
  - `account_summaries` – account(s) linked to the alert

- **`output/alert_001.json` … `output/alert_010.json`** – One file per alert (only when using `--per-alert`).

Output is written under the `output/` directory (created automatically if it does not exist).
