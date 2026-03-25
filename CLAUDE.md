# TM Fallbearbeitung – Synthetic Data Generator

This project generates synthetic Transaction Monitoring (TM) alerts for Fallbearbeitung (case processing). Each alert includes KYC records, alerted transactions, transaction history, and account summaries.

## Project Overview

- **Purpose**: Generate synthetic TM alerts conforming to a 132-field German schema
- **Language**: Python
- **Dependencies**: Faker (German locale), no other major dependencies
- **Output**: JSON files containing synthetic customer and transaction data

## Commands

### Generate Alerts

```bash
# Default: Generate alerts to output/alerts.json (default count in src/main.py, e.g. 20)
python run.py

# Count and per-alert files
python run.py -n 50
python run.py --per-alert

# Equivalent
python -m src.main
python -m src.main -n 50 --per-alert
```

### Dependencies

```bash
pip install -r requirements.txt
```

## Architecture

### Key Files

| File | Purpose |
|------|---------|
| `src/main.py` | Entry point - parses args, generates alerts, writes JSON |
| `src/models.py` | Dataclass models for all alert structures (Alert, CustomerProfile, Transaction, etc.) |
| `src/generators.py` | Core generation logic - creates synthetic data using Faker |
| `run.py` | Recommended CLI entry (`python run.py`; same as `python -m src.main`) |

### Data Models

The project uses dataclasses in `src/models.py`:

- **Alert** - Top-level alert record
- **CustomerProfile** - KYC data, risk rating, PEP/sanctions flags
- **RuleTriggered** - Detection rules that fired
- **TriggerTransaction** - Transactions that triggered the alert
- **HistoryTransaction** - 90-day transaction history
- **BehaviorStats** - Customer behavior metrics (velocity, volumes, risk indicators)
- **AccountSummary** - Account balance and status

### Alert Types

1. **structuring** - Multiple transactions just below reporting threshold
2. **velocity** - Unusually high transaction count in short window
3. **high_risk_country** - Transfers to/from high-risk jurisdictions (IR, KP, SY, AF, YE, MM, LY, SO)
4. **large_single_transaction** - Single large transaction exceeding customer norm
5. **unusual_pattern** - Behavioral anomaly vs historical profile

### Output

- `output/alerts.json` - Always created (default count set in `src/main.py`, overridable with `-n`)
- `output/alert_001.json` … - Per-alert files (only with `--per-alert`)

Each alert contains ~132 fields covering customer, transactions, rules, and behavior stats.

## Important Details

### Reproducibility

The project uses a fixed random seed (42) for deterministic output:
```python
SEED = 42
random.seed(SEED)
Faker.seed(SEED)
```

### Time Windows

- Generation "now": 2026-03-01
- Alert window: February 2026
- History output: Last 90 days from generation date

### Balance Maintenance

Running account balance is tracked and maintained throughout transaction generation—balance never goes negative.

## No Test/Lint Commands

This is a simple data generator with no test suite or linting configured.