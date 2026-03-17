"""Generators for synthetic KYC, accounts, transactions, and alerts."""
import random
from datetime import datetime, timedelta

from faker import Faker

from src.models import (
    AccountSummary,
    Address,
    Alert,
    IdDocument,
    KYC,
    Transaction,
)

# Reproducible data
SEED = 42
random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

ALERT_TYPES = [
    "structuring",
    "velocity",
    "high_risk_country",
    "large_single_transaction",
    "unusual_pattern",
]
ALERT_STATUSES = ["open", "in_review", "closed"]
RISK_RATINGS = ["low", "medium", "high"]
ACCOUNT_TYPES = ["checking", "savings"]
ACCOUNT_STATUSES = ["active", "blocked", "closed"]
TX_TYPES = ["SEPA_CT, SEPA_INST, CARD_POS, ATM_WITHDRAWAL, CASH_DEPOSIT"]
CURRENCIES = ["EUR", "CHF", "USD"]
ID_DOC_TYPES = ["passport", "national_id"]


def generate_kyc(customer_id: str) -> KYC:
    dob = fake.date_of_birth(minimum_age=25, maximum_age=75)
    return KYC(
        customer_id=customer_id,
        full_name=fake.name(),
        date_of_birth=dob.isoformat(),
        address=Address(
            street=fake.street_address(),
            city=fake.city(),
            country=fake.country_code(),
        ),
        id_document=IdDocument(
            type=random.choice(ID_DOC_TYPES),
            number=fake.bothify(text="??########"),
        ),
        risk_rating=random.choice(RISK_RATINGS),
        pep_flag=random.choice([True, False]),
        sanctions_flag=False,
        customer_since=fake.date_between(start_date="-10y", end_date="-1y").isoformat(),
    )


def generate_account_summary(customer_id: str, account_id: str, currency: str | None = None) -> AccountSummary:
    opened = fake.date_between(start_date="-8y", end_date="-6m")
    return AccountSummary(
        account_id=account_id,
        customer_id=customer_id,
        balance=round(random.uniform(1000, 500000), 2),
        currency=currency or random.choice(CURRENCIES),
        account_type=random.choice(ACCOUNT_TYPES),
        opened_at=opened.isoformat(),
        status=random.choices(ACCOUNT_STATUSES, weights=[85, 10, 5])[0],
    )


def generate_transaction(
    transaction_id: str,
    date: datetime,
    currency: str,
    *,
    amount_range: tuple[float, float] | None = None,
    tx_type: str | None = None,
    direction: str | None = None,
) -> Transaction:
    if amount_range is None:
        amount_range = (50.0, 15000.0)
    amount = round(random.uniform(amount_range[0], amount_range[1]), 2)
    if direction is None:
        direction = random.choice(["in", "out"])
    return Transaction(
        transaction_id=transaction_id,
        date=date.isoformat(),
        amount=amount,
        currency=currency,
        counterparty_name=fake.company() if random.random() > 0.3 else fake.name(),
        counterparty_iban=fake.iban(),
        type=tx_type or random.choice(TX_TYPES),
        direction=direction,
        description=fake.sentence(nb_words=4).replace(".", "") or "Payment",
    )


def generate_alert(alert_index: int) -> Alert:
    """Generate one full alert with KYC, account(s), alerted transactions, and history."""
    alert_id = f"ALT-{alert_index:05d}"
    customer_id = f"CUST-{alert_index:05d}-{random.randint(100000, 999999)}"
    account_id = f"ACC-{alert_index:05d}-{random.randint(1000000, 9999999)}"

    alert_type = ALERT_TYPES[alert_index % len(ALERT_TYPES)]
    status = random.choice(ALERT_STATUSES)
    created_at = (datetime.now() - timedelta(days=random.randint(1, 90))).isoformat()
    risk_score = round(random.uniform(0.3, 0.95), 2)
    # Exactly 2 out of 10 alerts require SAR (e.g. indices 1 and 6)
    requires_sar = alert_index in (1, 6)

    kyc = generate_kyc(customer_id)
    currency = random.choice(CURRENCIES)
    primary_account = generate_account_summary(customer_id, account_id, currency)
    account_summaries = [primary_account]
    if random.random() < 0.5:
        extra_account_id = f"ACC-{alert_index:05d}-{random.randint(2000000, 2999999)}"
        account_summaries.append(generate_account_summary(customer_id, extra_account_id, currency))

    # Date range for transactions (e.g. last 90 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)

    num_alerted = random.randint(1, 5)
    num_extra_history = random.randint(
        max(0, 5 - num_alerted),
        20 - num_alerted,
    )
    total_tx = num_alerted + num_extra_history

    # Build alerted transactions first (shape depends on alert_type)
    alerted_txs: list[Transaction] = []
    tx_id = 0
    if alert_type == "structuring":
        # Many just-below-threshold amounts (e.g. 8k–9.5k)
        for _ in range(num_alerted):
            tx_id += 1
            d = start_date + timedelta(days=random.randint(0, 80))
            t = generate_transaction(
                f"TX-{account_id}-{tx_id:04d}",
                d,
                currency,
                amount_range=(8000.0, 9500.0),
            )
            alerted_txs.append(t)
    elif alert_type == "velocity":
        # Several transactions in a short window
        base_day = random.randint(0, 70)
        for i in range(num_alerted):
            tx_id += 1
            d = start_date + timedelta(days=base_day, hours=i * 2)
            alerted_txs.append(
                generate_transaction(f"TX-{account_id}-{tx_id:04d}", d, currency),
            )
    elif alert_type == "high_risk_country":
        for _ in range(num_alerted):
            tx_id += 1
            d = start_date + timedelta(days=random.randint(0, 80))
            alerted_txs.append(
                generate_transaction(f"TX-{account_id}-{tx_id:04d}", d, currency),
            )
    elif alert_type == "large_single_transaction":
        num_alerted = 1
        tx_id += 1
        d = start_date + timedelta(days=random.randint(0, 80))
        alerted_txs.append(
            generate_transaction(
                f"TX-{account_id}-{tx_id:04d}",
                d,
                currency,
                amount_range=(100000.0, 500000.0),
            ),
        )
    else:
        # unusual_pattern
        for _ in range(num_alerted):
            tx_id += 1
            d = start_date + timedelta(days=random.randint(0, 80))
            alerted_txs.append(
                generate_transaction(f"TX-{account_id}-{tx_id:04d}", d, currency),
            )

    # Transaction history: alerted first, then extra (by date)
    all_txs: list[Transaction] = list(alerted_txs)
    for _ in range(num_extra_history):
        tx_id += 1
        d = start_date + timedelta(
            days=random.randint(0, 80),
            hours=random.randint(0, 23),
        )
        all_txs.append(
            generate_transaction(f"TX-{account_id}-{tx_id:04d}", d, currency),
        )
    all_txs.sort(key=lambda t: t.date)

    return Alert(
        alert_id=alert_id,
        type=alert_type,
        status=status,
        created_at=created_at,
        risk_score=risk_score,
        customer_id=customer_id,
        account_id=account_id,
        requires_sar=requires_sar,
        kyc=kyc,
        alerted_transactions=alerted_txs,
        transaction_history=all_txs,
        account_summaries=account_summaries,
    )
