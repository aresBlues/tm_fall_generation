"""Generators for synthetic TM Fallbearbeitung alerts.

Produces alerts conforming to the 132-field schema in alerts_de_schema.xlsx.
Transaction history spans 12 months; trigger transactions and alert.created_at
fall within a single 1-month alert window.  Running account balance is maintained
(always >= 0) and stamped on trigger transactions.
"""
from __future__ import annotations

import random
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from faker import Faker

from src.models import (
    AccountSummary,
    Address,
    Alert,
    BehaviorStats,
    CustomerLast12mStats,
    CustomerProfile,
    HistoryTransaction,
    IdDocument,
    RuleTriggered,
    TriggerTransaction,
    UBO,
)

# ---------------------------------------------------------------------------
# Reproducible data
# ---------------------------------------------------------------------------
SEED = 42
random.seed(SEED)
fake = Faker("de_DE")
Faker.seed(SEED)

# ---------------------------------------------------------------------------
# Time windows
# ---------------------------------------------------------------------------
# "Now" for generation purposes — end of the data window
GENERATION_NOW = datetime(2026, 3, 1)
HISTORY_MONTHS = 12  # full transaction pool
HISTORY_START = GENERATION_NOW - timedelta(days=365)
# Alert window: Feb 1 – Feb 28, 2026
ALERT_WINDOW_START = datetime(2026, 2, 1)
ALERT_WINDOW_END = datetime(2026, 2, 28, 23, 59, 59)
# History output window (last 90 days from GENERATION_NOW)
HISTORY_OUTPUT_START = GENERATION_NOW - timedelta(days=90)

# ---------------------------------------------------------------------------
# Constants / enumerations
# ---------------------------------------------------------------------------
ALERT_TYPES = [
    "structuring",
    "velocity",
    "high_risk_country",
    "large_single_transaction",
    "unusual_pattern",
]
ALERT_STATUSES = ["open", "in_review", "closed"]
RISK_RATINGS = ["low", "medium", "high"]
ACCOUNT_TYPES = ["checking", "savings", "business"]
ACCOUNT_STATUSES = ["active", "blocked", "closed"]
PAYMENT_RAILS = ["SEPA_CT", "SEPA_INST", "CARD_POS", "ATM_WITHDRAWAL", "CASH_DEPOSIT", "SWIFT"]
BOOKING_CHANNELS = ["mobile", "online_banking", "atm", "card_terminal"]
TX_TYPES = ["transfer", "cash", "wire", "card"]
CURRENCIES = ["EUR", "CHF", "USD"]
ID_DOC_TYPES = ["passport", "national_id", "residence_permit"]
KYC_STATUSES = ["VERIFIED", "PENDING", "REJECTED"]
EMPLOYMENT_STATUSES = ["EMPLOYED", "SELF_EMPLOYED", "UNEMPLOYED", "STUDENT", "RETIRED"]
INDUSTRIES = [
    "Finance", "Technology", "Healthcare", "Retail", "Manufacturing",
    "Real Estate", "Consulting", "Legal Services", "Import/Export", "Gastronomy",
]
ACCOUNT_PURPOSES = [
    "daily expenses", "salary account", "business operations",
    "savings", "investment", "international transfers",
]

HIGH_RISK_COUNTRIES = {"IR", "KP", "SY", "AF", "YE", "MM", "LY", "SO"}

# Rule definitions per alert type  (rule_id, EN name, DE name)
RULES_BY_TYPE: dict[str, list[tuple[str, str, str]]] = {
    "structuring": [
        ("RULE-STR-001", "Structuring Detection", "Strukturierungserkennung"),
        ("RULE-STR-002", "Threshold Avoidance Pattern", "Schwellenwertumgehungsmuster"),
    ],
    "velocity": [
        ("RULE-VEL-001", "High Velocity Transactions", "Hochgeschwindigkeitstransaktionen"),
        ("RULE-VEL-002", "Rapid Succession Alert", "Alarm bei schneller Abfolge"),
    ],
    "high_risk_country": [
        ("RULE-HRC-001", "High-Risk Country Transfer", "Hochrisikoland-Überweisung"),
        ("RULE-HRC-002", "Sanctioned Jurisdiction", "Sanktionierte Jurisdiktion"),
    ],
    "large_single_transaction": [
        ("RULE-LST-001", "Large Single Transaction", "Große Einzeltransaktion"),
        ("RULE-LST-002", "Threshold Exceedance", "Schwellenwertüberschreitung"),
    ],
    "unusual_pattern": [
        ("RULE-UPA-001", "Unusual Transaction Pattern", "Ungewöhnliches Transaktionsmuster"),
        ("RULE-UPA-002", "Behavioral Anomaly", "Verhaltensanomalie"),
    ],
}

ALERT_REASON_SUMMARIES: dict[str, str] = {
    "structuring": "Multiple transactions just below reporting threshold detected within a short period.",
    "velocity": "Unusually high number of transactions detected in a short time window.",
    "high_risk_country": "Funds transferred to or from a high-risk jurisdiction.",
    "large_single_transaction": "Single transaction significantly exceeding normal customer behavior.",
    "unusual_pattern": "Transaction pattern deviates substantially from historical customer profile.",
}


GERMAN_MONTHS = [
    "Januar", "Februar", "März", "April", "Mai", "Juni",
    "Juli", "August", "September", "Oktober", "November", "Dezember",
]

_PAYMENT_REFS_IN_TRANSFER = [
    "Gehalt {month} {year}",
    "Miete {month} {year}",
    "Mieteinnahme {month} {year}",
    "Gutschrift Dauerauftrag",
    "Rückerstattung Rechnung {ref}",
    "Einzahlung Sparvertrag",
    "Honorar {month} {year}",
    "Provision Q{quarter}/{year}",
]
_PAYMENT_REFS_OUT_TRANSFER = [
    "Miete {month} {year}",
    "Dauerauftrag Strom/Gas",
    "Versicherungsbeitrag {month} {year}",
    "Rechnung Nr. {ref}",
    "Zahlung an {name}",
    "Ratenzahlung Kredit {ref}",
    "Telefonrechnung {month} {year}",
    "Internet Rechnung {month}/{year}",
    "Mitgliedsbeitrag {year}",
    "KFZ-Steuer {year}",
]
_PAYMENT_REFS_CASH_IN = [
    "Bargeldeinzahlung",
    "Bargeldeinzahlung Filiale",
    "Einzahlung am Automaten",
]
_PAYMENT_REFS_CASH_OUT = [
    "Bargeldauszahlung",
    "Geldautomat {city}",
    "ATM Auszahlung",
]
_PAYMENT_REFS_CARD = [
    "Kartenzahlung {name}",
    "POS {name}",
    "EC-Kartenzahlung {name}",
    "Kontaktlos {name}",
]
_PAYMENT_REFS_WIRE = [
    "Auslandsüberweisung {name}",
    "SWIFT Transfer Ref {ref}",
    "Internationale Zahlung {ref}",
    "Wire Transfer an {name}",
]


def _generate_payment_reference(
    tx_type: str, direction: str, dt: datetime, cp_name: str,
) -> str:
    month = GERMAN_MONTHS[dt.month - 1]
    year = dt.year
    quarter = (dt.month - 1) // 3 + 1
    ref = fake.numerify(text="#####")

    if tx_type == "cash":
        pool = _PAYMENT_REFS_CASH_IN if direction == "in" else _PAYMENT_REFS_CASH_OUT
    elif tx_type == "card":
        pool = _PAYMENT_REFS_CARD
    elif tx_type == "wire":
        pool = _PAYMENT_REFS_WIRE
    elif direction == "in":
        pool = _PAYMENT_REFS_IN_TRANSFER
    else:
        pool = _PAYMENT_REFS_OUT_TRANSFER

    template = random.choice(pool)
    return template.format(
        month=month, year=year, quarter=quarter,
        ref=ref, name=cp_name, city=fake.city(),
    )


# ---------------------------------------------------------------------------
# Address generation
# ---------------------------------------------------------------------------

def _generate_address() -> Address:
    return Address(
        street=fake.street_address(),
        postal_code=fake.postcode(),
        city=fake.city(),
        country="DE",
    )


# ---------------------------------------------------------------------------
# Customer profile
# ---------------------------------------------------------------------------

def generate_customer_profile(customer_id: str) -> CustomerProfile:
    first = fake.first_name()
    last = fake.last_name()
    dob = fake.date_of_birth(minimum_age=25, maximum_age=75)
    customer_type = random.choices(["private", "business"], weights=[70, 30])[0]

    legal = _generate_address()

    issued = fake.date_between(start_date="-10y", end_date="-1y")
    expires = issued.replace(year=issued.year + 10)

    ubo: list[UBO] = []
    if customer_type == "business":
        num_ubo = random.randint(1, 3)
        remaining = 100.0
        for j in range(num_ubo):
            pct = round(random.uniform(10, remaining - 10 * (num_ubo - j - 1)), 1) if j < num_ubo - 1 else round(remaining, 1)
            remaining -= pct
            ubo.append(UBO(name=fake.name(), ownership_percentage=pct))

    monthly_income = round(random.uniform(2000, 15000), 0)

    return CustomerProfile(
        customer_id=customer_id,
        first_name=first,
        last_name=last,
        full_name=f"{first} {last}",
        date_of_birth=dob.isoformat(),
        place_of_birth=fake.city(),
        nationality=random.choice(["DE", "DE", "DE", "AT", "CH", "FR", "TR", "PL"]),
        residency_country="DE",
        kyc_status=random.choices(KYC_STATUSES, weights=[85, 10, 5])[0],
        customer_since=fake.date_between(start_date="-10y", end_date="-1y").isoformat(),
        email=fake.email(),
        phone_number=fake.phone_number(),
        legal_address=legal,
        id_document=IdDocument(
            type=random.choice(ID_DOC_TYPES),
            number=fake.bothify(text="??########"),
            issued_at=issued.isoformat(),
            expires_at=expires.isoformat(),
        ),
        pep_flag=random.random() < 0.05,
        sanctions_flag=False,
        customer_risk_rating=random.choice(RISK_RATINGS),
        employment_status=random.choice(EMPLOYMENT_STATUSES),
        industry=random.choice(INDUSTRIES),
        account_purpose=random.choice(ACCOUNT_PURPOSES),
        expected_monthly_income=monthly_income,
        expected_monthly_turnover=round(monthly_income * random.uniform(0.8, 2.5), 0),
        customer_type=customer_type,
        ubo=ubo,
        alerts_last_12m=random.randint(0, 3),
    )


# ---------------------------------------------------------------------------
# Rules triggered
# ---------------------------------------------------------------------------

def generate_rules_triggered(alert_type: str, risk_score: float) -> list[RuleTriggered]:
    pool = RULES_BY_TYPE.get(alert_type, RULES_BY_TYPE["unusual_pattern"])
    n = min(len(pool), random.randint(1, 2))
    chosen = random.sample(pool, n)
    rules = []
    remaining_score = risk_score
    for i, (rid, en, de) in enumerate(chosen):
        contrib = round(remaining_score / (n - i), 2) if i < n - 1 else round(remaining_score, 2)
        remaining_score -= contrib
        rules.append(RuleTriggered(rule_id=rid, rule_name_en=en, rule_name_de=de, score_contribution=contrib))
    return rules


# ---------------------------------------------------------------------------
# Account summary
# ---------------------------------------------------------------------------

def generate_account_summary(account_id: str, currency: str, balance: float) -> AccountSummary:
    opened = fake.date_between(start_date="-8y", end_date="-6m")
    return AccountSummary(
        account_id=account_id,
        balance=round(balance, 2),
        currency=currency,
        account_type=random.choice(ACCOUNT_TYPES),
        opened_at=opened.isoformat(),
        status=random.choices(ACCOUNT_STATUSES, weights=[85, 10, 5])[0],
    )


# ---------------------------------------------------------------------------
# Internal transaction record (used during 12-month simulation)
# ---------------------------------------------------------------------------

class _InternalTx:
    """Mutable record used while building the 12-month pool."""
    __slots__ = (
        "account_id", "tx_id", "dt", "amount", "currency", "direction", "tx_type",
        "payment_rail", "booking_channel", "payment_reference",
        "cp_name", "cp_iban", "cp_bic", "cp_bank", "cp_country",
        "cash_tx_type", "atm_city", "atm_country",
        "balance_after", "is_trigger",
    )

    def __init__(self) -> None:
        self.account_id: str = ""
        self.tx_id: str = ""
        self.dt: datetime = datetime.min
        self.amount: float = 0.0
        self.currency: str = "EUR"
        self.direction: str = "out"
        self.tx_type: str = "transfer"
        self.payment_rail: str = "SEPA_CT"
        self.booking_channel: str = "online_banking"
        self.payment_reference: str = ""
        self.cp_name: str = ""
        self.cp_iban: str = ""
        self.cp_bic: str = ""
        self.cp_bank: str = ""
        self.cp_country: str = "DE"
        self.cash_tx_type: str | None = None
        self.atm_city: str | None = None
        self.atm_country: str | None = None
        self.balance_after: float = 0.0
        self.is_trigger: bool = False


def _random_dt_between(start: datetime, end: datetime) -> datetime:
    delta = (end - start).total_seconds()
    return start + timedelta(seconds=random.uniform(0, max(delta, 1)))


def _iban_country(iban: str) -> str:
    return iban[:2] if len(iban) >= 2 else "DE"


# ---------------------------------------------------------------------------
# Transaction pool generation (12-month + trigger shaping)
# ---------------------------------------------------------------------------

def _generate_tx_pool(
    account_id: str,
    currency: str,
    opening_balance: float,
    alert_type: str,
    num_trigger: int,
    num_background: int | None = None,
) -> tuple[list[_InternalTx], float]:
    """Build the full 12-month transaction pool and return (pool, final_balance).

    1. Generate background transactions (12 months).
    2. Generate trigger transactions (within alert window).
    3. Merge, sort chronologically, walk with running balance.
    """

    # -- Background transactions (spread over 12 months) --------------------
    if num_background is None:
        num_background = random.randint(40, 80)
    pool: list[_InternalTx] = []
    tx_counter = 0

    for _ in range(num_background):
        tx_counter += 1
        tx = _InternalTx()
        tx.account_id = account_id
        tx.tx_id = f"TX-{account_id}-{tx_counter:04d}"
        tx.dt = _random_dt_between(HISTORY_START, GENERATION_NOW)
        tx.currency = currency
        tx.direction = random.choice(["in", "out"])
        tx.tx_type = random.choice(TX_TYPES)
        tx.payment_rail = random.choice(PAYMENT_RAILS)
        tx.booking_channel = random.choice(BOOKING_CHANNELS)
        tx.cp_name = fake.company() if random.random() > 0.3 else fake.name()
        tx.cp_iban = fake.iban()
        tx.cp_country = _iban_country(tx.cp_iban)
        tx.cp_bic = fake.bothify(text="????DE##???")
        tx.cp_bank = fake.company() + " Bank"
        tx.is_trigger = False

        # Cash-specific
        if tx.payment_rail in ("CASH_DEPOSIT", "ATM_WITHDRAWAL"):
            tx.cash_tx_type = "deposit" if tx.payment_rail == "CASH_DEPOSIT" else "withdrawal"
            tx.atm_city = fake.city()
            tx.atm_country = "DE"
            if tx.payment_rail == "CASH_DEPOSIT":
                tx.direction = "in"
            else:
                tx.direction = "out"
        else:
            tx.cash_tx_type = None
            tx.atm_city = None
            tx.atm_country = None

        pool.append(tx)

    # -- Trigger transactions (within alert window) -------------------------
    trigger_txs: list[_InternalTx] = []
    for i in range(num_trigger):
        tx_counter += 1
        tx = _InternalTx()
        tx.account_id = account_id
        tx.tx_id = f"TX-{account_id}-{tx_counter:04d}"
        tx.currency = currency
        tx.is_trigger = True
        tx.cp_name = fake.company() if random.random() > 0.3 else fake.name()
        tx.cp_iban = fake.iban()
        tx.cp_country = _iban_country(tx.cp_iban)
        tx.cp_bic = fake.bothify(text="????DE##???")
        tx.cp_bank = fake.company() + " Bank"
        tx.booking_channel = random.choice(BOOKING_CHANNELS)

        # Shape by alert type
        if alert_type == "structuring":
            tx.dt = _random_dt_between(ALERT_WINDOW_START, ALERT_WINDOW_END)
            tx.direction = "out"
            tx.tx_type = "transfer"
            tx.payment_rail = "SEPA_CT"
            # Amount set during balance walk (8000–9500)
        elif alert_type == "velocity":
            base = _random_dt_between(ALERT_WINDOW_START, ALERT_WINDOW_START + timedelta(days=20))
            tx.dt = base + timedelta(hours=i * 2)
            tx.direction = random.choice(["in", "out"])
            tx.tx_type = random.choice(TX_TYPES)
            tx.payment_rail = random.choice(PAYMENT_RAILS)
        elif alert_type == "high_risk_country":
            tx.dt = _random_dt_between(ALERT_WINDOW_START, ALERT_WINDOW_END)
            tx.direction = "out"
            tx.tx_type = "wire"
            tx.payment_rail = "SWIFT"
            tx.cp_country = random.choice(list(HIGH_RISK_COUNTRIES))
            tx.cp_iban = tx.cp_country + fake.bothify(text="####################")
        elif alert_type == "large_single_transaction":
            tx.dt = _random_dt_between(ALERT_WINDOW_START, ALERT_WINDOW_END)
            tx.direction = random.choice(["in", "out"])
            tx.tx_type = "wire"
            tx.payment_rail = "SWIFT"
        else:  # unusual_pattern
            tx.dt = _random_dt_between(ALERT_WINDOW_START, ALERT_WINDOW_END)
            tx.direction = random.choice(["in", "out"])
            tx.tx_type = random.choice(TX_TYPES)
            tx.payment_rail = random.choice(PAYMENT_RAILS)

        # Cash specifics for trigger
        if tx.payment_rail in ("CASH_DEPOSIT", "ATM_WITHDRAWAL"):
            tx.cash_tx_type = "deposit" if tx.payment_rail == "CASH_DEPOSIT" else "withdrawal"
            tx.atm_city = fake.city()
            tx.atm_country = "DE"
        else:
            tx.cash_tx_type = None
            tx.atm_city = None
            tx.atm_country = None

        trigger_txs.append(tx)

    pool.extend(trigger_txs)

    # -- Sort chronologically and assign amounts via running balance ---------
    pool.sort(key=lambda t: t.dt)

    balance = opening_balance
    for tx in pool:
        if tx.direction == "out":
            # Determine max amount allowed
            max_out = balance  # keep balance >= 0
            if max_out <= 0:
                # Flip to inbound if we can't afford any outflow
                tx.direction = "in"
                tx.amount = round(random.uniform(500, 5000), 2)
                balance += tx.amount
            else:
                if tx.is_trigger and alert_type == "structuring":
                    desired = round(random.uniform(8000, 9500), 2)
                    tx.amount = min(desired, max_out)
                elif tx.is_trigger and alert_type == "large_single_transaction":
                    desired = round(random.uniform(100000, 500000), 2)
                    tx.amount = min(desired, max_out)
                else:
                    upper = min(15000.0, max_out)
                    tx.amount = round(random.uniform(50, max(50, upper)), 2)
                balance -= tx.amount
        else:  # direction == "in"
            if tx.is_trigger and alert_type == "large_single_transaction":
                tx.amount = round(random.uniform(100000, 500000), 2)
            else:
                tx.amount = round(random.uniform(50, 15000), 2)
            balance += tx.amount

        tx.balance_after = round(balance, 2)

    # -- Assign payment references (after balance walk, direction is final) --
    for tx in pool:
        tx.payment_reference = _generate_payment_reference(
            tx.tx_type, tx.direction, tx.dt, tx.cp_name,
        )

    return pool, round(balance, 2)


# ---------------------------------------------------------------------------
# Behavior stats computation
# ---------------------------------------------------------------------------

def compute_behavior_stats(
    pool: list[_InternalTx],
    alert_created_at: datetime,
) -> BehaviorStats:
    now = alert_created_at
    d7 = now - timedelta(days=7)
    d30 = now - timedelta(days=30)
    d90 = now - timedelta(days=90)
    d365 = now - timedelta(days=365)

    # Counters
    count_7d = count_30d = 0
    cash_in_12m = cash_out_12m = 0.0
    in_vol_30d = out_vol_30d = 0.0
    amounts_3m: list[float] = []

    cp_first_seen: dict[str, str] = {}
    cp_country: dict[str, str] = {}
    cp_freq: dict[str, int] = defaultdict(int)

    total_vol_12m = 0.0
    txn_count_12m = 0

    for tx in pool:
        if tx.dt < d365 or tx.dt > now:
            continue
        txn_count_12m += 1
        total_vol_12m += tx.amount

        # 7d / 30d counts
        if tx.dt >= d7:
            count_7d += 1
        if tx.dt >= d30:
            count_30d += 1
            if tx.direction == "in":
                in_vol_30d += tx.amount
            else:
                out_vol_30d += tx.amount

        # Cash
        if tx.tx_type == "cash":
            if tx.direction == "in":
                cash_in_12m += tx.amount
            else:
                cash_out_12m += tx.amount

        # 3-month amounts
        if tx.dt >= d90:
            amounts_3m.append(tx.amount)

        # Counterparty tracking (for unique/new/high-risk counts)
        name = tx.cp_name
        dt_str = tx.dt.strftime("%Y-%m-%d")
        if name not in cp_first_seen or dt_str < cp_first_seen[name]:
            cp_first_seen[name] = dt_str
        cp_country[name] = tx.cp_country
        cp_freq[name] += 1

    avg_3m = round(sum(amounts_3m) / len(amounts_3m), 2) if amounts_3m else 0.0
    trigger_amounts = [tx.amount for tx in pool if tx.is_trigger]
    avg_trigger = sum(trigger_amounts) / len(trigger_amounts) if trigger_amounts else avg_3m
    multiplier = round(avg_trigger / avg_3m, 2) if avg_3m > 0 else 1.0

    all_cps = set(cp_freq.keys())
    new_30d_names = {tx.cp_name for tx in pool if d30 <= tx.dt <= now}
    old_names = {n for n, fs in cp_first_seen.items() if fs < d30.strftime("%Y-%m-%d")}
    new_cps_30d = new_30d_names - old_names
    hr_cps = {n for n, c in cp_country.items() if c in HIGH_RISK_COUNTRIES}

    has_hr_country = any(tx.cp_country in HIGH_RISK_COUNTRIES for tx in pool if tx.is_trigger)

    return BehaviorStats(
        transaction_count_7d=count_7d,
        transaction_count_30d=count_30d,
        cash_in_12m=round(cash_in_12m, 2),
        cash_out_12m=round(cash_out_12m, 2),
        incoming_volume_30d=round(in_vol_30d, 2),
        outgoing_volume_30d=round(out_vol_30d, 2),
        avg_tx_amount_3m=avg_3m,
        amount_multiplier_vs_3m=multiplier,
        unique_counterparties_12m=len(all_cps),
        new_counterparties_30d=len(new_cps_30d),
        high_risk_counterparties_12m=len(hr_cps),
        peer_group_deviation=round(random.uniform(-2.0, 3.0), 2),
        suspicious_keyword_hit=random.random() < 0.1,
        high_risk_country_hit=has_hr_country,
        risky_bank_hit=random.random() < 0.05,
        customer_last_12m_stats=CustomerLast12mStats(
            total_volume=round(total_vol_12m, 2),
            avg_monthly_volume=round(total_vol_12m / 12, 2),
            txn_count=txn_count_12m,
        ),
    )


# ---------------------------------------------------------------------------
# Conversion helpers: _InternalTx → output models
# ---------------------------------------------------------------------------

def _to_trigger(tx: _InternalTx) -> TriggerTransaction:
    return TriggerTransaction(
        account_id=tx.account_id,
        transaction_id=tx.tx_id,
        timestamp=tx.dt.isoformat(),
        amount=tx.amount,
        currency=tx.currency,
        direction=tx.direction,
        payment_rail=tx.payment_rail,
        booking_channel=tx.booking_channel,
        payment_reference=tx.payment_reference,
        type=tx.tx_type,
        counterparty_name=tx.cp_name,
        counterparty_iban=tx.cp_iban,
        counterparty_bic=tx.cp_bic,
        counterparty_bank_name=tx.cp_bank,
        counterparty_country_iso=tx.cp_country,
        cash_transaction_type=tx.cash_tx_type,
        atm_city=tx.atm_city,
        atm_country=tx.atm_country,
        remaining_account_balance_after_tx=tx.balance_after,
    )


def _to_history(tx: _InternalTx) -> HistoryTransaction:
    return HistoryTransaction(
        account_id=tx.account_id,
        transaction_id=tx.tx_id,
        timestamp=tx.dt.isoformat(),
        amount=tx.amount,
        currency=tx.currency,
        direction=tx.direction,
        type=tx.tx_type,
        counterparty_name=tx.cp_name,
        counterparty_iban=tx.cp_iban,
        counterparty_bic=tx.cp_bic,
        counterparty_bank_name=tx.cp_bank,
        counterparty_country_iso=tx.cp_country,
        payment_reference=tx.payment_reference,
        remaining_account_balance_after_tx=tx.balance_after,
    )


# ---------------------------------------------------------------------------
# Main alert generator
# ---------------------------------------------------------------------------

def generate_alert(alert_index: int) -> Alert:
    """Generate one full alert conforming to the 132-field schema."""
    alert_id = f"ALT-{alert_index:05d}"
    customer_id = f"CUST-{alert_index:05d}-{random.randint(100000, 999999)}"
    account_id = f"ACC-{alert_index:05d}-{random.randint(1000000, 9999999)}"

    alert_type = ALERT_TYPES[alert_index % len(ALERT_TYPES)]
    status = random.choice(ALERT_STATUSES)
    created_at_dt = _random_dt_between(ALERT_WINDOW_START, ALERT_WINDOW_END)
    created_at = created_at_dt.isoformat()
    risk_score = round(random.uniform(0.3, 0.95), 2)
    requires_sar = alert_index in (1, 6)

    # Customer
    profile = generate_customer_profile(customer_id)

    # Rules
    rules = generate_rules_triggered(alert_type, risk_score)
    primary_rule_id = rules[0].rule_id if rules else ""

    # Currency — mostly EUR
    currency = random.choices(CURRENCIES, weights=[80, 10, 10])[0]

    # Number of trigger transactions
    num_trigger = 1 if alert_type == "large_single_transaction" else random.randint(2, 5)

    # Opening balance
    opening_balance = round(random.uniform(5000, 150000), 2)

    # Build 12-month pool with running balance
    pool, final_balance = _generate_tx_pool(account_id, currency, opening_balance, alert_type, num_trigger)

    # Account summary (balance = final running balance)
    primary_account = generate_account_summary(account_id, currency, final_balance)
    account_summaries = [primary_account]

    # Secondary account (30% chance) — background transactions only
    secondary_pool: list[_InternalTx] = []
    if random.random() < 0.3:
        extra_id = f"ACC-{alert_index:05d}-{random.randint(2000000, 2999999)}"
        extra_opening = round(random.uniform(1000, 50000), 2)
        secondary_pool, extra_final = _generate_tx_pool(
            extra_id, currency, extra_opening, alert_type,
            num_trigger=0, num_background=random.randint(10, 25),
        )
        account_summaries.append(generate_account_summary(extra_id, currency, extra_final))

    # Merge pools for stats and history
    all_txs = pool + secondary_pool

    # Split pool → trigger + history output
    trigger_out = [_to_trigger(tx) for tx in pool if tx.is_trigger]
    history_out = [_to_history(tx) for tx in all_txs if HISTORY_OUTPUT_START <= tx.dt <= created_at_dt]
    history_out.sort(key=lambda h: h.timestamp)

    # Behavior stats (from full 12-month pool across all accounts)
    bstats = compute_behavior_stats(all_txs, created_at_dt)

    return Alert(
        alert_id=alert_id,
        created_at=created_at,
        status=status,
        risk_score=risk_score,
        requires_sar=requires_sar,
        primary_rule_id=primary_rule_id,
        alert_reason_summary=ALERT_REASON_SUMMARIES.get(alert_type, ""),
        rules_triggered=rules,
        customer_profile=profile,
        trigger_transactions=trigger_out,
        transaction_history=history_out,
        behavior_stats=bstats,
        account_summaries=account_summaries,
    )
