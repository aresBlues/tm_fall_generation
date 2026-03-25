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
from decimal import ROUND_HALF_UP, Decimal
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

_MONEY_QUANT = Decimal("1")


def _money_dec(value: float | Decimal | str) -> Decimal:
    """Quantize to whole currency units (half-up)."""
    if isinstance(value, Decimal):
        d = value
    else:
        d = Decimal(str(value))
    return d.quantize(_MONEY_QUANT, rounding=ROUND_HALF_UP)


def _money_float(value: float | Decimal | str) -> float:
    """JSON-safe float: monetary values with no fractional cents/units."""
    return float(_money_dec(value))


# Counterparty fields when type=cash (no third party on statement) — keep strings for downstream parsers
COUNTERPARTY_NA = "-"


def _is_cash_tx_type(tx_type: str) -> bool:
    """Only `type: cash` omits counterparty (filled with '-'); other types keep generated CP."""
    return tx_type == "cash"

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

# Plausible EUR bands (min, max inclusive) per template — keeps references aligned with amounts.
_EUR_ANY_HI = 10_000_000.0

# Short / generic references (no placeholders); wide band so they always qualify as fallback.
_PAYMENT_REFS_GENERIC: list[tuple[str, float, float]] = [
    ("Diverses", 0.0, _EUR_ANY_HI),
    ("Unbekannt", 0.0, _EUR_ANY_HI),
    ("Wie besprochen", 0.0, _EUR_ANY_HI),
    ("Dankeschön", 0.0, _EUR_ANY_HI),
    ("Gefälligkeit", 0.0, _EUR_ANY_HI),
    ("Freundschaftsdienst", 0.0, _EUR_ANY_HI),
    ("Rückzahlung", 0.0, _EUR_ANY_HI),
    ("Schulden beglichen", 0.0, _EUR_ANY_HI),
    ("Provision", 0.0, _EUR_ANY_HI),
    ("Gebühr", 0.0, _EUR_ANY_HI),
    ("Honorar", 0.0, _EUR_ANY_HI),
    ("Privat", 0.0, _EUR_ANY_HI),
    ("Ware", 0.0, _EUR_ANY_HI),
    ("Dienstleistung erbracht", 0.0, _EUR_ANY_HI),
    ("Familie", 0.0, _EUR_ANY_HI),
    ("Family support", 0.0, _EUR_ANY_HI),
    ("Tickets", 0.0, _EUR_ANY_HI),
    ("Consulting Fee", 0.0, _EUR_ANY_HI),
    ("Schulden", 0.0, _EUR_ANY_HI),
]

_PAYMENT_REFS_IN_TRANSFER: list[tuple[str, float, float]] = [
    ("Gehalt {month} {year}", 1500.0, 12000.0),
    ("Miete {month} {year}", 400.0, 2000.0),
    ("Mieteinnahme {month} {year}", 400.0, 2000.0),
    ("Mieteinnahme Gewerbe – {month} {year}", 5000.0, 25000.0),
    ("Gutschrift Dauerauftrag", 50.0, 50000.0),
    ("Rückerstattung Rechnung {ref}", 20.0, 50000.0),
    ("Einzahlung Sparvertrag", 100.0, 200000.0),
    ("Honorar {month} {year}", 500.0, 50000.0),
    ("Provision Q{quarter}/{year}", 500.0, 100000.0),
    ("Rückzahlung Urlaubskasse {year}", 100.0, 5000.0),
    ("Reisekostenerstattung {month} {year} – Dienstreise {city}", 50.0, 5000.0),
    ("Provisionszahlung Q{quarter} {year}", 500.0, 100000.0),
    ("Freelancer-Honorar – Projekt bei {name}", 1000.0, 100000.0),
    ("Lieferantenrechnung – Charge {ref_inv}", 5000.0, 500000.0),
    ("Gutschrift – Rechnung Nr. {ref_inv}", 500.0, 500000.0),
    ("Überweisung von {name}", 50.0, 500000.0),
    ("Stromrechnung Q{quarter} {year} – Gutschrift Stadtwerke {city}", 40.0, 800.0),
    ("Rückerstattung Steuer {year}", 100.0, 50000.0),
]

_PAYMENT_REFS_OUT_TRANSFER: list[tuple[str, float, float]] = [
    ("Miete {month} {year}", 400.0, 2000.0),
    ("Miete {month} {year} – Wohnung {street}", 400.0, 2000.0),
    ("Büromiete {month} {year} – {city}", 5000.0, 25000.0),
    ("Dauerauftrag Strom/Gas", 40.0, 500.0),
    ("Stromrechnung Q{quarter} {year} – Stadtwerke {city}", 40.0, 500.0),
    ("Versicherungsbeitrag {month} {year}", 30.0, 900.0),
    ("Rechnung Nr. {ref}", 500.0, 500000.0),
    ("Rechnung Nr. {ref_inv}", 500.0, 500000.0),
    ("Zahlung an {name}", 50.0, 500000.0),
    ("Ratenzahlung Kredit {ref}", 100.0, 8000.0),
    ("Telefonrechnung {month} {year}", 15.0, 200.0),
    ("Handy-Rechnung Telekom {month} {year}", 15.0, 200.0),
    ("Internet Rechnung {month}/{year}", 10.0, 120.0),
    ("Mitgliedsbeitrag {year}", 30.0, 5000.0),
    ("KFZ-Steuer {year}", 50.0, 5000.0),
    ("Lagermiete {month} – Logistikzentrum {city}", 3000.0, 100000.0),
    ("Kindergartenbeitrag {month} – {city}", 60.0, 600.0),
    ("Überweisung Lieferant – Anzahlung Auftrag #{ref}", 5000.0, 500000.0),
    ("Beratungshonorar – IT-Projekt Ref {ref}", 2000.0, 100000.0),
    ("Jahresabonnement Microsoft 365 Business", 20.0, 400.0),
    ("Arbeitnehmer-Anteil Sozialversicherung {month} {year}", 400.0, 2500.0),
    ("Buchhaltersoftware DATEV – Monatslizenz {month} {year}", 30.0, 600.0),
    ("Fortbildungsseminar – IHK {city}", 200.0, 8000.0),
    ("Catering Betriebsfeier {month} – {name}", 500.0, 50000.0),
    ("Fahrzeug-Leasing Rate {month} – {ref}", 200.0, 2500.0),
    ("Messestand-Gebühr {city} {year}", 5000.0, 500000.0),
    ("GEZ-Beitrag", 10.0, 30.0),
    ("Sportverein Mitgliedsbeitrag {month}", 10.0, 250.0),
]

_PAYMENT_REFS_CASH_IN: list[tuple[str, float, float]] = [
    ("Bargeldeinzahlung", 50.0, 50000.0),
    ("Bargeldeinzahlung Filiale", 50.0, 50000.0),
    ("Einzahlung am Automaten", 20.0, 15000.0),
    ("Bargeldeinzahlung – {city}", 50.0, 50000.0),
    ("Urlaubsanzahlung – Hotel {city}", 100.0, 15000.0),
]

_PAYMENT_REFS_CASH_OUT: list[tuple[str, float, float]] = [
    ("Bargeldauszahlung", 20.0, 1000.0),
    ("Geldautomat {city}", 20.0, 1000.0),
    ("ATM Auszahlung", 20.0, 1000.0),
    ("Ausflug", 20.0, 1000.0),
    ("Essen gehen", 10.0, 500.0),
    ("Einkaufen", 10.0, 1000.0),
]

_PAYMENT_REFS_CARD: list[tuple[str, float, float]] = [
    ("Kartenzahlung {name}", 5.0, 500.0),
    ("POS {name}", 5.0, 500.0),
    ("EC-Kartenzahlung {name}", 5.0, 500.0),
    ("Kontaktlos {name}", 5.0, 500.0),
    ("REWE Einkauf vom {date_dm}", 5.0, 500.0),
    ("Amazon-Bestellung", 5.0, 2000.0),
    ("Größerer Einzelhandels-Einkauf {name}", 500.0, 2000.0),
    ("Monatliche Netflix-Gebühr", 5.0, 25.0),
    ("Handy-Rechnung Telekom {month} {year}", 15.0, 200.0),
    ("Fitnessstudio-Mitgliedschaft {month}", 10.0, 150.0),
    ("Messestand-Gebühr {city} {year}", 5000.0, 500000.0),
    ("Zahnarztrechnung – Behandlung {date_dm}", 50.0, 5000.0),
    ("Geburtstagsgeschenk für {name}", 5.0, 500.0),
]

_PAYMENT_REFS_WIRE: list[tuple[str, float, float]] = [
    ("Auslandsüberweisung {name}", 1000.0, 1_000_000.0),
    ("Auslandsüberweisung {name} Ref {ref_inv}", 1000.0, 1_000_000.0),
    ("SWIFT Transfer Ref {ref}", 1000.0, 1_000_000.0),
    ("Internationale Zahlung {ref}", 1000.0, 1_000_000.0),
    ("Internationale Zahlung Auftrag #{ref}", 1000.0, 1_000_000.0),
    ("Wire Transfer an {name}", 1000.0, 1_000_000.0),
]


def _filter_ref_templates_by_amount(pool: list[tuple[str, float, float]], amount: float) -> list[str]:
    return [tpl for tpl, lo, hi in pool if lo <= amount <= hi]


def _pick_ref_template(
    pool: list[tuple[str, float, float]],
    amount: float,
    fallback: list[tuple[str, float, float]] | None = None,
) -> str:
    candidates = _filter_ref_templates_by_amount(pool, amount)
    if candidates:
        return random.choice(candidates)
    if fallback is not None:
        fb = _filter_ref_templates_by_amount(fallback, amount)
        if fb:
            return random.choice(fb)
    generic = _filter_ref_templates_by_amount(_PAYMENT_REFS_GENERIC, amount)
    if generic:
        return random.choice(generic)
    return random.choice([t for t, _, _ in _PAYMENT_REFS_GENERIC])


def _generate_payment_reference(
    tx_type: str, direction: str, dt: datetime, cp_name: str, amount: float,
) -> str:
    month = GERMAN_MONTHS[dt.month - 1]
    year = dt.year
    quarter = (dt.month - 1) // 3 + 1
    ref = fake.numerify(text="#####")
    date_dm = f"{dt.day:02d}.{dt.month:02d}.{dt.year}"
    street = fake.street_address()
    ref_inv = f"{year}-{fake.numerify(text='###')}"
    city = fake.city()
    name_for_template = "" if cp_name == COUNTERPARTY_NA else cp_name

    if random.random() < 0.12:
        template = _pick_ref_template(_PAYMENT_REFS_GENERIC, amount)
    elif tx_type == "cash":
        pool = _PAYMENT_REFS_CASH_IN if direction == "in" else _PAYMENT_REFS_CASH_OUT
        template = _pick_ref_template(pool, amount, fallback=_PAYMENT_REFS_GENERIC)
    elif tx_type == "card":
        template = _pick_ref_template(_PAYMENT_REFS_CARD, amount, fallback=_PAYMENT_REFS_GENERIC)
    elif tx_type == "wire":
        template = _pick_ref_template(_PAYMENT_REFS_WIRE, amount, fallback=_PAYMENT_REFS_GENERIC)
    elif direction == "in":
        template = _pick_ref_template(_PAYMENT_REFS_IN_TRANSFER, amount, fallback=_PAYMENT_REFS_GENERIC)
    else:
        template = _pick_ref_template(_PAYMENT_REFS_OUT_TRANSFER, amount, fallback=_PAYMENT_REFS_GENERIC)

    return template.format(
        month=month,
        year=year,
        quarter=quarter,
        ref=ref,
        ref_inv=ref_inv,
        name=name_for_template,
        city=city,
        date_dm=date_dm,
        street=street,
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
            pct = round(random.uniform(10, remaining - 10 * (num_ubo - j - 1)), 0) if j < num_ubo - 1 else round(remaining, 0)
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

def generate_rules_triggered(alert_type: str) -> list[RuleTriggered]:
    pool = RULES_BY_TYPE.get(alert_type, RULES_BY_TYPE["unusual_pattern"])
    n = min(len(pool), random.randint(1, 2))
    chosen = random.sample(pool, n)
    return [
        RuleTriggered(rule_id=rid, rule_name_en=en, rule_name_de=de)
        for rid, en, de in chosen
    ]


# ---------------------------------------------------------------------------
# Account summary
# ---------------------------------------------------------------------------

def generate_account_summary(account_id: str, currency: str, balance: float) -> AccountSummary:
    opened = fake.date_between(start_date="-8y", end_date="-6m")
    return AccountSummary(
        account_id=account_id,
        balance=_money_float(balance),
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


def _truncated_gauss(
    lo: float,
    hi: float,
    mu: float | None = None,
    sigma: float | None = None,
) -> Decimal:
    """Sample from a Gaussian, rejecting until the value lies in [lo, hi]; return whole-unit Decimal."""
    if hi <= lo:
        return _money_dec(lo)
    if mu is None:
        mu = (lo + hi) / 2.0
    if sigma is None:
        sigma = max((hi - lo) / 6.0, 1e-6)
    for _ in range(300):
        x = random.gauss(mu, sigma)
        if lo <= x <= hi:
            return _money_dec(x)
    return _money_dec(random.uniform(lo, hi))


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

    balance = _money_dec(opening_balance)
    for tx in pool:
        amt: Decimal
        if tx.direction == "out":
            max_out = balance
            if max_out <= 0:
                tx.direction = "in"
                amt = _truncated_gauss(500.0, 5000.0)
                balance = balance + amt
            else:
                if tx.is_trigger and alert_type == "structuring":
                    desired = _truncated_gauss(
                        8000.0, 9500.0, mu=8750.0, sigma=(9500.0 - 8000.0) / 6.0,
                    )
                    amt = min(desired, max_out)
                elif tx.is_trigger and alert_type == "large_single_transaction":
                    desired = _truncated_gauss(
                        100000.0, 500000.0, mu=300000.0, sigma=(500000.0 - 100000.0) / 6.0,
                    )
                    amt = min(desired, max_out)
                else:
                    upper = min(Decimal("15000"), max_out)
                    hi = max(Decimal("50"), upper)
                    amt = _truncated_gauss(50.0, float(hi))
                balance = balance - amt
        else:
            if tx.is_trigger and alert_type == "large_single_transaction":
                amt = _truncated_gauss(
                    100000.0, 500000.0, mu=300000.0, sigma=(500000.0 - 100000.0) / 6.0,
                )
            else:
                amt = _truncated_gauss(50.0, 15000.0)
            balance = balance + amt

        tx.amount = _money_float(amt)
        tx.balance_after = _money_float(balance)

    # -- type=cash only: no counterparty on statement (placeholder for downstream) --
    for tx in pool:
        if _is_cash_tx_type(tx.tx_type):
            tx.cp_name = COUNTERPARTY_NA
            tx.cp_iban = COUNTERPARTY_NA
            tx.cp_bic = COUNTERPARTY_NA
            tx.cp_bank = COUNTERPARTY_NA
            tx.cp_country = COUNTERPARTY_NA

    # -- Assign payment references (after balance walk, direction is final) --
    for tx in pool:
        tx.payment_reference = _generate_payment_reference(
            tx.tx_type, tx.direction, tx.dt, tx.cp_name, tx.amount,
        )

    return pool, _money_float(balance)


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
        if tx.cp_name != COUNTERPARTY_NA:
            name = tx.cp_name
            dt_str = tx.dt.strftime("%Y-%m-%d")
            if name not in cp_first_seen or dt_str < cp_first_seen[name]:
                cp_first_seen[name] = dt_str
            cp_country[name] = tx.cp_country
            cp_freq[name] += 1

    avg_3m = (
        _money_float(sum(amounts_3m) / len(amounts_3m)) if amounts_3m else 0.0
    )
    trigger_amounts = [tx.amount for tx in pool if tx.is_trigger]
    avg_trigger = sum(trigger_amounts) / len(trigger_amounts) if trigger_amounts else avg_3m
    multiplier = _money_float(avg_trigger / avg_3m) if avg_3m > 0 else 1.0

    all_cps = set(cp_freq.keys())
    new_30d_names = {
        tx.cp_name for tx in pool
        if d30 <= tx.dt <= now and tx.cp_name != COUNTERPARTY_NA
    }
    old_names = {n for n, fs in cp_first_seen.items() if fs < d30.strftime("%Y-%m-%d")}
    new_cps_30d = new_30d_names - old_names
    hr_cps = {n for n, c in cp_country.items() if c in HIGH_RISK_COUNTRIES}

    has_hr_country = any(
        tx.cp_country in HIGH_RISK_COUNTRIES
        for tx in pool
        if tx.is_trigger and tx.cp_country != COUNTERPARTY_NA
    )

    return BehaviorStats(
        transaction_count_7d=count_7d,
        transaction_count_30d=count_30d,
        cash_in_12m=_money_float(cash_in_12m),
        cash_out_12m=_money_float(cash_out_12m),
        incoming_volume_30d=_money_float(in_vol_30d),
        outgoing_volume_30d=_money_float(out_vol_30d),
        avg_tx_amount_3m=avg_3m,
        amount_multiplier_vs_3m=multiplier,
        unique_counterparties_12m=len(all_cps),
        new_counterparties_30d=len(new_cps_30d),
        high_risk_counterparties_12m=len(hr_cps),
        peer_group_deviation=round(random.uniform(-2.0, 3.0), 0),
        suspicious_keyword_hit=random.random() < 0.1,
        high_risk_country_hit=has_hr_country,
        risky_bank_hit=random.random() < 0.05,
        customer_last_12m_stats=CustomerLast12mStats(
            total_volume=_money_float(total_vol_12m),
            avg_monthly_volume=_money_float(total_vol_12m / 12),
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
    # Customer
    profile = generate_customer_profile(customer_id)

    # Rules
    rules = generate_rules_triggered(alert_type)

    # Currency — mostly EUR
    currency = random.choices(CURRENCIES, weights=[80, 10, 10])[0]

    # Number of trigger transactions
    num_trigger = 1 if alert_type == "large_single_transaction" else random.randint(2, 5)

    # Opening balance
    opening_balance = _money_float(
        _truncated_gauss(5000.0, 150000.0, mu=77500.0, sigma=(150000.0 - 5000.0) / 6.0),
    )

    # Build 12-month pool with running balance
    pool, final_balance = _generate_tx_pool(account_id, currency, opening_balance, alert_type, num_trigger)

    # Account summary (balance = final running balance)
    primary_account = generate_account_summary(account_id, currency, final_balance)
    account_summaries = [primary_account]

    # Secondary account (30% chance) — background transactions only
    secondary_pool: list[_InternalTx] = []
    if random.random() < 0.3:
        extra_id = f"ACC-{alert_index:05d}-{random.randint(2000000, 2999999)}"
        extra_opening = _money_float(
            _truncated_gauss(1000.0, 50000.0, mu=25500.0, sigma=(50000.0 - 1000.0) / 6.0),
        )
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
        alert_reason_summary=ALERT_REASON_SUMMARIES.get(alert_type, ""),
        rules_triggered=rules,
        customer_profile=profile,
        trigger_transactions=trigger_out,
        transaction_history=history_out,
        behavior_stats=bstats,
        account_summaries=account_summaries,
    )
