"""Data models for TM Fallbearbeitung alerts (JSON-serializable).

Aligned with the 132-field schema defined in alerts_de_schema.xlsx.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Shared / nested models
# ---------------------------------------------------------------------------

@dataclass
class Address:
    street: str
    postal_code: str
    city: str
    country: str  # ISO-2

    def to_dict(self) -> dict[str, Any]:
        return {
            "street": self.street,
            "postal_code": self.postal_code,
            "city": self.city,
            "country": self.country,
        }


@dataclass
class IdDocument:
    type: str        # passport | national_id | residence_permit
    number: str
    issued_at: str   # YYYY-MM-DD
    expires_at: str  # YYYY-MM-DD

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "number": self.number,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
        }


@dataclass
class UBO:
    name: str
    ownership_percentage: float

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "ownership_percentage": self.ownership_percentage}


# ---------------------------------------------------------------------------
# Customer profile (replaces old KYC)
# ---------------------------------------------------------------------------

@dataclass
class CustomerProfile:
    customer_id: str
    first_name: str
    last_name: str
    full_name: str
    date_of_birth: str          # YYYY-MM-DD
    place_of_birth: str
    nationality: str            # ISO-2
    residency_country: str      # ISO-2
    kyc_status: str             # VERIFIED | PENDING | REJECTED
    customer_since: str         # YYYY-MM-DD
    email: str
    phone_number: str

    legal_address: Address
    shipping_address: Address
    id_document_address: Address
    business_address: Address

    id_document: IdDocument

    pep_flag: bool
    sanctions_flag: bool
    customer_risk_rating: str   # low | medium | high
    employment_status: str      # EMPLOYED | SELF_EMPLOYED | UNEMPLOYED | STUDENT | RETIRED
    industry: str
    account_purpose: str
    expected_monthly_income: float
    expected_monthly_turnover: float
    customer_type: str          # private | business
    ubo: list[UBO] = field(default_factory=list)
    alerts_last_12m: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "customer_id": self.customer_id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "full_name": self.full_name,
            "date_of_birth": self.date_of_birth,
            "place_of_birth": self.place_of_birth,
            "nationality": self.nationality,
            "residency_country": self.residency_country,
            "kyc_status": self.kyc_status,
            "customer_since": self.customer_since,
            "email": self.email,
            "phone_number": self.phone_number,
            "legal_address": self.legal_address.to_dict(),
            "shipping_address": self.shipping_address.to_dict(),
            "id_document_address": self.id_document_address.to_dict(),
            "business_address": self.business_address.to_dict(),
            "id_document": self.id_document.to_dict(),
            "pep_flag": self.pep_flag,
            "sanctions_flag": self.sanctions_flag,
            "customer_risk_rating": self.customer_risk_rating,
            "employment_status": self.employment_status,
            "industry": self.industry,
            "account_purpose": self.account_purpose,
            "expected_monthly_income": self.expected_monthly_income,
            "expected_monthly_turnover": self.expected_monthly_turnover,
            "customer_type": self.customer_type,
            "ubo": [u.to_dict() for u in self.ubo],
            "alerts_last_12m": self.alerts_last_12m,
        }


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

@dataclass
class RuleTriggered:
    rule_id: str
    rule_name_en: str
    rule_name_de: str
    score_contribution: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "rule_name_en": self.rule_name_en,
            "rule_name_de": self.rule_name_de,
            "score_contribution": self.score_contribution,
        }


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------

@dataclass
class TriggerTransaction:
    """A transaction that triggered the alert — rich detail."""
    transaction_id: str
    timestamp: str
    amount: float
    currency: str
    direction: str                          # in | out
    payment_rail: str                       # SEPA_CT | SEPA_INST | CARD_POS | ATM_WITHDRAWAL | CASH_DEPOSIT | SWIFT
    booking_channel: str                    # mobile | online_banking | atm | card_terminal
    payment_reference: str
    type: str                               # transfer | cash | wire | card
    counterparty_name: str
    counterparty_iban: str
    counterparty_bic: str
    counterparty_bank_name: str
    counterparty_country_iso: str           # ISO-2
    cash_transaction_type: str | None       # deposit | withdrawal | null
    atm_city: str | None
    atm_country: str | None                 # ISO-2
    remaining_account_balance_after_tx: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "timestamp": self.timestamp,
            "amount": self.amount,
            "currency": self.currency,
            "direction": self.direction,
            "payment_rail": self.payment_rail,
            "booking_channel": self.booking_channel,
            "payment_reference": self.payment_reference,
            "type": self.type,
            "counterparty_name": self.counterparty_name,
            "counterparty_iban": self.counterparty_iban,
            "counterparty_bic": self.counterparty_bic,
            "counterparty_bank_name": self.counterparty_bank_name,
            "counterparty_country_iso": self.counterparty_country_iso,
            "cash_transaction_type": self.cash_transaction_type,
            "atm_city": self.atm_city,
            "atm_country": self.atm_country,
            "remaining_account_balance_after_tx": self.remaining_account_balance_after_tx,
        }


@dataclass
class HistoryTransaction:
    """A transaction in the 90-day history window — lighter detail."""
    transaction_id: str
    timestamp: str
    amount: float
    currency: str
    direction: str               # in | out
    type: str                    # transfer | wire | cash | card
    counterparty_name: str
    counterparty_iban: str
    counterparty_country_iso: str  # ISO-2
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "timestamp": self.timestamp,
            "amount": self.amount,
            "currency": self.currency,
            "direction": self.direction,
            "type": self.type,
            "counterparty_name": self.counterparty_name,
            "counterparty_iban": self.counterparty_iban,
            "counterparty_country_iso": self.counterparty_country_iso,
            "description": self.description,
        }


# ---------------------------------------------------------------------------
# Behavior stats
# ---------------------------------------------------------------------------

@dataclass
class CounterpartyStat:
    name: str
    country_iso: str
    seen_before: bool
    frequency_12m: int
    total_volume_12m: float
    first_seen: str   # YYYY-MM-DD
    last_seen: str    # YYYY-MM-DD

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "country_iso": self.country_iso,
            "seen_before": self.seen_before,
            "frequency_12m": self.frequency_12m,
            "total_volume_12m": self.total_volume_12m,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
        }


@dataclass
class CustomerLast12mStats:
    total_volume: float
    avg_monthly_volume: float
    txn_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_volume": self.total_volume,
            "avg_monthly_volume": self.avg_monthly_volume,
            "txn_count": self.txn_count,
        }


@dataclass
class BehaviorStats:
    transaction_count_7d: int
    transaction_count_30d: int
    cash_in_12m: float
    cash_out_12m: float
    incoming_volume_30d: float
    outgoing_volume_30d: float
    avg_tx_amount_3m: float
    amount_multiplier_vs_3m: float

    unique_counterparties_12m: int
    new_counterparties_30d: int
    high_risk_counterparties_12m: int

    counterparty_stats: list[CounterpartyStat] = field(default_factory=list)
    country_frequency: dict[str, int] = field(default_factory=dict)

    peer_group_deviation: float = 0.0
    suspicious_keyword_hit: bool = False
    high_risk_country_hit: bool = False
    risky_bank_hit: bool = False

    customer_last_12m_stats: CustomerLast12mStats | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "transaction_count_7d": self.transaction_count_7d,
            "transaction_count_30d": self.transaction_count_30d,
            "cash_in_12m": self.cash_in_12m,
            "cash_out_12m": self.cash_out_12m,
            "incoming_volume_30d": self.incoming_volume_30d,
            "outgoing_volume_30d": self.outgoing_volume_30d,
            "avg_tx_amount_3m": self.avg_tx_amount_3m,
            "amount_multiplier_vs_3m": self.amount_multiplier_vs_3m,
            "unique_counterparties_12m": self.unique_counterparties_12m,
            "new_counterparties_30d": self.new_counterparties_30d,
            "high_risk_counterparties_12m": self.high_risk_counterparties_12m,
            "counterparty_stats": [c.to_dict() for c in self.counterparty_stats],
            "country_frequency": self.country_frequency,
            "peer_group_deviation": self.peer_group_deviation,
            "suspicious_keyword_hit": self.suspicious_keyword_hit,
            "high_risk_country_hit": self.high_risk_country_hit,
            "risky_bank_hit": self.risky_bank_hit,
            "customer_last_12m_stats": self.customer_last_12m_stats.to_dict() if self.customer_last_12m_stats else None,
        }


# ---------------------------------------------------------------------------
# Counterparty profile
# ---------------------------------------------------------------------------

@dataclass
class CounterpartyProfile:
    type: str                        # private | business
    risk_signals: list[str] = field(default_factory=list)
    known_relationship: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "risk_signals": self.risk_signals,
            "known_relationship": self.known_relationship,
        }


# ---------------------------------------------------------------------------
# Investigation context
# ---------------------------------------------------------------------------

@dataclass
class InvestigationContext:
    case_id: str
    investigation_status: str         # open | in_review | closed
    analyst_decision: str | None      # SAR | NO_SAR | ESCALATE | null
    sar_recommended: bool
    sar_reason: str
    source_of_funds_summary: str
    customer_statement: str
    business_purpose: str
    investigation_notes: str
    rule_name_en: str
    rule_name_de: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "investigation_status": self.investigation_status,
            "analyst_decision": self.analyst_decision,
            "sar_recommended": self.sar_recommended,
            "sar_reason": self.sar_reason,
            "source_of_funds_summary": self.source_of_funds_summary,
            "customer_statement": self.customer_statement,
            "business_purpose": self.business_purpose,
            "investigation_notes": self.investigation_notes,
            "rule_name_en": self.rule_name_en,
            "rule_name_de": self.rule_name_de,
        }


# ---------------------------------------------------------------------------
# Attachments
# ---------------------------------------------------------------------------

@dataclass
class Attachment:
    type: str          # invoice | screenshot | document
    description: str
    url: str

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "description": self.description, "url": self.url}


# ---------------------------------------------------------------------------
# Account summary
# ---------------------------------------------------------------------------

@dataclass
class AccountSummary:
    account_id: str
    balance: float
    currency: str
    account_type: str   # checking | savings | business
    opened_at: str      # YYYY-MM-DD
    status: str         # active | blocked | closed

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "balance": self.balance,
            "currency": self.currency,
            "account_type": self.account_type,
            "opened_at": self.opened_at,
            "status": self.status,
        }


# ---------------------------------------------------------------------------
# Top-level alert
# ---------------------------------------------------------------------------

@dataclass
class Alert:
    alert_id: str
    created_at: str
    status: str                      # open | in_review | closed
    risk_score: float
    requires_sar: bool

    primary_rule_id: str
    alert_reason_summary: str

    rules_triggered: list[RuleTriggered]
    customer_profile: CustomerProfile
    trigger_transactions: list[TriggerTransaction]
    transaction_history: list[HistoryTransaction]
    behavior_stats: BehaviorStats
    counterparty_profile: CounterpartyProfile
    investigation_context: InvestigationContext
    attachments: list[Attachment]
    account_summaries: list[AccountSummary]

    def to_dict(self) -> dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "created_at": self.created_at,
            "status": self.status,
            "risk_score": self.risk_score,
            "requires_sar": self.requires_sar,
            "primary_rule_id": self.primary_rule_id,
            "alert_reason_summary": self.alert_reason_summary,
            "rules_triggered": [r.to_dict() for r in self.rules_triggered],
            "customer_profile": self.customer_profile.to_dict(),
            "trigger_transactions": [t.to_dict() for t in self.trigger_transactions],
            "transaction_history": [t.to_dict() for t in self.transaction_history],
            "behavior_stats": self.behavior_stats.to_dict(),
            "counterparty_profile": self.counterparty_profile.to_dict(),
            "investigation_context": self.investigation_context.to_dict(),
            "attachments": [a.to_dict() for a in self.attachments],
            "account_summaries": [a.to_dict() for a in self.account_summaries],
        }
