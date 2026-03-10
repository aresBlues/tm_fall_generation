"""Data models for TM Fallbearbeitung alerts (JSON-serializable)."""
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Address:
    street: str
    city: str
    country: str

    def to_dict(self) -> dict[str, Any]:
        return {"street": self.street, "city": self.city, "country": self.country}


@dataclass
class IdDocument:
    type: str
    number: str

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "number": self.number}


@dataclass
class KYC:
    customer_id: str
    full_name: str
    date_of_birth: str
    address: Address
    id_document: IdDocument
    risk_rating: str
    pep_flag: bool
    sanctions_flag: bool
    customer_since: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "customer_id": self.customer_id,
            "full_name": self.full_name,
            "date_of_birth": self.date_of_birth,
            "address": self.address.to_dict(),
            "id_document": self.id_document.to_dict(),
            "risk_rating": self.risk_rating,
            "pep_flag": self.pep_flag,
            "sanctions_flag": self.sanctions_flag,
            "customer_since": self.customer_since,
        }


@dataclass
class AccountSummary:
    account_id: str
    customer_id: str
    balance: float
    currency: str
    account_type: str
    opened_at: str
    status: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "customer_id": self.customer_id,
            "balance": self.balance,
            "currency": self.currency,
            "account_type": self.account_type,
            "opened_at": self.opened_at,
            "status": self.status,
        }


@dataclass
class Transaction:
    transaction_id: str
    date: str
    amount: float
    currency: str
    counterparty_name: str
    counterparty_iban: str
    type: str
    direction: str
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "date": self.date,
            "amount": self.amount,
            "currency": self.currency,
            "counterparty_name": self.counterparty_name,
            "counterparty_iban": self.counterparty_iban,
            "type": self.type,
            "direction": self.direction,
            "description": self.description,
        }


@dataclass
class Alert:
    alert_id: str
    type: str
    status: str
    created_at: str
    risk_score: float
    customer_id: str
    account_id: str
    requires_sar: bool
    kyc: KYC
    alerted_transactions: list[Transaction]
    transaction_history: list[Transaction]
    account_summaries: list[AccountSummary]

    def to_dict(self) -> dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "type": self.type,
            "status": self.status,
            "created_at": self.created_at,
            "risk_score": self.risk_score,
            "customer_id": self.customer_id,
            "account_id": self.account_id,
            "requires_sar": self.requires_sar,
            "kyc": self.kyc.to_dict(),
            "alerted_transactions": [t.to_dict() for t in self.alerted_transactions],
            "transaction_history": [t.to_dict() for t in self.transaction_history],
            "account_summaries": [a.to_dict() for a in self.account_summaries],
        }
