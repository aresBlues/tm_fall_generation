"""Generators for synthetic TM Fallbearbeitung alerts.

Produces alerts conforming to the 132-field schema in alerts_de_schema.xlsx.
Transaction history spans 12 months; trigger transactions and alert.created_at
fall within a single 1-month alert window.  Running account balance is maintained
(always >= 0) and stamped on trigger transactions.
"""
from __future__ import annotations

import random
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, NamedTuple

from faker import Faker


def _id_expiry_plus_years(issued: date, years: int = 10) -> date:
    """ID document expiry = issued + `years`. Safe when issued is Feb 29 and target year is not a leap year."""
    try:
        return issued.replace(year=issued.year + years)
    except ValueError:
        return issued.replace(year=issued.year + years, day=28)


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
#SEED = 42
#random.seed(SEED)
fake = Faker("de_DE")
#Faker.seed(SEED)

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

# Coherent triples (type, rail, channel): every transaction uses one row so rail matches type and channel.
_COHERENT_INBOUND: list[tuple[str, str, str]] = [
    ("transfer", "SEPA_CT", "online_banking"),
    ("transfer", "SEPA_CT", "mobile"),
    ("transfer", "SEPA_INST", "online_banking"),
    ("transfer", "SEPA_INST", "mobile"),
    ("card", "CARD_POS", "card_terminal"),
    ("card", "CARD_POS", "mobile"),
    ("cash", "CASH_DEPOSIT", "atm"),
    ("wire", "SWIFT", "online_banking"),
    ("wire", "SWIFT", "mobile"),
]
_COHERENT_OUTBOUND: list[tuple[str, str, str]] = [
    ("transfer", "SEPA_CT", "online_banking"),
    ("transfer", "SEPA_CT", "mobile"),
    ("transfer", "SEPA_INST", "online_banking"),
    ("transfer", "SEPA_INST", "mobile"),
    ("card", "CARD_POS", "card_terminal"),
    ("card", "CARD_POS", "mobile"),
    ("cash", "ATM_WITHDRAWAL", "atm"),
    ("wire", "SWIFT", "online_banking"),
    ("wire", "SWIFT", "mobile"),
]
_COHERENT_UNION: list[tuple[str, str, str]] = list(
    dict.fromkeys([*_COHERENT_INBOUND, *_COHERENT_OUTBOUND]),
)

ACCOUNT_CURRENCY = "EUR"
ID_DOC_TYPES = ["passport", "national_id", "residence_permit"]
KYC_STATUSES = ["VERIFIED", "PENDING", "REJECTED"]
EMPLOYMENT_STATUSES = ["EMPLOYED", "SELF_EMPLOYED", "UNEMPLOYED", "STUDENT", "RETIRED"]
INDUSTRIES = [
    "Finance", "Technology", "Healthcare", "Retail", "Manufacturing",
    "Real Estate", "Consulting", "Legal Services", "Import/Export", "Gastronomy",
]

# Industries plausible for a given employment (synthetic customers only; public figures keep Excel-driven industry).
_INCOMPATIBLE_INDUSTRY_EMPLOYMENT: dict[str, frozenset[str]] = {
    "STUDENT": frozenset({"Finance", "Legal Services", "Consulting", "Real Estate"}),
    "UNEMPLOYED": frozenset({"Finance", "Legal Services", "Consulting"}),
}


def _industries_for_employment(employment: str) -> list[str]:
    blocked = _INCOMPATIBLE_INDUSTRY_EMPLOYMENT.get(employment, frozenset())
    pool = [i for i in INDUSTRIES if i not in blocked]
    return pool if pool else list(INDUSTRIES)


_STUDENT_INCOME_TURNOVER_CAP = 3000.0
_HIGH_PAY_INDUSTRY = frozenset({"Finance", "Legal Services", "Consulting", "Real Estate", "Technology"})
_LOW_PAY_INDUSTRY = frozenset({"Retail", "Gastronomy"})


def _industry_income_multiplier(industry: str) -> float:
    if industry in _HIGH_PAY_INDUSTRY:
        return 1.28
    if industry in _LOW_PAY_INDUSTRY:
        return 0.78
    return 1.0


def _monthly_income_and_turnover(employment_status: str, industry: str) -> tuple[float, float]:
    """Coherent expected_monthly_income and expected_monthly_turnover (EUR) from employment + industry."""
    m = _industry_income_multiplier(industry)

    if employment_status == "STUDENT":
        inc = round(random.uniform(200.0, _STUDENT_INCOME_TURNOVER_CAP), 0)
        inc = min(inc, _STUDENT_INCOME_TURNOVER_CAP)
        to = round(min(_STUDENT_INCOME_TURNOVER_CAP, inc * random.uniform(0.85, 2.2)), 0)
        return float(min(inc, _STUDENT_INCOME_TURNOVER_CAP)), float(min(to, _STUDENT_INCOME_TURNOVER_CAP))

    if employment_status == "UNEMPLOYED":
        inc = round(random.uniform(0.0, 2200.0 * m), 0)
        to = round(min(4500.0 * m, inc * random.uniform(1.0, 2.4)), 0)
        return float(inc), float(to)

    if employment_status == "RETIRED":
        inc = round(random.uniform(900.0 * m, 6500.0 * m), 0)
        to = round(inc * random.uniform(0.85, 1.35), 0)
        return float(inc), float(to)

    if employment_status == "SELF_EMPLOYED":
        inc = round(random.uniform(2000.0 * m, 32000.0 * m), 0)
        to = round(inc * random.uniform(1.05, 3.2), 0)
        return float(inc), float(to)

    # EMPLOYED
    inc = round(random.uniform(2400.0 * m, 20000.0 * m), 0)
    to = round(inc * random.uniform(0.82, 2.45), 0)
    return float(inc), float(to)


ACCOUNT_PURPOSES = [
    "daily expenses", "salary account", "business operations",
    "savings", "investment", "international transfers",
]

# Consumer mail domains — GMX weighted higher than Yahoo / Gmail / Hotmail.
_CUSTOMER_EMAIL_DOMAIN_WEIGHTS: tuple[tuple[str, int], ...] = (
    ("gmx.de", 35),
    ("gmx.net", 25),
    ("yahoo.de", 12),
    ("gmail.com", 14),
    ("hotmail.com", 14),
)
_CUSTOMER_EMAIL_DOMAINS = [d for d, w in _CUSTOMER_EMAIL_DOMAIN_WEIGHTS for _ in range(w)]

HIGH_RISK_COUNTRIES = {"IR", "KP", "SY", "AF", "YE", "MM", "LY", "SO"}


def _normalize_german_substring(s: str) -> str:
    s = s.lower().strip()
    return s.replace("ä", "a").replace("ö", "o").replace("ü", "u").replace("ß", "ss")


def _bekannt_durch_implies_pep(text: str | None) -> bool:
    """True if Bekannt_durch (column E) indicates a politician / comparable PEP role."""
    if not text or not str(text).strip():
        return False
    t = _normalize_german_substring(str(text).strip())
    markers = (
        "politiker",
        "abgeordnet",
        "minister",
        "bundeskanzler",
        "kanzler",
        "regierung",
        "burgermeister",
        "diplomat",
        "europaabgeordnet",
        "landtagsabgeordnet",
        "bundestag",
        "senator",
        "mdb",
        "mdl",
    )
    return any(m in t for m in markers)


def _industry_from_bekannt_durch(text: str | None) -> str:
    """Map Bekannt_durch (column E) to an English industry label for customer_profile.industry."""
    if not text or not str(text).strip():
        return "Professional Services"
    if _bekannt_durch_implies_pep(text):
        return "Government & Public Sector"
    t = _normalize_german_substring(str(text).strip())
    if any(
        x in t
        for x in (
            "schauspieler",
            "filmschauspieler",
            "filmregisseur",
            "filmemacher",
            "fernsehmoderator",
        )
    ):
        return "Film & Television"
    if any(x in t for x in ("schriftsteller", "journalist")):
        return "Publishing & Media"
    if any(
        x in t
        for x in (
            "fussball",
            "tennis",
            "basketball",
            "handball",
            "hockey",
            "sportler",
            "athlet",
            "schwimmer",
            "bob",
            "ski",
            "ruder",
            "kanu",
            "judoka",
            "biathlet",
            "radrenn",
            "boxer",
            "leichtathlet",
            "badminton",
            "eishockey",
            "rollstuhl",
            "schach",
            "triathlet",
            "turmspringer",
            "gerateturner",
            "bahnrad",
        )
    ) or ("spieler" in t and "schauspieler" not in t and "filmschauspieler" not in t):
        return "Sports & Recreation"
    if any(x in t for x in ("sanger", "musiker", "rapper", "schlager", "discjockey", "arrangeur")):
        return "Music & Entertainment"
    if "model" in t:
        return "Fashion & Retail"
    if "youtuber" in t:
        return "Digital Media"
    if any(x in t for x in ("fotograf", "bildhauer", "druckgrafiker")):
        return "Arts & Creative Industries"
    if any(x in t for x in ("dozent", "hochschullehrer")):
        return "Education & Research"
    if "berufssoldat" in t:
        return "Defense & Security"
    return "Media & Entertainment"


# Source: input/verified_public_figures_extended.xlsx (Name, Geburtsdatum, oeffentlicher Wohnort, Bekannt_durch / Spalte E, industry)
VERIFIED_PUBLIC_FIGURES: list[tuple[str, str, str, str | None, str | None, str | None, str]] = [
    ('Sandra', 'Bullock', 'Sandra Bullock', '1964-07-26', 'Austin', 'Schauspieler', 'Film & Television'),
    ('Herta', 'Müller', 'Herta Müller', '1953-08-17', 'Berlin', 'Schriftsteller', 'Publishing & Media'),
    ('Kirsten', 'Dunst', 'Kirsten Dunst', '1982-04-30', 'Los Angeles', 'Schauspieler', 'Film & Television'),
    ('Manuel', 'Neuer', 'Manuel Neuer', '1986-03-27', 'Gelsenkirchen', 'Fußballspieler', 'Sports & Recreation'),
    ('Steffi', 'Graf', 'Steffi Graf', '1969-06-14', 'Las Vegas Valley', 'Tennisspieler', 'Sports & Recreation'),
    ('Christoph', 'Waltz', 'Christoph Waltz', '1956-10-04', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Boris', 'Becker', 'Boris Becker', '1967-11-22', 'München', 'Tennisspieler', 'Sports & Recreation'),
    ('Heidi', 'Klum', 'Heidi Klum', '1973-06-01', 'Bel Air', 'Model', 'Fashion & Retail'),
    ('Werner', 'Herzog', 'Werner Herzog', '1942-09-05', 'Los Angeles', 'Filmregisseur', 'Film & Television'),
    ('Angelique', 'Kerber', 'Angelique Kerber', '1988-01-18', 'Kiel', 'Tennisspieler', 'Sports & Recreation'),
    ('Roland', 'Emmerich', 'Roland Emmerich', '1955-11-10', 'London', 'Schriftsteller', 'Publishing & Media'),
    ('Dirk', 'Nowitzki', 'Dirk Nowitzki', '1978-06-19', 'Dallas', 'Basketballspieler', 'Sports & Recreation'),
    ('Daniel', 'Brühl', 'Daniel Brühl', '1978-06-16', 'Berlin', 'Schriftsteller', 'Publishing & Media'),
    ('Pierre', 'Littbarski', 'Pierre Littbarski', '1960-04-16', 'Berlin', 'Fußballspieler', 'Sports & Recreation'),
    ('Claudio', 'Pizarro', 'Claudio Pizarro', '1978-10-03', 'Callao', 'Fußballspieler', 'Sports & Recreation'),
    ('Lewon', 'Aronjan', 'Lewon Aronjan', '1982-10-06', 'Jerewan', 'Schachspieler', 'Sports & Recreation'),
    ('Dieter', 'Bohlen', 'Dieter Bohlen', '1954-02-07', 'Tötensen', 'Sänger', 'Music & Entertainment'),
    ('Til', 'Schweiger', 'Til Schweiger', '1963-12-19', 'Palma', 'Schauspieler', 'Film & Television'),
    ('Hanna', 'Schygulla', 'Hanna Schygulla', '1943-12-25', 'Chorzów', 'Sänger', 'Music & Entertainment'),
    ('Helene', 'Fischer', 'Helene Fischer', '1983-08-05', 'Inning am Ammersee', 'Schlagersänger', 'Music & Entertainment'),
    ('Jawed', 'Karim', 'Jawed Karim', '1979-10-28', 'Palo Alto', 'YouTuber', 'Digital Media'),
    ('Nena', '', 'Nena', '1960-03-24', 'Breckerfeld', 'Sänger', 'Music & Entertainment'),
    ('Sabine', 'Lisicki', 'Sabine Lisicki', '1989-09-22', 'Bradenton', 'Tennisspieler', 'Sports & Recreation'),
    ('Sibel', 'Kekilli', 'Sibel Kekilli', '1980-06-16', 'Hamburg', 'Schauspieler', 'Film & Television'),
    ('Alexander', 'Zverev', 'Alexander Zverev', '1997-04-20', 'Monte-Carlo', 'Tennisspieler', 'Sports & Recreation'),
    ('Magdalena', 'Neuner', 'Magdalena Neuner', '1987-02-09', 'Wallgau', 'Biathlet', 'Sports & Recreation'),
    ('Stefan', 'Raab', 'Stefan Raab', '1966-10-20', 'Hahnwald', 'Musiker', 'Music & Entertainment'),
    ('Wolfgang', 'Overath', 'Wolfgang Overath', '1943-09-29', 'Seligenthal', 'Fußballspieler', 'Sports & Recreation'),
    ('Thomas', 'Anders', 'Thomas Anders', '1963-03-01', 'Koblenz', 'Sänger', 'Music & Entertainment'),
    ('Peter', 'Sloterdijk', 'Peter Sloterdijk', '1947-06-26', 'Karlsruhe', 'Schriftsteller', 'Publishing & Media'),
    ('Tommy', 'Haas', 'Tommy Haas', '1978-04-03', 'Bradenton', 'Tennisspieler', 'Sports & Recreation'),
    ('You', 'Xie', 'You Xie', '1958-10-01', 'Bamberg', 'Journalist', 'Publishing & Media'),
    ('Alexandra', 'Maria Lara', 'Alexandra Maria Lara', '1978-11-12', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Jürgen', 'Prochnow', 'Jürgen Prochnow', '1941-06-10', 'Los Angeles', 'Musiker', 'Music & Entertainment'),
    ('Andrea', 'Petković', 'Andrea Petković', '1987-09-09', 'Darmstadt', 'Tennisspieler', 'Sports & Recreation'),
    ('Michael', 'Stich', 'Michael Stich', '1968-10-18', 'Hamburg', 'Tennisspieler', 'Sports & Recreation'),
    ('Bill', 'Kaulitz', 'Bill Kaulitz', '1989-09-01', 'Los Angeles', 'Sänger', 'Music & Entertainment'),
    ('Julia', 'Görges', 'Julia Görges', '1988-11-02', 'Bad Oldesloe', 'Tennisspieler', 'Sports & Recreation'),
    ('Wolf', 'Biermann', 'Wolf Biermann', '1936-11-15', 'Hamburg', 'Schriftsteller', 'Publishing & Media'),
    ('Annika', 'Beck', 'Annika Beck', '1994-02-16', 'Bonn', 'Tennisspieler', 'Sports & Recreation'),
    ('Maximilian', 'Mittelstädt', 'Maximilian Mittelstädt', '1997-03-18', 'Stuttgart', 'Fußballspieler', 'Sports & Recreation'),
    ('Kim', 'Petras', 'Kim Petras', '1992-08-27', 'Los Angeles', 'Sänger', 'Music & Entertainment'),
    ('Marcel', 'Kittel', 'Marcel Kittel', '1988-05-11', 'Twente', 'Radrennfahrer', 'Sports & Recreation'),
    ('Moritz', 'Bleibtreu', 'Moritz Bleibtreu', '1971-08-13', 'Hamburg', 'Filmschauspieler', 'Film & Television'),
    ('Nicolas', 'Kiefer', 'Nicolas Kiefer', '1977-07-05', 'Lehrte', 'Tennisspieler', 'Sports & Recreation'),
    ('Natalie', 'Horler', 'Natalie Horler', '1981-09-23', 'Bonn', 'Model', 'Fashion & Retail'),
    ('Zazie', 'Beetz', 'Zazie Beetz', '1991-06-01', 'Harlem', 'Schauspieler', 'Film & Television'),
    ('Mona', 'Barthel', 'Mona Barthel', '1990-07-11', 'Neumünster', 'Tennisspieler', 'Sports & Recreation'),
    ('Uwe', 'Boll', 'Uwe Boll', '1965-06-22', 'Vancouver', 'Filmregisseur', 'Film & Television'),
    ('Betty', 'Heidler', 'Betty Heidler', '1983-10-14', 'Frankfurt am Main', 'Leichtathlet', 'Sports & Recreation'),
    ('Christiane', 'Felscherinow', 'Christiane Felscherinow', '1962-05-20', 'Berlin', 'Sänger', 'Music & Entertainment'),
    ('Selma', 'Ergeç', 'Selma Ergeç', '1978-11-01', 'Istanbul', 'Model', 'Fashion & Retail'),
    ('Anke', 'Huber', 'Anke Huber', '1974-12-04', 'Ludwigshafen am Rhein', 'Tennisspieler', 'Sports & Recreation'),
    ('David', 'Kross', 'David Kross', '1990-07-04', 'Berlin-Mitte', 'Filmschauspieler', 'Film & Television'),
    ('Władysław', 'Kozakiewicz', 'Władysław Kozakiewicz', '1953-12-08', 'Bissendorf', 'Leichtathlet', 'Sports & Recreation'),
    ('Anna-Lena', 'Grönefeld', 'Anna-Lena Grönefeld', '1985-06-04', 'Saarbrücken', 'Tennisspieler', 'Sports & Recreation'),
    ('Florence', 'Kasumba', 'Florence Kasumba', '1976-10-26', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Florian', 'Mayer', 'Florian Mayer', '1983-10-05', 'Bayreuth', 'Tennisspieler', 'Sports & Recreation'),
    ('Martina', 'Gedeck', 'Martina Gedeck', '1961-09-14', 'Berlin', 'Musiker', 'Music & Entertainment'),
    ('Tom', 'Kaulitz', 'Tom Kaulitz', '1989-09-01', 'Bel Air', 'Schauspieler', 'Film & Television'),
    ('Janosch', '', 'Janosch', '1931-03-11', 'Teneriffa', 'Schriftsteller', 'Publishing & Media'),
    ('Levin', 'Öztunali', 'Levin Öztunali', '1996-03-15', 'Hamburg', 'Fußballspieler', 'Sports & Recreation'),
    ('Ricco', 'Groß', 'Ricco Groß', '1970-08-22', 'Ruhpolding', 'Biathlet', 'Sports & Recreation'),
    ('Benjamin', 'Becker', 'Benjamin Becker', '1981-06-16', 'Dallas', 'Tennisspieler', 'Sports & Recreation'),
    ('Evelyn', 'Sharma', 'Evelyn Sharma', '1989-07-12', 'Toronto', 'Model', 'Fashion & Retail'),
    ('Nora', 'Tschirner', 'Nora Tschirner', '1981-06-12', 'Berlin', 'Sänger', 'Music & Entertainment'),
    ('Elyas', 'M’Barek', 'Elyas M’Barek', '1982-05-29', 'München', 'Schauspieler', 'Film & Television'),
    ('Udo', 'Lindenberg', 'Udo Lindenberg', '1946-05-17', 'Hotel Atlantic', 'Sänger', 'Music & Entertainment'),
    ('Boris', 'Kodjoe', 'Boris Kodjoe', '1973-03-08', 'Los Angeles', 'Model', 'Fashion & Retail'),
    ('Bushido', '', 'Bushido', '1978-09-28', 'Bezirk Mitte', 'Rapper', 'Music & Entertainment'),
    ('Erik', 'Möller', 'Erik Möller', '1979-01-01', 'San Francisco Bay Area', 'Journalist', 'Publishing & Media'),
    ('Johannes', 'Vetter', 'Johannes Vetter', '1993-03-26', 'Kenzingen', 'Leichtathlet', 'Sports & Recreation'),
    ('Mischa', 'Zverev', 'Mischa Zverev', '1987-08-22', 'Monte-Carlo', 'Tennisspieler', 'Sports & Recreation'),
    ('Sebastian', 'Brendel', 'Sebastian Brendel', '1988-03-12', 'Potsdam', 'Kanute', 'Sports & Recreation'),
    ('Uschi', 'Disl', 'Uschi Disl', '1970-11-15', 'Kössen', 'Biathlet', 'Sports & Recreation'),
    ('Xavier', 'Naidoo', 'Xavier Naidoo', '1971-10-02', 'Heidelberg', 'Rapper', 'Music & Entertainment'),
    ('Andreas', 'Krieger', 'Andreas Krieger', '1965-07-20', 'Berlin', 'Leichtathlet', 'Sports & Recreation'),
    ('Ayọ', '', 'Ayọ', '1980-09-14', 'New York City', 'Sänger', 'Music & Entertainment'),
    ('H.P.', 'Baxxter', 'H.P. Baxxter', '1964-03-16', 'Duvenstedt', 'Sänger', 'Music & Entertainment'),
    ('Jan-Lennard', 'Struff', 'Jan-Lennard Struff', '1990-04-25', 'Warstein', 'Tennisspieler', 'Sports & Recreation'),
    ('Marian', 'Gold', 'Marian Gold', '1954-05-26', 'Münster', 'Sänger', 'Music & Entertainment'),
    ('Martina', 'Beck', 'Martina Beck', '1979-09-21', 'Mittenwald', 'Biathlet', 'Sports & Recreation'),
    ('Stephan', 'Schröck', 'Stephan Schröck', '1986-08-21', 'Schweinfurt', 'Fußballspieler', 'Sports & Recreation'),
    ('Thomas', 'Gottschalk', 'Thomas Gottschalk', '1950-05-18', 'Baden-Baden', 'Fernsehmoderator', 'Film & Television'),
    ('Andreas', 'Birnbacher', 'Andreas Birnbacher', '1981-09-11', 'Schleching', 'Biathlet', 'Sports & Recreation'),
    ('Carina', 'Witthöft', 'Carina Witthöft', '1995-02-16', 'Hamburg', 'Tennisspieler', 'Sports & Recreation'),
    ('Dustin', 'Brown', 'Dustin Brown', '1984-12-08', 'Montego Bay', 'Tennisspieler', 'Sports & Recreation'),
    ('Florian', 'Wellbrock', 'Florian Wellbrock', '1997-08-19', 'Magdeburg', 'Schwimmer', 'Sports & Recreation'),
    ('Gojko', 'Mitić', 'Gojko Mitić', '1940-06-13', 'Leskovac', 'Schauspieler', 'Film & Television'),
    ('Gréta', 'Arn', 'Gréta Arn', '1979-04-13', 'Budapest', 'Tennisspieler', 'Sports & Recreation'),
    ('Josefa', 'Idem', 'Josefa Idem', '1964-09-23', 'Santerno', 'Kanute', 'Sports & Recreation'),
    ('Karin', 'Schubert', 'Karin Schubert', '1944-11-26', 'Manziana', 'Model', 'Fashion & Retail'),
    ('Marcel', 'Nguyen', 'Marcel Nguyen', '1987-09-08', 'Stuttgart', 'Geräteturner', 'Sports & Recreation'),
    ('Philipp', 'Petzschner', 'Philipp Petzschner', '1984-03-24', 'Pulheim', 'Tennisspieler', 'Sports & Recreation'),
    ('André', 'Lange', 'André Lange', '1973-06-28', 'Suhl', 'Bobfahrer', 'Sports & Recreation'),
    ('Florian', 'Munteanu', 'Florian Munteanu', '1990-10-13', 'Los Angeles', 'Schauspieler', 'Film & Television'),
    ('Francesco', 'Friedrich', 'Francesco Friedrich', '1990-05-02', 'Pirna', 'Bobfahrer', 'Sports & Recreation'),
    ('Jan', 'Frodeno', 'Jan Frodeno', '1981-08-18', 'Andorra', 'Triathlet', 'Sports & Recreation'),
    ('Jonas', 'Deichmann', 'Jonas Deichmann', '1987-04-15', 'Kanton Solothurn', 'Triathlet', 'Sports & Recreation'),
    ('Judith', 'Hermann', 'Judith Hermann', '1970-05-15', 'Berlin', 'Schriftsteller', 'Publishing & Media'),
    ('Judith', 'Rakers', 'Judith Rakers', '1976-01-06', 'Hamburg', 'Fernsehmoderator', 'Film & Television'),
    ('Kristina', 'Vogel', 'Kristina Vogel', '1990-11-10', 'Erfurt', 'Bahnradfahrer', 'Sports & Recreation'),
    ('Richard', 'David Precht', 'Richard David Precht', '1964-12-08', 'Düsseldorf', 'Journalist', 'Publishing & Media'),
    ('Evi', 'Sachenbacher-Stehle', 'Evi Sachenbacher-Stehle', '1980-11-27', 'Reit im Winkl', 'Biathlet', 'Sports & Recreation'),
    ('Hany', 'Mukhtar', 'Hany Mukhtar', '1995-03-21', 'Berlin', 'Fußballspieler', 'Sports & Recreation'),
    ('Hazel', 'Brugger', 'Hazel Brugger', '1993-12-09', 'Köln', 'Schauspieler', 'Film & Television'),
    ('Johannes', 'Rydzek', 'Johannes Rydzek', '1991-12-09', 'Oberstdorf', 'Skispringer', 'Sports & Recreation'),
    ('Jordan', 'Torunarigha', 'Jordan Torunarigha', '1997-08-07', 'Berlin', 'Fußballspieler', 'Sports & Recreation'),
    ('Björn', 'Phau', 'Björn Phau', '1979-10-04', 'Darmstadt', 'Tennisspieler', 'Sports & Recreation'),
    ('David', 'Prinosil', 'David Prinosil', '1973-03-09', 'Prag', 'Tennisspieler', 'Sports & Recreation'),
    ('Jannik', 'Schümann', 'Jannik Schümann', '1992-07-23', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Jessica', 'Schwarz', 'Jessica Schwarz', '1977-05-05', 'Berlin', 'Model', 'Fashion & Retail'),
    ('Josefine', 'Preuß', 'Josefine Preuß', '1986-01-13', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Judith', 'Schalansky', 'Judith Schalansky', '1980-09-20', 'Berlin', 'Schriftsteller', 'Publishing & Media'),
    ('Kerstin', 'Garefrekes', 'Kerstin Garefrekes', '1979-09-04', 'Steinbeck', 'Fußballspieler', 'Sports & Recreation'),
    ('Kevin', 'Kuske', 'Kevin Kuske', '1979-01-04', 'Potsdam', 'Bobfahrer', 'Sports & Recreation'),
    ('Lisa', 'Buckwitz', 'Lisa Buckwitz', '1994-12-02', 'Schöneiche bei Berlin', 'Bobfahrer', 'Sports & Recreation'),
    ('Rebekka', 'Haase', 'Rebekka Haase', '1993-01-02', 'Chemnitz', 'Leichtathlet', 'Sports & Recreation'),
    ('Shermine', 'Shahrivar', 'Shermine Shahrivar', '1982-11-20', 'New York City', 'Model', 'Fashion & Retail'),
    ('Andrea', 'Berg', 'Andrea Berg', '1966-01-28', 'Aspach', 'Sänger', 'Music & Entertainment'),
    ('Christoph', 'Brüx', 'Christoph Brüx', '1965-12-13', 'Hamburg', 'Arrangeur', 'Music & Entertainment'),
    ('Julia', 'Taubitz', 'Julia Taubitz', '1996-03-01', 'Annaberg-Buchholz', 'Berufssoldat', 'Defense & Security'),
    ('Kathrin', 'Schmidt', 'Kathrin Schmidt', '1958-03-12', 'Mahlsdorf', 'Journalist', 'Publishing & Media'),
    ('Martina', 'Müller', 'Martina Müller', '1982-10-11', 'Sehnde', 'Tennisspieler', 'Sports & Recreation'),
    ('Otto', 'Waalkes', 'Otto Waalkes', '1948-07-22', 'Blankenese', 'Sänger', 'Music & Entertainment'),
    ('Robert', 'Bartko', 'Robert Bartko', '1975-12-23', 'Ludwigsfelde', 'Radrennfahrer', 'Sports & Recreation'),
    ('Stefan', 'Kapičić', 'Stefan Kapičić', '1978-12-01', 'Belgrad', 'Schauspieler', 'Film & Television'),
    ('Vincent', 'Keymer', 'Vincent Keymer', '2004-11-15', 'Wien', 'Schachspieler', 'Sports & Recreation'),
    ('Andreas', 'Beck', 'Andreas Beck', '1986-02-05', 'Ravensburg', 'Tennisspieler', 'Sports & Recreation'),
    ('Andreas', 'Pietschmann', 'Andreas Pietschmann', '1969-03-22', 'Berlin', 'Filmschauspieler', 'Film & Television'),
    ('Barbara', 'Rittner', 'Barbara Rittner', '1973-04-25', 'Köln', 'Tennisspieler', 'Sports & Recreation'),
    ('Christopher', 'Kas', 'Christopher Kas', '1980-06-13', 'Trostberg', 'Tennisspieler', 'Sports & Recreation'),
    ('Jil', 'Sander', 'Jil Sander', '1943-11-27', 'Hamburg', 'Journalist', 'Publishing & Media'),
    ('Julia', 'Schruff', 'Julia Schruff', '1982-08-16', 'Augsburg', 'Tennisspieler', 'Sports & Recreation'),
    ('Jurij', 'Koch', 'Jurij Koch', '1936-09-15', 'Sielow', 'Journalist', 'Publishing & Media'),
    ('Lena', 'Gercke', 'Lena Gercke', '1988-02-29', 'Cloppenburg', 'Model', 'Fashion & Retail'),
    ('Matthias', 'Bachinger', 'Matthias Bachinger', '1987-04-02', 'Hebertshausen', 'Tennisspieler', 'Sports & Recreation'),
    ('Michael', 'Berrer', 'Michael Berrer', '1980-07-01', 'Stuttgart', 'Tennisspieler', 'Sports & Recreation'),
    ('Norbert', 'Haug', 'Norbert Haug', '1952-11-24', 'Stuttgart', 'Autorennfahrer', 'Media & Entertainment'),
    ('Peter', 'Gojowczyk', 'Peter Gojowczyk', '1989-07-15', 'Erdweg', 'Tennisspieler', 'Sports & Recreation'),
    ('Reinhard', 'Mey', 'Reinhard Mey', '1942-12-21', 'Berlin', 'Sänger', 'Music & Entertainment'),
    ('Simon', 'Greul', 'Simon Greul', '1981-04-13', 'Stuttgart', 'Tennisspieler', 'Sports & Recreation'),
    ('Tobias', 'Kamke', 'Tobias Kamke', '1986-05-21', 'Hamburg', 'Tennisspieler', 'Sports & Recreation'),
    ('Arthur', 'Abele', 'Arthur Abele', '1986-07-30', 'Ulm', 'Leichtathlet', 'Sports & Recreation'),
    ('Atiye', 'Deniz', 'Atiye Deniz', '1988-11-22', 'Istanbul', 'Sänger', 'Music & Entertainment'),
    ('Baran', 'bo Odar', 'Baran bo Odar', '1978-04-18', 'Berlin', 'Filmregisseur', 'Film & Television'),
    ('Carl-Uwe', 'Steeb', 'Carl-Uwe Steeb', '1967-09-01', 'Reith bei Kitzbühel', 'Tennisspieler', 'Sports & Recreation'),
    ('Daniel', 'Brands', 'Daniel Brands', '1987-07-17', 'Deggendorf', 'Tennisspieler', 'Sports & Recreation'),
    ('Denis', 'Gremelmayr', 'Denis Gremelmayr', '1981-08-16', 'Lampertheim', 'Tennisspieler', 'Sports & Recreation'),
    ('Jasmin', 'Glaesser', 'Jasmin Glaesser', '1992-07-08', 'Coquitlam', 'Radrennfahrer', 'Sports & Recreation'),
    ('Lukas', 'Dauser', 'Lukas Dauser', '1993-06-15', 'Berlin', 'Turner', 'Media & Entertainment'),
    ('Michael', 'Kohlmann', 'Michael Kohlmann', '1974-01-11', 'Herdecke', 'Tennisspieler', 'Sports & Recreation'),
    ('Natalia', 'Wörner', 'Natalia Wörner', '1967-09-07', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Oliver', 'Zeidler', 'Oliver Zeidler', '1996-07-24', 'Schwaig bei Nürnberg', 'Schwimmer', 'Sports & Recreation'),
    ('Peter', 'Schilling', 'Peter Schilling', '1956-01-28', 'München', 'Sänger', 'Music & Entertainment'),
    ('Ralf', 'Rothmann', 'Ralf Rothmann', '1953-05-10', 'Berlin', 'Schriftsteller', 'Publishing & Media'),
    ('Ruby', 'O. Fee', 'Ruby O. Fee', '1996-02-07', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Stipe', 'Erceg', 'Stipe Erceg', '1974-10-30', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Wilhelm', 'Bungert', 'Wilhelm Bungert', '1939-04-01', 'Düsseldorf', 'Tennisspieler', 'Sports & Recreation'),
    ('Alexander', 'Waske', 'Alexander Waske', '1975-03-31', 'Frankfurt am Main', 'Tennisspieler', 'Sports & Recreation'),
    ('Cedrik-Marcel', 'Stebe', 'Cedrik-Marcel Stebe', '1990-10-09', 'Vaihingen an der Enz', 'Tennisspieler', 'Sports & Recreation'),
    ('Charlotte', 'Link', 'Charlotte Link', '1963-10-05', 'Wiesbaden', 'Schriftsteller', 'Publishing & Media'),
    ('Dieter', 'Hallervorden', 'Dieter Hallervorden', '1935-09-05', 'Schloss Costaérès', 'Sänger', 'Music & Entertainment'),
    ('Helmut', 'Josef Geier', 'Helmut Josef Geier', '1962-09-06', 'München', 'Discjockey', 'Music & Entertainment'),
    ('Jasmin', 'Wöhr', 'Jasmin Wöhr', '1980-08-21', 'Balingen', 'Tennisspieler', 'Sports & Recreation'),
    ('Laura', 'Nolte', 'Laura Nolte', '1998-11-23', 'Dortmund', 'Bobfahrer', 'Sports & Recreation'),
    ('Maria', 'Simon', 'Maria Simon', '1976-02-06', 'Berlin', 'Filmschauspieler', 'Film & Television'),
    ('Steffen', 'Blochwitz', 'Steffen Blochwitz', '1967-09-08', 'Cottbus', 'Radrennfahrer', 'Sports & Recreation'),
    ('Timo', 'Bernhard', 'Timo Bernhard', '1981-02-24', 'Dittweiler', 'Autorennfahrer', 'Media & Entertainment'),
    ('Bassam', 'Tibi', 'Bassam Tibi', '1944-04-04', 'Göttingen', 'Schriftsteller', 'Publishing & Media'),
    ('Bettina', 'Bunge', 'Bettina Bunge', '1963-06-13', 'Monte-Carlo', 'Tennisspieler', 'Sports & Recreation'),
    ('Dinah', 'Pfizenmaier', 'Dinah Pfizenmaier', '1992-01-13', 'Kamen', 'Tennisspieler', 'Sports & Recreation'),
    ('Enrico', 'Kühn', 'Enrico Kühn', '1977-03-10', 'Bad Langensalza', 'Bobfahrer', 'Sports & Recreation'),
    ('Karsten', 'Braasch', 'Karsten Braasch', '1967-07-14', 'Ratingen', 'Tennisspieler', 'Sports & Recreation'),
    ('Martin', 'Emmrich', 'Martin Emmrich', '1984-12-17', 'Solingen', 'Tennisspieler', 'Sports & Recreation'),
    ('Maximilian', 'Marterer', 'Maximilian Marterer', '1995-06-15', 'Stein', 'Tennisspieler', 'Sports & Recreation'),
    ('Nadine', 'Horchler', 'Nadine Horchler', '1986-06-21', 'Mittenwald', 'Biathlet', 'Sports & Recreation'),
    ('Palina', 'Rojinski', 'Palina Rojinski', '1985-04-21', 'Berlin', 'Model', 'Fashion & Retail'),
    ('Rico', 'Freimuth', 'Rico Freimuth', '1988-03-14', 'Halle (Saale)', 'Leichtathlet', 'Sports & Recreation'),
    ('Sandra', 'Kiriasis', 'Sandra Kiriasis', '1975-01-04', 'Winterberg', 'Bobfahrer', 'Sports & Recreation'),
    ('Selina', 'Freitag', 'Selina Freitag', '2001-05-19', 'Breitenbrunn/Erzgebirge', 'Skispringer', 'Sports & Recreation'),
    ('Teresa', 'Orlowski', 'Teresa Orlowski', '1953-07-29', 'Marbella', 'Filmschauspieler', 'Film & Television'),
    ('Thomas', 'Florschütz', 'Thomas Florschütz', '1978-02-20', 'Erfurt', 'Bobfahrer', 'Sports & Recreation'),
    ('Wolfgang', 'Hohlbein', 'Wolfgang Hohlbein', '1953-08-15', 'Neuss', 'Schriftsteller', 'Publishing & Media'),
    ('Alexander', 'Wolf', 'Alexander Wolf', '1978-12-21', 'Herges-Hallenberg', 'Biathlet', 'Sports & Recreation'),
    ('André', 'Willms', 'André Willms', '1972-09-18', 'Magdeburg', 'Ruderer', 'Sports & Recreation'),
    ('Christoph', 'Stephan', 'Christoph Stephan', '1986-01-12', 'Oberhof', 'Biathlet', 'Sports & Recreation'),
    ('Daniel', 'Altmaier', 'Daniel Altmaier', '1998-09-12', 'Kempen', 'Tennisspieler', 'Sports & Recreation'),
    ('Dea', 'Loher', 'Dea Loher', '1964-04-20', 'Berlin', 'Schriftsteller', 'Publishing & Media'),
    ('Dominik', 'Graf', 'Dominik Graf', '1952-09-06', 'München', 'Schauspieler', 'Film & Television'),
    ('Giovanna', 'Scoccimarro', 'Giovanna Scoccimarro', '1997-11-10', 'Hannover', 'Judoka', 'Sports & Recreation'),
    ('Henryk', 'M. Broder', 'Henryk M. Broder', '1946-08-20', 'Israel', 'Journalist', 'Publishing & Media'),
    ('Jan', 'Josef Liefers', 'Jan Josef Liefers', '1964-08-08', 'Berlin', 'Sänger', 'Music & Entertainment'),
    ('Kathrin', 'Hitzer', 'Kathrin Hitzer', '1986-09-03', 'Ruhpolding', 'Biathlet', 'Sports & Recreation'),
    ('Markus', 'Zimmermann', 'Markus Zimmermann', '1964-09-04', 'Schönau am Königssee', 'Bobfahrer', 'Sports & Recreation'),
    ('Paula', 'Kalenberg', 'Paula Kalenberg', '1986-11-09', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('René', 'Hoppe', 'René Hoppe', '1976-12-09', 'Oberhof', 'Bobfahrer', 'Sports & Recreation'),
    ('Sara', 'Nuru', 'Sara Nuru', '1989-08-19', 'Erding', 'Model', 'Fashion & Retail'),
    ('Tomas', 'Behrend', 'Tomas Behrend', '1974-12-12', 'Alsdorf', 'Tennisspieler', 'Sports & Recreation'),
    ('Verona', 'Pooth', 'Verona Pooth', '1968-04-30', 'Dubai', 'Model', 'Fashion & Retail'),
    ('Anja', 'Silja', 'Anja Silja', '1935-04-17', 'Paris', 'Musiker', 'Music & Entertainment'),
    ('Axel', 'Prahl', 'Axel Prahl', '1960-03-26', 'Berlin', 'Musiker', 'Music & Entertainment'),
    ('Deborah', 'Levi', 'Deborah Levi', '1997-08-28', 'Frankfurt am Main', 'Bobfahrer', 'Sports & Recreation'),
    ('Doris', 'Schröder-Köpf', 'Doris Schröder-Köpf', '1963-08-05', 'Hannover', 'Journalist', 'Publishing & Media'),
    ('Florian', 'Bauer', 'Florian Bauer', '1994-02-11', 'München', 'Bobfahrer', 'Sports & Recreation'),
    ('Hans', 'Haacke', 'Hans Haacke', '1936-08-12', 'Westbeth Artists Community', 'Druckgrafiker', 'Arts & Creative Industries'),
    ('Lars', 'Burgsmüller', 'Lars Burgsmüller', '1975-12-06', 'Altstätten', 'Tennisspieler', 'Sports & Recreation'),
    ('Marlene', 'Weingärtner', 'Marlene Weingärtner', '1980-01-30', 'Ulm', 'Tennisspieler', 'Sports & Recreation'),
    ('Martin', 'Putze', 'Martin Putze', '1985-01-14', 'Bad Sulza', 'Bobfahrer', 'Sports & Recreation'),
    ('Micaela', 'Schäfer', 'Micaela Schäfer', '1983-11-01', 'Hellersdorf', 'Sänger', 'Music & Entertainment'),
    ('Philipp', 'Marx', 'Philipp Marx', '1982-02-03', 'Seeheim-Jugenheim', 'Tennisspieler', 'Sports & Recreation'),
    ('Rudolf', 'Martin', 'Rudolf Martin', '1967-07-31', 'Los Angeles', 'Schauspieler', 'Film & Television'),
    ('Suzanne', 'Bernert', 'Suzanne Bernert', '1982-09-26', 'Mumbai', 'Model', 'Fashion & Retail'),
    ('Svenja', 'Jung', 'Svenja Jung', '1993-05-28', 'Berlin', 'Filmschauspieler', 'Film & Television'),
    ('Benjamin', 'Sadler', 'Benjamin Sadler', '1971-02-12', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Dieter', 'Kindlmann', 'Dieter Kindlmann', '1982-06-03', 'Blaichach', 'Tennisspieler', 'Sports & Recreation'),
    ('Elisabeth', 'Seitz', 'Elisabeth Seitz', '1993-11-04', 'Stuttgart', 'Turner', 'Media & Entertainment'),
    ('Flula', 'Borg', 'Flula Borg', '1982-03-28', 'Los Angeles', 'Sänger', 'Music & Entertainment'),
    ('Frank', 'Moser', 'Frank Moser', '1976-09-23', 'Baden-Baden', 'Tennisspieler', 'Sports & Recreation'),
    ('Gero', 'Kretschmer', 'Gero Kretschmer', '1985-05-06', 'Köln', 'Tennisspieler', 'Sports & Recreation'),
    ('Jasmin', 'Grabowski', 'Jasmin Grabowski', '1991-11-07', 'Zweibrücken', 'Judoka', 'Sports & Recreation'),
    ('Julian', 'Reister', 'Julian Reister', '1986-04-02', 'Reinbek', 'Tennisspieler', 'Sports & Recreation'),
    ('Kim', 'Kalicki', 'Kim Kalicki', '1997-06-27', 'Wiesbaden', 'Bobfahrer', 'Sports & Recreation'),
    ('Matthias', 'Blübaum', 'Matthias Blübaum', '1997-04-18', 'Bielefeld', 'Schachspieler', 'Sports & Recreation'),
    ('Matthias', 'Sommer', 'Matthias Sommer', '1991-12-03', 'Bochum', 'Bobfahrer', 'Sports & Recreation'),
    ('Nadiuska', '', 'Nadiuska', '1952-01-19', 'Ciempozuelos', 'Schauspieler', 'Film & Television'),
    ('Patrik', 'Kühnen', 'Patrik Kühnen', '1966-02-11', 'Berlin', 'Tennisspieler', 'Sports & Recreation'),
    ('Sebastian', 'Urzendowsky', 'Sebastian Urzendowsky', '1985-05-28', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Simon', 'Stadler', 'Simon Stadler', '1983-07-20', 'Heidelberg', 'Tennisspieler', 'Sports & Recreation'),
    ('Sophia', 'Thomalla', 'Sophia Thomalla', '1989-10-06', 'Holländisches Viertel', 'Model', 'Fashion & Retail'),
    ('Wolfgang', 'Beltracchi', 'Wolfgang Beltracchi', '1951-02-04', 'Herdern', 'Schriftsteller', 'Publishing & Media'),
    ('Álvaro', 'Brechner', 'Álvaro Brechner', '1976-04-09', 'Madrid', 'Filmregisseur', 'Film & Television'),
    ('Alex', 'Satschko', 'Alex Satschko', '1980-11-12', 'München', 'Tennisspieler', 'Sports & Recreation'),
    ('Alexander', 'Popp', 'Alexander Popp', '1976-11-04', 'Mannheim', 'Tennisspieler', 'Sports & Recreation'),
    ('Daniel', 'Graf', 'Daniel Graf', '1981-09-07', 'Siegsdorf', 'Biathlet', 'Sports & Recreation'),
    ('Elena', 'Lilik', 'Elena Lilik', '1998-09-14', 'Augsburg', 'Sportler', 'Sports & Recreation'),
    ('Emma', 'Schweiger', 'Emma Schweiger', '2002-10-26', 'Hamburg', 'Filmschauspieler', 'Film & Television'),
    ('Eric', 'Franke', 'Eric Franke', '1989-08-16', 'Berlin', 'Bobfahrer', 'Sports & Recreation'),
    ('Florian', 'Graf', 'Florian Graf', '1988-07-24', 'Ruhpolding', 'Biathlet', 'Sports & Recreation'),
    ('Herrmann', 'Zschoche', 'Herrmann Zschoche', '1934-11-25', 'Storkow (Mark)', 'Filmregisseur', 'Film & Television'),
    ('Jenny', 'Elvers', 'Jenny Elvers', '1972-05-11', 'Marbella', 'Fernsehmoderator', 'Film & Television'),
    ('Manfred', 'Beer', 'Manfred Beer', '1953-12-02', 'Zinnwald-Georgenfeld', 'Biathlet', 'Sports & Recreation'),
    ('Marija', 'Petrowna Maksakowa', 'Marija Petrowna Maksakowa', '1977-07-24', 'Kiew', 'Fernsehmoderator', 'Film & Television'),
    ('Maybrit', 'Illner', 'Maybrit Illner', '1965-01-12', 'Berlin', 'Fernsehmoderator', 'Film & Television'),
    ('Philipp', 'Walsleben', 'Philipp Walsleben', '1987-11-19', 'Kleinmachnow', 'Radrennfahrer', 'Sports & Recreation'),
    ('Radost', 'Bokel', 'Radost Bokel', '1975-06-04', 'Rodgau', 'Model', 'Fashion & Retail'),
    ('Rüdiger', 'Vogler', 'Rüdiger Vogler', '1942-05-14', 'Paris', 'Schauspieler', 'Film & Television'),
    ('Sabine', 'Hack', 'Sabine Hack', '1969-07-12', 'Sarasota County', 'Tennisspieler', 'Sports & Recreation'),
    ('Sophie', 'Scheder', 'Sophie Scheder', '1997-01-07', 'Chemnitz', 'Turner', 'Media & Entertainment'),
    ('Stefan', 'Klein', 'Stefan Klein', '1965-10-05', 'Berlin', 'Journalist', 'Publishing & Media'),
    ('Tim', 'Hecker', 'Tim Hecker', '1997-01-01', 'Berlin', 'Kanute', 'Sports & Recreation'),
    ('Wolfgang', 'Joop', 'Wolfgang Joop', '1944-11-18', 'Braunschweig', 'Schriftsteller', 'Publishing & Media'),
    ('Alexander', 'Swerew', 'Alexander Swerew', '1960-01-22', 'Hamburg', 'Tennisspieler', 'Sports & Recreation'),
    ('Anca', 'Barna', 'Anca Barna', '1977-05-14', 'Nürnberg', 'Tennisspieler', 'Sports & Recreation'),
    ('Burhan', 'Qurbani', 'Burhan Qurbani', '1980-11-15', 'Erkelenz', 'Schauspieler', 'Film & Television'),
    ('Collien', 'Fernandes', 'Collien Fernandes', '1981-09-26', 'Hamburg', 'Fernsehmoderator', 'Film & Television'),
    ('Damian', 'Boeselager', 'Damian Boeselager', '1988-03-08', 'Berlin', 'Journalist', 'Publishing & Media'),
    ('Daniela', 'Katzenberger', 'Daniela Katzenberger', '1986-10-01', 'Mallorca', 'Model', 'Fashion & Retail'),
    ('Dominik', 'Meffert', 'Dominik Meffert', '1981-04-09', 'Köln', 'Tennisspieler', 'Sports & Recreation'),
    ('Gina', 'Stiebitz', 'Gina Stiebitz', '1997-10-17', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Guy', 'Spier', 'Guy Spier', '1966-02-04', 'Richmond', 'Schriftsteller', 'Publishing & Media'),
    ('Jasmin', 'Schwiers', 'Jasmin Schwiers', '1982-08-11', 'Lichtenbusch', 'Schauspieler', 'Film & Television'),
    ('Jürgen', 'Elsässer', 'Jürgen Elsässer', '1957-01-20', 'Falkensee', 'Journalist', 'Publishing & Media'),
    ('Karolin', 'Horchler', 'Karolin Horchler', '1989-05-09', 'Ruhpolding', 'Biathlet', 'Sports & Recreation'),
    ('Michel', 'Friedman', 'Michel Friedman', '1956-02-25', 'Cannes', 'Fernsehmoderator', 'Film & Television'),
    ('Norbert', 'Weisser', 'Norbert Weisser', '1946-07-09', 'Venice', 'Filmschauspieler', 'Film & Television'),
    ('Stefan', 'Mücke', 'Stefan Mücke', '1981-11-22', 'Berlin', 'Autorennfahrer', 'Media & Entertainment'),
    ('Alexander', 'Rădulescu', 'Alexander Rădulescu', '1974-12-07', 'München', 'Tennisspieler', 'Sports & Recreation'),
    ('André', 'Weis', 'André Weis', '1989-09-30', 'Boppard', 'Fußballspieler', 'Sports & Recreation'),
    ('Anne', 'Will', 'Anne Will', '1966-03-18', 'Berlin', 'Fernsehmoderator', 'Film & Television'),
    ('Astrid', 'M. Fünderich', 'Astrid M. Fünderich', '1963-09-27', 'Stuttgart', 'Schauspieler', 'Film & Television'),
    ('Barbara', 'Becker', 'Barbara Becker', '1966-11-01', 'Miami', 'Schauspieler', 'Film & Television'),
    ('Bastian', 'Knittel', 'Bastian Knittel', '1983-08-08', 'Stuttgart', 'Tennisspieler', 'Sports & Recreation'),
    ('Bianka', 'Lamade', 'Bianka Lamade', '1982-08-30', 'Straßburg', 'Tennisspieler', 'Sports & Recreation'),
    ('Christoph', 'M. Ohrt', 'Christoph M. Ohrt', '1960-03-30', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Dmitrij', 'Kollars', 'Dmitrij Kollars', '1999-08-13', 'Hamburg', 'Schachspieler', 'Sports & Recreation'),
    ('Eko', 'Fresh', 'Eko Fresh', '1983-09-03', 'Mönchengladbach', 'Sänger', 'Music & Entertainment'),
    ('Frank', 'Baltrusch', 'Frank Baltrusch', '1964-03-21', 'Magdeburg', 'Schwimmer', 'Sports & Recreation'),
    ('Gloria', 'Friedmann', 'Gloria Friedmann', '1950-01-01', 'Aignay-le-Duc', 'Bildhauer', 'Arts & Creative Industries'),
    ('Hendrik', 'Dreekmann', 'Hendrik Dreekmann', '1975-01-29', 'Bielefeld', 'Tennisspieler', 'Sports & Recreation'),
    ('Jana', 'Kandarr', 'Jana Kandarr', '1976-09-21', 'München', 'Tennisspieler', 'Sports & Recreation'),
    ('Kim', 'Bui', 'Kim Bui', '1989-01-20', 'Ehningen', 'Turner', 'Media & Entertainment'),
    ('Kollegah', '', 'Kollegah', '1984-08-03', 'Düsseldorf', 'Rapper', 'Music & Entertainment'),
    ('Lars', 'Kraume', 'Lars Kraume', '1973-02-24', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Luna', 'Schweiger', 'Luna Schweiger', '1997-01-11', 'Hamburg', 'Schauspieler', 'Film & Television'),
    ('Markus', 'Hantschk', 'Markus Hantschk', '1977-11-19', 'Böbrach', 'Tennisspieler', 'Sports & Recreation'),
    ('Michael', 'Fuchs', 'Michael Fuchs', '1982-04-22', 'Saarbrücken', 'Badmintonspieler', 'Sports & Recreation'),
    ('Michel', 'Heßmann', 'Michel Heßmann', '2001-04-06', 'Freiburg im Breisgau', 'Radrennfahrer', 'Sports & Recreation'),
    ('Mido', 'Hamada', 'Mido Hamada', '1971-01-01', 'London', 'Schauspieler', 'Film & Television'),
    ('Olga', 'Konon', 'Olga Konon', '1989-11-11', 'Saarbrücken', 'Badmintonspieler', 'Sports & Recreation'),
    ('Oliver', 'Mark', 'Oliver Mark', '1963-02-20', 'Berlin', 'Fotograf', 'Arts & Creative Industries'),
    ('Ottfried', 'Fischer', 'Ottfried Fischer', '1953-11-07', 'Passau', 'Schauspieler', 'Film & Television'),
    ('Pauline', 'Schäfer-Betz', 'Pauline Schäfer-Betz', '1997-01-04', 'Chemnitz', 'Turner', 'Media & Entertainment'),
    ('Roland', 'Kaiser', 'Roland Kaiser', '1952-05-10', 'Münster', 'Sänger', 'Music & Entertainment'),
    ('Sarah', 'Kuttner', 'Sarah Kuttner', '1979-01-29', 'Berlin', 'Fernsehmoderator', 'Film & Television'),
    ('Shirin', 'David', 'Shirin David', '1995-04-11', 'Berlin', 'Sänger', 'Music & Entertainment'),
    ('Ulrich', 'Pinner', 'Ulrich Pinner', '1954-02-07', 'Essen', 'Tennisspieler', 'Sports & Recreation'),
    ('Ursula', 'Buchfellner', 'Ursula Buchfellner', '1961-06-08', 'Hasenbergl', 'Model', 'Fashion & Retail'),
    ('Vivian', 'Heisen', 'Vivian Heisen', '1993-12-27', 'Wiefelstede', 'Tennisspieler', 'Sports & Recreation'),
    ('Adeline', 'Rudolph', 'Adeline Rudolph', '1995-02-10', 'Los Angeles', 'Schauspieler', 'Film & Television'),
    ('Anna', 'Schudt', 'Anna Schudt', '1974-03-23', 'Düsseldorf', 'Schauspieler', 'Film & Television'),
    ('Chris', 'Kraus', 'Chris Kraus', '1963-01-01', 'Berlin', 'Schriftsteller', 'Publishing & Media'),
    ('Christian', 'Poser', 'Christian Poser', '1986-08-16', 'Potsdam', 'Bobfahrer', 'Sports & Recreation'),
    ('Christian', 'Saceanu', 'Christian Saceanu', '1968-07-08', 'Neuss', 'Tennisspieler', 'Sports & Recreation'),
    ('Daniel', 'Elsner', 'Daniel Elsner', '1979-01-04', 'Memmingerberg', 'Tennisspieler', 'Sports & Recreation'),
    ('Dieter', 'Nuhr', 'Dieter Nuhr', '1960-10-29', 'Ratingen', 'Fernsehmoderator', 'Film & Television'),
    ('Dietrich', 'Brüggemann', 'Dietrich Brüggemann', '1976-02-23', 'Berlin', 'Musiker', 'Music & Entertainment'),
    ('Eva', 'Christian', 'Eva Christian', '1937-05-27', 'München', 'Filmschauspieler', 'Film & Television'),
    ('Florian', 'Baak', 'Florian Baak', '1999-03-18', 'Berlin', 'Fußballspieler', 'Sports & Recreation'),
    ('Jannis', 'Bäcker', 'Jannis Bäcker', '1985-01-01', 'Holzwickede', 'Bobfahrer', 'Sports & Recreation'),
    ('Jochen', 'Horst', 'Jochen Horst', '1961-09-07', 'Spanien', 'Schauspieler', 'Film & Television'),
    ('Juju', '', 'Juju', '1992-11-20', 'Berlin', 'Sänger', 'Music & Entertainment'),
    ('Klaus-Jürgen', 'Wrede', 'Klaus-Jürgen Wrede', '1963-08-19', 'Arnsberg', 'Schriftsteller', 'Publishing & Media'),
    ('Melanie', 'Müller', 'Melanie Müller', '1988-06-10', 'Leipzig', 'Popsänger', 'Music & Entertainment'),
    ('Michael', 'Kumpfmüller', 'Michael Kumpfmüller', '1961-07-21', 'Berlin', 'Journalist', 'Publishing & Media'),
    ('Nico', 'Santos', 'Nico Santos', '1993-01-07', 'Berlin', 'Sänger', 'Music & Entertainment'),
    ('Nina', 'George', 'Nina George', '1973-08-30', 'Berlin', 'Journalist', 'Publishing & Media'),
    ('Sanna', 'Englund', 'Sanna Englund', '1975-04-18', 'Berlin', 'Model', 'Fashion & Retail'),
    ('Schwesta', 'Ewa', 'Schwesta Ewa', '1984-07-16', 'Frankfurt am Main', 'Sänger', 'Music & Entertainment'),
    ('Tatjana', 'Rühl', 'Tatjana Rühl', '1965-01-18', 'Handewitt', 'Handballspieler', 'Sports & Recreation'),
    ('Torsten', 'Voges', 'Torsten Voges', '1961-12-17', 'Los Angeles', 'Schauspieler', 'Film & Television'),
    ('Achim', 'Reichel', 'Achim Reichel', '1944-01-28', 'Hummelsbüttel', 'Sänger', 'Music & Entertainment'),
    ('Alina', 'Bronsky', 'Alina Bronsky', '1978-01-01', 'Berlin', 'Schriftsteller', 'Publishing & Media'),
    ('Andrea', 'Kiewel', 'Andrea Kiewel', '1965-06-10', 'Tel Aviv-Jaffa', 'Schwimmer', 'Sports & Recreation'),
    ('Andreas', 'Bohnenstengel', 'Andreas Bohnenstengel', '1970-06-09', 'München', 'Dozent', 'Education & Research'),
    ('Andreas', 'Bredau', 'Andreas Bredau', '1984-03-21', 'Großkayna', 'Bobfahrer', 'Sports & Recreation'),
    ('Andreas', 'Dorau', 'Andreas Dorau', '1964-01-19', 'Hamburg', 'Sänger', 'Music & Entertainment'),
    ('Andreas', 'Toba', 'Andreas Toba', '1990-10-07', 'Hannover', 'Turner', 'Media & Entertainment'),
    ('Angelika', 'Bachmann', 'Angelika Bachmann', '1979-05-16', 'München', 'Tennisspieler', 'Sports & Recreation'),
    ('Angelika', 'Roesch', 'Angelika Roesch', '1977-06-08', 'Oberweier', 'Tennisspieler', 'Sports & Recreation'),
    ('Angelina', 'Maccarone', 'Angelina Maccarone', '1965-08-21', 'Berlin', 'Filmregisseur', 'Film & Television'),
    ('Anja', 'Schüte', 'Anja Schüte', '1964-09-02', 'Oslo', 'Filmschauspieler', 'Film & Television'),
    ('Annika', 'Drazek', 'Annika Drazek', '1995-04-11', 'Gladbeck', 'Bobfahrer', 'Sports & Recreation'),
    ('Arifin', 'Putra', 'Arifin Putra', '1987-05-01', 'Jakarta', 'Schauspieler', 'Film & Television'),
    ('Birgit', 'Michels', 'Birgit Michels', '1984-09-28', 'Bonn', 'Badmintonspieler', 'Sports & Recreation'),
    ('Christian', 'Ulmen', 'Christian Ulmen', '1975-09-22', 'Palma', 'Fernsehmoderator', 'Film & Television'),
    ('Christiane', 'Gohl', 'Christiane Gohl', '1958-01-01', 'Los Gallardos', 'Schriftsteller', 'Publishing & Media'),
    ('Christin', 'Senkel', 'Christin Senkel', '1987-08-31', 'Oberhof', 'Bobfahrer', 'Sports & Recreation'),
    ('Daniel', 'Richter', 'Daniel Richter', '1962-12-18', 'Eutin', 'Schauspieler', 'Film & Television'),
    ('Dirk', 'Dier', 'Dirk Dier', '1972-02-16', 'Blieskastel', 'Tennisspieler', 'Sports & Recreation'),
    ('Emma', 'Becker', 'Emma Becker', '1988-12-14', 'Berlin', 'Schriftsteller', 'Publishing & Media'),
    ('Emma', 'Malewski', 'Emma Malewski', '2004-07-18', 'Chemnitz', 'Turner', 'Media & Entertainment'),
    ('Frank', 'Witzel', 'Frank Witzel', '1955-01-01', 'Offenbach am Main', 'Musiker', 'Music & Entertainment'),
    ('Gerhard', 'Polt', 'Gerhard Polt', '1942-05-07', 'München', 'Schriftsteller', 'Publishing & Media'),
    ('Gina-Lisa', 'Lohfink', 'Gina-Lisa Lohfink', '1986-09-23', 'Hasselbach', 'Model', 'Fashion & Retail'),
    ('Ines', 'Schwerdtner', 'Ines Schwerdtner', '1989-08-26', 'Berlin', 'Journalist', 'Publishing & Media'),
    ('Juergen', 'Teller', 'Juergen Teller', '1964-01-28', 'London', 'Fotograf', 'Arts & Creative Industries'),
    ('Juliane', 'Banse', 'Juliane Banse', '1969-07-10', 'Dießen am Ammersee', 'Musiker', 'Music & Entertainment'),
    ('Juliane', 'Lorenz', 'Juliane Lorenz', '1957-08-02', 'München', 'Schauspieler', 'Film & Television'),
    ('Juliane', 'Werding', 'Juliane Werding', '1956-07-19', 'Starnberg', 'Sänger', 'Music & Entertainment'),
    ('Julius', 'Kade', 'Julius Kade', '1999-05-20', 'Berlin', 'Fußballspieler', 'Sports & Recreation'),
    ('Karin', 'Kschwendt', 'Karin Kschwendt', '1968-09-14', 'Wien', 'Tennisspieler', 'Sports & Recreation'),
    ('Kolja', 'Afriyie', 'Kolja Afriyie', '1982-04-06', 'Flensburg', 'Fußballspieler', 'Sports & Recreation'),
    ('Larissa', 'Mondrus', 'Larissa Mondrus', '1943-11-15', 'München', 'Sänger', 'Music & Entertainment'),
    ('Lilly', 'Krug', 'Lilly Krug', '2001-06-05', 'München', 'Model', 'Fashion & Retail'),
    ('Lukas', 'Rieger', 'Lukas Rieger', '1999-06-03', 'Dubai', 'Sänger', 'Music & Entertainment'),
    ('Mark', 'Lamsfuß', 'Mark Lamsfuß', '1994-04-19', 'Saarbrücken', 'Badmintonspieler', 'Sports & Recreation'),
    ('Marvin', 'Seidel', 'Marvin Seidel', '1995-11-09', 'St. Ingbert', 'Badmintonspieler', 'Sports & Recreation'),
    ('Matthias', 'Bertsch', 'Matthias Bertsch', '1966-11-15', 'Mödling', 'Musiker', 'Music & Entertainment'),
    ('Minh-Khai', 'Phan-Thi', 'Minh-Khai Phan-Thi', '1974-02-19', 'Berlin', 'Fernsehmoderator', 'Film & Television'),
    ('Nadja', 'Becker', 'Nadja Becker', '1978-10-25', 'Köln', 'Schauspieler', 'Film & Television'),
    ('Norman', 'Ohler', 'Norman Ohler', '1970-02-04', 'Berlin', 'Journalist', 'Publishing & Media'),
    ('Patrick', 'Kohlmann', 'Patrick Kohlmann', '1983-02-25', 'Dortmund', 'Fußballspieler', 'Sports & Recreation'),
    ('Patrycja', 'Volny', 'Patrycja Volny', '1988-02-05', 'Hongkong', 'Sänger', 'Music & Entertainment'),
    ('Ralf', 'Isau', 'Ralf Isau', '1956-03-02', 'Asperg', 'Schriftsteller', 'Publishing & Media'),
    ('Robert', 'Leipertz', 'Robert Leipertz', '1993-02-01', 'Jülich', 'Fußballspieler', 'Sports & Recreation'),
    ('Sabina', 'Began', 'Sabina Began', '1974-10-22', 'Italien', 'Schauspieler', 'Film & Television'),
    ('Stefanie', 'Horn', 'Stefanie Horn', '1991-01-09', 'Brescia', 'Kanute', 'Sports & Recreation'),
    ('Vanessa', 'Radman', 'Vanessa Radman', '1974-01-01', 'Wuppertal', 'Schauspieler', 'Film & Television'),
    ('Vladimir', 'Burlakov', 'Vladimir Burlakov', '1987-01-01', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Carsten', 'Arriens', 'Carsten Arriens', '1969-04-11', 'München', 'Tennisspieler', 'Sports & Recreation'),
    ('Christiane', 'Pilz', 'Christiane Pilz', '1975-08-03', 'Rostock', 'Schwimmer', 'Sports & Recreation'),
    ('Edin', 'Hasanović', 'Edin Hasanović', '1992-04-02', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Ella', 'Endlich', 'Ella Endlich', '1984-06-18', 'Berlin', 'Sänger', 'Music & Entertainment'),
    ('Emil', 'Hurezeanu', 'Emil Hurezeanu', '1955-08-26', 'Wien', 'Journalist', 'Publishing & Media'),
    ('Erwin', 'Hadewicz', 'Erwin Hadewicz', '1951-04-02', 'Ellwangen', 'Fußballspieler', 'Sports & Recreation'),
    ('Eva', 'Wilms', 'Eva Wilms', '1952-07-28', 'Essen', 'Leichtathlet', 'Sports & Recreation'),
    ('Fabian', 'Roth', 'Fabian Roth', '1995-11-29', 'Saarbrücken', 'Badmintonspieler', 'Sports & Recreation'),
    ('Fadhil', 'al-Azzawi', 'Fadhil al-Azzawi', '1940-01-01', 'Berlin', 'Journalist', 'Publishing & Media'),
    ('Florian', 'Kohls', 'Florian Kohls', '1995-04-03', 'Berlin', 'Fußballspieler', 'Sports & Recreation'),
    ('Frank', 'Bornemann', 'Frank Bornemann', '1945-04-27', 'Hannover', 'Sänger', 'Music & Entertainment'),
    ('Frank', 'Elstner', 'Frank Elstner', '1942-04-19', 'Baden-Baden', 'Fernsehmoderator', 'Film & Television'),
    ('Howard', 'Carpendale', 'Howard Carpendale', '1946-01-14', 'München', 'Sänger', 'Music & Entertainment'),
    ('Isabel', 'Lohau', 'Isabel Lohau', '1992-03-17', 'Mülheim an der Ruhr', 'Badmintonspieler', 'Sports & Recreation'),
    ('Isolde', 'Barth', 'Isolde Barth', '1948-08-24', 'München', 'Filmschauspieler', 'Film & Television'),
    ('Jana', 'Beller', 'Jana Beller', '1990-10-27', 'Lippramsdorf', 'Model', 'Fashion & Retail'),
    ('John', 'Tripp', 'John Tripp', '1977-05-04', 'Kingston', 'Eishockeyspieler', 'Sports & Recreation'),
    ('Julia', 'Neigel', 'Julia Neigel', '1966-04-19', 'Ludwigshafen am Rhein', 'Sänger', 'Music & Entertainment'),
    ('Katja', 'Krasavice', 'Katja Krasavice', '1996-08-10', 'Leipzig', 'Sänger', 'Music & Entertainment'),
    ('Kimberly', 'Ann Voltemas', 'Kimberly Ann Voltemas', '1992-01-22', 'Bangkok', 'Model', 'Fashion & Retail'),
    ('Klaus', 'Eberhard', 'Klaus Eberhard', '1957-09-15', 'Berlin', 'Tennisspieler', 'Sports & Recreation'),
    ('Konrad', 'Adam', 'Konrad Adam', '1942-03-01', 'Oberursel (Taunus)', 'Journalist', 'Publishing & Media'),
    ('Lars', 'Koslowski', 'Lars Koslowski', '1971-05-22', 'Vellmar', 'Tennisspieler', 'Sports & Recreation'),
    ('Marco', 'Girnth', 'Marco Girnth', '1970-02-10', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Michael', 'Wendler', 'Michael Wendler', '1972-06-22', 'Cape Coral', 'Sänger', 'Music & Entertainment'),
    ('Nicki', '', 'Nicki', '1966-11-02', 'Plattling', 'Sänger', 'Music & Entertainment'),
    ('Nicolaus', 'Fest', 'Nicolaus Fest', '1962-07-01', 'Kroatien', 'Journalist', 'Publishing & Media'),
    ('Oliver', 'Gross', 'Oliver Gross', '1973-06-17', 'München', 'Tennisspieler', 'Sports & Recreation'),
    ('Petra', 'Haltmayr', 'Petra Haltmayr', '1975-09-16', 'Rettenberg', 'Skirennläufer', 'Sports & Recreation'),
    ('Rafed', 'El-Masri', 'Rafed El-Masri', '1982-08-10', 'Berlin', 'Schwimmer', 'Sports & Recreation'),
    ('Rainer', 'Rupp', 'Rainer Rupp', '1945-09-21', 'Justizvollzugsanstalt Saarbrücken', 'Schriftsteller', 'Publishing & Media'),
    ('Rezo', '', 'Rezo', '1992-08-14', 'Aachen', 'Musiker', 'Music & Entertainment'),
    ('Rosa', 'Klöser', 'Rosa Klöser', '1996-06-24', 'Kopenhagen', 'Radrennfahrer', 'Sports & Recreation'),
    ('Rüdiger', 'Dorn', 'Rüdiger Dorn', '1969-01-01', 'Pfofeld', 'Schriftsteller', 'Publishing & Media'),
    ('Sabine', 'Ellerbrock', 'Sabine Ellerbrock', '1975-11-01', 'Bielefeld', 'Rollstuhltennisspieler', 'Sports & Recreation'),
    ('Sarah', 'Voss', 'Sarah Voss', '1999-10-21', 'Dormagen', 'Turner', 'Media & Entertainment'),
    ('Stephanie', 'Stumph', 'Stephanie Stumph', '1984-07-07', 'Dresden', 'Filmschauspieler', 'Film & Television'),
    ('Ulla', 'von Brandenburg', 'Ulla von Brandenburg', '1974-01-01', "Nogent-l'Artaud", 'Fotograf', 'Arts & Creative Industries'),
    ('Ulrich', 'Wickert', 'Ulrich Wickert', '1942-12-02', 'Heidelberg', 'Fernsehmoderator', 'Film & Television'),
    ('Uwe', 'Ommer', 'Uwe Ommer', '1943-01-01', 'Paris', 'Fotograf', 'Arts & Creative Industries'),
    ('Verena', 'von Strenge', 'Verena von Strenge', '1975-07-27', 'Berlin', 'Sänger', 'Music & Entertainment'),
    ('Yvonne', 'Li', 'Yvonne Li', '1998-05-30', 'Mülheim an der Ruhr', 'Badmintonspieler', 'Sports & Recreation'),
    ('Ada', 'Mee', 'Ada Mee', '1946-01-01', 'Heidelberg', 'Fotograf', 'Arts & Creative Industries'),
    ('Andreas', 'Giebel', 'Andreas Giebel', '1958-06-04', 'München', 'Schauspieler', 'Film & Television'),
    ('Anne', 'Haigis', 'Anne Haigis', '1955-12-09', 'Bonn', 'Sänger', 'Music & Entertainment'),
    ('Carla', 'Nelte', 'Carla Nelte', '1990-09-21', 'Oberhausen', 'Badmintonspieler', 'Sports & Recreation'),
    ('Christoph', 'John', 'Christoph John', '1958-12-24', 'Heidenheim an der Brenz', 'Fußballspieler', 'Sports & Recreation'),
    ('Daniel', 'Donskoy', 'Daniel Donskoy', '1990-01-27', 'London', 'Sänger', 'Music & Entertainment'),
    ('Daniel', 'Lopes', 'Daniel Lopes', '1976-11-12', 'Schloß Holte', 'Sänger', 'Music & Entertainment'),
    ('Dieter', 'Appelt', 'Dieter Appelt', '1935-03-03', 'Berlin', 'Bildhauer', 'Arts & Creative Industries'),
    ('Edward', 'Lee Spence', 'Edward Lee Spence', '1947-11-06', 'Summerville', 'Journalist', 'Publishing & Media'),
    ('Fiona', 'Erdmann', 'Fiona Erdmann', '1988-09-09', 'Dubai', 'Model', 'Fashion & Retail'),
    ('Günter', 'Sommer', 'Günter Sommer', '1943-08-25', 'Radebeul', 'Hochschullehrer', 'Education & Research'),
    ('Helen', 'Kevric', 'Helen Kevric', '2008-03-21', 'Ostfildern', 'Turner', 'Media & Entertainment'),
    ('Helga', 'Schneider', 'Helga Schneider', '1937-11-17', 'Bologna', 'Schriftsteller', 'Publishing & Media'),
    ('Henning', 'Lohner', 'Henning Lohner', '1961-07-17', 'Los Angeles', 'Filmregisseur', 'Film & Television'),
    ('Hildegard', 'Westerkamp', 'Hildegard Westerkamp', '1946-04-08', 'Vancouver', 'Schriftsteller', 'Publishing & Media'),
    ('Jan', 'Weiler', 'Jan Weiler', '1967-10-28', 'Icking', 'Journalist', 'Publishing & Media'),
    ('Jana', 'Schmidt', 'Jana Schmidt', '1972-12-13', 'Rostock', 'Leichtathlet', 'Sports & Recreation'),
    ('Jens', 'Winter', 'Jens Winter', '1965-05-26', 'Berlin', 'Filmschauspieler', 'Film & Television'),
    ('Jochen', 'Schropp', 'Jochen Schropp', '1978-11-22', 'Langgöns', 'Fernsehmoderator', 'Film & Television'),
    ('Josephine', 'Meckseper', 'Josephine Meckseper', '1964-01-01', 'New York City', 'Filmemacher', 'Film & Television'),
    ('Julian', 'Engels', 'Julian Engels', '1993-04-22', 'Dülmen', 'Fußballspieler', 'Sports & Recreation'),
    ('Kai', 'Schäfer', 'Kai Schäfer', '1993-06-13', 'Mülheim an der Ruhr', 'Badmintonspieler', 'Sports & Recreation'),
    ('Katja', 'Abel', 'Katja Abel', '1983-04-08', 'Berlin', 'Geräteturner', 'Sports & Recreation'),
    ('Kira', 'Lipperheide', 'Kira Lipperheide', '2000-02-07', 'Castrop-Rauxel', 'Bobfahrer', 'Sports & Recreation'),
    ('Kolja', 'Pusch', 'Kolja Pusch', '1993-02-12', 'Wuppertal', 'Fußballspieler', 'Sports & Recreation'),
    ('Lars', 'Rehmann', 'Lars Rehmann', '1975-05-21', 'Salzburg', 'Tennisspieler', 'Sports & Recreation'),
    ('Leonie', 'Fiebig', 'Leonie Fiebig', '1990-05-24', 'Köln', 'Bobfahrer', 'Sports & Recreation'),
    ('Lilli', 'Schweiger', 'Lilli Schweiger', '1998-07-17', 'Hamburg', 'Schauspieler', 'Film & Television'),
    ('Lisa', 'Fitz', 'Lisa Fitz', '1951-09-15', 'Rottal (Bayern)', 'Schauspieler', 'Film & Television'),
    ('Lisa', 'Maria Potthoff', 'Lisa Maria Potthoff', '1978-07-25', 'Berlin', 'Schauspieler', 'Film & Television'),
    ('Lovelyn', 'Enebechi', 'Lovelyn Enebechi', '1996-10-21', 'Alsterdorf', 'Model', 'Fashion & Retail'),
    ('Margarete', 'Stokowski', 'Margarete Stokowski', '1986-04-14', 'Berlin', 'Schriftsteller', 'Publishing & Media'),
    ('Martin', 'Hyun', 'Martin Hyun', '1979-05-04', 'Brüssel', 'Schriftsteller', 'Publishing & Media'),
    ('Matthias', 'Becker', 'Matthias Becker', '1974-04-19', 'Frankfurt am Main', 'Fußballspieler', 'Sports & Recreation'),
    ('Michael', 'Seidenbecher', 'Michael Seidenbecher', '1984-11-06', 'Erfurt', 'Radrennfahrer', 'Sports & Recreation'),
    ('Nils', 'Langer', 'Nils Langer', '1990-01-25', 'Affalterbach', 'Tennisspieler', 'Sports & Recreation'),
    ('Nina', 'Chuba', 'Nina Chuba', '1998-10-14', 'Berlin', 'Sänger', 'Music & Entertainment'),
    ('Sandra', 'Marinello', 'Sandra Marinello', '1983-05-29', 'Düsseldorf', 'Badmintonspieler', 'Sports & Recreation'),
    ('Sascha', 'Lobo', 'Sascha Lobo', '1975-05-11', 'Prenzlauer Berg', 'Journalist', 'Publishing & Media'),
    ('Tina', 'Ruland', 'Tina Ruland', '1966-10-09', 'Berlin', 'Schriftsteller', 'Publishing & Media'),
    ('Andreas', 'Wessels', 'Andreas Wessels', '1964-07-06', 'Uedem', 'Fußballspieler', 'Sports & Recreation'),
    ('Felix', 'Petermann', 'Felix Petermann', '1984-04-11', 'Füssen', 'Eishockeyspieler', 'Sports & Recreation'),
    ('Holger', 'C. Gotha', 'Holger C. Gotha', '1960-12-07', 'München', 'Schauspieler', 'Film & Television'),
    ('Jens-Peter', 'Berndt', 'Jens-Peter Berndt', '1963-08-17', 'Potsdam', 'Schwimmer', 'Sports & Recreation'),
    ('Jürgen', 'Theobaldy', 'Jürgen Theobaldy', '1944-03-07', 'Schweiz', 'Schriftsteller', 'Publishing & Media'),
    ('Karsten', 'Thormaehlen', 'Karsten Thormaehlen', '1965-07-28', 'Wiesbaden', 'Fotograf', 'Arts & Creative Industries'),
    ('Marco', 'Grimm', 'Marco Grimm', '1972-06-16', 'Baden-Baden', 'Fußballspieler', 'Sports & Recreation'),
    ('Michael', 'Kühntopf', 'Michael Kühntopf', '1957-08-11', 'Seeland', 'Schriftsteller', 'Publishing & Media'),
    ('Peter', 'Klocke', 'Peter Klocke', '1957-12-20', 'Rellinghausen', 'Schauspieler', 'Film & Television'),
    ('Petra', 'Reski', 'Petra Reski', '1958-01-01', 'Venedig', 'Journalist', 'Publishing & Media'),
    ('Sebastian', 'Enderle', 'Sebastian Enderle', '1989-05-29', 'Ulm', 'Fußballspieler', 'Sports & Recreation'),
    ('Simon', 'Pierro', 'Simon Pierro', '1978-10-02', 'Berlin', 'Fernsehmoderator', 'Film & Television'),
    ('Stefan', 'Mross', 'Stefan Mross', '1975-11-26', 'Unterwössen', 'Sänger', 'Music & Entertainment'),
    ('Tilman', 'Spengler', 'Tilman Spengler', '1947-03-02', 'Ambach', 'Journalist', 'Publishing & Media'),
    ('Tyron', 'Montgomery', 'Tyron Montgomery', '1967-04-09', 'München', 'Filmregisseur', 'Film & Television'),
    ('Wolfgang', 'Krewe', 'Wolfgang Krewe', '1966-10-20', 'Berlin', 'Schauspieler', 'Film & Television'),
]

def public_figure_indices_for_batch(total: int) -> set[int]:
    """Alert indices [0, total) that use a verified public figure (~10% of total, rounded).

    Uses a dedicated RNG stream so the choice does not consume the global `random` sequence
    used inside `generate_alert` (reproducibility).
    """
    if total <= 0:
        return set()
    num = max(0, min(total, int(total * 0.1 + 0.5)))
    if num == 0:
        return set()
    rng = random.Random(total)
    return set(rng.sample(range(total), num))


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


def _apply_cash_auxiliary_fields(tx: _InternalTx) -> None:
    if tx.payment_rail in ("CASH_DEPOSIT", "ATM_WITHDRAWAL"):
        tx.cash_tx_type = "deposit" if tx.payment_rail == "CASH_DEPOSIT" else "withdrawal"
        tx.atm_city = fake.city()
        tx.atm_country = "DE"
    else:
        tx.cash_tx_type = None
        tx.atm_city = None
        tx.atm_country = None


def _set_coherent_profile(tx: _InternalTx, profile: tuple[str, str, str]) -> None:
    tx.tx_type, tx.payment_rail, tx.booking_channel = profile
    _apply_cash_auxiliary_fields(tx)


def _pick_weighted_coherent_profile() -> tuple[str, str, str]:
    """Background / generic triggers: mostly domestic transfer, then card, cash, intl wire."""
    roll = random.random()
    if roll < 0.52:
        opts = [p for p in _COHERENT_UNION if p[0] == "transfer"]
    elif roll < 0.72:
        opts = [p for p in _COHERENT_UNION if p[0] == "card"]
    elif roll < 0.86:
        opts = [p for p in _COHERENT_UNION if p[0] == "cash"]
    else:
        opts = [p for p in _COHERENT_UNION if p[0] == "wire"]
    return random.choice(opts)


def _assign_profile_and_direction_for_new_tx(tx: _InternalTx, profile: tuple[str, str, str]) -> None:
    _set_coherent_profile(tx, profile)
    if tx.payment_rail == "CASH_DEPOSIT":
        tx.direction = "in"
    elif tx.payment_rail == "ATM_WITHDRAWAL":
        tx.direction = "out"
    else:
        tx.direction = random.choice(["in", "out"])


def _coherence_repair_rail_vs_direction(tx: _InternalTx) -> None:
    """After balance walk, fix impossible pairs (e.g. ATM withdrawal shown as inbound)."""
    if tx.payment_rail == "ATM_WITHDRAWAL" and tx.direction == "in":
        _set_coherent_profile(tx, random.choice(_COHERENT_INBOUND))
    elif tx.payment_rail == "CASH_DEPOSIT" and tx.direction == "out":
        _set_coherent_profile(tx, random.choice(_COHERENT_OUTBOUND))


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

def generate_customer_profile(
    customer_id: str,
    *,
    verified_figure_row: tuple[str, str, str, str | None, str | None, str | None, str] | None = None,
) -> CustomerProfile:
    if verified_figure_row is not None:
        first, last, full, dob_s, city_hint, bekannt_durch, industry = verified_figure_row
        full_name = full
        if dob_s:
            dob = datetime.fromisoformat(dob_s).date()
        else:
            dob = fake.date_of_birth(minimum_age=25, maximum_age=75)
        place_of_birth = city_hint if city_hint else fake.city()
        customer_type = "private"
        legal = _generate_address()
        issued = fake.date_between(start_date="-10y", end_date="-1y")
        expires = _id_expiry_plus_years(issued)
        pep_flag = _bekannt_durch_implies_pep(bekannt_durch)
        is_public_figure = True
        ubo: list[UBO] = []
        profile_industry = industry
        emp_status = random.choice(EMPLOYMENT_STATUSES)
        risk_rating = random.choice(RISK_RATINGS)
        monthly_income, monthly_turnover = _monthly_income_and_turnover(emp_status, profile_industry)
    else:
        first = fake.first_name()
        last = fake.last_name()
        full_name = f"{first} {last}"
        dob = fake.date_of_birth(minimum_age=25, maximum_age=75)
        place_of_birth = fake.city()
        customer_type = random.choices(["private", "business"], weights=[70, 30])[0]
        legal = _generate_address()
        issued = fake.date_between(start_date="-10y", end_date="-1y")
        expires = _id_expiry_plus_years(issued)
        ubo = []
        if customer_type == "business":
            num_ubo = random.randint(1, 3)
            remaining = 100.0
            for j in range(num_ubo):
                pct = round(random.uniform(10, remaining - 10 * (num_ubo - j - 1)), 0) if j < num_ubo - 1 else round(remaining, 0)
                remaining -= pct
                ubo.append(UBO(name=fake.name(), ownership_percentage=pct))
        pep_flag = random.random() < 0.05
        is_public_figure = False
        emp_status = random.choice(EMPLOYMENT_STATUSES)
        profile_industry = random.choice(_industries_for_employment(emp_status))
        risk_rating = random.choice(RISK_RATINGS)
        monthly_income, monthly_turnover = _monthly_income_and_turnover(emp_status, profile_industry)

    return CustomerProfile(
        customer_id=customer_id,
        first_name=first,
        last_name=last,
        full_name=full_name,
        date_of_birth=dob.isoformat(),
        place_of_birth=place_of_birth,
        nationality=random.choice(["DE", "DE", "DE", "AT", "CH", "FR", "TR", "PL"]),
        residency_country="DE",
        kyc_status=random.choices(KYC_STATUSES, weights=[85, 10, 5])[0],
        customer_since=fake.date_between(start_date="-10y", end_date="-1y").isoformat(),
        email=fake.email(domain=random.choice(_CUSTOMER_EMAIL_DOMAINS)),
        phone_number=fake.phone_number(),
        legal_address=legal,
        id_document=IdDocument(
            type=random.choice(ID_DOC_TYPES),
            number=fake.bothify(text="??########"),
            issued_at=issued.isoformat(),
            expires_at=expires.isoformat(),
        ),
        pep_flag=pep_flag,
        sanctions_flag=False,
        customer_risk_rating=risk_rating,
        employment_status=emp_status,
        industry=profile_industry,
        account_purpose=random.choice(ACCOUNT_PURPOSES),
        expected_monthly_income=monthly_income,
        expected_monthly_turnover=monthly_turnover,
        customer_type=customer_type,
        ubo=ubo,
        alerts_last_12m=random.randint(0, 3),
        public_figure=is_public_figure,
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
        "planned_amount", "payment_reference_preset",
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
        self.planned_amount: Decimal | None = None
        self.payment_reference_preset: str | None = None


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


def _opening_balance_params(
    employment_status: str,
    customer_risk_rating: str,
    industry: str,
    *,
    secondary: bool,
) -> tuple[float, float, float, float]:
    """Return (lo, hi, mu, sigma) for truncated Gaussian opening balance."""
    if secondary:
        lo, hi = 500.0, 50000.0
        if employment_status == "STUDENT":
            mu = 3500.0
        elif employment_status == "UNEMPLOYED":
            mu = 5000.0
        elif employment_status == "RETIRED":
            mu = 15000.0
        elif employment_status == "SELF_EMPLOYED":
            mu = 22000.0
        else:
            mu = 18000.0
    else:
        lo, hi = 5000.0, 150000.0
        if employment_status == "STUDENT":
            mu = 12000.0
            lo, hi = 500.0, 35000.0
        elif employment_status == "UNEMPLOYED":
            mu = 18000.0
            lo, hi = 1000.0, 60000.0
        elif employment_status == "RETIRED":
            mu = 55000.0
        elif employment_status == "SELF_EMPLOYED":
            mu = 90000.0
            hi = 180000.0
        else:
            mu = 77500.0

    high_industry = {"Finance", "Legal Services", "Consulting", "Real Estate"}
    low_industry = {"Retail", "Gastronomy"}
    if industry in high_industry:
        mu *= 1.2
        if not secondary:
            hi = min(hi * 1.05, 200000.0)
    elif industry in low_industry:
        mu *= 0.88
        lo = max(500.0, lo * 0.95)

    if customer_risk_rating == "high":
        mu *= 1.12
    elif customer_risk_rating == "medium":
        mu *= 1.04

    mu = max(lo, min(mu, hi))
    sigma = max((hi - lo) / 6.0, 1e-6)
    return lo, hi, mu, sigma


def _opening_balance_for_profile(
    employment_status: str,
    customer_risk_rating: str,
    industry: str,
    *,
    secondary: bool,
) -> float:
    lo, hi, mu, sigma = _opening_balance_params(
        employment_status, customer_risk_rating, industry, secondary=secondary
    )
    return _money_float(_truncated_gauss(lo, hi, mu=mu, sigma=sigma))


def _iban_country(iban: str) -> str:
    return iban[:2] if len(iban) >= 2 else "DE"


# Counterparty Faker locales (EUR accounts only): Germany + France IBAN/BIC pools.
_EUR_FAKER_LOCALES: tuple[str, ...] = ("de_DE", "fr_FR")

_FAKER_BY_LOCALE: dict[str, Faker] = {}


def _faker_locale(locale: str) -> Faker:
    """Single Faker instance per allowed locale (counterparty paths only)."""
    if locale not in _FAKER_BY_LOCALE:
        _FAKER_BY_LOCALE[locale] = Faker(locale)
    return _FAKER_BY_LOCALE[locale]


def _bic_for_country_code(country_iso2: str) -> str:
    """8-char SWIFT-style BIC with ISO country at positions 5–6."""
    lf = _faker_locale(random.choice(_EUR_FAKER_LOCALES))
    bank4 = lf.bothify(text="????").upper()
    loc2 = lf.bothify(text="??").upper()
    return f"{bank4}{country_iso2.upper()}{loc2}"


def _eur_counterparty_banking_fields() -> tuple[str, str, str, str]:
    """(iban, country_iso2, bic, bank_name) for EUR-denominated accounts."""
    loc = random.choice(_EUR_FAKER_LOCALES)
    lf = _faker_locale(loc)
    iban = lf.iban()
    country = _iban_country(iban)
    bic = lf.swift8()
    bank = lf.company() + " Bank"
    return iban, country, bic, bank


def _counterparty_name_faker() -> Faker:
    return _faker_locale(random.choice(_EUR_FAKER_LOCALES))


def _fill_counterparty_non_cash(tx: _InternalTx) -> None:
    nf = _counterparty_name_faker()
    tx.cp_name = nf.company() if random.random() > 0.3 else nf.name()
    iban, country, bic, bank = _eur_counterparty_banking_fields()
    tx.cp_iban = iban
    tx.cp_country = country
    tx.cp_bic = bic
    tx.cp_bank = bank


class _PrivateMonthlyBudget(NamedTuple):
    """Recurring private outflows vs one monthly salary (whole EUR)."""
    gehalt: Decimal
    miete: Decimal
    telefon: Decimal
    auto_rate: Decimal
    fitness: Decimal


def _private_recurring_amounts(gehalt: int) -> _PrivateMonthlyBudget:
    """Miete at most 30% of Gehalt; other bills from remainder with slack; sum(out) < gehalt."""
    G = max(int(gehalt), 1500)
    Gd = _money_dec(G)
    m_cap = Gd * Decimal("0.30")
    miete = _money_dec(random.uniform(G * 0.18, G * 0.30))
    if miete > m_cap:
        miete = _money_dec(m_cap)
    slack = _money_dec(G * random.uniform(0.08, 0.15))
    pool = Gd - miete - slack
    min_pool = _money_dec(15) + _money_dec(100) + _money_dec(25)
    if pool < min_pool:
        slack = max(_money_dec(1), Gd - miete - min_pool)
        pool = Gd - miete - slack
    if pool < min_pool:
        miete = min(m_cap, max(_money_dec(400), Gd - slack - min_pool))
        pool = Gd - miete - slack

    w1, w2, w3 = random.random(), random.random(), random.random()
    ws = w1 + w2 + w3
    t_raw = _money_dec(float(pool) * (w1 / ws))
    a_raw = _money_dec(float(pool) * (w2 / ws))
    f_raw = pool - t_raw - a_raw

    telefon = max(_money_dec(15), min(t_raw, _money_dec(120)))
    auto_rate = max(_money_dec(100), min(a_raw, _money_dec(min(2500, float(pool)))))
    fitness = max(_money_dec(25), min(f_raw, _money_dec(150)))

    three = telefon + auto_rate + fitness
    if three > pool and three > 0:
        factor = float(pool / three) * 0.99
        telefon = max(_money_dec(15), _money_dec(float(telefon) * factor))
        auto_rate = max(_money_dec(100), _money_dec(float(auto_rate) * factor))
        fitness = max(_money_dec(25), pool - telefon - auto_rate)
        if fitness < _money_dec(25):
            fitness = _money_dec(25)
            auto_rate = max(_money_dec(100), pool - telefon - fitness)

    return _PrivateMonthlyBudget(gehalt=Gd, miete=miete, telefon=telefon, auto_rate=auto_rate, fitness=fitness)


def _iter_calendar_months(start: datetime, end: datetime) -> list[tuple[int, int]]:
    """(year, month) for each calendar month overlapping [start, end]."""
    months: list[tuple[int, int]] = []
    y, m = start.year, start.month
    while True:
        first = datetime(y, m, 1)
        if first > end:
            break
        if y > end.year or (y == end.year and m > end.month):
            break
        months.append((y, m))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return months


def _days_in_month(year: int, month: int) -> int:
    if month == 12:
        nxt = datetime(year + 1, 1, 1)
    else:
        nxt = datetime(year, month + 1, 1)
    return (nxt - timedelta(days=1)).day


def _month_datetime(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
    dim = _days_in_month(year, month)
    d = min(day, dim)
    return datetime(year, month, d, hour, minute, random.randint(0, 59))


def _sepa_transfer_in_profile() -> tuple[str, str, str]:
    opts = [p for p in _COHERENT_INBOUND if p[0] == "transfer" and p[1] == "SEPA_CT"]
    return random.choice(opts)


def _sepa_transfer_out_profile() -> tuple[str, str, str]:
    opts = [p for p in _COHERENT_OUTBOUND if p[0] == "transfer" and p[1] == "SEPA_CT"]
    return random.choice(opts)


def _build_private_recurring_txs(
    account_id: str,
    currency: str,
    budget: _PrivateMonthlyBudget,
    tx_counter_start: int,
) -> tuple[list[_InternalTx], int]:
    """One Gehalt in + Miete / Telefon / Auto / Fitness out per calendar month in history window."""
    txs: list[_InternalTx] = []
    n = tx_counter_start
    for year, month in _iter_calendar_months(HISTORY_START, GENERATION_NOW):
        month_de = GERMAN_MONTHS[month - 1]
        # In before out same month: Gehalt on 25–28, outs on earlier month days would break;
        # use Gehalt last, so day ordering: outs on 2–20, Gehalt on 25–28.
        miete_dt = _month_datetime(year, month, random.randint(1, 3), 8, random.randint(0, 45))
        telefon_dt = _month_datetime(year, month, random.randint(10, 15), 9, random.randint(0, 50))
        fitness_dt = _month_datetime(year, month, random.randint(5, 9), 10, random.randint(0, 55))
        auto_dt = _month_datetime(year, month, random.randint(4, 8), 11, random.randint(0, 40))
        gehalt_dt = _month_datetime(year, month, random.randint(25, 28), 14, random.randint(0, 59))

        def append_out(dt: datetime, amt: Decimal, ref: str) -> None:
            nonlocal n
            n += 1
            tx = _InternalTx()
            tx.account_id = account_id
            tx.tx_id = f"TX-{account_id}-{n:04d}"
            tx.dt = dt
            tx.currency = currency
            tx.is_trigger = False
            _set_coherent_profile(tx, _sepa_transfer_out_profile())
            tx.direction = "out"
            tx.planned_amount = amt
            tx.payment_reference_preset = ref
            _fill_counterparty_non_cash(tx)
            txs.append(tx)

        append_out(
            miete_dt,
            budget.miete,
            f"Miete {month_de} {year}",
        )
        append_out(
            telefon_dt,
            budget.telefon,
            f"Telefonrechnung {month_de} {year}",
        )
        append_out(
            fitness_dt,
            budget.fitness,
            f"Fitnessstudio-Mitgliedschaft {month_de}",
        )
        append_out(
            auto_dt,
            budget.auto_rate,
            f"Fahrzeug-Leasing Rate {month_de} – {fake.numerify(text='####')}",
        )

        n += 1
        tx_in = _InternalTx()
        tx_in.account_id = account_id
        tx_in.tx_id = f"TX-{account_id}-{n:04d}"
        tx_in.dt = gehalt_dt
        tx_in.currency = currency
        tx_in.is_trigger = False
        _set_coherent_profile(tx_in, _sepa_transfer_in_profile())
        tx_in.direction = "in"
        tx_in.planned_amount = budget.gehalt
        tx_in.payment_reference_preset = f"Gehalt {month_de} {year}"
        _fill_counterparty_non_cash(tx_in)
        txs.append(tx_in)

    return txs, n


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
    private_monthly_income: int | None = None,
) -> tuple[list[_InternalTx], float]:
    """Build the full 12-month transaction pool and return (pool, final_balance).

    1. Optional private recurring (Gehalt + Miete + bills) for each calendar month.
    2. Generate background transactions (12 months).
    3. Generate trigger transactions (within alert window).
    4. Merge, sort chronologically (inflows before outflows same timestamp), walk balance.
    """

    pool: list[_InternalTx] = []
    tx_counter = 0
    recurring: list[_InternalTx] = []
    if private_monthly_income is not None:
        budget = _private_recurring_amounts(private_monthly_income)
        recurring, tx_counter = _build_private_recurring_txs(
            account_id, currency, budget, tx_counter,
        )

    # -- Background transactions (spread over 12 months) --------------------
    if num_background is None:
        num_background = random.randint(40, 80)
    if recurring:
        num_background = max(15, num_background - len(recurring))
    pool.extend(recurring)

    for _ in range(num_background):
        tx_counter += 1
        tx = _InternalTx()
        tx.account_id = account_id
        tx.tx_id = f"TX-{account_id}-{tx_counter:04d}"
        tx.dt = _random_dt_between(HISTORY_START, GENERATION_NOW)
        tx.currency = currency
        tx.is_trigger = False
        _assign_profile_and_direction_for_new_tx(tx, _pick_weighted_coherent_profile())
        if _is_cash_tx_type(tx.tx_type):
            tx.cp_name = COUNTERPARTY_NA
            tx.cp_iban = COUNTERPARTY_NA
            tx.cp_bic = COUNTERPARTY_NA
            tx.cp_bank = COUNTERPARTY_NA
            tx.cp_country = COUNTERPARTY_NA
        else:
            _fill_counterparty_non_cash(tx)

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
        # Shape by alert type (coherent type / rail / channel)
        if alert_type == "structuring":
            tx.dt = _random_dt_between(ALERT_WINDOW_START, ALERT_WINDOW_END)
            sepa_out = [p for p in _COHERENT_OUTBOUND if p[0] == "transfer" and p[1] == "SEPA_CT"]
            _set_coherent_profile(tx, random.choice(sepa_out))
            tx.direction = "out"
            # Amount set during balance walk (8000–9500)
        elif alert_type == "velocity":
            base = _random_dt_between(ALERT_WINDOW_START, ALERT_WINDOW_START + timedelta(days=20))
            tx.dt = base + timedelta(hours=i * 2)
            _assign_profile_and_direction_for_new_tx(tx, _pick_weighted_coherent_profile())
        elif alert_type == "high_risk_country":
            tx.dt = _random_dt_between(ALERT_WINDOW_START, ALERT_WINDOW_END)
            wire_out = [p for p in _COHERENT_OUTBOUND if p[0] == "wire"]
            _set_coherent_profile(tx, random.choice(wire_out))
            tx.direction = "out"
            tx.cp_country = random.choice(list(HIGH_RISK_COUNTRIES))
            tx.cp_iban = tx.cp_country + _faker_locale(
                random.choice(_EUR_FAKER_LOCALES),
            ).bothify(text="####################")
        elif alert_type == "large_single_transaction":
            tx.dt = _random_dt_between(ALERT_WINDOW_START, ALERT_WINDOW_END)
            tx.direction = random.choice(["in", "out"])
            if tx.direction == "in":
                wire_in = [p for p in _COHERENT_INBOUND if p[0] == "wire"]
                _set_coherent_profile(tx, random.choice(wire_in))
            else:
                wire_out = [p for p in _COHERENT_OUTBOUND if p[0] == "wire"]
                _set_coherent_profile(tx, random.choice(wire_out))
        else:  # unusual_pattern
            tx.dt = _random_dt_between(ALERT_WINDOW_START, ALERT_WINDOW_END)
            _assign_profile_and_direction_for_new_tx(tx, _pick_weighted_coherent_profile())

        if alert_type != "high_risk_country":
            if _is_cash_tx_type(tx.tx_type):
                tx.cp_name = COUNTERPARTY_NA
                tx.cp_iban = COUNTERPARTY_NA
                tx.cp_bic = COUNTERPARTY_NA
                tx.cp_bank = COUNTERPARTY_NA
                tx.cp_country = COUNTERPARTY_NA
            else:
                _fill_counterparty_non_cash(tx)
        else:
            hf = _faker_locale(random.choice(_EUR_FAKER_LOCALES))
            tx.cp_name = hf.company() if random.random() > 0.3 else hf.name()
            tx.cp_bic = _bic_for_country_code(tx.cp_country)
            tx.cp_bank = hf.company() + " Bank"

        trigger_txs.append(tx)

    pool.extend(trigger_txs)

    # -- Sort chronologically; inbound before outbound when timestamps tie --------
    pool.sort(key=lambda t: (t.dt, 0 if t.direction == "in" else 1))

    balance = _money_dec(opening_balance)
    for tx in pool:
        amt: Decimal
        if tx.planned_amount is not None:
            if tx.direction == "in":
                amt = tx.planned_amount
                balance = balance + amt
            else:
                max_out = balance
                if max_out <= 0:
                    # Never emit 0-amount outbound (private recurring): credit planned amount as inbound.
                    tx.direction = "in"
                    tx.payment_reference_preset = None
                    amt = tx.planned_amount
                    balance = balance + amt
                else:
                    amt = min(tx.planned_amount, max_out)
                    balance = balance - amt
            tx.amount = _money_float(amt)
            tx.balance_after = _money_float(balance)
            continue

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

    for tx in pool:
        _coherence_repair_rail_vs_direction(tx)

    for tx in pool:
        if _is_cash_tx_type(tx.tx_type):
            tx.cp_name = COUNTERPARTY_NA
            tx.cp_iban = COUNTERPARTY_NA
            tx.cp_bic = COUNTERPARTY_NA
            tx.cp_bank = COUNTERPARTY_NA
            tx.cp_country = COUNTERPARTY_NA
        elif tx.cp_name == COUNTERPARTY_NA or tx.cp_iban == COUNTERPARTY_NA:
            _fill_counterparty_non_cash(tx)

    # -- Assign payment references (after balance walk, direction is final) --
    for tx in pool:
        if tx.payment_reference_preset:
            tx.payment_reference = tx.payment_reference_preset
        else:
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
        payment_rail=tx.payment_rail,
        booking_channel=tx.booking_channel,
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

def generate_alert(alert_index: int, *, use_public_figure: bool = False) -> Alert:
    """Generate one full alert conforming to the 132-field schema."""
    alert_id = f"ALT-{alert_index:05d}"
    customer_id = f"CUST-{alert_index:05d}-{random.randint(100000, 999999)}"
    account_id = f"ACC-{alert_index:05d}-{random.randint(1000000, 9999999)}"

    alert_type = ALERT_TYPES[alert_index % len(ALERT_TYPES)]
    status = random.choice(ALERT_STATUSES)
    created_at_dt = _random_dt_between(ALERT_WINDOW_START, ALERT_WINDOW_END)
    created_at = created_at_dt.isoformat()
    # Customer
    if use_public_figure:
        n_pf = len(VERIFIED_PUBLIC_FIGURES)
        pf = VERIFIED_PUBLIC_FIGURES[(alert_index * 7919 + n_pf) % n_pf]
        profile = generate_customer_profile(customer_id, verified_figure_row=pf)
    else:
        profile = generate_customer_profile(customer_id)

    # Rules
    rules = generate_rules_triggered(alert_type)

    currency = ACCOUNT_CURRENCY

    # Number of trigger transactions
    num_trigger = 1 if alert_type == "large_single_transaction" else random.randint(2, 5)

    # Opening balance (from employment, risk, industry)
    opening_balance = _opening_balance_for_profile(
        profile.employment_status,
        profile.customer_risk_rating,
        profile.industry,
        secondary=False,
    )

    private_income = (
        profile.expected_monthly_income if profile.customer_type == "private" else None
    )
    pool, final_balance = _generate_tx_pool(
        account_id,
        currency,
        opening_balance,
        alert_type,
        num_trigger,
        private_monthly_income=private_income,
    )

    # Account summary (balance = final running balance)
    primary_account = generate_account_summary(account_id, currency, final_balance)
    account_summaries = [primary_account]

    # Secondary account (30% chance) — background transactions only
    secondary_pool: list[_InternalTx] = []
    if random.random() < 0.3:
        extra_id = f"ACC-{alert_index:05d}-{random.randint(2000000, 2999999)}"
        extra_opening = _opening_balance_for_profile(
            profile.employment_status,
            profile.customer_risk_rating,
            profile.industry,
            secondary=True,
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
