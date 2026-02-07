"""Party-name normalization and date parsing utilities."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional


# ── Party name normalization ───────────────────────────────────────────────

# Suffixes that are semantically irrelevant for matching.
_ENTITY_SUFFIXES = re.compile(
    r"\b(LLC|L\.L\.C\.|Inc\.?|Incorporated|Ltd\.?|Limited|Corp\.?|Corporation"
    r"|LLP|L\.L\.P\.|LP|L\.P\.|PLC|P\.L\.C\.|GmbH|AG|SA|SAS|NV|BV"
    r"|Co\.?|Company|& Co\.?|Group|Holdings?|International|Intl\.?)\b",
    re.IGNORECASE,
)

_WHITESPACE = re.compile(r"\s+")
_PUNCT = re.compile(r"[^\w\s]")


def normalize_party_name(raw: str) -> str:
    """Normalize a party / company name for matching.

    Steps:
    1. Strip leading/trailing whitespace.
    2. Remove common entity suffixes (LLC, Inc, Ltd, ...).
    3. Remove remaining punctuation.
    4. Collapse whitespace.
    5. Lowercase.
    """
    name = raw.strip()
    name = _ENTITY_SUFFIXES.sub("", name)
    name = _PUNCT.sub(" ", name)
    name = _WHITESPACE.sub(" ", name).strip()
    return name.lower()


# ── Date parsing ───────────────────────────────────────────────────────────

# Common date formats found in contracts.
_DATE_FORMATS = [
    "%B %d, %Y",       # January 1, 2024
    "%b %d, %Y",       # Jan 1, 2024
    "%m/%d/%Y",         # 01/01/2024
    "%d/%m/%Y",         # 01/01/2024 (ambiguous, but included)
    "%Y-%m-%d",         # 2024-01-01
    "%m-%d-%Y",         # 01-01-2024
    "%d %B %Y",         # 1 January 2024
    "%d %b %Y",         # 1 Jan 2024
    "%B %d %Y",         # January 1 2024 (no comma)
    "%m.%d.%Y",         # 01.01.2024
    "%d.%m.%Y",         # 01.01.2024
]


def parse_date(raw: str) -> Optional[datetime]:
    """Attempt to parse *raw* using common contract date formats.

    Returns ``None`` if no format matches.
    """
    cleaned = raw.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return None


# Date-mention regex: captures most US / ISO date strings.
_DATE_PATTERN = re.compile(
    r"\b("
    # Month name DD, YYYY
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December"
    r"|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
    r"\.?\s+\d{1,2},?\s+\d{4}"
    r"|"
    # MM/DD/YYYY or DD/MM/YYYY
    r"\d{1,2}[/\-.]\d{1,2}[/\-.]\d{4}"
    r"|"
    # YYYY-MM-DD
    r"\d{4}-\d{2}-\d{2}"
    r")\b",
    re.IGNORECASE,
)


def extract_date_strings(text: str) -> list[str]:
    """Return all date-like strings found in *text*."""
    return _DATE_PATTERN.findall(text)
