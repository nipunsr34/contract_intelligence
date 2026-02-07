"""Phase 2 -- Document-level metadata extraction.

Two-step approach:
1. Deterministic candidate extraction via regex (cheap + robust).
2. LLM "resolver" chooses final values from the candidate list.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from integrations.azure_docint import LayoutBlock
from integrations.openai_client import chat_json
from utils.normalization import (
    extract_date_strings,
    normalize_party_name,
    parse_date,
)
from utils.validation import MetadataResolverOutput, validate_metadata_output

import config

logger = logging.getLogger(__name__)


# ── Data structures ────────────────────────────────────────────────────────

@dataclass
class EvidenceSpan:
    """A text snippet with its provenance."""
    text: str
    page: int
    span_start: int
    span_end: int


@dataclass
class MetadataCandidates:
    """All regex-extracted candidates for document-level metadata."""
    party_candidates: list[EvidenceSpan] = field(default_factory=list)
    date_candidates: list[EvidenceSpan] = field(default_factory=list)
    term_candidates: list[EvidenceSpan] = field(default_factory=list)


@dataclass
class ResolvedMetadata:
    """Final metadata chosen by the LLM resolver."""
    effective_ts: Optional[datetime] = None
    effective_date_source: Optional[str] = None
    partyA_norm: Optional[str] = None
    partyB_norm: Optional[str] = None
    term_start_ts: Optional[datetime] = None
    term_end_ts: Optional[datetime] = None
    doc_type: Optional[str] = None
    confidence: float = 0.0


# ── Step 2.1  Deterministic candidate extraction ──────────────────────────

# Party patterns
_BETWEEN_AND = re.compile(
    r"(?:between|by and between)\s+(.+?)\s+(?:and|&)\s+(.+?)(?:\.|,|\()",
    re.IGNORECASE | re.DOTALL,
)
_PARTY_LABEL = re.compile(
    r"(?:Party\s*[AB12]|Client|Vendor|Supplier|Licensee|Licensor|Contractor|Company|Customer)"
    r"\s*[:=]\s*(.+?)(?:\n|$)",
    re.IGNORECASE,
)

# Date cue phrases
_DATE_CUE = re.compile(
    r"(?:Effective\s+Date|dated\s+as\s+of|commencement\s+date|latest\s+of\s+(?:the\s+)?signature\s+dates?)"
    r"\s*[:=]?\s*(.+?)(?:\.|;|\n|$)",
    re.IGNORECASE,
)

# Term patterns
_TERM_CUE = re.compile(
    r"(?:Initial\s+Term|Term|Renewal|Termination(?:\s+for\s+Convenience)?)"
    r"\s*[:=]?\s*(.+?)(?:\.|;|\n|$)",
    re.IGNORECASE,
)


def _full_text_from_blocks(blocks: list[LayoutBlock]) -> str:
    """Concatenate all block texts into one string."""
    return "\n".join(b.text for b in blocks)


def extract_candidates(blocks: list[LayoutBlock]) -> MetadataCandidates:
    """Extract regex-based candidates for parties, dates, and term.

    Each candidate stores the evidence snippet with its provenance (page/span).
    """
    candidates = MetadataCandidates()

    for block in blocks:
        text = block.text

        # -- Parties --
        for m in _BETWEEN_AND.finditer(text):
            for group_text in [m.group(1).strip(), m.group(2).strip()]:
                candidates.party_candidates.append(
                    EvidenceSpan(
                        text=group_text,
                        page=block.page,
                        span_start=block.span_start + m.start(),
                        span_end=block.span_start + m.end(),
                    )
                )
        for m in _PARTY_LABEL.finditer(text):
            candidates.party_candidates.append(
                EvidenceSpan(
                    text=m.group(1).strip(),
                    page=block.page,
                    span_start=block.span_start + m.start(),
                    span_end=block.span_start + m.end(),
                )
            )

        # -- Dates --
        for m in _DATE_CUE.finditer(text):
            cue_text = m.group(0).strip()
            candidates.date_candidates.append(
                EvidenceSpan(
                    text=cue_text,
                    page=block.page,
                    span_start=block.span_start + m.start(),
                    span_end=block.span_start + m.end(),
                )
            )
        # Also grab standalone date strings
        for date_str in extract_date_strings(text):
            candidates.date_candidates.append(
                EvidenceSpan(
                    text=date_str,
                    page=block.page,
                    span_start=block.span_start,
                    span_end=block.span_end,
                )
            )

        # -- Term --
        for m in _TERM_CUE.finditer(text):
            candidates.term_candidates.append(
                EvidenceSpan(
                    text=m.group(0).strip(),
                    page=block.page,
                    span_start=block.span_start + m.start(),
                    span_end=block.span_start + m.end(),
                )
            )

    return candidates


# ── Step 2.2  LLM Resolver ────────────────────────────────────────────────

_RESOLVER_SYSTEM_PROMPT = """\
You are a contract metadata extraction assistant. Given candidate extractions
from a contract document, choose the correct final values.

Rules:
- effective_ts: the date the contract becomes effective. If the contract says
  "effective date = latest of signature dates", pick the latest date you see.
- partyA_norm / partyB_norm: the two primary contracting parties (normalized
  names, no LLC/Inc/Ltd suffixes).
- doc_type: one of master, amendment, restatement, sow, addendum.  Infer from
  context (e.g., "Amendment No. 2" → amendment).
- term_start_ts / term_end_ts: start and end of the contract term.
- confidence: your confidence in the overall extraction (0.0 to 1.0).

Return ONLY valid JSON with these keys:
{
  "effective_ts": "YYYY-MM-DD" or null,
  "effective_date_source": "description of evidence",
  "partyA_norm": "normalized name" or null,
  "partyB_norm": "normalized name" or null,
  "term_start_ts": "YYYY-MM-DD" or null,
  "term_end_ts": "YYYY-MM-DD" or null,
  "doc_type": "master|amendment|restatement|sow|addendum" or null,
  "confidence": 0.0-1.0
}
"""


def _build_resolver_prompt(candidates: MetadataCandidates) -> str:
    """Build the user prompt for the LLM resolver."""
    sections: list[str] = []

    if candidates.party_candidates:
        lines = [f"  - \"{c.text}\" (page {c.page})" for c in candidates.party_candidates]
        sections.append("Party candidates:\n" + "\n".join(lines))

    if candidates.date_candidates:
        lines = [f"  - \"{c.text}\" (page {c.page})" for c in candidates.date_candidates]
        sections.append("Date candidates:\n" + "\n".join(lines))

    if candidates.term_candidates:
        lines = [f"  - \"{c.text}\" (page {c.page})" for c in candidates.term_candidates]
        sections.append("Term candidates:\n" + "\n".join(lines))

    if not sections:
        sections.append("No candidates were extracted from this document.")

    return "\n\n".join(sections)


def resolve_metadata(
    blocks: list[LayoutBlock],
    candidates: MetadataCandidates | None = None,
) -> ResolvedMetadata:
    """Run the two-step metadata extraction pipeline.

    1. Extract candidates (if not supplied).
    2. Call the LLM resolver.
    3. Validate and return.
    """
    if candidates is None:
        candidates = extract_candidates(blocks)

    user_prompt = _build_resolver_prompt(candidates)

    raw_output: dict[str, Any] = chat_json(
        system_prompt=_RESOLVER_SYSTEM_PROMPT,
        user_prompt=user_prompt,
    )

    validated: MetadataResolverOutput = validate_metadata_output(raw_output)

    # Convert string dates to datetime
    effective = parse_date(validated.effective_ts) if validated.effective_ts else None
    term_start = parse_date(validated.term_start_ts) if validated.term_start_ts else None
    term_end = parse_date(validated.term_end_ts) if validated.term_end_ts else None

    result = ResolvedMetadata(
        effective_ts=effective,
        effective_date_source=validated.effective_date_source,
        partyA_norm=normalize_party_name(validated.partyA_norm) if validated.partyA_norm else None,
        partyB_norm=normalize_party_name(validated.partyB_norm) if validated.partyB_norm else None,
        term_start_ts=term_start,
        term_end_ts=term_end,
        doc_type=validated.doc_type,
        confidence=validated.confidence,
    )

    if result.confidence < config.METADATA_CONFIDENCE_THRESHOLD:
        logger.warning(
            "Metadata confidence %.2f is below threshold %.2f -- flagging for HITL",
            result.confidence,
            config.METADATA_CONFIDENCE_THRESHOLD,
        )

    return result
