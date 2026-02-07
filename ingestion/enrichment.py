"""Phase 5 -- Clause enrichment via GPT.

For every clause chunk, call the LLM with structured context and request a
strict JSON output describing the clause's canonical section, change action,
extracted facts, and confidence.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from db.models import ClauseNode
from ingestion.clause import ClauseChunk
from integrations.openai_client import chat_json
from utils.normalization import parse_date
from utils.validation import ClauseEnrichmentOutput, validate_enrichment_output

import config

logger = logging.getLogger(__name__)


# ── System prompt ─────────────────────────────────────────────────────────

_ENRICHMENT_SYSTEM_PROMPT = """\
You are a contract clause analysis assistant. You receive a single clause
chunk from a contract document along with contextual metadata. Your job is
to extract structured information about this clause.

Rules:
- canonical_section_id: the section number this clause belongs to (e.g., "5.3").
  If there is no explicit number, use "semantic:<normalized_title>".
- referenced_section_id: if this clause references a section from a base
  agreement (e.g., "Section 5.3 of the Agreement"), include the reference.
- change_action: one of REPLACE, APPEND, ADD_NEW, DELETE, NO_CHANGE.
  - REPLACE: this clause replaces an existing section entirely.
  - APPEND: this clause adds to an existing section.
  - ADD_NEW: this is a brand-new section not in the base agreement.
  - DELETE: this clause deletes an existing section.
  - NO_CHANGE: this is a standard clause with no modification semantics.
- modifies_section_id: the section being modified (if different from
  canonical_section_id).
- effective_ts: a specific effective date for this clause if stated
  (format: YYYY-MM-DD), otherwise null (inherits the document effective date).
- extracted_facts: key factual data (liability caps, amounts, percentages,
  durations, etc.) as a JSON object.
- confidence: your confidence in the extraction (0.0 to 1.0).

Return ONLY valid JSON:
{
  "canonical_section_id": "...",
  "referenced_section_id": "..." or null,
  "change_action": "REPLACE|APPEND|ADD_NEW|DELETE|NO_CHANGE",
  "modifies_section_id": "..." or null,
  "effective_ts": "YYYY-MM-DD" or null,
  "extracted_facts": { ... } or null,
  "confidence": 0.0-1.0
}
"""


def _build_enrichment_prompt(
    chunk: ClauseChunk,
    doc_type: Optional[str],
    doc_version: Optional[int],
    doc_effective_ts: Optional[datetime],
    referenced_text: Optional[str] = None,
) -> str:
    """Build the user prompt for clause enrichment."""
    parts: list[str] = []
    parts.append(f"Heading: {chunk.heading}")
    parts.append(f"Clause text:\n{chunk.text}")
    if doc_type:
        parts.append(f"Document type: {doc_type}")
    if doc_version is not None:
        parts.append(f"Document version: {doc_version}")
    if doc_effective_ts:
        parts.append(f"Document effective date: {doc_effective_ts.strftime('%Y-%m-%d')}")
    if referenced_text:
        parts.append(f"Referenced section text (from base agreement):\n{referenced_text}")
    return "\n\n".join(parts)


@dataclass
class EnrichedClause:
    """Result of enriching a single clause chunk."""
    chunk: ClauseChunk
    canonical_section_id: str
    referenced_section_id: Optional[str]
    change_action: Optional[str]
    modifies_section_id: Optional[str]
    effective_ts: Optional[datetime]
    extracted_facts: Optional[dict[str, Any]]
    confidence: float


from dataclasses import dataclass


def enrich_clause(
    chunk: ClauseChunk,
    doc_type: Optional[str] = None,
    doc_version: Optional[int] = None,
    doc_effective_ts: Optional[datetime] = None,
    referenced_text: Optional[str] = None,
) -> EnrichedClause:
    """Run LLM enrichment on a single clause chunk.

    If the LLM returns invalid JSON, retries once with expanded context.
    If confidence is below the threshold, logs a warning for HITL routing.
    """
    user_prompt = _build_enrichment_prompt(
        chunk, doc_type, doc_version, doc_effective_ts, referenced_text
    )

    raw: dict[str, Any] = chat_json(
        system_prompt=_ENRICHMENT_SYSTEM_PROMPT,
        user_prompt=user_prompt,
        max_retries=1,
    )

    validated: ClauseEnrichmentOutput = validate_enrichment_output(raw)

    # Use the LLM's canonical_section_id if it refined the chunk's tier-assigned one,
    # but fall back to the chunk's own assignment if the LLM returns nothing.
    canonical = validated.canonical_section_id or chunk.canonical_section_id

    effective = parse_date(validated.effective_ts) if validated.effective_ts else None

    result = EnrichedClause(
        chunk=chunk,
        canonical_section_id=canonical,
        referenced_section_id=validated.referenced_section_id or chunk.referenced_section_id,
        change_action=validated.change_action,
        modifies_section_id=validated.modifies_section_id,
        effective_ts=effective,
        extracted_facts=validated.extracted_facts,
        confidence=validated.confidence,
    )

    if result.confidence < config.ENRICHMENT_CONFIDENCE_THRESHOLD:
        logger.warning(
            "Clause enrichment confidence %.2f < threshold %.2f for section '%s' -- "
            "flagging for HITL / PageIndex fallback",
            result.confidence,
            config.ENRICHMENT_CONFIDENCE_THRESHOLD,
            canonical,
        )

    return result


def enrich_all_clauses(
    chunks: list[ClauseChunk],
    doc_type: Optional[str] = None,
    doc_version: Optional[int] = None,
    doc_effective_ts: Optional[datetime] = None,
) -> list[EnrichedClause]:
    """Enrich every clause chunk in sequence.

    TODO: consider async / batching for throughput.
    """
    results: list[EnrichedClause] = []
    for i, chunk in enumerate(chunks):
        logger.info("Enriching clause %d/%d: %s", i + 1, len(chunks), chunk.canonical_section_id or chunk.heading)
        enriched = enrich_clause(
            chunk,
            doc_type=doc_type,
            doc_version=doc_version,
            doc_effective_ts=doc_effective_ts,
        )
        results.append(enriched)
    return results
