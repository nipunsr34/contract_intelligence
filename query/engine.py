"""Phase 8 -- Time-travel query engine.

Supports four scenarios, all backed by SQL-first logic:

1. CURRENT      -- "What is the liability cap as of today?"
2. HISTORY      -- "How has section 5.3 changed over time?"
3. CHANGE_TRACK -- "What did Amendment 2 change in section 5.3?"
4. AS_OF        -- "What was section 5.3 on 2023-06-01?"
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import and_
from sqlalchemy.orm import Session

import config as app_config
from db.models import (
    ClauseNode,
    ContractDocument,
    Family,
    FamilySectionCurrent,
)
from materialization.current_state import materialize_section

logger = logging.getLogger(__name__)


# ── Result types ──────────────────────────────────────────────────────────

@dataclass
class QueryResult:
    """Generic query result."""
    scenario: str
    family_id: str
    canonical_section_id: str
    data: Any = None
    error: Optional[str] = None


@dataclass
class TimelineEntry:
    """One entry in a clause's change timeline."""
    doc_version: Optional[int]
    doc_type: Optional[str]
    change_action: Optional[str]
    effective_ts: Optional[datetime]
    clause_text: str
    extracted_facts: Optional[dict[str, Any]] = None
    confidence: Optional[float] = None


# ── Family resolution ─────────────────────────────────────────────────────

def _resolve_family(
    session: Session,
    family_id: Optional[str] = None,
    party_a: Optional[str] = None,
    party_b: Optional[str] = None,
) -> Optional[Family]:
    """Find a family by ID or by normalized party names."""
    if family_id:
        return session.get(Family, family_id)
    if party_a and party_b:
        from ingestion.hashing import compute_family_keys_hash

        keys_hash = compute_family_keys_hash(party_a, party_b)
        return (
            session.query(Family)
            .filter(Family.family_keys_hash == keys_hash)
            .first()
        )
    return None


def _resolve_section_id(
    session: Session,
    family_id: str,
    section_query: str,
) -> str:
    """Resolve a section query to a ``canonical_section_id``.

    First tries exact match, then tries ``semantic:`` prefix, then fuzzy.
    """
    # Exact match.
    exists = (
        session.query(ClauseNode)
        .filter(
            and_(
                ClauseNode.family_id == family_id,
                ClauseNode.canonical_section_id == section_query,
            )
        )
        .first()
    )
    if exists:
        return section_query

    # Try semantic prefix.
    semantic_id = f"semantic:{section_query.lower().replace(' ', '_')}"
    exists = (
        session.query(ClauseNode)
        .filter(
            and_(
                ClauseNode.family_id == family_id,
                ClauseNode.canonical_section_id == semantic_id,
            )
        )
        .first()
    )
    if exists:
        return semantic_id

    # Fuzzy: find sections whose ID contains the query string.
    candidates = (
        session.query(ClauseNode.canonical_section_id)
        .filter(
            and_(
                ClauseNode.family_id == family_id,
                ClauseNode.canonical_section_id.contains(section_query),
            )
        )
        .distinct()
        .all()
    )
    if candidates:
        return candidates[0][0]

    # Nothing found -- return the original query as-is.
    return section_query


# ── Scenario 1: CURRENT ──────────────────────────────────────────────────

def query_current(
    session: Session,
    section: str,
    family_id: Optional[str] = None,
    party_a: Optional[str] = None,
    party_b: Optional[str] = None,
) -> QueryResult:
    """Return the current text for a section in a family.

    Reads directly from ``family_section_current`` for O(1) lookup.
    """
    family = _resolve_family(session, family_id, party_a, party_b)
    if not family:
        return QueryResult(
            scenario="CURRENT",
            family_id=family_id or "",
            canonical_section_id=section,
            error="Family not found.",
        )

    fid = family.family_id
    resolved_section = _resolve_section_id(session, fid, section)

    current: FamilySectionCurrent | None = session.get(
        FamilySectionCurrent, (fid, resolved_section)
    )
    if not current:
        return QueryResult(
            scenario="CURRENT",
            family_id=fid,
            canonical_section_id=resolved_section,
            error=f"No current state found for section '{resolved_section}'.",
        )

    return QueryResult(
        scenario="CURRENT",
        family_id=fid,
        canonical_section_id=resolved_section,
        data={
            "composed_text": current.composed_text,
            "current_node_id": current.current_node_id,
            "current_effective_ts": (
                current.current_effective_ts.isoformat()
                if current.current_effective_ts
                else None
            ),
            "updated_at": (
                current.updated_at.isoformat() if current.updated_at else None
            ),
        },
    )


# ── Scenario 2: HISTORY ──────────────────────────────────────────────────

def query_history(
    session: Session,
    section: str,
    family_id: Optional[str] = None,
    party_a: Optional[str] = None,
    party_b: Optional[str] = None,
) -> QueryResult:
    """Return the full change timeline for a section."""
    family = _resolve_family(session, family_id, party_a, party_b)
    if not family:
        return QueryResult(
            scenario="HISTORY",
            family_id=family_id or "",
            canonical_section_id=section,
            error="Family not found.",
        )

    fid = family.family_id
    resolved_section = _resolve_section_id(session, fid, section)

    nodes: list[ClauseNode] = (
        session.query(ClauseNode)
        .filter(
            and_(
                ClauseNode.family_id == fid,
                ClauseNode.canonical_section_id == resolved_section,
            )
        )
        .order_by(ClauseNode.effective_ts.asc().nullslast())
        .all()
    )

    if not nodes:
        return QueryResult(
            scenario="HISTORY",
            family_id=fid,
            canonical_section_id=resolved_section,
            error=f"No history found for section '{resolved_section}'.",
        )

    timeline: list[dict[str, Any]] = []
    for node in nodes:
        doc: ContractDocument | None = session.get(ContractDocument, node.doc_id)
        entry = {
            "doc_version": doc.doc_version_timeline if doc else None,
            "doc_type": doc.doc_type if doc else None,
            "change_action": node.change_action,
            "effective_ts": node.effective_ts.isoformat() if node.effective_ts else None,
            "clause_text": node.clause_text,
            "extracted_facts": (
                json.loads(node.extracted_facts_json)
                if node.extracted_facts_json
                else None
            ),
            "confidence": node.confidence,
        }
        timeline.append(entry)

    return QueryResult(
        scenario="HISTORY",
        family_id=fid,
        canonical_section_id=resolved_section,
        data={"timeline": timeline, "total_versions": len(timeline)},
    )


# ── Scenario 3: CHANGE TRACKING ──────────────────────────────────────────

def query_changes(
    session: Session,
    section: str,
    doc_version: int,
    family_id: Optional[str] = None,
    party_a: Optional[str] = None,
    party_b: Optional[str] = None,
) -> QueryResult:
    """Return what a specific version changed in a section."""
    family = _resolve_family(session, family_id, party_a, party_b)
    if not family:
        return QueryResult(
            scenario="CHANGE_TRACK",
            family_id=family_id or "",
            canonical_section_id=section,
            error="Family not found.",
        )

    fid = family.family_id
    resolved_section = _resolve_section_id(session, fid, section)

    # Find the doc with this version.
    doc: ContractDocument | None = (
        session.query(ContractDocument)
        .filter(
            and_(
                ContractDocument.family_id == fid,
                ContractDocument.doc_version_timeline == doc_version,
            )
        )
        .first()
    )
    if not doc:
        return QueryResult(
            scenario="CHANGE_TRACK",
            family_id=fid,
            canonical_section_id=resolved_section,
            error=f"No document found with version {doc_version} in this family.",
        )

    nodes: list[ClauseNode] = (
        session.query(ClauseNode)
        .filter(
            and_(
                ClauseNode.doc_id == doc.doc_id,
                ClauseNode.canonical_section_id == resolved_section,
            )
        )
        .all()
    )

    if not nodes:
        return QueryResult(
            scenario="CHANGE_TRACK",
            family_id=fid,
            canonical_section_id=resolved_section,
            data={
                "doc_version": doc_version,
                "doc_type": doc.doc_type,
                "changes": [],
                "message": f"Version {doc_version} does not modify section '{resolved_section}'.",
            },
        )

    changes = []
    for node in nodes:
        changes.append({
            "change_action": node.change_action,
            "clause_text": node.clause_text,
            "modifies_section_id": node.modifies_section_id,
            "referenced_section_id": node.referenced_section_id,
            "effective_ts": node.effective_ts.isoformat() if node.effective_ts else None,
            "extracted_facts": (
                json.loads(node.extracted_facts_json)
                if node.extracted_facts_json
                else None
            ),
            "confidence": node.confidence,
        })

    return QueryResult(
        scenario="CHANGE_TRACK",
        family_id=fid,
        canonical_section_id=resolved_section,
        data={
            "doc_version": doc_version,
            "doc_type": doc.doc_type,
            "changes": changes,
        },
    )


# ── Scenario 4: AS-OF DATE ───────────────────────────────────────────────

def query_as_of(
    session: Session,
    section: str,
    as_of_date: datetime,
    family_id: Optional[str] = None,
    party_a: Optional[str] = None,
    party_b: Optional[str] = None,
) -> QueryResult:
    """Return the section state as of a specific date.

    Re-applies supersession rules using only nodes with
    ``effective_ts <= as_of_date``.
    """
    family = _resolve_family(session, family_id, party_a, party_b)
    if not family:
        return QueryResult(
            scenario="AS_OF",
            family_id=family_id or "",
            canonical_section_id=section,
            error="Family not found.",
        )

    fid = family.family_id
    resolved_section = _resolve_section_id(session, fid, section)

    nodes: list[ClauseNode] = (
        session.query(ClauseNode)
        .filter(
            and_(
                ClauseNode.family_id == fid,
                ClauseNode.canonical_section_id == resolved_section,
                ClauseNode.effective_ts <= as_of_date,
            )
        )
        .order_by(ClauseNode.effective_ts.asc().nullslast())
        .all()
    )

    if not nodes:
        return QueryResult(
            scenario="AS_OF",
            family_id=fid,
            canonical_section_id=resolved_section,
            error=f"No data for section '{resolved_section}' as of {as_of_date.date()}.",
        )

    # Re-apply supersession rules (same logic as materialization, inlined).
    restatement_doc_ids: set[str] = set()
    for node in nodes:
        doc = session.get(ContractDocument, node.doc_id)
        if doc and doc.doc_type == "restatement":
            restatement_doc_ids.add(doc.doc_id)

    base_text: Optional[str] = None
    base_node_id: Optional[str] = None
    base_effective: Optional[datetime] = None
    is_deleted = False
    append_chain: list[str] = []

    for node in nodes:
        action = (node.change_action or "NO_CHANGE").upper()

        if node.doc_id in restatement_doc_ids:
            base_text = node.clause_text
            base_node_id = node.node_id
            base_effective = node.effective_ts
            append_chain = []
            is_deleted = False
            continue

        if action == "REPLACE":
            base_text = node.clause_text
            base_node_id = node.node_id
            base_effective = node.effective_ts
            append_chain = []
            is_deleted = False
        elif action == "APPEND":
            append_chain.append(node.clause_text)
            base_node_id = node.node_id
            base_effective = node.effective_ts
            is_deleted = False
        elif action == "ADD_NEW":
            base_text = node.clause_text
            base_node_id = node.node_id
            base_effective = node.effective_ts
            append_chain = []
            is_deleted = False
        elif action == "DELETE":
            base_text = None
            base_node_id = None
            base_effective = node.effective_ts
            append_chain = []
            is_deleted = True
        elif action == "NO_CHANGE":
            if base_text is None:
                base_text = node.clause_text
                base_node_id = node.node_id
                base_effective = node.effective_ts

    if is_deleted:
        return QueryResult(
            scenario="AS_OF",
            family_id=fid,
            canonical_section_id=resolved_section,
            data={
                "as_of_date": as_of_date.isoformat(),
                "status": "DELETED",
                "composed_text": None,
            },
        )

    parts = [base_text] if base_text else []
    parts.extend(append_chain)
    composed = "\n\n".join(parts) if parts else None

    return QueryResult(
        scenario="AS_OF",
        family_id=fid,
        canonical_section_id=resolved_section,
        data={
            "as_of_date": as_of_date.isoformat(),
            "status": "ACTIVE",
            "composed_text": composed,
            "current_node_id": base_node_id,
            "effective_ts": base_effective.isoformat() if base_effective else None,
        },
    )


# ── Fallback B: PageIndex retriever fallback ──────────────────────────────

def query_with_pageindex_fallback(
    session: Session,
    section: str,
    pdf_path: str,
    family_id: Optional[str] = None,
    party_a: Optional[str] = None,
    party_b: Optional[str] = None,
) -> QueryResult:
    """Scenario 1 with PageIndex fallback.

    First tries the normal SQL-based ``query_current``.  If that fails (no
    result found), and PageIndex is enabled, falls back to PageIndex
    tree-search to locate the relevant section pages, then runs GPT
    extraction on the narrowed context.

    This is especially useful for:
    - Sections with semantic IDs that don't match the query string.
    - Documents where the clause segmentation missed a section.
    - Natural-language section queries (e.g., "limitation of liability").
    """
    # Try SQL first.
    result = query_current(session, section, family_id=family_id, party_a=party_a, party_b=party_b)
    if result.error is None:
        return result

    # SQL miss -- try PageIndex if enabled.
    if not app_config.PAGEINDEX_ENABLED:
        return result

    logger.info(
        "SQL lookup missed for '%s' -- trying PageIndex retrieval fallback", section
    )

    try:
        from integrations.pageindex_stub import retrieve_with_pageindex

        retrieved = retrieve_with_pageindex(
            pdf_path,
            section,
            model=app_config.OPENAI_MODEL,
            top_k=app_config.PAGEINDEX_RETRIEVAL_TOP_K,
        )

        if not retrieved:
            return QueryResult(
                scenario="CURRENT_PAGEINDEX_FALLBACK",
                family_id=family_id or "",
                canonical_section_id=section,
                error="PageIndex tree search also found no matching sections.",
            )

        # Use the top result.
        top = retrieved[0]

        # Optionally run GPT extraction on the narrowed context.
        extracted_text = top.text or ""
        if extracted_text:
            from integrations.openai_client import chat_json

            extraction = chat_json(
                system_prompt=(
                    "You are a contract clause extraction assistant. Extract "
                    "the specific clause or section text relevant to the query. "
                    "Return JSON: {\"extracted_text\": \"...\", \"confidence\": 0.0-1.0}"
                ),
                user_prompt=(
                    f"Query: {section}\n\n"
                    f"Document section (pages {top.start_page}-{top.end_page}):\n"
                    f"{extracted_text[:8000]}"
                ),
            )
            extracted_text = extraction.get("extracted_text", extracted_text)

        return QueryResult(
            scenario="CURRENT_PAGEINDEX_FALLBACK",
            family_id=family_id or "",
            canonical_section_id=section,
            data={
                "composed_text": extracted_text,
                "source": "pageindex_tree_search",
                "pageindex_node_id": top.node_id,
                "pageindex_title": top.title,
                "pages": f"{top.start_page}-{top.end_page}",
                "relevance_score": top.relevance_score,
            },
        )

    except Exception as exc:
        logger.error("PageIndex retrieval fallback failed: %s", exc)
        return QueryResult(
            scenario="CURRENT_PAGEINDEX_FALLBACK",
            family_id=family_id or "",
            canonical_section_id=section,
            error=f"PageIndex fallback error: {exc}",
        )


# ── Fallback C helper: Vision OCR re-ingestion ───────────────────────────

def reingest_with_vision_ocr(
    pdf_path: str,
    session: Session,
) -> Optional[str]:
    """Re-extract text from a scanned PDF using PageIndex vision OCR.

    If ``PAGEINDEX_ENABLED`` is true and the document quality is low, this
    function uses vision-model-based extraction (no traditional OCR) to get
    markdown text from the PDF page images, then returns it for re-processing.

    Parameters
    ----------
    pdf_path : str
        Path to the scanned PDF.
    session : Session
        Active SQLAlchemy session (for looking up doc quality).

    Returns
    -------
    str or None
        Extracted markdown text, or ``None`` if vision OCR is not available.
    """
    if not app_config.PAGEINDEX_ENABLED:
        logger.info("PageIndex not enabled -- skipping vision OCR")
        return None

    try:
        from integrations.pageindex_stub import ocr_with_pageindex

        text = ocr_with_pageindex(
            pdf_path,
            model=app_config.PAGEINDEX_VISION_MODEL,
        )
        return text

    except Exception as exc:
        logger.error("Vision OCR failed: %s", exc)
        return None
