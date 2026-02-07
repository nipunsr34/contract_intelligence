"""Phase 7 -- Materialize the current state per (family, section).

After each document ingestion, for every affected
``(family_id, canonical_section_id)`` pair, apply supersession rules and
write the result into ``family_section_current``.

Supersession rules
------------------
- REPLACE   -> latest replaces all prior text.
- APPEND    -> compose base text + all APPEND nodes in chronological order.
- DELETE    -> section is deleted (current_node_id = None, composed_text = None).
- ADD_NEW   -> treat as a new baseline for that section.
- NO_CHANGE -> keep existing baseline (this node carries forward unchanged).
- Restatement doc_type -> reset baseline entirely (treat restated doc as new base).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import and_
from sqlalchemy.orm import Session

from db.models import ClauseNode, ContractDocument, FamilySectionCurrent

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def materialize_section(
    family_id: str,
    canonical_section_id: str,
    session: Session,
) -> Optional[FamilySectionCurrent]:
    """Recompute the current state for one (family, section) pair.

    Returns the upserted ``FamilySectionCurrent`` row, or ``None`` if the
    section ends up deleted.
    """
    # Fetch all clause nodes for this (family, section), ordered by effective_ts.
    nodes: list[ClauseNode] = (
        session.query(ClauseNode)
        .filter(
            and_(
                ClauseNode.family_id == family_id,
                ClauseNode.canonical_section_id == canonical_section_id,
            )
        )
        .order_by(ClauseNode.effective_ts.asc().nullslast(), ClauseNode.created_at.asc())
        .all()
    )

    if not nodes:
        # No nodes for this section -- remove current if it exists.
        existing = session.get(
            FamilySectionCurrent, (family_id, canonical_section_id)
        )
        if existing:
            session.delete(existing)
            session.flush()
        return None

    # Check if any contributing document is a restatement (resets baseline).
    restatement_doc_ids: set[str] = set()
    for node in nodes:
        doc: ContractDocument | None = session.get(ContractDocument, node.doc_id)
        if doc and doc.doc_type == "restatement":
            restatement_doc_ids.add(doc.doc_id)

    # Walk through nodes chronologically and apply supersession.
    base_text: Optional[str] = None
    base_node_id: Optional[str] = None
    base_effective_ts: Optional[datetime] = None
    is_deleted = False
    append_chain: list[str] = []

    for node in nodes:
        action = (node.change_action or "NO_CHANGE").upper()

        # If this node belongs to a restatement doc, reset baseline.
        if node.doc_id in restatement_doc_ids:
            base_text = node.clause_text
            base_node_id = node.node_id
            base_effective_ts = node.effective_ts
            append_chain = []
            is_deleted = False
            continue

        if action == "REPLACE":
            base_text = node.clause_text
            base_node_id = node.node_id
            base_effective_ts = node.effective_ts
            append_chain = []
            is_deleted = False

        elif action == "APPEND":
            append_chain.append(node.clause_text)
            base_node_id = node.node_id
            base_effective_ts = node.effective_ts
            is_deleted = False

        elif action == "ADD_NEW":
            # New baseline (section didn't exist before).
            if base_text is None:
                base_text = node.clause_text
                base_node_id = node.node_id
                base_effective_ts = node.effective_ts
                is_deleted = False
            else:
                # Section already exists -- treat ADD_NEW as REPLACE for safety.
                base_text = node.clause_text
                base_node_id = node.node_id
                base_effective_ts = node.effective_ts
                append_chain = []
                is_deleted = False

        elif action == "DELETE":
            base_text = None
            base_node_id = None
            base_effective_ts = node.effective_ts
            append_chain = []
            is_deleted = True

        elif action == "NO_CHANGE":
            # Carry forward -- only set baseline if none exists yet.
            if base_text is None:
                base_text = node.clause_text
                base_node_id = node.node_id
                base_effective_ts = node.effective_ts
                is_deleted = False

    # Compose final text.
    if is_deleted:
        composed_text = None
        current_node_id = None
    else:
        parts = [base_text] if base_text else []
        parts.extend(append_chain)
        composed_text = "\n\n".join(parts) if parts else None
        current_node_id = base_node_id

    # Upsert into family_section_current.
    row = session.get(FamilySectionCurrent, (family_id, canonical_section_id))
    if row is None:
        if composed_text is None and current_node_id is None:
            return None  # Section is deleted or empty, nothing to store.
        row = FamilySectionCurrent(
            family_id=family_id,
            canonical_section_id=canonical_section_id,
        )
        session.add(row)

    row.current_node_id = current_node_id
    row.current_effective_ts = base_effective_ts
    row.composed_text = composed_text
    row.updated_at = _utcnow()
    session.flush()

    logger.info(
        "Materialized (%s, %s) -> node=%s deleted=%s",
        family_id,
        canonical_section_id,
        current_node_id,
        is_deleted,
    )
    return row


def materialize_family(family_id: str, session: Session) -> int:
    """Recompute current state for ALL sections in a family.

    Returns the number of sections materialized.
    """
    # Find all distinct canonical_section_ids for this family.
    section_ids: list[str] = [
        row[0]
        for row in (
            session.query(ClauseNode.canonical_section_id)
            .filter(ClauseNode.family_id == family_id)
            .distinct()
            .all()
        )
    ]

    count = 0
    for sid in section_ids:
        materialize_section(family_id, sid, session)
        count += 1

    logger.info("Materialized %d sections for family %s", count, family_id)
    return count


def materialize_affected_sections(
    family_id: str,
    affected_section_ids: list[str],
    session: Session,
) -> int:
    """Recompute current state only for the sections impacted by a new doc.

    Returns the number of sections materialized.
    """
    count = 0
    for sid in affected_section_ids:
        materialize_section(family_id, sid, session)
        count += 1

    logger.info(
        "Materialized %d affected sections for family %s", count, family_id
    )
    return count
