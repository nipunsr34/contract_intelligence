"""Phase 3 -- Family matching and document versioning.

A *family* groups contracts between the same normalized parties.  When a new
document is ingested we either match it to an existing family or create one,
then assign both an ingestion-order version and a timeline-order version.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from db.models import ContractDocument, Family
from ingestion.hashing import compute_family_id, compute_family_keys_hash

logger = logging.getLogger(__name__)


def match_or_create_family(
    partyA_norm: str,
    partyB_norm: str,
    session: Session,
) -> str:
    """Find an existing family for the two parties, or create a new one.

    Parameters
    ----------
    partyA_norm, partyB_norm : str
        Normalized party names (lowercase, no suffixes).
    session : Session
        Active SQLAlchemy session.

    Returns
    -------
    family_id : str
        The deterministic family ID.
    """
    keys_hash = compute_family_keys_hash(partyA_norm, partyB_norm)
    family_id = compute_family_id(keys_hash)

    existing: Family | None = (
        session.query(Family)
        .filter(Family.family_keys_hash == keys_hash)
        .first()
    )

    if existing is not None:
        logger.info("Matched existing family %s", existing.family_id)
        return existing.family_id

    # Create a new family.  Sort names so partyA < partyB deterministically.
    sorted_names = sorted([partyA_norm.strip().lower(), partyB_norm.strip().lower()])
    new_family = Family(
        family_id=family_id,
        family_keys_hash=keys_hash,
        partyA_norm=sorted_names[0],
        partyB_norm=sorted_names[1],
    )
    session.add(new_family)
    session.flush()
    logger.info("Created new family %s for parties %s", family_id, sorted_names)
    return family_id


def assign_doc_versions(
    doc_id: str,
    family_id: str,
    effective_ts: Optional[datetime],
    session: Session,
) -> tuple[int, int]:
    """Assign ingestion-order and timeline-order version numbers.

    Parameters
    ----------
    doc_id : str
        Document ID to update.
    family_id : str
        Family that this document belongs to.
    effective_ts : datetime or None
        Effective date of the document (used for timeline ordering).
    session : Session
        Active SQLAlchemy session.

    Returns
    -------
    (doc_version_ingest, doc_version_timeline)
    """
    # -- Ingestion order --
    max_ingest: int = (
        session.query(func.coalesce(func.max(ContractDocument.doc_version_ingest), 0))
        .filter(ContractDocument.family_id == family_id)
        .scalar()
    ) or 0
    doc_version_ingest = max_ingest + 1

    # -- Timeline order --
    # We recompute ranks across ALL docs in this family (handles out-of-order
    # arrival).  First update this doc's effective_ts so the ranking includes it.
    doc: ContractDocument | None = session.get(ContractDocument, doc_id)
    if doc is not None:
        doc.family_id = family_id
        doc.doc_version_ingest = doc_version_ingest
        doc.effective_ts = effective_ts
        session.flush()

    # Query all docs in the family sorted by effective_ts (nulls last).
    family_docs: list[ContractDocument] = (
        session.query(ContractDocument)
        .filter(ContractDocument.family_id == family_id)
        .order_by(
            ContractDocument.effective_ts.asc().nullslast(),
            ContractDocument.created_at.asc(),
        )
        .all()
    )

    doc_version_timeline = 0
    for rank, fd in enumerate(family_docs, start=1):
        fd.doc_version_timeline = rank
        if fd.doc_id == doc_id:
            doc_version_timeline = rank

    session.flush()

    logger.info(
        "doc %s: version_ingest=%d, version_timeline=%d",
        doc_id,
        doc_version_ingest,
        doc_version_timeline,
    )
    return doc_version_ingest, doc_version_timeline
