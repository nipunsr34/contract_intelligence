"""Phase 1 -- Layout extraction orchestrator.

Runs Azure Document Intelligence on a PDF, stores the raw JSON on the
``contract_document`` row, and returns a list of ``LayoutBlock`` objects
for downstream processing.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy.orm import Session

from db.models import ContractDocument
from integrations.azure_docint import (
    LayoutBlock,
    analyze_layout,
    assess_quality,
    parse_layout_blocks,
)

logger = logging.getLogger(__name__)


def extract_layout(
    pdf_bytes: bytes,
    doc_id: str,
    session: Session,
) -> tuple[list[LayoutBlock], str]:
    """Run layout extraction and persist the raw output.

    Parameters
    ----------
    pdf_bytes : bytes
        Raw PDF content.
    doc_id : str
        Deterministic document ID (already inserted into ``contract_document``).
    session : Session
        Active SQLAlchemy session.

    Returns
    -------
    blocks : list[LayoutBlock]
        Parsed layout blocks.
    doc_quality : str
        ``"normal"`` or ``"low"`` -- indicates whether PageIndex fallback
        should be considered.
    """
    logger.info("Running Azure Document Intelligence layout extraction for doc %s", doc_id)

    raw_result: dict[str, Any] = analyze_layout(pdf_bytes)
    blocks: list[LayoutBlock] = parse_layout_blocks(raw_result)
    doc_quality: str = assess_quality(blocks)

    # Persist raw JSON + quality flag on the document row.
    doc: ContractDocument | None = session.get(ContractDocument, doc_id)
    if doc is not None:
        doc.raw_layout_json = json.dumps(raw_result)
        doc.doc_quality = doc_quality
        session.flush()

    logger.info(
        "Layout extraction complete: %d blocks, quality=%s", len(blocks), doc_quality
    )
    return blocks, doc_quality
