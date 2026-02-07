"""Phase 9 -- Real-time ingestion orchestrator pipeline.

Single entry point: ``ingest_document(pdf_path)`` runs every phase in order:

  file_hash check -> DocInt layout -> metadata resolver -> family match ->
  version assign -> clause segment -> enrich each clause -> validate ->
  upsert SQL + Chroma + LlamaIndex -> recompute family_section_current

Early exit if ``file_hash`` already exists (idempotency).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from db.models import ContractDocument
from db.session import get_session
from ingestion.hashing import compute_doc_id, compute_file_hash

logger = logging.getLogger(__name__)


@dataclass
class IngestionResult:
    """Summary returned after ingesting one document."""
    doc_id: str
    family_id: Optional[str]
    file_hash: str
    doc_type: Optional[str]
    doc_version_ingest: Optional[int]
    doc_version_timeline: Optional[int]
    num_clauses: int
    sections_materialized: int
    skipped: bool = False
    error: Optional[str] = None


def ingest_document(
    pdf_path: str,
    session: Session | None = None,
    force: bool = False,
) -> IngestionResult:
    """Run the full ingestion pipeline on a single PDF.

    Parameters
    ----------
    pdf_path : str
        Path to the PDF file.
    session : Session, optional
        An existing SQLAlchemy session.  If ``None``, one will be created.
    force : bool
        If ``True``, re-process even if the file hash already exists.

    Returns
    -------
    IngestionResult
    """
    own_session = session is None
    if own_session:
        session = get_session()

    try:
        return _run_pipeline(pdf_path, session, force)
    except Exception as exc:
        logger.exception("Ingestion failed for %s", pdf_path)
        if own_session:
            session.rollback()
        raise
    finally:
        if own_session:
            session.close()


def _run_pipeline(
    pdf_path: str,
    session: Session,
    force: bool,
) -> IngestionResult:
    """Internal pipeline logic."""

    # ── Read PDF bytes ────────────────────────────────────────────────────
    pdf_bytes = Path(pdf_path).read_bytes()

    # ── Phase 0: Hashing + idempotency check ─────────────────────────────
    fhash = compute_file_hash(pdf_bytes)
    doc_id = compute_doc_id(fhash)

    existing: ContractDocument | None = (
        session.query(ContractDocument)
        .filter(ContractDocument.file_hash == fhash)
        .first()
    )
    if existing and not force:
        logger.info("Document %s already ingested (hash %s). Skipping.", doc_id, fhash)
        return IngestionResult(
            doc_id=doc_id,
            family_id=existing.family_id,
            file_hash=fhash,
            doc_type=existing.doc_type,
            doc_version_ingest=existing.doc_version_ingest,
            doc_version_timeline=existing.doc_version_timeline,
            num_clauses=0,
            sections_materialized=0,
            skipped=True,
        )

    # Create the document row (will be enriched as we progress).
    doc = ContractDocument(doc_id=doc_id, file_hash=fhash)
    session.merge(doc)
    session.flush()

    # ── Phase 1: Layout extraction ────────────────────────────────────────
    from ingestion.layout import extract_layout
    import config as app_config

    try:
        blocks, doc_quality = extract_layout(pdf_bytes, doc_id, session)
    except Exception as layout_exc:
        logger.warning("DocInt layout extraction failed: %s", layout_exc)
        blocks, doc_quality = [], "low"

    # Usability check: blocks may exist but contain garbage (all-whitespace,
    # single-char fragments, garbled OCR).  Treat unusable blocks the same as
    # empty blocks so fallbacks can kick in.
    if blocks:
        from integrations.azure_docint import are_blocks_usable

        if not are_blocks_usable(blocks):
            logger.warning(
                "Layout blocks exist (%d) but are not usable (too little text) "
                "-- treating as empty",
                len(blocks),
            )
            blocks = []
            doc_quality = "low"

    # Fallback C: if DocInt produced no usable blocks and PageIndex is enabled,
    # try vision-based OCR-free extraction.
    if not blocks and app_config.PAGEINDEX_ENABLED:
        logger.info("No layout blocks from DocInt -- trying PageIndex vision OCR (Fallback C)")
        try:
            from integrations.pageindex_stub import ocr_with_pageindex
            from integrations.azure_docint import LayoutBlock

            vision_text = ocr_with_pageindex(
                pdf_path, model=app_config.PAGEINDEX_VISION_MODEL
            )
            if vision_text:
                # Convert the markdown text into synthetic layout blocks.
                lines = vision_text.split("\n")
                span_offset = 0
                for line in lines:
                    stripped = line.strip()
                    if not stripped:
                        continue
                    # Detect markdown headings as sectionHeading.
                    if stripped.startswith("#"):
                        heading_text = stripped.lstrip("#").strip()
                        blocks.append(LayoutBlock(
                            role="sectionHeading",
                            text=heading_text,
                            page=1,
                            span_start=span_offset,
                            span_end=span_offset + len(heading_text),
                        ))
                    else:
                        blocks.append(LayoutBlock(
                            role="content",
                            text=stripped,
                            page=1,
                            span_start=span_offset,
                            span_end=span_offset + len(stripped),
                        ))
                    span_offset += len(stripped) + 1
                doc_quality = "normal"  # vision OCR succeeded
                logger.info("Vision OCR produced %d layout blocks", len(blocks))
        except Exception as vision_exc:
            logger.error("PageIndex vision OCR also failed: %s", vision_exc)

    # ── Phase 2: Metadata extraction ──────────────────────────────────────
    from ingestion.metadata import extract_candidates, resolve_metadata

    candidates = extract_candidates(blocks)
    metadata = resolve_metadata(blocks, candidates)

    # Update document row.
    doc = session.get(ContractDocument, doc_id)
    if doc:
        doc.effective_ts = metadata.effective_ts
        doc.term_start_ts = metadata.term_start_ts
        doc.term_end_ts = metadata.term_end_ts
        doc.doc_type = metadata.doc_type
        doc.parties_normalized_json = json.dumps({
            "partyA": metadata.partyA_norm,
            "partyB": metadata.partyB_norm,
        })
        doc.confidence_scores = json.dumps({"metadata": metadata.confidence})
        if metadata.confidence < 0.7:
            doc.doc_quality = "low"
        session.flush()

    # ── Phase 3: Family matching + versioning ─────────────────────────────
    from ingestion.family import assign_doc_versions, match_or_create_family

    family_id: Optional[str] = None
    doc_version_ingest: Optional[int] = None
    doc_version_timeline: Optional[int] = None

    if metadata.partyA_norm and metadata.partyB_norm:
        family_id = match_or_create_family(
            metadata.partyA_norm, metadata.partyB_norm, session
        )
        doc_version_ingest, doc_version_timeline = assign_doc_versions(
            doc_id, family_id, metadata.effective_ts, session
        )
    else:
        logger.warning("Could not extract parties for doc %s -- skipping family matching", doc_id)

    # ── Phase 4: Clause segmentation ──────────────────────────────────────
    #    Fallback A: if DocInt quality is low and PageIndex is enabled,
    #    use PageIndex to generate better section boundaries.
    from ingestion.clause import assign_canonical_ids, build_clause_chunks
    import config as app_config

    if doc_quality == "low" and app_config.PAGEINDEX_ENABLED:
        logger.info("DocInt quality is low -- falling back to PageIndex sectionization")
        try:
            from integrations.pageindex_stub import (
                pageindex_nodes_to_layout_blocks,
                sectionize_with_pageindex,
            )
            pi_nodes = sectionize_with_pageindex(
                pdf_path, model=app_config.PAGEINDEX_MODEL
            )
            blocks = pageindex_nodes_to_layout_blocks(pi_nodes, pdf_path)
            logger.info("PageIndex produced %d layout blocks (replacing DocInt)", len(blocks))
        except Exception as exc:
            logger.warning("PageIndex sectionization failed, using DocInt blocks: %s", exc)

    chunks = build_clause_chunks(blocks)
    chunks = assign_canonical_ids(chunks)
    logger.info("Segmented %d clause chunks", len(chunks))

    # ── Phase 5: Clause enrichment ────────────────────────────────────────
    from ingestion.enrichment import enrich_all_clauses

    enriched_clauses = enrich_all_clauses(
        chunks,
        doc_type=metadata.doc_type,
        doc_version=doc_version_timeline,
        doc_effective_ts=metadata.effective_ts,
    )

    # ── Phase 6: Store ────────────────────────────────────────────────────
    from ingestion.store import (
        store_all_clauses_sql,
        store_clauses_chroma,
        store_clauses_llamaindex,
    )

    clause_nodes = store_all_clauses_sql(
        enriched_clauses,
        doc_id=doc_id,
        family_id=family_id or "",
        doc_effective_ts=metadata.effective_ts,
        session=session,
    )

    # Chroma + LlamaIndex (non-critical -- log errors but don't fail).
    try:
        store_clauses_chroma(clause_nodes, doc_version=doc_version_timeline)
    except Exception as exc:
        logger.error("Failed to store in Chroma: %s", exc)

    try:
        store_clauses_llamaindex(clause_nodes)
    except Exception as exc:
        logger.error("Failed to store in LlamaIndex: %s", exc)

    # ── Phase 7: Materialize current state ────────────────────────────────
    from materialization.current_state import materialize_affected_sections

    affected_sections = list({ec.canonical_section_id for ec in enriched_clauses})
    sections_materialized = 0
    if family_id:
        sections_materialized = materialize_affected_sections(
            family_id, affected_sections, session
        )

    # ── Commit ────────────────────────────────────────────────────────────
    session.commit()

    result = IngestionResult(
        doc_id=doc_id,
        family_id=family_id,
        file_hash=fhash,
        doc_type=metadata.doc_type,
        doc_version_ingest=doc_version_ingest,
        doc_version_timeline=doc_version_timeline,
        num_clauses=len(clause_nodes),
        sections_materialized=sections_materialized,
    )
    logger.info("Ingestion complete: %s", result)
    return result


def ingest_directory(dir_path: str, session: Session | None = None) -> list[IngestionResult]:
    """Batch-ingest all PDFs in a directory.

    Parameters
    ----------
    dir_path : str
        Path to a directory containing PDF files.
    session : Session, optional
        Shared session.  If ``None``, each file gets its own session.

    Returns
    -------
    list[IngestionResult]
    """
    pdf_dir = Path(dir_path)
    if not pdf_dir.is_dir():
        raise ValueError(f"Not a directory: {dir_path}")

    pdf_files = sorted(pdf_dir.glob("*.pdf"))
    if not pdf_files:
        logger.warning("No PDF files found in %s", dir_path)
        return []

    results: list[IngestionResult] = []
    for pdf_file in pdf_files:
        logger.info("Ingesting %s", pdf_file)
        try:
            result = ingest_document(str(pdf_file), session=session)
            results.append(result)
        except Exception as exc:
            logger.error("Failed to ingest %s: %s", pdf_file, exc)
            results.append(
                IngestionResult(
                    doc_id="",
                    family_id=None,
                    file_hash="",
                    doc_type=None,
                    doc_version_ingest=None,
                    doc_version_timeline=None,
                    num_clauses=0,
                    sections_materialized=0,
                    error=str(exc),
                )
            )

    return results
