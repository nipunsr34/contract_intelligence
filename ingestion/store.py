"""Phase 6 -- Store enriched clauses to SQL + Chroma + LlamaIndex.

SQL is the source of truth.  Chroma and LlamaIndex are secondary indexes
that can be rebuilt from SQL at any time.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from db.models import ClauseNode
from ingestion.clause import ClauseChunk
from ingestion.enrichment import EnrichedClause
from ingestion.hashing import compute_node_id

logger = logging.getLogger(__name__)


# ── Step 6.1  SQL (source of truth) ───────────────────────────────────────

def store_clause_sql(
    enriched: EnrichedClause,
    doc_id: str,
    family_id: str,
    doc_effective_ts: Optional[datetime],
    session: Session,
) -> ClauseNode:
    """Insert or update a clause node in SQL.

    Returns the created / updated ``ClauseNode`` ORM object.
    """
    chunk = enriched.chunk

    node_id = compute_node_id(
        doc_id=doc_id,
        page=chunk.page,
        span_start=chunk.span_start,
        span_end=chunk.span_end,
        canonical_section_id=enriched.canonical_section_id,
    )

    effective = enriched.effective_ts or doc_effective_ts

    node = ClauseNode(
        node_id=node_id,
        doc_id=doc_id,
        family_id=family_id,
        canonical_section_id=enriched.canonical_section_id,
        section_title=chunk.section_title or chunk.heading or None,
        referenced_section_id=enriched.referenced_section_id,
        change_action=enriched.change_action,
        modifies_section_id=enriched.modifies_section_id,
        effective_ts=effective,
        page=chunk.page,
        span_start=chunk.span_start,
        span_end=chunk.span_end,
        bbox_json=None,
        clause_text=chunk.text,
        extracted_facts_json=(
            json.dumps(enriched.extracted_facts) if enriched.extracted_facts else None
        ),
        confidence=enriched.confidence,
        embedding_id=None,  # will be set after Chroma upsert
    )

    # Upsert: merge on primary key.
    session.merge(node)
    session.flush()
    logger.debug("Stored clause node %s (section %s)", node_id, enriched.canonical_section_id)
    return node


def store_all_clauses_sql(
    enriched_clauses: list[EnrichedClause],
    doc_id: str,
    family_id: str,
    doc_effective_ts: Optional[datetime],
    session: Session,
) -> list[ClauseNode]:
    """Store all enriched clauses to SQL."""
    nodes: list[ClauseNode] = []
    for enriched in enriched_clauses:
        node = store_clause_sql(enriched, doc_id, family_id, doc_effective_ts, session)
        nodes.append(node)
    logger.info("Stored %d clause nodes to SQL for doc %s", len(nodes), doc_id)
    return nodes


# ── Step 6.2  Chroma ──────────────────────────────────────────────────────

def store_clauses_chroma(
    nodes: list[ClauseNode],
    doc_version: Optional[int] = None,
) -> None:
    """Embed and upsert clause nodes into ChromaDB.

    Uses the OpenAI embedding model via ``openai_client.embed_texts``.
    """
    if not nodes:
        return

    from integrations.openai_client import embed_texts
    from integrations.chroma_store import upsert_clauses_batch

    texts = [n.clause_text for n in nodes]
    embeddings = embed_texts(texts)

    node_ids = [n.node_id for n in nodes]
    metadatas = []
    for n in nodes:
        metadatas.append({
            "family_id": n.family_id,
            "canonical_section_id": n.canonical_section_id,
            "effective_ts": n.effective_ts.isoformat() if n.effective_ts else "",
            "doc_version": doc_version or 0,
            "page": n.page or 0,
            "change_action": n.change_action or "NO_CHANGE",
            "doc_id": n.doc_id,
        })

    upsert_clauses_batch(node_ids, texts, embeddings, metadatas)

    # Store embedding_id back on the SQL nodes (same as node_id in Chroma).
    # (The caller should commit the session after this.)
    for node in nodes:
        node.embedding_id = node.node_id


# ── Step 6.3  LlamaIndex ─────────────────────────────────────────────────

def store_clauses_llamaindex(nodes: list[ClauseNode]) -> None:
    """Insert clause nodes into the LlamaIndex index.

    Wraps each ``ClauseNode`` as a LlamaIndex ``TextNode``.
    """
    if not nodes:
        return

    from llama_index.core.schema import TextNode
    from integrations.llamaindex_store import insert_nodes

    li_nodes = []
    for n in nodes:
        metadata = {
            "family_id": n.family_id or "",
            "canonical_section_id": n.canonical_section_id or "",
            "effective_ts": n.effective_ts.isoformat() if n.effective_ts else "",
            "change_action": n.change_action or "NO_CHANGE",
            "page": n.page or 0,
            "doc_id": n.doc_id or "",
        }
        li_node = TextNode(
            text=n.clause_text,
            id_=n.node_id,
            metadata=metadata,
        )
        li_nodes.append(li_node)

    insert_nodes(li_nodes)
