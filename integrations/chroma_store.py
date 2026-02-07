"""ChromaDB vector store wrapper.

Manages a Chroma collection for clause embeddings with metadata filtering.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

import config

logger = logging.getLogger(__name__)


def _get_client():
    """Return a persistent Chroma client."""
    import chromadb

    return chromadb.PersistentClient(path=config.CHROMA_PERSIST_DIR)


def _get_collection(client=None):
    """Get or create the clause collection."""
    if client is None:
        client = _get_client()
    return client.get_or_create_collection(
        name=config.CHROMA_COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )


def upsert_clause(
    node_id: str,
    clause_text: str,
    embedding: list[float],
    metadata: dict[str, Any],
) -> None:
    """Upsert a single clause embedding into Chroma.

    Parameters
    ----------
    node_id : str
        Deterministic clause node ID (used as Chroma document ID).
    clause_text : str
        The clause text (stored as the Chroma document).
    embedding : list[float]
        Pre-computed embedding vector.
    metadata : dict
        Must include: family_id, canonical_section_id, effective_ts,
        doc_version, page, change_action.
    """
    collection = _get_collection()

    # Chroma metadata values must be str, int, float, or bool.
    clean_meta: dict[str, Any] = {}
    for k, v in metadata.items():
        if v is None:
            continue
        if isinstance(v, datetime):
            clean_meta[k] = v.isoformat()
        elif isinstance(v, (str, int, float, bool)):
            clean_meta[k] = v
        else:
            clean_meta[k] = str(v)

    collection.upsert(
        ids=[node_id],
        documents=[clause_text],
        embeddings=[embedding],
        metadatas=[clean_meta],
    )
    logger.debug("Upserted clause %s into Chroma", node_id)


def upsert_clauses_batch(
    node_ids: list[str],
    texts: list[str],
    embeddings: list[list[float]],
    metadatas: list[dict[str, Any]],
) -> None:
    """Batch-upsert clause embeddings into Chroma."""
    collection = _get_collection()

    clean_metas: list[dict[str, Any]] = []
    for meta in metadatas:
        clean: dict[str, Any] = {}
        for k, v in meta.items():
            if v is None:
                continue
            if isinstance(v, datetime):
                clean[k] = v.isoformat()
            elif isinstance(v, (str, int, float, bool)):
                clean[k] = v
            else:
                clean[k] = str(v)
        clean_metas.append(clean)

    collection.upsert(
        ids=node_ids,
        documents=texts,
        embeddings=embeddings,
        metadatas=clean_metas,
    )
    logger.info("Batch-upserted %d clauses into Chroma", len(node_ids))


def query_clauses(
    query_embedding: list[float],
    n_results: int = 10,
    where: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Query the clause collection.

    Parameters
    ----------
    query_embedding : list[float]
        Query vector.
    n_results : int
        Max results.
    where : dict, optional
        Chroma ``where`` filter (e.g., ``{"family_id": "abc123"}``).

    Returns
    -------
    dict
        Chroma query result with keys: ids, documents, metadatas, distances.
    """
    collection = _get_collection()
    kwargs: dict[str, Any] = {
        "query_embeddings": [query_embedding],
        "n_results": n_results,
    }
    if where:
        kwargs["where"] = where
    return collection.query(**kwargs)
