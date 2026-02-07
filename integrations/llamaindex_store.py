"""LlamaIndex integration wrapper.

Uses ``ChromaVectorStore`` from ``llama-index-vector-stores-chroma`` to
expose the same Chroma collection through LlamaIndex's ``VectorStoreIndex``
with metadata filtering support and incremental inserts.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import config

logger = logging.getLogger(__name__)

# Module-level singletons (lazily initialised).
_index = None
_vector_store = None


def _get_vector_store():
    """Create or return the ChromaVectorStore backed by the same collection."""
    global _vector_store
    if _vector_store is not None:
        return _vector_store

    import chromadb
    from llama_index.vector_stores.chroma import ChromaVectorStore

    chroma_client = chromadb.PersistentClient(path=config.CHROMA_PERSIST_DIR)
    collection = chroma_client.get_or_create_collection(
        name=config.CHROMA_COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )
    _vector_store = ChromaVectorStore(chroma_collection=collection)
    return _vector_store


def get_index():
    """Return the VectorStoreIndex (creates it if needed)."""
    global _index
    if _index is not None:
        return _index

    from llama_index.core import StorageContext, VectorStoreIndex

    vector_store = _get_vector_store()
    storage_context = StorageContext.from_defaults(vector_store=vector_store)
    _index = VectorStoreIndex.from_vector_store(
        vector_store,
        storage_context=storage_context,
    )
    return _index


def insert_nodes(nodes: list[Any]) -> None:
    """Incrementally insert LlamaIndex ``TextNode`` objects into the index.

    Parameters
    ----------
    nodes : list
        LlamaIndex ``TextNode`` (or subclass) instances with metadata set.
    """
    index = get_index()
    for node in nodes:
        index.insert_nodes([node])
    logger.info("Inserted %d nodes into LlamaIndex index", len(nodes))


def query(
    query_text: str,
    similarity_top_k: int = 10,
    filters: Optional[dict[str, Any]] = None,
) -> Any:
    """Run a query against the LlamaIndex index.

    Parameters
    ----------
    query_text : str
        Natural-language query.
    similarity_top_k : int
        Number of results.
    filters : dict, optional
        Metadata filters (converted to LlamaIndex ``MetadataFilters``).

    Returns
    -------
    LlamaIndex query response.
    """
    from llama_index.core.vector_stores import (
        ExactMatchFilter,
        MetadataFilters,
    )

    index = get_index()
    query_engine_kwargs: dict[str, Any] = {"similarity_top_k": similarity_top_k}

    if filters:
        exact_filters = [
            ExactMatchFilter(key=k, value=v) for k, v in filters.items()
        ]
        query_engine_kwargs["filters"] = MetadataFilters(filters=exact_filters)

    query_engine = index.as_query_engine(**query_engine_kwargs)
    return query_engine.query(query_text)
