"""Deterministic hashing for idempotent ingestion.

Every entity in the system gets a hash-based ID so that reprocessing the same
PDF never creates duplicates -- it only upserts.

Rules
-----
- ``file_hash``  = SHA-256 of the raw PDF bytes.
- ``doc_id``     = SHA-256 of the ``file_hash`` string.
- ``node_id``    = SHA-256 of ``doc_id|page|span_start|span_end|canonical_section_id``.
- ``family_id``  = SHA-256 of ``family_keys_hash``.
"""

import hashlib


def compute_file_hash(pdf_bytes: bytes) -> str:
    """Return the SHA-256 hex digest of raw PDF bytes."""
    return hashlib.sha256(pdf_bytes).hexdigest()


def compute_doc_id(fhash: str) -> str:
    """Derive a deterministic ``doc_id`` from the file hash."""
    return hashlib.sha256(fhash.encode()).hexdigest()


def compute_node_id(
    doc_id: str,
    page: int,
    span_start: int,
    span_end: int,
    canonical_section_id: str,
) -> str:
    """Derive a deterministic ``node_id`` from clause provenance fields."""
    key = f"{doc_id}|{page}|{span_start}|{span_end}|{canonical_section_id}"
    return hashlib.sha256(key.encode()).hexdigest()


def compute_family_keys_hash(partyA_norm: str, partyB_norm: str) -> str:
    """Hash the sorted normalized party names to produce a family key.

    Sorting ensures that ("Acme", "Widget") and ("Widget", "Acme") yield the
    same hash.
    """
    parts = sorted([partyA_norm.strip().lower(), partyB_norm.strip().lower()])
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()


def compute_family_id(family_keys_hash: str) -> str:
    """Derive ``family_id`` from ``family_keys_hash``."""
    return hashlib.sha256(family_keys_hash.encode()).hexdigest()
