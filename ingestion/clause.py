"""Phase 4 -- Clause segmentation and canonical section-ID assignment.

Two steps:
1. Build clause chunks from layout blocks (heading-based + numbering split).
2. Assign a stable ``canonical_section_id`` using a 3-tier strategy.
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from typing import Optional

from integrations.azure_docint import LayoutBlock

logger = logging.getLogger(__name__)


# ── Data structures ────────────────────────────────────────────────────────

@dataclass
class ClauseChunk:
    """A single clause chunk ready for enrichment."""
    text: str
    heading: str
    page: int
    span_start: int
    span_end: int
    canonical_section_id: str = ""
    referenced_section_id: Optional[str] = None
    section_title: Optional[str] = None
    tier: str = ""  # A, B, or C


# ── Step 4.1  Clause chunk building ───────────────────────────────────────

_HEADING_ROLES = {"title", "sectionHeading"}

# Numbering pattern:  "5.3", "5.3(a)", "10.1.2", etc.
_NUMBERING_RE = re.compile(
    r"^\s*((?:\d+\.)+\d*)\s*(?:\([a-z]\))?\s",
)


def _is_heading(block: LayoutBlock) -> bool:
    return block.role in _HEADING_ROLES


def build_clause_chunks(blocks: list[LayoutBlock]) -> list[ClauseChunk]:
    """Group layout blocks into clause chunks.

    Strategy:
    - Use heading-role blocks as section boundaries.
    - Within each section, further split on numbering patterns.
    """
    if not blocks:
        return []

    chunks: list[ClauseChunk] = []
    current_heading = ""
    current_texts: list[str] = []
    current_page = 1
    current_span_start = 0
    current_span_end = 0

    def _flush():
        nonlocal current_texts
        if current_texts:
            full_text = "\n".join(current_texts).strip()
            if full_text:
                # Sub-split on numbering patterns within the accumulated text.
                sub_chunks = _split_on_numbering(
                    full_text,
                    heading=current_heading,
                    page=current_page,
                    span_start=current_span_start,
                    span_end=current_span_end,
                )
                chunks.extend(sub_chunks)
            current_texts = []

    for block in blocks:
        if _is_heading(block):
            _flush()
            current_heading = block.text
            current_page = block.page
            current_span_start = block.span_start
            current_span_end = block.span_end
        else:
            current_texts.append(block.text)
            if not current_texts or current_span_start == 0:
                current_span_start = block.span_start
            current_span_end = max(current_span_end, block.span_end)
            current_page = block.page

    _flush()
    return chunks


def _split_on_numbering(
    text: str,
    heading: str,
    page: int,
    span_start: int,
    span_end: int,
) -> list[ClauseChunk]:
    """Split a section's text into sub-clauses by numbering patterns."""
    lines = text.split("\n")
    sub_chunks: list[ClauseChunk] = []
    current_lines: list[str] = []

    def _emit():
        nonlocal current_lines
        if current_lines:
            chunk_text = "\n".join(current_lines).strip()
            if chunk_text:
                # Approximate span offsets (best effort).
                sub_chunks.append(
                    ClauseChunk(
                        text=chunk_text,
                        heading=heading,
                        page=page,
                        span_start=span_start,
                        span_end=span_end,
                    )
                )
            current_lines = []

    for line in lines:
        if _NUMBERING_RE.match(line) and current_lines:
            _emit()
        current_lines.append(line)

    _emit()

    # If no sub-splits occurred, return the whole text as one chunk.
    if not sub_chunks:
        sub_chunks.append(
            ClauseChunk(
                text=text,
                heading=heading,
                page=page,
                span_start=span_start,
                span_end=span_end,
            )
        )

    return sub_chunks


# ── Step 4.2  Canonical section ID assignment ─────────────────────────────

# Tier A: explicit section number in heading or first line.
_SECTION_NUMBER_RE = re.compile(
    r"(?:Section\s+)?((?:\d+\.)+\d*)",
    re.IGNORECASE,
)

# Tier B: amendment reference  "Section 5.3 of the Agreement"
_AMENDMENT_REF_RE = re.compile(
    r"Section\s+((?:\d+\.)+\d*)\s+of\s+(?:the\s+)?(?:Agreement|Contract|Master)",
    re.IGNORECASE,
)


def _normalize_title(title: str) -> str:
    """Turn a heading like 'Limitation of Liability' into 'limitation_of_liability'."""
    cleaned = re.sub(r"[^\w\s]", "", title)
    cleaned = re.sub(r"\s+", "_", cleaned.strip())
    return cleaned.lower()


def assign_canonical_ids(chunks: list[ClauseChunk]) -> list[ClauseChunk]:
    """Assign ``canonical_section_id`` to each chunk using the 3-tier strategy.

    Tier A (best): explicit "5.3" present → ``canonical = "5.3"``
    Tier B: amendment reference ("Section 5.3 of the Agreement") → ``canonical = "5.3"``
    Tier C (fallback): no numbering → ``canonical = "semantic:<normalized_title>"``
    """
    for chunk in chunks:
        # Try Tier B first -- amendment reference is more specific than a bare
        # section number and should take priority (e.g., "Section 5.3 of the
        # Agreement is hereby replaced" is an amendment reference, not just a
        # section heading).
        m = _AMENDMENT_REF_RE.search(chunk.text)
        if m:
            ref_id = m.group(1).rstrip(".")
            chunk.canonical_section_id = ref_id
            chunk.referenced_section_id = ref_id
            chunk.tier = "B"
            continue

        # Try Tier A -- explicit numbering in heading or first line.
        combined = f"{chunk.heading}\n{chunk.text.split(chr(10))[0]}"
        m = _SECTION_NUMBER_RE.search(combined)
        if m:
            chunk.canonical_section_id = m.group(1).rstrip(".")
            chunk.tier = "A"
            continue

        # Tier C -- semantic title.
        title = chunk.heading or "untitled"
        normalized = _normalize_title(title)
        chunk.canonical_section_id = f"semantic:{normalized}"
        chunk.section_title = chunk.heading
        chunk.tier = "C"

    return chunks
