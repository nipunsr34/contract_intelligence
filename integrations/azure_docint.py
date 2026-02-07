"""Azure Document Intelligence client wrapper.

Calls the ``prebuilt-layout`` model to extract structured layout from PDFs
including paragraph roles (title, sectionHeading, pageHeader, pageFooter, etc.),
bounding boxes, and page spans.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from typing import Any

import config

logger = logging.getLogger(__name__)


@dataclass
class BBox:
    """Bounding box for a text span (page coordinates)."""
    x: float = 0.0
    y: float = 0.0
    width: float = 0.0
    height: float = 0.0


@dataclass
class LayoutBlock:
    """A single layout element extracted by Document Intelligence."""
    role: str  # title | sectionHeading | pageHeader | pageFooter | ...
    text: str
    page: int
    span_start: int
    span_end: int
    bbox: BBox = field(default_factory=BBox)

    def to_dict(self) -> dict:
        return asdict(self)


def analyze_layout(pdf_bytes: bytes) -> dict[str, Any]:
    """Run Azure Document Intelligence ``prebuilt-layout`` on *pdf_bytes*.

    Returns the raw JSON response from the service.

    Raises
    ------
    RuntimeError
        If the Azure credentials are not configured.
    """
    if not config.AZURE_DOCINT_ENDPOINT or not config.AZURE_DOCINT_KEY:
        raise RuntimeError(
            "Azure Document Intelligence credentials are not configured. "
            "Set AZURE_DOCINT_ENDPOINT and AZURE_DOCINT_KEY in your .env file."
        )

    from azure.ai.documentintelligence import DocumentIntelligenceClient
    from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
    from azure.core.credentials import AzureKeyCredential

    client = DocumentIntelligenceClient(
        endpoint=config.AZURE_DOCINT_ENDPOINT,
        credential=AzureKeyCredential(config.AZURE_DOCINT_KEY),
    )

    poller = client.begin_analyze_document(
        model_id="prebuilt-layout",
        analyze_request=AnalyzeDocumentRequest(bytes_source=pdf_bytes),
        output_content_format="markdown",
    )
    result = poller.result()

    # Convert the SDK result to a plain dict / JSON-serialisable object.
    raw: dict[str, Any] = result.as_dict() if hasattr(result, "as_dict") else json.loads(str(result))
    return raw


def parse_layout_blocks(raw_result: dict[str, Any]) -> list[LayoutBlock]:
    """Convert the raw DocInt JSON into a list of ``LayoutBlock`` objects.

    Focuses on paragraphs and their roles.  Falls back to ``"content"`` role
    when no explicit role is provided by the API.
    """
    blocks: list[LayoutBlock] = []
    paragraphs = raw_result.get("analyzeResult", raw_result).get("paragraphs", [])

    for para in paragraphs:
        role = para.get("role", "content")
        text = para.get("content", "")
        # spans
        spans = para.get("spans", [{}])
        span = spans[0] if spans else {}
        span_start = span.get("offset", 0)
        span_end = span_start + span.get("length", len(text))

        # bounding regions -> page + bbox
        regions = para.get("boundingRegions", [{}])
        region = regions[0] if regions else {}
        page = region.get("pageNumber", 1)
        polygon = region.get("polygon", [])

        bbox = BBox()
        if len(polygon) >= 8:
            # polygon is [x1,y1, x2,y2, x3,y3, x4,y4]
            xs = [polygon[i] for i in range(0, 8, 2)]
            ys = [polygon[i] for i in range(1, 8, 2)]
            bbox = BBox(
                x=min(xs),
                y=min(ys),
                width=max(xs) - min(xs),
                height=max(ys) - min(ys),
            )

        blocks.append(
            LayoutBlock(
                role=role,
                text=text,
                page=page,
                span_start=span_start,
                span_end=span_end,
                bbox=bbox,
            )
        )

    return blocks


def assess_quality(blocks: list[LayoutBlock]) -> str:
    """Return ``'normal'`` or ``'low'`` based on heading quality.

    If fewer than 5 % of blocks have an explicit heading role the document
    is flagged as low quality (candidate for PageIndex fallback).
    """
    if not blocks:
        return "low"
    heading_roles = {"title", "sectionHeading"}
    heading_count = sum(1 for b in blocks if b.role in heading_roles)
    ratio = heading_count / len(blocks)
    return "normal" if ratio >= 0.05 else "low"


# Thresholds for usability checks.
_MIN_TOTAL_TEXT_CHARS = 100       # total text across all blocks
_MIN_AVG_BLOCK_CHARS = 5         # average non-whitespace chars per block
_MIN_BLOCK_COUNT = 2             # at least this many blocks


def are_blocks_usable(blocks: list[LayoutBlock]) -> bool:
    """Check whether layout blocks contain enough real text to be useful.

    This catches cases where DocInt *succeeds* (no exception, blocks returned)
    but the content is garbage -- e.g. all-whitespace blocks, garbled OCR
    producing single-character fragments, or a corrupted PDF yielding near-
    empty text.

    Returns ``True`` if the blocks pass all sanity checks, ``False`` otherwise.
    """
    if not blocks or len(blocks) < _MIN_BLOCK_COUNT:
        return False

    # Total non-whitespace text across all blocks.
    total_chars = sum(len(b.text.strip()) for b in blocks)
    if total_chars < _MIN_TOTAL_TEXT_CHARS:
        return False

    # Average non-whitespace text per block.
    avg_chars = total_chars / len(blocks)
    if avg_chars < _MIN_AVG_BLOCK_CHARS:
        return False

    return True
