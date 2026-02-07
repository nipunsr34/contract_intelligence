"""PageIndex integration -- full implementation.

Three integration points for the PageIndex library
(https://github.com/VectifyAI/PageIndex):

A) Better sectionization when DocInt headings are weak.
B) Reasoning-based retrieval ("tree search") when embeddings miss.
C) OCR-free vision fallback for scanned PDFs.

PageIndex is imported lazily so the rest of the system works even if the
``pageindex`` package is not installed.  Set ``PAGEINDEX_ENABLED=true``
in ``.env`` and ensure ``CHATGPT_API_KEY`` is set (PageIndex reads it
from the environment via ``python-dotenv``).
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import config as app_config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PageIndexNode:
    """A node from PageIndex's hierarchical TOC tree."""
    title: str
    node_id: str
    start_page: int
    end_page: int
    summary: Optional[str] = None
    text: Optional[str] = None
    children: list["PageIndexNode"] = field(default_factory=list)


@dataclass
class RetrievedSection:
    """A section retrieved via PageIndex's reasoning-based tree search."""
    title: str
    node_id: str
    start_page: int
    end_page: int
    relevance_score: float
    text: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers: convert PageIndex raw tree dicts → our dataclasses
# ---------------------------------------------------------------------------

def _raw_tree_to_nodes(tree: list[dict] | dict) -> list[PageIndexNode]:
    """Recursively convert the raw PageIndex tree JSON into PageIndexNode objects."""
    if isinstance(tree, dict):
        tree = [tree]

    nodes: list[PageIndexNode] = []
    for item in tree:
        children = _raw_tree_to_nodes(item.get("nodes", []))
        node = PageIndexNode(
            title=item.get("title", ""),
            node_id=item.get("node_id", ""),
            start_page=item.get("start_index", 1),
            end_page=item.get("end_index", 1),
            summary=item.get("summary"),
            text=item.get("text"),
            children=children,
        )
        nodes.append(node)
    return nodes


def _flatten_nodes(nodes: list[PageIndexNode]) -> list[PageIndexNode]:
    """Flatten a tree of PageIndexNode into a list (pre-order traversal)."""
    result: list[PageIndexNode] = []
    for node in nodes:
        result.append(node)
        result.extend(_flatten_nodes(node.children))
    return result


def _tree_to_compact_json(nodes: list[PageIndexNode]) -> list[dict]:
    """Convert nodes to a compact JSON representation for LLM prompts."""
    result: list[dict] = []
    for node in nodes:
        entry: dict[str, Any] = {
            "title": node.title,
            "node_id": node.node_id,
            "start_page": node.start_page,
            "end_page": node.end_page,
        }
        if node.summary:
            entry["summary"] = node.summary
        if node.children:
            entry["children"] = _tree_to_compact_json(node.children)
        result.append(entry)
    return result


# ---------------------------------------------------------------------------
# Cache for generated trees (avoids re-processing the same PDF)
# ---------------------------------------------------------------------------

_tree_cache: dict[str, list[PageIndexNode]] = {}
_raw_tree_cache: dict[str, list[dict]] = {}


def _cache_key(pdf_path: str) -> str:
    return str(Path(pdf_path).resolve())


# ---------------------------------------------------------------------------
# A) Sectionization fallback
# ---------------------------------------------------------------------------

def sectionize_with_pageindex(
    pdf_path: str,
    *,
    model: str | None = None,
    force: bool = False,
) -> list[PageIndexNode]:
    """Fallback A: generate a hierarchical TOC tree using PageIndex.

    Use this when Azure Document Intelligence returns poor heading quality
    (``doc_quality == 'low'``).  PageIndex's tree structure provides natural
    section boundaries without chunking.

    The tree is cached in memory so repeated calls for the same PDF are free.

    Parameters
    ----------
    pdf_path : str
        Path to the PDF file.
    model : str, optional
        OpenAI model for PageIndex (default: ``gpt-4o-2024-11-20``).
    force : bool
        Re-generate even if cached.

    Returns
    -------
    list[PageIndexNode]
        Top-level tree nodes (each may have ``children``).
    """
    key = _cache_key(pdf_path)
    if not force and key in _tree_cache:
        logger.info("PageIndex tree cache hit for %s", pdf_path)
        return _tree_cache[key]

    logger.info("Running PageIndex sectionization on %s", pdf_path)

    # Ensure CHATGPT_API_KEY is set for PageIndex.
    if app_config.OPENAI_API_KEY and not os.environ.get("CHATGPT_API_KEY"):
        os.environ["CHATGPT_API_KEY"] = app_config.OPENAI_API_KEY

    from pageindex import config as pi_config, page_index_main

    used_model = model or app_config.PAGEINDEX_MODEL
    opt = pi_config(
        model=used_model,
        if_add_node_id="yes",
        if_add_node_summary="yes",
        if_add_doc_description="no",
        if_add_node_text="yes",
    )

    raw_tree: list[dict] = page_index_main(pdf_path, opt)
    logger.info("PageIndex returned tree with %d top-level nodes", len(raw_tree))

    # Save raw tree for retrieval re-use.
    _raw_tree_cache[key] = raw_tree

    # Also persist to disk as JSON for debugging / offline use.
    results_dir = Path("./pageindex_results")
    results_dir.mkdir(exist_ok=True)
    pdf_name = Path(pdf_path).stem
    with open(results_dir / f"{pdf_name}_tree.json", "w") as f:
        json.dump(raw_tree, f, indent=2, ensure_ascii=False)

    nodes = _raw_tree_to_nodes(raw_tree)
    _tree_cache[key] = nodes
    return nodes


def pageindex_nodes_to_layout_blocks(nodes: list[PageIndexNode], pdf_path: str):
    """Convert PageIndex nodes into LayoutBlock objects for the clause pipeline.

    This bridges Fallback A into the existing Phase 4 clause segmentation:
    each PageIndex leaf node becomes a heading + content block.

    Parameters
    ----------
    nodes : list[PageIndexNode]
        The PageIndex tree (may be nested).
    pdf_path : str
        Path to the PDF (used to extract page text if node text is missing).

    Returns
    -------
    list[LayoutBlock]
        Blocks suitable for ``build_clause_chunks`` / ``assign_canonical_ids``.
    """
    from integrations.azure_docint import LayoutBlock

    flat = _flatten_nodes(nodes)
    blocks: list[LayoutBlock] = []
    span_offset = 0

    for node in flat:
        # Heading block.
        heading_text = node.title
        blocks.append(LayoutBlock(
            role="sectionHeading",
            text=heading_text,
            page=node.start_page,
            span_start=span_offset,
            span_end=span_offset + len(heading_text),
        ))
        span_offset += len(heading_text) + 1

        # Content block (use node text if available, else extract from PDF).
        content_text = node.text or ""
        if not content_text and pdf_path:
            try:
                import PyPDF2
                reader = PyPDF2.PdfReader(pdf_path)
                page_texts = []
                for pg in range(node.start_page - 1, min(node.end_page, len(reader.pages))):
                    page_texts.append(reader.pages[pg].extract_text() or "")
                content_text = "\n".join(page_texts)
            except Exception as exc:
                logger.warning("Could not extract text for node %s: %s", node.node_id, exc)
                content_text = ""

        if content_text.strip():
            blocks.append(LayoutBlock(
                role="content",
                text=content_text,
                page=node.start_page,
                span_start=span_offset,
                span_end=span_offset + len(content_text),
            ))
            span_offset += len(content_text) + 1

    return blocks


# ---------------------------------------------------------------------------
# B) Retriever fallback -- LLM tree search
# ---------------------------------------------------------------------------

def retrieve_with_pageindex(
    pdf_path: str,
    query: str,
    *,
    model: str | None = None,
    top_k: int = 5,
) -> list[RetrievedSection]:
    """Fallback B: reasoning-based retrieval when embeddings miss.

    Uses LLM tree search over the PageIndex tree structure.  The LLM reasons
    about which nodes are most likely to contain the answer, rather than
    relying on vector similarity.

    Parameters
    ----------
    pdf_path : str
        Path to the original PDF.
    query : str
        Natural-language query (e.g., "limitation of liability").
    model : str, optional
        OpenAI model for the tree search prompt.
    top_k : int
        Maximum number of sections to return.

    Returns
    -------
    list[RetrievedSection]
        Matching sections ordered by relevance, with page ranges and text.
    """
    logger.info("PageIndex tree-search retrieval: query='%s'", query)

    # Ensure we have a tree.  Build one if not cached.
    key = _cache_key(pdf_path)
    if key not in _tree_cache:
        sectionize_with_pageindex(pdf_path, model=model)

    nodes = _tree_cache[key]
    compact_tree = _tree_to_compact_json(nodes)

    # Ensure CHATGPT_API_KEY is set.
    if app_config.OPENAI_API_KEY and not os.environ.get("CHATGPT_API_KEY"):
        os.environ["CHATGPT_API_KEY"] = app_config.OPENAI_API_KEY

    # Build the tree-search prompt (following PageIndex's recommended approach).
    tree_json_str = json.dumps(compact_tree, indent=2, ensure_ascii=False)
    prompt = f"""You are given a query and the tree structure of a document.
You need to find all nodes that are likely to contain the answer.
Return at most {top_k} nodes, ordered by relevance (most relevant first).
For each node, provide a relevance_score between 0.0 and 1.0.

Query: {query}

Document tree structure:
{tree_json_str}

Reply in the following JSON format:
{{
    "thinking": "<your reasoning>",
    "results": [
        {{
            "node_id": "<node_id>",
            "relevance_score": 0.0-1.0
        }}
    ]
}}
Directly return the final JSON structure. Do not output anything else."""

    from integrations.openai_client import chat_json

    used_model = model or app_config.OPENAI_MODEL
    response = chat_json(
        system_prompt="You are a document retrieval assistant.",
        user_prompt=prompt,
        model=used_model,
    )

    # Map results back to our data classes.
    flat = _flatten_nodes(nodes)
    node_map = {n.node_id: n for n in flat}

    results: list[RetrievedSection] = []
    for item in response.get("results", []):
        nid = str(item.get("node_id", ""))
        score = float(item.get("relevance_score", 0.0))
        matched_node = node_map.get(nid)

        if matched_node is None:
            logger.warning("Tree search returned unknown node_id '%s', skipping", nid)
            continue

        # Extract text for the matched node if not already present.
        text = matched_node.text or ""
        if not text and pdf_path:
            try:
                import PyPDF2
                reader = PyPDF2.PdfReader(pdf_path)
                page_texts = []
                for pg in range(matched_node.start_page - 1,
                                min(matched_node.end_page, len(reader.pages))):
                    page_texts.append(reader.pages[pg].extract_text() or "")
                text = "\n".join(page_texts)
            except Exception:
                text = ""

        results.append(RetrievedSection(
            title=matched_node.title,
            node_id=nid,
            start_page=matched_node.start_page,
            end_page=matched_node.end_page,
            relevance_score=score,
            text=text,
        ))

    results.sort(key=lambda r: r.relevance_score, reverse=True)
    logger.info("PageIndex tree search returned %d results", len(results))
    return results[:top_k]


# ---------------------------------------------------------------------------
# C) OCR-free vision fallback
# ---------------------------------------------------------------------------

def ocr_with_pageindex(
    pdf_path: str,
    *,
    model: str = "gpt-5-mini",
    max_pages: int | None = None,
) -> str:
    """Fallback C: vision-based OCR-free text extraction for scanned PDFs.

    Converts each PDF page to an image and sends it to a vision-capable
    OpenAI model to extract text as markdown.  This avoids traditional OCR
    entirely and works well on complex layouts, scans, and handwriting.

    Parameters
    ----------
    pdf_path : str
        Path to the PDF file.
    model : str
        Vision-capable model (must support image inputs, e.g., ``gpt-4o``).
    max_pages : int, optional
        Limit the number of pages to process (for cost control).

    Returns
    -------
    str
        Extracted markdown text from all processed pages.
    """
    logger.info("Running PageIndex vision OCR on %s", pdf_path)

    if app_config.OPENAI_API_KEY and not os.environ.get("CHATGPT_API_KEY"):
        os.environ["CHATGPT_API_KEY"] = app_config.OPENAI_API_KEY

    import base64
    import pymupdf
    from openai import OpenAI

    client = OpenAI(api_key=app_config.OPENAI_API_KEY)

    doc = pymupdf.open(pdf_path)
    total_pages = len(doc)
    pages_to_process = min(total_pages, max_pages) if max_pages else total_pages

    all_text: list[str] = []

    for page_num in range(pages_to_process):
        page = doc[page_num]
        # Render page to image (300 DPI for good quality).
        pix = page.get_pixmap(dpi=200)
        img_bytes = pix.tobytes("png")
        img_b64 = base64.b64encode(img_bytes).decode("utf-8")

        response = client.chat.completions.create(
            model=model,
            temperature=0,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                "Extract ALL text from this document page image. "
                                "Preserve the structure using markdown formatting "
                                "(headings, lists, tables, etc.). "
                                "Return ONLY the extracted text, nothing else."
                            ),
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{img_b64}",
                            },
                        },
                    ],
                }
            ],
        )
        page_text = response.choices[0].message.content or ""
        all_text.append(f"<!-- Page {page_num + 1} -->\n{page_text}")
        logger.debug("Vision OCR: extracted page %d/%d", page_num + 1, pages_to_process)

    doc.close()

    full_text = "\n\n".join(all_text)
    logger.info("Vision OCR complete: %d pages, %d chars", pages_to_process, len(full_text))
    return full_text
