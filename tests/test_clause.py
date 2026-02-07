"""Tests for Phase 4 -- clause segmentation and canonical section IDs."""

import pytest
from integrations.azure_docint import LayoutBlock
from ingestion.clause import assign_canonical_ids, build_clause_chunks


def _block(role: str, text: str, page: int = 1, span_start: int = 0, span_end: int = 0) -> LayoutBlock:
    return LayoutBlock(role=role, text=text, page=page, span_start=span_start, span_end=span_end or len(text))


class TestBuildClauseChunks:
    def test_single_section(self):
        blocks = [
            _block("sectionHeading", "Limitation of Liability"),
            _block("content", "The total liability shall not exceed $1,000,000."),
        ]
        chunks = build_clause_chunks(blocks)
        assert len(chunks) >= 1
        assert "liability" in chunks[0].text.lower()

    def test_multiple_sections(self):
        blocks = [
            _block("sectionHeading", "Section 1: Definitions"),
            _block("content", "In this Agreement, the following terms..."),
            _block("sectionHeading", "Section 2: Scope of Work"),
            _block("content", "The Contractor shall provide..."),
        ]
        chunks = build_clause_chunks(blocks)
        assert len(chunks) >= 2

    def test_numbering_split(self):
        blocks = [
            _block("sectionHeading", "Section 5: Terms"),
            _block("content", "5.1 The initial term is 12 months.\n5.2 Renewal is automatic.\n5.3 Either party may terminate."),
        ]
        chunks = build_clause_chunks(blocks)
        assert len(chunks) >= 3

    def test_empty_blocks(self):
        chunks = build_clause_chunks([])
        assert chunks == []


class TestAssignCanonicalIds:
    def test_tier_a_explicit_number(self):
        blocks = [
            _block("sectionHeading", "5.3 Limitation of Liability"),
            _block("content", "Total liability shall not exceed $1M."),
        ]
        chunks = build_clause_chunks(blocks)
        chunks = assign_canonical_ids(chunks)
        assert chunks[0].canonical_section_id == "5.3"
        assert chunks[0].tier == "A"

    def test_tier_b_amendment_reference(self):
        blocks = [
            _block("sectionHeading", "Amendment"),
            _block("content", "Section 5.3 of the Agreement is hereby replaced with the following:"),
        ]
        chunks = build_clause_chunks(blocks)
        chunks = assign_canonical_ids(chunks)
        # Should detect the amendment reference.
        has_ref = any(c.tier == "B" and c.canonical_section_id == "5.3" for c in chunks)
        assert has_ref

    def test_tier_c_semantic_fallback(self):
        blocks = [
            _block("sectionHeading", "Limitation of Liability"),
            _block("content", "Total liability shall not exceed one million dollars."),
        ]
        chunks = build_clause_chunks(blocks)
        chunks = assign_canonical_ids(chunks)
        assert chunks[0].canonical_section_id.startswith("semantic:")
        assert chunks[0].tier == "C"
        assert "limitation" in chunks[0].canonical_section_id

    def test_mixed_tiers(self):
        blocks = [
            _block("sectionHeading", "1.1 Definitions"),
            _block("content", "Terms defined herein."),
            _block("sectionHeading", "Governing Law"),
            _block("content", "This agreement shall be governed by..."),
        ]
        chunks = build_clause_chunks(blocks)
        chunks = assign_canonical_ids(chunks)
        tiers = [c.tier for c in chunks]
        assert "A" in tiers
        assert "C" in tiers
