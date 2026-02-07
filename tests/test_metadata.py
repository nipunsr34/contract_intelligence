"""Tests for Phase 2 -- metadata candidate extraction and normalization."""

import pytest
from utils.normalization import (
    extract_date_strings,
    normalize_party_name,
    parse_date,
)
from ingestion.metadata import extract_candidates
from integrations.azure_docint import LayoutBlock


class TestNormalizePartyName:
    def test_strip_llc(self):
        assert normalize_party_name("Acme LLC") == "acme"

    def test_strip_inc(self):
        assert normalize_party_name("Widget Inc.") == "widget"

    def test_strip_corp(self):
        assert normalize_party_name("Big Corp") == "big"

    def test_strip_limited(self):
        assert normalize_party_name("Global Services Limited") == "global services"

    def test_collapse_whitespace(self):
        assert normalize_party_name("  Acme   Corp   LLC  ") == "acme"

    def test_remove_punctuation(self):
        assert normalize_party_name("O'Brien & Associates, Inc.") == "o brien associates"

    def test_lowercase(self):
        assert normalize_party_name("ACME HOLDINGS") == "acme"


class TestParseDate:
    def test_us_format(self):
        dt = parse_date("January 1, 2024")
        assert dt is not None
        assert dt.year == 2024 and dt.month == 1 and dt.day == 1

    def test_iso_format(self):
        dt = parse_date("2024-01-15")
        assert dt is not None
        assert dt.year == 2024 and dt.month == 1 and dt.day == 15

    def test_slash_format(self):
        dt = parse_date("01/15/2024")
        assert dt is not None

    def test_invalid(self):
        assert parse_date("not a date") is None


class TestExtractDateStrings:
    def test_finds_us_date(self):
        text = "This agreement is dated January 15, 2024."
        dates = extract_date_strings(text)
        assert len(dates) >= 1
        assert "January 15, 2024" in dates

    def test_finds_iso_date(self):
        text = "Effective date: 2024-01-15."
        dates = extract_date_strings(text)
        assert "2024-01-15" in dates

    def test_finds_multiple(self):
        text = "From January 1, 2024 to December 31, 2024."
        dates = extract_date_strings(text)
        assert len(dates) >= 2


class TestExtractCandidates:
    def test_between_and_pattern(self):
        block = LayoutBlock(
            role="content",
            text='This Agreement is entered into between Acme Corp. and Widget Inc.',
            page=1,
            span_start=0,
            span_end=64,
        )
        candidates = extract_candidates([block])
        party_texts = [c.text for c in candidates.party_candidates]
        assert any("Acme" in t for t in party_texts)

    def test_date_cue_extraction(self):
        block = LayoutBlock(
            role="content",
            text='Effective Date: January 1, 2024',
            page=1,
            span_start=0,
            span_end=31,
        )
        candidates = extract_candidates([block])
        assert len(candidates.date_candidates) >= 1

    def test_term_extraction(self):
        block = LayoutBlock(
            role="content",
            text='Initial Term: 12 months from the Effective Date.',
            page=2,
            span_start=100,
            span_end=150,
        )
        candidates = extract_candidates([block])
        assert len(candidates.term_candidates) >= 1
