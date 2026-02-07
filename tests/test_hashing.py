"""Tests for ingestion.hashing -- deterministic ID generation."""

import pytest
from ingestion.hashing import (
    compute_doc_id,
    compute_family_id,
    compute_family_keys_hash,
    compute_file_hash,
    compute_node_id,
)


class TestFileHash:
    def test_deterministic(self):
        data = b"Hello, contract world!"
        assert compute_file_hash(data) == compute_file_hash(data)

    def test_different_data(self):
        assert compute_file_hash(b"a") != compute_file_hash(b"b")

    def test_empty(self):
        h = compute_file_hash(b"")
        assert isinstance(h, str) and len(h) == 64


class TestDocId:
    def test_deterministic(self):
        fhash = "abc123"
        assert compute_doc_id(fhash) == compute_doc_id(fhash)

    def test_different_from_file_hash(self):
        fhash = "abc123"
        assert compute_doc_id(fhash) != fhash


class TestNodeId:
    def test_deterministic(self):
        nid = compute_node_id("doc1", 1, 0, 100, "5.3")
        assert nid == compute_node_id("doc1", 1, 0, 100, "5.3")

    def test_different_section(self):
        a = compute_node_id("doc1", 1, 0, 100, "5.3")
        b = compute_node_id("doc1", 1, 0, 100, "5.4")
        assert a != b

    def test_different_page(self):
        a = compute_node_id("doc1", 1, 0, 100, "5.3")
        b = compute_node_id("doc1", 2, 0, 100, "5.3")
        assert a != b


class TestFamilyKeysHash:
    def test_order_invariant(self):
        h1 = compute_family_keys_hash("Acme Corp", "Widget Inc")
        h2 = compute_family_keys_hash("Widget Inc", "Acme Corp")
        assert h1 == h2

    def test_case_insensitive(self):
        h1 = compute_family_keys_hash("acme corp", "widget inc")
        h2 = compute_family_keys_hash("ACME CORP", "WIDGET INC")
        assert h1 == h2

    def test_different_parties(self):
        h1 = compute_family_keys_hash("Acme", "Widget")
        h2 = compute_family_keys_hash("Acme", "Gadget")
        assert h1 != h2


class TestFamilyId:
    def test_deterministic(self):
        keys_hash = "abc"
        assert compute_family_id(keys_hash) == compute_family_id(keys_hash)
