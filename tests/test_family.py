"""Tests for Phase 3 -- family matching and document versioning."""

import pytest
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from db.models import Base, ContractDocument, Family
from ingestion.family import assign_doc_versions, match_or_create_family
from ingestion.hashing import compute_doc_id, compute_file_hash


@pytest.fixture
def session():
    """Create an in-memory SQLite session for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    # Enable FK enforcement.
    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def _pragma(dbapi_conn, _):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    with Session(engine) as sess:
        yield sess


class TestMatchOrCreateFamily:
    def test_creates_new_family(self, session):
        fid = match_or_create_family("acme", "widget", session)
        assert fid
        family = session.get(Family, fid)
        assert family is not None
        assert family.partyA_norm in ("acme", "widget")

    def test_matches_existing_family(self, session):
        fid1 = match_or_create_family("acme", "widget", session)
        fid2 = match_or_create_family("acme", "widget", session)
        assert fid1 == fid2

    def test_order_invariant(self, session):
        fid1 = match_or_create_family("acme", "widget", session)
        fid2 = match_or_create_family("widget", "acme", session)
        assert fid1 == fid2

    def test_different_parties_different_family(self, session):
        fid1 = match_or_create_family("acme", "widget", session)
        fid2 = match_or_create_family("acme", "gadget", session)
        assert fid1 != fid2


class TestAssignDocVersions:
    def test_first_document(self, session):
        fid = match_or_create_family("acme", "widget", session)

        fhash = compute_file_hash(b"doc1 content")
        doc_id = compute_doc_id(fhash)
        doc = ContractDocument(doc_id=doc_id, file_hash=fhash, family_id=fid)
        session.add(doc)
        session.flush()

        v_ingest, v_timeline = assign_doc_versions(
            doc_id, fid, datetime(2024, 1, 1, tzinfo=timezone.utc), session
        )
        assert v_ingest == 1
        assert v_timeline == 1

    def test_second_document_later_date(self, session):
        fid = match_or_create_family("acme", "widget", session)

        # First doc.
        fhash1 = compute_file_hash(b"doc1")
        did1 = compute_doc_id(fhash1)
        session.add(ContractDocument(doc_id=did1, file_hash=fhash1, family_id=fid))
        session.flush()
        assign_doc_versions(did1, fid, datetime(2024, 1, 1, tzinfo=timezone.utc), session)

        # Second doc (later date).
        fhash2 = compute_file_hash(b"doc2")
        did2 = compute_doc_id(fhash2)
        session.add(ContractDocument(doc_id=did2, file_hash=fhash2, family_id=fid))
        session.flush()
        v_ingest, v_timeline = assign_doc_versions(
            did2, fid, datetime(2024, 6, 1, tzinfo=timezone.utc), session
        )
        assert v_ingest == 2
        assert v_timeline == 2

    def test_out_of_order_arrival(self, session):
        fid = match_or_create_family("acme", "widget", session)

        # First ingested doc has a LATER effective date.
        fhash1 = compute_file_hash(b"doc_later")
        did1 = compute_doc_id(fhash1)
        session.add(ContractDocument(doc_id=did1, file_hash=fhash1, family_id=fid))
        session.flush()
        assign_doc_versions(did1, fid, datetime(2024, 6, 1, tzinfo=timezone.utc), session)

        # Second ingested doc has an EARLIER effective date.
        fhash2 = compute_file_hash(b"doc_earlier")
        did2 = compute_doc_id(fhash2)
        session.add(ContractDocument(doc_id=did2, file_hash=fhash2, family_id=fid))
        session.flush()
        v_ingest, v_timeline = assign_doc_versions(
            did2, fid, datetime(2024, 1, 1, tzinfo=timezone.utc), session
        )

        # Ingest order: doc2 is second.
        assert v_ingest == 2
        # Timeline order: doc2 is FIRST (earlier date).
        assert v_timeline == 1
