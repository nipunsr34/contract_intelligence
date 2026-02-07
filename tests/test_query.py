"""Tests for Phase 8 -- time-travel query engine."""

import pytest
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from db.models import Base, ClauseNode, ContractDocument, Family, FamilySectionCurrent
from ingestion.hashing import (
    compute_doc_id,
    compute_family_id,
    compute_family_keys_hash,
    compute_file_hash,
    compute_node_id,
)
from materialization.current_state import materialize_section
from query.engine import query_as_of, query_changes, query_current, query_history


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def _pragma(dbapi_conn, _):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    with Session(engine) as sess:
        yield sess


def _setup_family_and_docs(session: Session):
    """Create a family with two docs and clause nodes."""
    keys_hash = compute_family_keys_hash("acme", "widget")
    family_id = compute_family_id(keys_hash)

    family = Family(
        family_id=family_id,
        family_keys_hash=keys_hash,
        partyA_norm="acme",
        partyB_norm="widget",
    )
    session.add(family)

    # Doc 1: master agreement
    fhash1 = compute_file_hash(b"master_doc")
    did1 = compute_doc_id(fhash1)
    doc1 = ContractDocument(
        doc_id=did1,
        file_hash=fhash1,
        family_id=family_id,
        doc_type="master",
        doc_version_ingest=1,
        doc_version_timeline=1,
        effective_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    session.add(doc1)

    # Doc 2: amendment
    fhash2 = compute_file_hash(b"amendment_doc")
    did2 = compute_doc_id(fhash2)
    doc2 = ContractDocument(
        doc_id=did2,
        file_hash=fhash2,
        family_id=family_id,
        doc_type="amendment",
        doc_version_ingest=2,
        doc_version_timeline=2,
        effective_ts=datetime(2024, 6, 1, tzinfo=timezone.utc),
    )
    session.add(doc2)
    session.flush()

    # Clause nodes for doc 1 (section 5.3 -- original).
    nid1 = compute_node_id(did1, 3, 100, 200, "5.3")
    node1 = ClauseNode(
        node_id=nid1,
        doc_id=did1,
        family_id=family_id,
        canonical_section_id="5.3",
        section_title="Limitation of Liability",
        change_action="NO_CHANGE",
        effective_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        page=3,
        span_start=100,
        span_end=200,
        clause_text="Total liability shall not exceed $500,000.",
        confidence=0.95,
    )
    session.add(node1)

    # Clause nodes for doc 2 (section 5.3 -- REPLACE).
    nid2 = compute_node_id(did2, 1, 50, 150, "5.3")
    node2 = ClauseNode(
        node_id=nid2,
        doc_id=did2,
        family_id=family_id,
        canonical_section_id="5.3",
        section_title="Limitation of Liability",
        change_action="REPLACE",
        effective_ts=datetime(2024, 6, 1, tzinfo=timezone.utc),
        page=1,
        span_start=50,
        span_end=150,
        clause_text="Total liability shall not exceed $1,000,000.",
        confidence=0.92,
    )
    session.add(node2)
    session.flush()

    # Materialize current state.
    materialize_section(family_id, "5.3", session)
    session.flush()

    return family_id, did1, did2


class TestQueryCurrent:
    def test_returns_latest_text(self, session):
        family_id, _, _ = _setup_family_and_docs(session)
        result = query_current(session, "5.3", family_id=family_id)
        assert result.error is None
        assert "$1,000,000" in result.data["composed_text"]

    def test_not_found_section(self, session):
        family_id, _, _ = _setup_family_and_docs(session)
        result = query_current(session, "99.99", family_id=family_id)
        assert result.error is not None

    def test_not_found_family(self, session):
        result = query_current(session, "5.3", family_id="nonexistent")
        assert result.error is not None


class TestQueryHistory:
    def test_returns_timeline(self, session):
        family_id, _, _ = _setup_family_and_docs(session)
        result = query_history(session, "5.3", family_id=family_id)
        assert result.error is None
        assert result.data["total_versions"] == 2
        timeline = result.data["timeline"]
        assert timeline[0]["clause_text"].startswith("Total liability")
        assert timeline[1]["change_action"] == "REPLACE"


class TestQueryChanges:
    def test_amendment_changes(self, session):
        family_id, _, _ = _setup_family_and_docs(session)
        result = query_changes(session, "5.3", doc_version=2, family_id=family_id)
        assert result.error is None
        changes = result.data["changes"]
        assert len(changes) == 1
        assert changes[0]["change_action"] == "REPLACE"

    def test_no_changes_for_section(self, session):
        family_id, _, _ = _setup_family_and_docs(session)
        result = query_changes(session, "10.1", doc_version=2, family_id=family_id)
        assert result.data["changes"] == []


class TestQueryAsOf:
    def test_before_amendment(self, session):
        family_id, _, _ = _setup_family_and_docs(session)
        result = query_as_of(
            session,
            "5.3",
            datetime(2024, 3, 1, tzinfo=timezone.utc),
            family_id=family_id,
        )
        assert result.error is None
        assert "$500,000" in result.data["composed_text"]

    def test_after_amendment(self, session):
        family_id, _, _ = _setup_family_and_docs(session)
        result = query_as_of(
            session,
            "5.3",
            datetime(2024, 7, 1, tzinfo=timezone.utc),
            family_id=family_id,
        )
        assert result.error is None
        assert "$1,000,000" in result.data["composed_text"]
