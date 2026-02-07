"""SQLAlchemy ORM models for the contract hierarchy system.

Tables
------
- family              Groups contracts between the same normalized parties.
- contract_document   One row per ingested PDF.
- clause_node         One row per clause span extracted from a document.
- family_section_current  Materialized "current truth" per (family, section).
"""

from datetime import datetime, timezone

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, relationship


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


# ── Family ─────────────────────────────────────────────────────────────────

class Family(Base):
    __tablename__ = "family"

    family_id = Column(String, primary_key=True)
    family_keys_hash = Column(String, unique=True, nullable=False)
    partyA_norm = Column(String, nullable=False)
    partyB_norm = Column(String, nullable=False)
    created_at = Column(DateTime, default=_utcnow)

    # relationships
    documents = relationship("ContractDocument", back_populates="family")
    clause_nodes = relationship(
        "ClauseNode",
        back_populates="family",
        foreign_keys="ClauseNode.family_id",
    )
    current_sections = relationship("FamilySectionCurrent", back_populates="family")


# ── Contract Document ──────────────────────────────────────────────────────

class ContractDocument(Base):
    __tablename__ = "contract_document"

    doc_id = Column(String, primary_key=True)
    file_hash = Column(String, unique=True, nullable=False)
    family_id = Column(String, ForeignKey("family.family_id"), nullable=True)
    doc_type = Column(String, nullable=True)  # master/amendment/restatement/sow/addendum
    doc_version_ingest = Column(Integer, nullable=True)
    doc_version_timeline = Column(Integer, nullable=True)
    effective_ts = Column(DateTime, nullable=True)
    term_start_ts = Column(DateTime, nullable=True)
    term_end_ts = Column(DateTime, nullable=True)
    parties_normalized_json = Column(Text, nullable=True)
    confidence_scores = Column(Text, nullable=True)
    raw_layout_json = Column(Text, nullable=True)
    doc_quality = Column(String, default="normal")  # normal / low
    created_at = Column(DateTime, default=_utcnow)

    __table_args__ = (
        CheckConstraint(
            "doc_type IN ('master','amendment','restatement','sow','addendum') OR doc_type IS NULL",
            name="ck_doc_type",
        ),
        CheckConstraint(
            "doc_quality IN ('normal','low')",
            name="ck_doc_quality",
        ),
        Index("ix_contract_document_family_id", "family_id"),
        Index("ix_contract_document_effective_ts", "effective_ts"),
    )

    # relationships
    family = relationship("Family", back_populates="documents")
    clause_nodes = relationship("ClauseNode", back_populates="document")


# ── Clause Node ────────────────────────────────────────────────────────────

class ClauseNode(Base):
    __tablename__ = "clause_node"

    node_id = Column(String, primary_key=True)
    doc_id = Column(String, ForeignKey("contract_document.doc_id"), nullable=False)
    family_id = Column(String, ForeignKey("family.family_id"), nullable=False)
    canonical_section_id = Column(String, nullable=False)
    section_title = Column(String, nullable=True)
    referenced_section_id = Column(String, nullable=True)
    change_action = Column(String, nullable=True)
    modifies_section_id = Column(String, nullable=True)
    effective_ts = Column(DateTime, nullable=True)
    page = Column(Integer, nullable=True)
    span_start = Column(Integer, nullable=True)
    span_end = Column(Integer, nullable=True)
    bbox_json = Column(Text, nullable=True)
    clause_text = Column(Text, nullable=False)
    extracted_facts_json = Column(Text, nullable=True)
    confidence = Column(Float, nullable=True)
    embedding_id = Column(String, nullable=True)
    created_at = Column(DateTime, default=_utcnow)

    __table_args__ = (
        CheckConstraint(
            "change_action IN ('REPLACE','APPEND','ADD_NEW','DELETE','NO_CHANGE') "
            "OR change_action IS NULL",
            name="ck_change_action",
        ),
        Index("ix_clause_node_doc_id", "doc_id"),
        Index("ix_clause_node_family_id", "family_id"),
        Index("ix_clause_node_family_section", "family_id", "canonical_section_id"),
        Index("ix_clause_node_effective_ts", "effective_ts"),
    )

    # relationships
    document = relationship("ContractDocument", back_populates="clause_nodes")
    family = relationship(
        "Family",
        back_populates="clause_nodes",
        foreign_keys=[family_id],
    )


# ── Family Section Current (materialized view) ────────────────────────────

class FamilySectionCurrent(Base):
    __tablename__ = "family_section_current"

    family_id = Column(
        String, ForeignKey("family.family_id"), primary_key=True
    )
    canonical_section_id = Column(String, primary_key=True)
    current_node_id = Column(
        String, ForeignKey("clause_node.node_id"), nullable=True
    )
    current_effective_ts = Column(DateTime, nullable=True)
    composed_text = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    # relationships
    family = relationship("Family", back_populates="current_sections")
    current_node = relationship("ClauseNode")
