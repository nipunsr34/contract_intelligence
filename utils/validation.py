"""Pydantic models for validating LLM JSON outputs."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, field_validator


# ── Metadata resolver output ──────────────────────────────────────────────

class MetadataResolverOutput(BaseModel):
    """Validated output of the document-level metadata LLM resolver."""

    effective_ts: Optional[str] = None
    effective_date_source: Optional[str] = None
    partyA_norm: Optional[str] = None
    partyB_norm: Optional[str] = None
    term_start_ts: Optional[str] = None
    term_end_ts: Optional[str] = None
    doc_type: Optional[str] = None
    confidence: float = 0.0

    @field_validator("confidence")
    @classmethod
    def clamp_confidence(cls, v: float) -> float:
        return max(0.0, min(1.0, v))

    @field_validator("doc_type")
    @classmethod
    def validate_doc_type(cls, v: Optional[str]) -> Optional[str]:
        allowed = {"master", "amendment", "restatement", "sow", "addendum", None}
        if v is not None and v.lower() not in allowed:
            return None
        return v.lower() if v else None


# ── Clause enrichment output ──────────────────────────────────────────────

class ClauseEnrichmentOutput(BaseModel):
    """Validated output of the per-clause enrichment LLM."""

    canonical_section_id: Optional[str] = None
    referenced_section_id: Optional[str] = None
    change_action: Optional[str] = None
    modifies_section_id: Optional[str] = None
    effective_ts: Optional[str] = None
    extracted_facts: Optional[dict[str, Any]] = None
    confidence: float = 0.0

    @field_validator("confidence")
    @classmethod
    def clamp_confidence(cls, v: float) -> float:
        return max(0.0, min(1.0, v))

    @field_validator("change_action")
    @classmethod
    def validate_change_action(cls, v: Optional[str]) -> Optional[str]:
        allowed = {"REPLACE", "APPEND", "ADD_NEW", "DELETE", "NO_CHANGE", None}
        if v is not None:
            v_upper = v.upper()
            if v_upper not in allowed:
                return None
            return v_upper
        return None


def validate_metadata_output(data: dict) -> MetadataResolverOutput:
    """Parse and validate the metadata resolver JSON."""
    return MetadataResolverOutput.model_validate(data)


def validate_enrichment_output(data: dict) -> ClauseEnrichmentOutput:
    """Parse and validate the clause enrichment JSON."""
    return ClauseEnrichmentOutput.model_validate(data)
