"""
Contract Intelligence System -- Streamlit UI

A multi-page Streamlit application for ingesting, querying, and managing
contract documents through the contract hierarchy system.

Run with:
    streamlit run streamlit_app.py
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import streamlit as st

# ---------------------------------------------------------------------------
# Page config (must be first Streamlit call)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Contract Intelligence",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("streamlit_app")


# ---------------------------------------------------------------------------
# Database helpers (cached)
# ---------------------------------------------------------------------------
@st.cache_resource
def _get_session():
    """Return a reusable SQLAlchemy session."""
    from db.session import get_session
    return get_session()


def _ensure_tables():
    """Create DB tables if they don't exist yet."""
    from db.migrations import create_all_tables
    create_all_tables()


def _get_fresh_session():
    """Return a new session (for write operations that need isolation)."""
    from db.session import get_session
    return get_session()


# ---------------------------------------------------------------------------
# Data-loading helpers
# ---------------------------------------------------------------------------
@st.cache_data(ttl=30)
def _load_families() -> list[dict[str, Any]]:
    """Load all families from the database."""
    from db.models import Family
    session = _get_session()
    families = session.query(Family).order_by(Family.created_at.desc()).all()
    return [
        {
            "family_id": f.family_id,
            "partyA_norm": f.partyA_norm,
            "partyB_norm": f.partyB_norm,
            "created_at": f.created_at.isoformat() if f.created_at else None,
            "label": f"{f.partyA_norm} <> {f.partyB_norm}",
        }
        for f in families
    ]


@st.cache_data(ttl=30)
def _load_documents(family_id: Optional[str] = None) -> list[dict[str, Any]]:
    """Load contract documents, optionally filtered by family."""
    from db.models import ContractDocument
    session = _get_session()
    q = session.query(ContractDocument).order_by(
        ContractDocument.effective_ts.asc().nullslast()
    )
    if family_id:
        q = q.filter(ContractDocument.family_id == family_id)
    docs = q.all()
    return [
        {
            "doc_id": d.doc_id,
            "file_hash": d.file_hash,
            "family_id": d.family_id,
            "doc_type": d.doc_type,
            "doc_version_ingest": d.doc_version_ingest,
            "doc_version_timeline": d.doc_version_timeline,
            "effective_ts": d.effective_ts.isoformat() if d.effective_ts else None,
            "term_start_ts": d.term_start_ts.isoformat() if d.term_start_ts else None,
            "term_end_ts": d.term_end_ts.isoformat() if d.term_end_ts else None,
            "doc_quality": d.doc_quality,
            "confidence": json.loads(d.confidence_scores).get("metadata")
            if d.confidence_scores
            else None,
            "created_at": d.created_at.isoformat() if d.created_at else None,
        }
        for d in docs
    ]


@st.cache_data(ttl=30)
def _load_sections(family_id: str) -> list[str]:
    """Load distinct canonical_section_ids for a family."""
    from db.models import ClauseNode
    session = _get_session()
    rows = (
        session.query(ClauseNode.canonical_section_id)
        .filter(ClauseNode.family_id == family_id)
        .distinct()
        .order_by(ClauseNode.canonical_section_id)
        .all()
    )
    return [r[0] for r in rows]


@st.cache_data(ttl=30)
def _count_stats() -> dict[str, int]:
    """Quick aggregate stats for the dashboard."""
    from db.models import ClauseNode, ContractDocument, Family, FamilySectionCurrent
    session = _get_session()
    return {
        "families": session.query(Family).count(),
        "documents": session.query(ContractDocument).count(),
        "clauses": session.query(ClauseNode).count(),
        "current_sections": session.query(FamilySectionCurrent).count(),
    }


@st.cache_data(ttl=30)
def _load_hitl_candidates() -> list[dict[str, Any]]:
    """Load documents with low confidence that need human review."""
    from db.models import ContractDocument
    session = _get_session()
    docs = (
        session.query(ContractDocument)
        .filter(ContractDocument.doc_quality == "low")
        .order_by(ContractDocument.created_at.desc())
        .all()
    )
    results = []
    for d in docs:
        conf = None
        if d.confidence_scores:
            try:
                conf = json.loads(d.confidence_scores).get("metadata")
            except (json.JSONDecodeError, AttributeError):
                pass
        results.append({
            "doc_id": d.doc_id,
            "family_id": d.family_id,
            "doc_type": d.doc_type,
            "effective_ts": d.effective_ts.isoformat() if d.effective_ts else None,
            "confidence": conf,
            "parties_json": d.parties_normalized_json,
            "doc_quality": d.doc_quality,
            "created_at": d.created_at.isoformat() if d.created_at else None,
        })
    return results


# ═══════════════════════════════════════════════════════════════════════════
# SIDEBAR NAVIGATION
# ═══════════════════════════════════════════════════════════════════════════
st.sidebar.title("Contract Intelligence")
page = st.sidebar.radio(
    "Navigate",
    [
        "Dashboard",
        "Ingest Documents",
        "Query Contracts",
        "Browse Families",
        "HITL Review",
        "Analytics",
        "Settings",
    ],
    index=0,
)


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════
def _page_dashboard():
    st.header("Dashboard")

    _ensure_tables()

    stats = _count_stats()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Families", stats["families"])
    c2.metric("Documents", stats["documents"])
    c3.metric("Clause Nodes", stats["clauses"])
    c4.metric("Current Sections", stats["current_sections"])

    st.divider()

    # Quick actions
    st.subheader("Quick Actions")
    qa1, qa2, qa3 = st.columns(3)
    with qa1:
        if st.button("Upload Documents", use_container_width=True):
            st.session_state["_nav"] = "Ingest Documents"
            st.rerun()
    with qa2:
        if st.button("Query a Contract", use_container_width=True):
            st.session_state["_nav"] = "Query Contracts"
            st.rerun()
    with qa3:
        if st.button("Browse Families", use_container_width=True):
            st.session_state["_nav"] = "Browse Families"
            st.rerun()

    st.divider()

    # HITL alerts
    hitl = _load_hitl_candidates()
    if hitl:
        st.warning(
            f"{len(hitl)} document(s) flagged for human review (low confidence / low quality)."
        )

    # Recent documents
    st.subheader("Recent Documents")
    docs = _load_documents()
    if docs:
        for d in docs[:10]:
            doc_type_badge = d["doc_type"] or "unknown"
            conf_str = f"{d['confidence']:.0%}" if d.get("confidence") else "N/A"
            st.markdown(
                f"- **{d['doc_id'][:12]}...** | type: `{doc_type_badge}` | "
                f"effective: {d.get('effective_ts', 'N/A')} | "
                f"confidence: {conf_str} | quality: `{d.get('doc_quality', 'N/A')}`"
            )
    else:
        st.info("No documents ingested yet. Upload PDFs to get started.")


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: INGEST DOCUMENTS
# ═══════════════════════════════════════════════════════════════════════════
def _page_ingest():
    st.header("Ingest Documents")

    _ensure_tables()

    # File upload
    uploaded_files = st.file_uploader(
        "Upload contract PDF(s)",
        type=["pdf"],
        accept_multiple_files=True,
        help="Upload one or more contract PDFs for ingestion.",
    )

    # Options
    with st.expander("Advanced Options"):
        force = st.checkbox(
            "Force re-processing",
            value=False,
            help="Re-process even if the document has already been ingested.",
        )
        enable_pageindex = st.checkbox(
            "Enable PageIndex fallbacks",
            value=False,
            help="Use PageIndex for low-quality documents (requires PageIndex library).",
        )

    if st.button("Process Documents", type="primary", disabled=not uploaded_files):
        if not uploaded_files:
            st.warning("Please upload at least one PDF file.")
            return

        # Apply PageIndex setting temporarily
        import config as app_config
        original_pi = app_config.PAGEINDEX_ENABLED
        app_config.PAGEINDEX_ENABLED = enable_pageindex

        from ingestion.pipeline import ingest_document

        progress_bar = st.progress(0)
        status_container = st.container()
        results = []

        for i, uploaded_file in enumerate(uploaded_files):
            progress = (i + 1) / len(uploaded_files)
            progress_bar.progress(progress, text=f"Processing {uploaded_file.name}...")

            with status_container.status(
                f"Ingesting {uploaded_file.name}...", expanded=True
            ) as status:
                try:
                    # Save to temp file
                    with tempfile.NamedTemporaryFile(
                        suffix=".pdf", delete=False
                    ) as tmp:
                        tmp.write(uploaded_file.read())
                        tmp_path = tmp.name

                    st.write(f"Phase 0: Hashing...")
                    session = _get_fresh_session()
                    result = ingest_document(tmp_path, session=session, force=force)

                    if result.skipped:
                        status.update(
                            label=f"{uploaded_file.name}: Skipped (already ingested)",
                            state="complete",
                        )
                        st.info("Document already exists in the database.")
                    elif result.error:
                        status.update(
                            label=f"{uploaded_file.name}: Error",
                            state="error",
                        )
                        st.error(f"Error: {result.error}")
                    else:
                        status.update(
                            label=f"{uploaded_file.name}: Success",
                            state="complete",
                        )
                        st.success(
                            f"Ingested successfully: "
                            f"type=`{result.doc_type}`, "
                            f"family=`{result.family_id[:12] + '...' if result.family_id else 'N/A'}`, "
                            f"clauses={result.num_clauses}, "
                            f"sections_materialized={result.sections_materialized}"
                        )
                    results.append(result)

                except Exception as exc:
                    status.update(
                        label=f"{uploaded_file.name}: Failed",
                        state="error",
                    )
                    st.error(f"Ingestion failed: {exc}")
                finally:
                    # Clean up temp file
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass

        # Restore PageIndex setting
        app_config.PAGEINDEX_ENABLED = original_pi

        progress_bar.progress(1.0, text="Done!")

        # Clear caches so dashboard reflects new data
        _count_stats.clear()
        _load_families.clear()
        _load_documents.clear()

        # Summary
        st.divider()
        st.subheader("Ingestion Summary")
        ok = sum(1 for r in results if not r.skipped and not r.error)
        skipped = sum(1 for r in results if r.skipped)
        errored = sum(1 for r in results if r.error)
        c1, c2, c3 = st.columns(3)
        c1.metric("Successful", ok)
        c2.metric("Skipped", skipped)
        c3.metric("Errors", errored)


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: QUERY CONTRACTS
# ═══════════════════════════════════════════════════════════════════════════
def _page_query():
    st.header("Query Contracts")

    _ensure_tables()

    families = _load_families()
    if not families:
        st.info("No contract families found. Ingest some documents first.")
        return

    # -- Family picker --
    family_options = {f["label"]: f["family_id"] for f in families}
    selected_label = st.selectbox("Select Family", list(family_options.keys()))
    family_id = family_options[selected_label]

    # -- Section picker --
    sections = _load_sections(family_id)
    if not sections:
        st.warning("No sections found for this family.")
        return

    section = st.selectbox(
        "Select Section",
        sections,
        help="Canonical section ID (e.g. 5.3 or semantic:limitation_of_liability)",
    )

    st.divider()

    # -- Scenario tabs --
    tab_current, tab_history, tab_changes, tab_asof, tab_search = st.tabs(
        ["Current State", "History", "Change Tracking", "As-Of Date", "Semantic Search"]
    )

    session = _get_session()

    # ---- TAB: CURRENT ----
    with tab_current:
        if st.button("Get Current State", key="btn_current"):
            from query.engine import query_current

            result = query_current(session, section, family_id=family_id)
            if result.error:
                st.error(result.error)
            else:
                data = result.data
                c1, c2 = st.columns([1, 1])
                with c1:
                    st.metric(
                        "Effective Since",
                        data.get("current_effective_ts", "N/A"),
                    )
                with c2:
                    st.metric("Last Updated", data.get("updated_at", "N/A"))

                st.subheader("Composed Text")
                st.markdown(
                    f"```\n{data.get('composed_text', '(empty)')}\n```"
                )

                with st.expander("Raw Response"):
                    st.json(data)

    # ---- TAB: HISTORY ----
    with tab_history:
        if st.button("Get Full History", key="btn_history"):
            from query.engine import query_history

            result = query_history(session, section, family_id=family_id)
            if result.error:
                st.error(result.error)
            else:
                data = result.data
                st.metric("Total Versions", data["total_versions"])

                for i, entry in enumerate(data["timeline"]):
                    action = entry.get("change_action", "N/A")
                    action_colors = {
                        "REPLACE": "red",
                        "APPEND": "blue",
                        "ADD_NEW": "green",
                        "DELETE": "orange",
                        "NO_CHANGE": "gray",
                    }
                    color = action_colors.get(action, "gray")

                    with st.container(border=True):
                        h1, h2, h3, h4 = st.columns([1, 1, 1, 1])
                        h1.markdown(f"**Version {entry.get('doc_version', '?')}**")
                        h2.markdown(
                            f"Type: `{entry.get('doc_type', 'N/A')}`"
                        )
                        h3.markdown(f"Action: :{color}[**{action}**]")
                        h4.markdown(
                            f"Effective: {entry.get('effective_ts', 'N/A')}"
                        )

                        st.text(entry.get("clause_text", "(empty)"))

                        if entry.get("extracted_facts"):
                            with st.expander("Extracted Facts"):
                                st.json(entry["extracted_facts"])

                        if entry.get("confidence") is not None:
                            st.progress(
                                entry["confidence"],
                                text=f"Confidence: {entry['confidence']:.0%}",
                            )

    # ---- TAB: CHANGES ----
    with tab_changes:
        docs = _load_documents(family_id)
        versions = sorted(
            set(
                d["doc_version_timeline"]
                for d in docs
                if d.get("doc_version_timeline") is not None
            )
        )
        if not versions:
            st.info("No versioned documents found.")
        else:
            doc_version = st.selectbox(
                "Select Document Version", versions, key="change_version"
            )
            if st.button("Get Changes", key="btn_changes"):
                from query.engine import query_changes

                result = query_changes(
                    session, section, doc_version, family_id=family_id
                )
                if result.error:
                    st.error(result.error)
                else:
                    data = result.data
                    changes = data.get("changes", [])
                    if not changes:
                        st.info(
                            f"Version {doc_version} does not modify section '{section}'."
                        )
                    else:
                        for change in changes:
                            with st.container(border=True):
                                st.markdown(
                                    f"**Action:** `{change.get('change_action', 'N/A')}`"
                                )
                                if change.get("referenced_section_id"):
                                    st.markdown(
                                        f"References section: `{change['referenced_section_id']}`"
                                    )
                                st.text(change.get("clause_text", "(empty)"))
                                if change.get("extracted_facts"):
                                    with st.expander("Extracted Facts"):
                                        st.json(change["extracted_facts"])

    # ---- TAB: AS-OF ----
    with tab_asof:
        as_of_date = st.date_input(
            "As-Of Date",
            value=datetime.now().date(),
            key="asof_date",
        )
        if st.button("Query As-Of Date", key="btn_asof"):
            from query.engine import query_as_of

            as_of_dt = datetime(
                as_of_date.year,
                as_of_date.month,
                as_of_date.day,
                tzinfo=timezone.utc,
            )
            result = query_as_of(session, section, as_of_dt, family_id=family_id)
            if result.error:
                st.error(result.error)
            else:
                data = result.data
                status = data.get("status", "UNKNOWN")
                if status == "DELETED":
                    st.warning(
                        f"Section '{section}' was **deleted** as of {as_of_date}."
                    )
                else:
                    st.metric("Status", status)
                    st.metric(
                        "Effective Version Date",
                        data.get("effective_ts", "N/A"),
                    )
                    st.subheader("Composed Text at that Date")
                    st.markdown(
                        f"```\n{data.get('composed_text', '(empty)')}\n```"
                    )

                with st.expander("Raw Response"):
                    st.json(data)

    # ---- TAB: SEMANTIC SEARCH ----
    with tab_search:
        st.caption(
            "Natural language search over clause embeddings (requires ChromaDB)."
        )
        nl_query = st.text_input(
            "Search query",
            placeholder="e.g. What is the liability cap?",
            key="semantic_query",
        )
        top_k = st.slider("Results to return", 1, 20, 5, key="search_top_k")

        if st.button("Search", key="btn_search") and nl_query:
            try:
                from integrations.openai_client import embed_texts
                from integrations.chroma_store import query_clauses

                query_embedding = embed_texts([nl_query])[0]
                results = query_clauses(
                    query_embedding,
                    n_results=top_k,
                    where={"family_id": family_id},
                )
                ids = results.get("ids", [[]])[0]
                documents = results.get("documents", [[]])[0]
                metadatas = results.get("metadatas", [[]])[0]
                distances = results.get("distances", [[]])[0]

                if not ids:
                    st.info("No results found.")
                else:
                    for doc_text, meta, dist in zip(documents, metadatas, distances):
                        similarity = max(0, 1 - dist)
                        with st.container(border=True):
                            c1, c2 = st.columns([3, 1])
                            with c1:
                                st.markdown(
                                    f"**Section:** `{meta.get('canonical_section_id', '?')}`"
                                )
                                st.text(doc_text[:500])
                            with c2:
                                st.metric(
                                    "Similarity", f"{similarity:.0%}"
                                )
                                st.caption(
                                    f"Action: {meta.get('change_action', 'N/A')}"
                                )
                                st.caption(
                                    f"Page: {meta.get('page', 'N/A')}"
                                )
            except Exception as exc:
                st.error(f"Semantic search failed: {exc}")
                st.info(
                    "Make sure ChromaDB is populated and OpenAI API key is configured."
                )


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: BROWSE FAMILIES
# ═══════════════════════════════════════════════════════════════════════════
def _page_families():
    st.header("Contract Families")

    _ensure_tables()

    families = _load_families()
    if not families:
        st.info("No families found. Ingest some documents first.")
        return

    # Two-column layout: family list | family details
    col_list, col_detail = st.columns([1, 2])

    with col_list:
        st.subheader("Families")
        family_labels = [f["label"] for f in families]
        selected_idx = st.radio(
            "Select a family",
            range(len(family_labels)),
            format_func=lambda i: family_labels[i],
            label_visibility="collapsed",
        )
        selected_family = families[selected_idx]

    with col_detail:
        fid = selected_family["family_id"]
        st.subheader(selected_family["label"])
        st.caption(f"Family ID: `{fid}`")
        st.caption(f"Created: {selected_family.get('created_at', 'N/A')}")

        # Documents in this family
        st.markdown("---")
        st.markdown("**Documents**")
        docs = _load_documents(fid)
        if docs:
            for d in docs:
                doc_type = d.get("doc_type") or "unknown"
                version = d.get("doc_version_timeline")
                effective = d.get("effective_ts", "N/A")
                quality = d.get("doc_quality", "normal")
                quality_icon = "✅" if quality == "normal" else "⚠️"

                st.markdown(
                    f"- {quality_icon} **v{version}** `{doc_type}` | "
                    f"effective: {effective} | "
                    f"doc_id: `{d['doc_id'][:12]}...`"
                )
        else:
            st.info("No documents in this family.")

        # Sections
        st.markdown("---")
        st.markdown("**Sections**")
        sections = _load_sections(fid)
        if sections:
            for s in sections:
                st.markdown(f"- `{s}`")
        else:
            st.info("No clause sections found.")

        # Actions
        st.markdown("---")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Recompute Current State", key="recompute"):
                from materialization.current_state import materialize_family

                session = _get_fresh_session()
                count = materialize_family(fid, session)
                session.commit()
                session.close()
                _count_stats.clear()
                st.success(f"Materialized {count} sections.")
        with c2:
            if st.button("Export Family Data", key="export_family"):
                export_data = {
                    "family": selected_family,
                    "documents": docs,
                    "sections": sections,
                }
                st.download_button(
                    "Download JSON",
                    data=json.dumps(export_data, indent=2, default=str),
                    file_name=f"family_{fid[:12]}.json",
                    mime="application/json",
                )


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: HITL REVIEW
# ═══════════════════════════════════════════════════════════════════════════
def _page_hitl():
    st.header("Human-in-the-Loop Review")

    _ensure_tables()

    candidates = _load_hitl_candidates()
    if not candidates:
        st.success("No documents need review. All clear!")
        return

    st.warning(f"{len(candidates)} document(s) flagged for review.")

    for i, doc in enumerate(candidates):
        with st.expander(
            f"Document {i + 1}: {doc['doc_id'][:16]}... "
            f"(confidence: {doc['confidence']:.0%})"
            if doc.get("confidence")
            else f"Document {i + 1}: {doc['doc_id'][:16]}...",
            expanded=(i == 0),
        ):
            c1, c2 = st.columns(2)

            with c1:
                st.markdown("**Current Extraction (AI)**")
                st.markdown(f"- **Doc Type:** `{doc.get('doc_type', 'N/A')}`")
                st.markdown(
                    f"- **Effective Date:** {doc.get('effective_ts', 'N/A')}"
                )
                st.markdown(f"- **Quality:** `{doc.get('doc_quality', 'N/A')}`")
                if doc.get("parties_json"):
                    try:
                        parties = json.loads(doc["parties_json"])
                        st.markdown(
                            f"- **Party A:** `{parties.get('partyA', 'N/A')}`"
                        )
                        st.markdown(
                            f"- **Party B:** `{parties.get('partyB', 'N/A')}`"
                        )
                    except json.JSONDecodeError:
                        st.markdown("- **Parties:** (parse error)")
                if doc.get("confidence") is not None:
                    st.progress(
                        doc["confidence"],
                        text=f"Confidence: {doc['confidence']:.0%}",
                    )

            with c2:
                st.markdown("**Corrected Values (Human)**")
                new_type = st.selectbox(
                    "Doc Type",
                    ["master", "amendment", "restatement", "sow", "addendum"],
                    index=0,
                    key=f"hitl_type_{i}",
                )
                new_date = st.date_input(
                    "Effective Date", key=f"hitl_date_{i}"
                )
                new_party_a = st.text_input(
                    "Party A (normalized)", key=f"hitl_pa_{i}"
                )
                new_party_b = st.text_input(
                    "Party B (normalized)", key=f"hitl_pb_{i}"
                )

            col_approve, col_skip = st.columns(2)
            with col_approve:
                if st.button(
                    "Approve & Update", key=f"hitl_approve_{i}", type="primary"
                ):
                    st.info(
                        "HITL update logic would apply corrections here. "
                        "This requires re-running family matching and materialization "
                        "with the corrected metadata."
                    )
                    # NOTE: Full implementation would:
                    # 1. Update ContractDocument with corrected metadata
                    # 2. Re-run family matching with corrected parties
                    # 3. Re-materialize affected sections
                    # 4. Mark as reviewed
            with col_skip:
                if st.button("Skip", key=f"hitl_skip_{i}"):
                    st.info("Skipped.")


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════
def _page_analytics():
    st.header("Analytics & Insights")

    _ensure_tables()

    stats = _count_stats()
    if stats["documents"] == 0:
        st.info("No data to analyze. Ingest some documents first.")
        return

    # Top-level metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Families", stats["families"])
    c2.metric("Documents", stats["documents"])
    c3.metric("Clause Nodes", stats["clauses"])
    c4.metric("Current Sections", stats["current_sections"])

    st.divider()

    # Document type distribution
    docs = _load_documents()
    if docs:
        col_chart1, col_chart2 = st.columns(2)

        with col_chart1:
            st.subheader("Document Types")
            type_counts: dict[str, int] = {}
            for d in docs:
                dt = d.get("doc_type") or "unknown"
                type_counts[dt] = type_counts.get(dt, 0) + 1

            import plotly.express as px

            fig_types = px.pie(
                names=list(type_counts.keys()),
                values=list(type_counts.values()),
                title="Distribution of Document Types",
            )
            st.plotly_chart(fig_types, use_container_width=True)

        with col_chart2:
            st.subheader("Document Quality")
            quality_counts: dict[str, int] = {}
            for d in docs:
                q = d.get("doc_quality") or "unknown"
                quality_counts[q] = quality_counts.get(q, 0) + 1

            fig_quality = px.pie(
                names=list(quality_counts.keys()),
                values=list(quality_counts.values()),
                title="Quality Distribution",
                color_discrete_map={"normal": "#2ecc71", "low": "#e74c3c"},
            )
            st.plotly_chart(fig_quality, use_container_width=True)

        # Confidence distribution
        st.subheader("Confidence Distribution")
        confidences = [
            d["confidence"] for d in docs if d.get("confidence") is not None
        ]
        if confidences:
            fig_conf = px.histogram(
                x=confidences,
                nbins=20,
                labels={"x": "Confidence", "y": "Count"},
                title="Metadata Extraction Confidence",
            )
            fig_conf.add_vline(
                x=0.7,
                line_dash="dash",
                line_color="red",
                annotation_text="HITL Threshold (0.7)",
            )
            st.plotly_chart(fig_conf, use_container_width=True)

            avg_conf = sum(confidences) / len(confidences)
            high = sum(1 for c in confidences if c >= 0.8)
            medium = sum(1 for c in confidences if 0.7 <= c < 0.8)
            low = sum(1 for c in confidences if c < 0.7)

            cc1, cc2, cc3, cc4 = st.columns(4)
            cc1.metric("Avg Confidence", f"{avg_conf:.0%}")
            cc2.metric("High (>=80%)", high)
            cc3.metric("Medium (70-80%)", medium)
            cc4.metric("Low (<70%)", low)

        # Family activity
        st.divider()
        st.subheader("Families by Document Count")
        families = _load_families()
        family_doc_counts = []
        for f in families:
            f_docs = [d for d in docs if d.get("family_id") == f["family_id"]]
            family_doc_counts.append({
                "family": f["label"],
                "documents": len(f_docs),
            })
        family_doc_counts.sort(key=lambda x: x["documents"], reverse=True)

        if family_doc_counts:
            top_families = family_doc_counts[:10]
            fig_families = px.bar(
                x=[f["family"] for f in top_families],
                y=[f["documents"] for f in top_families],
                labels={"x": "Family", "y": "Documents"},
                title="Top Families by Document Count",
            )
            fig_families.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_families, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: SETTINGS
# ═══════════════════════════════════════════════════════════════════════════
def _page_settings():
    st.header("Settings")

    import config as app_config

    tab_general, tab_api, tab_thresholds = st.tabs(
        ["General", "API Keys", "Thresholds"]
    )

    with tab_general:
        st.subheader("Database")
        st.text_input("Database URL", value=app_config.DATABASE_URL, disabled=True)
        st.text_input(
            "ChromaDB Directory", value=app_config.CHROMA_PERSIST_DIR, disabled=True
        )
        st.text_input(
            "Chroma Collection",
            value=app_config.CHROMA_COLLECTION_NAME,
            disabled=True,
        )

        st.subheader("PageIndex")
        st.checkbox(
            "PageIndex Enabled",
            value=app_config.PAGEINDEX_ENABLED,
            disabled=True,
        )
        st.text_input(
            "PageIndex Model", value=app_config.PAGEINDEX_MODEL, disabled=True
        )

        st.caption(
            "To change these values, edit the `.env` file or set environment variables."
        )

    with tab_api:
        st.subheader("API Keys")
        st.text_input(
            "OpenAI API Key",
            value="***" + app_config.OPENAI_API_KEY[-4:]
            if len(app_config.OPENAI_API_KEY) > 4
            else "(not set)",
            disabled=True,
            type="password",
        )
        st.text_input("OpenAI Model", value=app_config.OPENAI_MODEL, disabled=True)
        st.text_input(
            "Embedding Model",
            value=app_config.OPENAI_EMBEDDING_MODEL,
            disabled=True,
        )

        st.divider()
        st.text_input(
            "Azure DocInt Endpoint",
            value=app_config.AZURE_DOCINT_ENDPOINT or "(not set)",
            disabled=True,
        )
        st.text_input(
            "Azure DocInt Key",
            value="***" + app_config.AZURE_DOCINT_KEY[-4:]
            if len(app_config.AZURE_DOCINT_KEY) > 4
            else "(not set)",
            disabled=True,
            type="password",
        )

        # Connection tests
        st.divider()
        st.subheader("Connection Tests")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Test OpenAI", key="test_openai"):
                try:
                    from integrations.openai_client import chat_json

                    resp = chat_json(
                        system_prompt="Reply with JSON: {\"status\": \"ok\"}",
                        user_prompt="ping",
                    )
                    if resp.get("status") == "ok":
                        st.success("OpenAI connection OK")
                    else:
                        st.warning(f"Unexpected response: {resp}")
                except Exception as exc:
                    st.error(f"OpenAI test failed: {exc}")
        with c2:
            if st.button("Test Database", key="test_db"):
                try:
                    session = _get_fresh_session()
                    session.execute(
                        __import__("sqlalchemy").text("SELECT 1")
                    )
                    session.close()
                    st.success("Database connection OK")
                except Exception as exc:
                    st.error(f"Database test failed: {exc}")

    with tab_thresholds:
        st.subheader("Confidence Thresholds")
        st.slider(
            "Metadata Confidence Threshold",
            0.0,
            1.0,
            app_config.METADATA_CONFIDENCE_THRESHOLD,
            0.05,
            disabled=True,
            help="Documents below this threshold are flagged for HITL review.",
        )
        st.slider(
            "Enrichment Confidence Threshold",
            0.0,
            1.0,
            app_config.ENRICHMENT_CONFIDENCE_THRESHOLD,
            0.05,
            disabled=True,
            help="Clauses below this threshold are flagged for review.",
        )
        st.caption(
            "To change thresholds, set METADATA_CONFIDENCE_THRESHOLD and "
            "ENRICHMENT_CONFIDENCE_THRESHOLD environment variables."
        )


# ═══════════════════════════════════════════════════════════════════════════
# ROUTER
# ═══════════════════════════════════════════════════════════════════════════
# Support quick-nav from dashboard buttons
if "_nav" in st.session_state:
    page = st.session_state.pop("_nav")

_PAGES = {
    "Dashboard": _page_dashboard,
    "Ingest Documents": _page_ingest,
    "Query Contracts": _page_query,
    "Browse Families": _page_families,
    "HITL Review": _page_hitl,
    "Analytics": _page_analytics,
    "Settings": _page_settings,
}

_PAGES[page]()
