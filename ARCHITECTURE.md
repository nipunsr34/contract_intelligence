# Architecture

This document describes the architecture of the Contract Hierarchy System -- a
vectorless, reasoning-based contract analysis pipeline that uses SQL as the
source of truth and supports time-travel queries over clause-level changes.

---

## High-Level Overview

The system ingests contract PDFs, extracts structured metadata and clause-level
data, groups contracts into "families" by party pair, tracks versions, enriches
each clause with an LLM, stores everything in a triple-write pattern
(SQL + ChromaDB + LlamaIndex), materializes a deterministic "current state" per
section, and exposes a query engine supporting four time-travel scenarios.

```
                         +-------------------+
                         |   Contract PDFs   |
                         +--------+----------+
                                  |
                                  v
                    +-----------------------------+
                    |   Ingestion Pipeline (P9)   |
                    |  pipeline.py orchestrator    |
                    +-----------------------------+
                    |  P0: Hashing / Idempotency   |
                    |  P1: Layout (Azure DocInt)   |
                    |  P1': PageIndex Fallback C   |
                    |  P2: Metadata (regex + LLM)  |
                    |  P3: Family Match + Version  |
                    |  P4: Clause Segmentation     |
                    |  P4': PageIndex Fallback A   |
                    |  P5: LLM Enrichment          |
                    |  P6: Triple Store            |
                    |  P7: Materialize Current     |
                    +-----------------------------+
                          |         |         |
                          v         v         v
                    +--------+ +--------+ +----------+
                    | SQLite | | Chroma | | LlamaIdx |
                    | (SoT)  | | (Vec)  | | (Query)  |
                    +--------+ +--------+ +----------+
                          |
                          v
                    +-----------------------------+
                    |   Query Engine (P8)          |
                    |  4 scenarios + PageIndex B   |
                    +-----------------------------+
                          |
                          v
                    +-----------------------------+
                    |        CLI (cli.py)          |
                    +-----------------------------+
```

---

## Project Structure

```
contract-hierarchy/
  config.py                 Central configuration (env vars, thresholds, models)
  cli.py                    Click CLI entry point

  db/
    models.py               SQLAlchemy ORM: Family, ContractDocument, ClauseNode,
                            FamilySectionCurrent
    session.py              Engine + session factory (SQLite with WAL + FK enforcement)
    migrations.py           create_all / drop_all via SQLAlchemy metadata

  ingestion/
    hashing.py              SHA-256 deterministic IDs (file_hash, doc_id, node_id)
    layout.py               Phase 1 orchestrator -- calls Azure DocInt
    metadata.py             Phase 2 -- regex candidates + LLM resolver
    family.py               Phase 3 -- family match/create + version assignment
    clause.py               Phase 4 -- heading-based + numbering-based segmentation
    enrichment.py           Phase 5 -- per-clause GPT enrichment
    store.py                Phase 6 -- triple write (SQL, Chroma, LlamaIndex)
    pipeline.py             Phase 9 -- single-document orchestrator

  materialization/
    current_state.py        Phase 7 -- supersession rules, family_section_current

  query/
    engine.py               Phase 8 -- 4 query scenarios + PageIndex fallback B

  integrations/
    azure_docint.py         Azure Document Intelligence SDK wrapper
    openai_client.py        OpenAI Chat + Embeddings wrapper
    chroma_store.py         ChromaDB persistent collection wrapper
    llamaindex_store.py     LlamaIndex VectorStoreIndex wrapper
    pageindex_stub.py       PageIndex integration (Fallbacks A, B, C)

  utils/
    normalization.py        Party name normalization, date parsing
    validation.py           Pydantic models for LLM output validation

  tests/
    test_hashing.py         Idempotency and hash determinism
    test_metadata.py        Regex extraction, date parsing, normalization
    test_family.py          Family matching, versioning, out-of-order arrival
    test_clause.py          Clause segmentation, 3-tier canonical IDs
    test_query.py           All 4 query scenarios with materialization
```

---

## Data Model

Four SQL tables form the source of truth.

### `family`

Groups contracts between the same two normalized parties.

| Column            | Type      | Notes                                  |
|-------------------|-----------|----------------------------------------|
| `family_id`       | TEXT PK   | SHA-256 of `family_keys_hash`          |
| `family_keys_hash`| TEXT UQ   | SHA-256 of sorted party names          |
| `partyA_norm`     | TEXT      | Normalized party A                     |
| `partyB_norm`     | TEXT      | Normalized party B                     |
| `created_at`      | TIMESTAMP | Auto                                   |

### `contract_document`

One row per ingested PDF.

| Column                   | Type      | Notes                                |
|--------------------------|-----------|--------------------------------------|
| `doc_id`                 | TEXT PK   | SHA-256 of `file_hash`               |
| `file_hash`              | TEXT UQ   | SHA-256 of PDF bytes                 |
| `family_id`              | TEXT FK   | -> family                            |
| `doc_type`               | TEXT      | master / amendment / restatement / sow / addendum |
| `doc_version_ingest`     | INTEGER   | Arrival order within family          |
| `doc_version_timeline`   | INTEGER   | Chronological order by effective_ts  |
| `effective_ts`           | TIMESTAMP | Contract effective date              |
| `term_start_ts`          | TIMESTAMP | Term start                           |
| `term_end_ts`            | TIMESTAMP | Term end                             |
| `parties_normalized_json`| TEXT      | JSON of normalized party names       |
| `confidence_scores`      | TEXT      | JSON of extraction confidences       |
| `raw_layout_json`        | TEXT      | Full Azure DocInt output             |
| `doc_quality`            | TEXT      | `normal` or `low`                    |
| `created_at`             | TIMESTAMP | Auto                                 |

### `clause_node`

One row per clause span extracted from a document.

| Column                | Type    | Notes                                    |
|-----------------------|---------|------------------------------------------|
| `node_id`             | TEXT PK | SHA-256 of doc_id + page + spans + section |
| `doc_id`              | TEXT FK | -> contract_document                     |
| `family_id`           | TEXT FK | -> family                                |
| `canonical_section_id`| TEXT    | Stable section identifier                |
| `section_title`       | TEXT    | Human-readable title                     |
| `referenced_section_id`| TEXT   | Section referenced by amendment          |
| `change_action`       | TEXT    | REPLACE / APPEND / ADD_NEW / DELETE / NO_CHANGE |
| `modifies_section_id` | TEXT    | Section being modified                   |
| `effective_ts`        | TIMESTAMP | Clause effective date                  |
| `page`                | INTEGER | Source page                              |
| `span_start`          | INTEGER | Character offset start                   |
| `span_end`            | INTEGER | Character offset end                     |
| `bbox_json`           | TEXT    | Bounding box JSON                        |
| `clause_text`         | TEXT    | Full clause text                         |
| `extracted_facts_json`| TEXT    | Structured facts (amounts, caps, etc.)   |
| `confidence`          | REAL    | Enrichment confidence 0.0-1.0           |
| `embedding_id`        | TEXT    | Chroma document ID                       |
| `created_at`          | TIMESTAMP | Auto                                   |

### `family_section_current`

Materialized current state per (family, section) pair.

| Column                | Type      | Notes                                  |
|-----------------------|-----------|----------------------------------------|
| `family_id`           | TEXT PK   | -> family (composite PK)               |
| `canonical_section_id`| TEXT PK   | (composite PK)                         |
| `current_node_id`     | TEXT FK   | -> clause_node (latest winner)         |
| `current_effective_ts`| TIMESTAMP | Effective date of current state        |
| `composed_text`       | TEXT      | Final text (base + appends)            |
| `updated_at`          | TIMESTAMP | Last materialization time              |

---

## Idempotency

Every entity gets a deterministic, content-derived ID:

- `file_hash = SHA256(pdf_bytes)` -- same PDF always produces the same hash.
- `doc_id = SHA256(file_hash)` -- derived from file hash.
- `node_id = SHA256(doc_id | page | span_start | span_end | canonical_section_id)` --
  derived from provenance.
- `family_id = SHA256(family_keys_hash)` where
  `family_keys_hash = SHA256(sorted(partyA, partyB))`.

Re-ingesting the same PDF is a no-op (early exit on `file_hash` match).
All writes use `INSERT OR REPLACE` / `session.merge()`.

---

## Pipeline Phases

### Phase 0 -- Hashing + Idempotency

Read the PDF, compute `file_hash` and `doc_id`. If the hash already exists in
`contract_document`, skip (unless `--force`).

### Phase 1 -- Layout Extraction

Call Azure Document Intelligence `prebuilt-layout` to get paragraph roles
(title, sectionHeading, pageHeader, pageFooter), bounding boxes, and spans.
Assess heading quality. If quality is low and PageIndex is enabled, Fallback C
(vision OCR) may be triggered.

### Phase 2 -- Metadata Extraction

**Step 2.1** -- Deterministic regex extraction of party names, dates, and
term candidates with evidence spans.

**Step 2.2** -- LLM resolver picks final values from the candidate list.
Output validated with Pydantic. Low confidence flags the doc for HITL.

### Phase 3 -- Family Matching + Versioning

Compute `family_keys_hash` from sorted normalized party names. Match or
create a family. Assign `doc_version_ingest` (arrival order) and
`doc_version_timeline` (chronological order). Timeline versions are
recomputed across the entire family to handle out-of-order arrival.

### Phase 4 -- Clause Segmentation

Group layout blocks by heading roles, then sub-split on numbering patterns
(`5.3`, `5.3(a)`, etc.). Assign `canonical_section_id` using a 3-tier
strategy:

- **Tier A**: Explicit section number in heading (`"5.3"`)
- **Tier B**: Amendment reference (`"Section 5.3 of the Agreement"`)
- **Tier C**: Semantic fallback (`"semantic:limitation_of_liability"`)

If doc quality is low and PageIndex is enabled, Fallback A replaces DocInt
blocks with PageIndex-generated hierarchical nodes.

### Phase 5 -- Clause Enrichment

For each clause chunk, call GPT with structured context. Strict JSON output:
`canonical_section_id`, `change_action`, `extracted_facts`, `confidence`.
Invalid JSON triggers one retry. Low confidence logs for HITL routing.

### Phase 6 -- Triple Store

1. **SQL** (source of truth) -- upsert `clause_node` rows.
2. **ChromaDB** -- embed clause text, upsert with metadata filters.
3. **LlamaIndex** -- wrap as `TextNode`, insert into `VectorStoreIndex`.

### Phase 7 -- Materialization

For each affected `(family_id, canonical_section_id)`, walk all clause nodes
chronologically and apply supersession rules:

- `REPLACE` -- latest replaces all prior.
- `APPEND` -- compose base + appended chain.
- `DELETE` -- mark section as deleted.
- `ADD_NEW` -- new baseline.
- Restatement -- reset baseline entirely.

Write result to `family_section_current`.

### Phase 8 -- Query Engine

Four scenarios, all SQL-first:

| Scenario | Description | Method |
|----------|-------------|--------|
| CURRENT | Latest clause text | Read `family_section_current` |
| HISTORY | Full change timeline | All nodes sorted by effective_ts |
| CHANGE_TRACK | What version N changed | Filter by doc_version |
| AS_OF | State at a past date | Nodes where effective_ts <= date, re-apply supersession |

Plus a PageIndex fallback (Fallback B): if SQL misses, tree-search retrieval.

### Phase 9 -- Pipeline Orchestrator

`ingest_document(pdf_path)` runs Phases 0-7 in sequence. Early exit on
duplicate hash. `ingest_directory(dir_path)` batch-processes all PDFs.

---

## PageIndex Integration

Three fallback integration points using the
[PageIndex](https://github.com/VectifyAI/PageIndex) library:

### Fallback A -- Better Sectionization

**When**: DocInt heading quality is low (`doc_quality == 'low'`).

**How**: Run `page_index_main()` to generate a hierarchical TOC tree.
Convert tree nodes into `LayoutBlock` objects via
`pageindex_nodes_to_layout_blocks()`. The clause pipeline then operates
on these higher-quality section boundaries.

### Fallback B -- Retriever Fallback

**When**: SQL query returns no results for a section.

**How**: Send the PageIndex tree structure + query to GPT for LLM-based
tree search. The model reasons about which nodes are most relevant
(by structure, not similarity). Extract text from the matched pages,
then run GPT extraction on the narrowed context.

### Fallback C -- Vision OCR

**When**: DocInt layout extraction fails or produces no blocks.

**How**: Render each PDF page as a PNG image using PyMuPDF. Send to a
vision model (GPT-5-mini) to extract markdown text. Convert the
markdown into synthetic `LayoutBlock` objects for the clause pipeline.

---

## External Dependencies

| Dependency | Purpose |
|------------|---------|
| SQLAlchemy | ORM + SQLite database |
| Azure AI Document Intelligence | PDF layout extraction |
| OpenAI | LLM enrichment + embeddings |
| ChromaDB | Vector store for clause embeddings |
| LlamaIndex | Vector store index with metadata filtering |
| PageIndex | Hierarchical TOC tree + reasoning-based retrieval |
| PyMuPDF | PDF page rendering for vision OCR |
| PyPDF2 | PDF text extraction (PageIndex dependency) |
| Pydantic | JSON output validation |
| Click | CLI framework |
| tiktoken | Token counting (PageIndex dependency) |

---

## Configuration

All settings are loaded from environment variables (`.env` file) via
`python-dotenv`. See `.env.example` for the full list. Key settings:

| Variable | Default | Purpose |
|----------|---------|---------|
| `OPENAI_API_KEY` | -- | OpenAI API key |
| `OPENAI_MODEL` | `gpt-5-mini` | Model for metadata + enrichment |
| `DATABASE_URL` | `sqlite:///contract_hierarchy.db` | SQLite path |
| `AZURE_DOCINT_ENDPOINT` | -- | Azure DocInt endpoint |
| `AZURE_DOCINT_KEY` | -- | Azure DocInt key |
| `PAGEINDEX_ENABLED` | `false` | Enable PageIndex fallbacks |
| `METADATA_CONFIDENCE_THRESHOLD` | `0.7` | Below this, flag for HITL |
| `ENRICHMENT_CONFIDENCE_THRESHOLD` | `0.7` | Below this, flag for HITL |

---

## Testing

52 unit tests cover the core logic without requiring external services:

- **test_hashing** -- Determinism, uniqueness, order invariance.
- **test_metadata** -- Party normalization, date parsing, regex extraction.
- **test_family** -- Family creation, dedup, order invariance, out-of-order versioning.
- **test_clause** -- Heading splits, numbering splits, all 3 canonical ID tiers.
- **test_query** -- All 4 scenarios with materialization against in-memory SQLite.

```bash
python -m pytest tests/ -v
```
