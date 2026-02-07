# Contract Hierarchy System

A vectorless, reasoning-based contract analysis pipeline that builds a hierarchical
tree index from contract PDFs and uses SQL as the source of truth for clause-level
tracking, family matching, version management, and time-travel queries.

## Features

- **Idempotent ingestion** -- SHA-256 hashing ensures reprocessing never duplicates.
- **Azure Document Intelligence** layout extraction with quality fallback hooks.
- **Family matching** -- automatically groups contracts between the same parties.
- **Document versioning** -- both ingestion-order and timeline-order versions.
- **3-tier canonical section IDs** -- explicit numbering, amendment references, or
  semantic titles.
- **LLM enrichment** -- GPT extracts change actions, facts, and confidence per clause.
- **Triple storage** -- SQL (source of truth) + ChromaDB + LlamaIndex.
- **Materialized current state** -- supersession rules (REPLACE, APPEND, DELETE, etc.)
  give deterministic answers.
- **Time-travel query engine** -- current, history, change-tracking, and as-of-date
  queries.
- **PageIndex integration points** -- stubs ready for sectionization fallback,
  retrieval fallback, and OCR-free vision fallback.

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with your API keys

# 3. Initialize the database
python cli.py init-db

# 4. Ingest a contract PDF
python cli.py ingest --pdf /path/to/contract.pdf

# 5. Query current state
python cli.py query current --family "AcmeCorp-WidgetInc" --section "5.3"
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `init-db` | Create all SQL tables |
| `ingest --pdf FILE` | Ingest a single PDF |
| `ingest --dir DIR` | Batch-ingest all PDFs in a directory |
| `query current` | Scenario 1 -- current clause text |
| `query history` | Scenario 2 -- change timeline |
| `query changes` | Scenario 3 -- what a specific version changed |
| `query as-of` | Scenario 4 -- clause state at a past date |
| `materialize` | Recompute current state for a family |

## Architecture

```
PDF ─► DocInt Layout ─► Metadata Extraction ─► Family Match ─► Version Assign
  ─► Clause Segmentation ─► LLM Enrichment ─► SQL + Chroma + LlamaIndex
  ─► Materialize Current State
```

## Project Structure

```
config.py                 Central config (env vars, thresholds, model names)
db/                       SQLAlchemy models, session factory, migrations
ingestion/                Pipeline phases 1-6 and orchestrator (phase 9)
materialization/          Phase 7 -- current-state materialization
query/                    Phase 8 -- time-travel query engine
integrations/             Azure DocInt, OpenAI, ChromaDB, LlamaIndex, PageIndex stubs
utils/                    Normalization, validation helpers
cli.py                    Click CLI entry point
tests/                    Unit tests
```
