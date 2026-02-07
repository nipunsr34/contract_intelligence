"""CLI entry point for the contract hierarchy system.

Commands
--------
- init-db       Create all SQL tables.
- ingest        Ingest a single PDF or all PDFs in a directory.
- query         Run one of the four query scenarios.
- materialize   Recompute current state for a family.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime

import click

# Set up logging early.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cli")


@click.group()
def cli():
    """Contract Hierarchy System -- CLI."""
    pass


# ── init-db ───────────────────────────────────────────────────────────────

@cli.command("init-db")
def init_db():
    """Create all SQL tables."""
    from db.migrations import create_all_tables

    engine = create_all_tables()
    click.echo(f"Database initialised at {engine.url}")


# ── ingest ────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--pdf", "pdf_path", type=click.Path(exists=True), help="Path to a single PDF.")
@click.option("--dir", "dir_path", type=click.Path(exists=True), help="Directory of PDFs.")
@click.option("--force", is_flag=True, help="Re-process even if file hash exists.")
def ingest(pdf_path: str | None, dir_path: str | None, force: bool):
    """Ingest contract PDF(s) through the full pipeline."""
    if not pdf_path and not dir_path:
        click.echo("Provide --pdf or --dir.", err=True)
        sys.exit(1)

    from db.migrations import create_all_tables
    create_all_tables()

    from ingestion.pipeline import ingest_directory, ingest_document

    if pdf_path:
        result = ingest_document(pdf_path, force=force)
        _print_result(result)
    elif dir_path:
        results = ingest_directory(dir_path)
        for r in results:
            _print_result(r)


def _print_result(result):
    """Pretty-print an IngestionResult."""
    if result.skipped:
        click.echo(f"SKIPPED  doc_id={result.doc_id[:12]}...  (already ingested)")
    elif result.error:
        click.echo(f"ERROR    {result.error}")
    else:
        click.echo(
            f"OK       doc_id={result.doc_id[:12]}...  "
            f"family={result.family_id[:12] + '...' if result.family_id else 'N/A'}  "
            f"type={result.doc_type or 'N/A'}  "
            f"v_ingest={result.doc_version_ingest}  "
            f"v_timeline={result.doc_version_timeline}  "
            f"clauses={result.num_clauses}  "
            f"sections_mat={result.sections_materialized}"
        )


# ── query ─────────────────────────────────────────────────────────────────

@cli.group()
def query():
    """Query the contract hierarchy."""
    pass


@query.command("current")
@click.option("--family", required=True, help="Family ID or party names (PartyA-PartyB).")
@click.option("--section", required=True, help="Section ID (e.g., 5.3).")
def query_current_cmd(family: str, section: str):
    """Scenario 1: Current clause text."""
    from db.session import get_session
    from query.engine import query_current

    session = get_session()
    family_id, party_a, party_b = _parse_family_arg(family)
    result = query_current(session, section, family_id=family_id, party_a=party_a, party_b=party_b)
    _print_query_result(result)
    session.close()


@query.command("history")
@click.option("--family", required=True, help="Family ID or party names.")
@click.option("--section", required=True, help="Section ID.")
def query_history_cmd(family: str, section: str):
    """Scenario 2: Change timeline for a section."""
    from db.session import get_session
    from query.engine import query_history

    session = get_session()
    family_id, party_a, party_b = _parse_family_arg(family)
    result = query_history(session, section, family_id=family_id, party_a=party_a, party_b=party_b)
    _print_query_result(result)
    session.close()


@query.command("changes")
@click.option("--family", required=True, help="Family ID or party names.")
@click.option("--section", required=True, help="Section ID.")
@click.option("--version", required=True, type=int, help="Document version number.")
def query_changes_cmd(family: str, section: str, version: int):
    """Scenario 3: What a specific version changed."""
    from db.session import get_session
    from query.engine import query_changes

    session = get_session()
    family_id, party_a, party_b = _parse_family_arg(family)
    result = query_changes(session, section, version, family_id=family_id, party_a=party_a, party_b=party_b)
    _print_query_result(result)
    session.close()


@query.command("as-of")
@click.option("--family", required=True, help="Family ID or party names.")
@click.option("--section", required=True, help="Section ID.")
@click.option("--date", required=True, help="As-of date (YYYY-MM-DD).")
def query_as_of_cmd(family: str, section: str, date: str):
    """Scenario 4: Section state at a past date."""
    from db.session import get_session
    from query.engine import query_as_of

    session = get_session()
    family_id, party_a, party_b = _parse_family_arg(family)
    as_of_date = datetime.strptime(date, "%Y-%m-%d")
    result = query_as_of(session, section, as_of_date, family_id=family_id, party_a=party_a, party_b=party_b)
    _print_query_result(result)
    session.close()


@query.command("search")
@click.option("--family", required=True, help="Family ID or party names.")
@click.option("--section", required=True, help="Section query (e.g., 'limitation of liability').")
@click.option("--pdf", "pdf_path", type=click.Path(exists=True), required=True,
              help="Path to the original PDF (needed for PageIndex tree search).")
def query_search_cmd(family: str, section: str, pdf_path: str):
    """Query with PageIndex tree-search fallback (Fallback B).

    First tries SQL, then falls back to PageIndex reasoning-based retrieval.
    """
    from db.session import get_session
    from query.engine import query_with_pageindex_fallback

    session = get_session()
    family_id, party_a, party_b = _parse_family_arg(family)
    result = query_with_pageindex_fallback(
        session, section, pdf_path, family_id=family_id, party_a=party_a, party_b=party_b
    )
    _print_query_result(result)
    session.close()


def _parse_family_arg(family: str) -> tuple[str | None, str | None, str | None]:
    """Parse --family as either a raw ID or 'PartyA-PartyB' pair."""
    if "-" in family and len(family.split("-")) == 2:
        parts = family.split("-")
        return None, parts[0].strip(), parts[1].strip()
    return family, None, None


def _print_query_result(result):
    """Pretty-print a QueryResult."""
    if result.error:
        click.echo(f"ERROR: {result.error}", err=True)
    else:
        click.echo(json.dumps(result.data, indent=2, default=str))


# ── materialize ───────────────────────────────────────────────────────────

@cli.command()
@click.option("--family", required=True, help="Family ID to recompute.")
def materialize(family: str):
    """Recompute current state for a family."""
    from db.session import get_session
    from materialization.current_state import materialize_family

    session = get_session()
    count = materialize_family(family, session)
    session.commit()
    click.echo(f"Materialized {count} sections for family {family}.")
    session.close()


if __name__ == "__main__":
    cli()
