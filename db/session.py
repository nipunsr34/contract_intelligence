"""Database engine and session factory for the contract hierarchy system."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

import config


def get_engine(url: str | None = None):
    """Create a SQLAlchemy engine.

    Parameters
    ----------
    url : str, optional
        Override the DATABASE_URL from config.  Useful for tests.
    """
    db_url = url or config.DATABASE_URL
    # SQLite-specific: enable WAL mode for better concurrent reads and
    # foreign-key enforcement.
    connect_args = {}
    if db_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    engine = create_engine(db_url, connect_args=connect_args, echo=False)
    # Enable FK enforcement for SQLite
    if db_url.startswith("sqlite"):
        from sqlalchemy import event

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_conn, _connection_record):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    return engine


def get_session_factory(engine=None) -> sessionmaker[Session]:
    """Return a sessionmaker bound to *engine* (or the default engine)."""
    if engine is None:
        engine = get_engine()
    return sessionmaker(bind=engine, expire_on_commit=False)


def get_session(engine=None) -> Session:
    """Convenience: return a single new session."""
    factory = get_session_factory(engine)
    return factory()
