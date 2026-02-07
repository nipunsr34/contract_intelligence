"""Schema setup / migrations for the contract hierarchy database.

For this project we use SQLAlchemy's ``create_all`` to create tables from the
ORM models.  A future version could swap in Alembic for proper migrations.
"""

from db.models import Base
from db.session import get_engine


def create_all_tables(engine=None):
    """Create every table defined in ``db.models`` if it doesn't exist yet."""
    if engine is None:
        engine = get_engine()
    Base.metadata.create_all(engine)
    return engine


def drop_all_tables(engine=None):
    """Drop all tables.  **Destructive** -- use only in tests or resets."""
    if engine is None:
        engine = get_engine()
    Base.metadata.drop_all(engine)
    return engine
