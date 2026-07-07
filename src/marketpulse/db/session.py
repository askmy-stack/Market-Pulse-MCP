"""Database session helpers."""

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy.orm import Session

from marketpulse.db.models import get_session_factory
from marketpulse.db.models import init_db as _init_db


def init_db() -> None:
    _init_db()


@contextmanager
def get_db() -> Generator[Session, None, None]:
    session = get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
