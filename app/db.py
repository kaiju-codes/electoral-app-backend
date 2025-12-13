from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from .config import get_settings


settings = get_settings()

engine = create_engine(settings.database_url, future=True, echo=False)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

Base = declarative_base()


def check_database_connection() -> None:
    """
    Check if database connection is available.
    Raises an exception if the connection cannot be established.
    This will cause the application startup to fail.
    """
    try:
        with engine.connect() as connection:
            # Execute a simple query to verify connection
            connection.execute(text("SELECT 1"))
        return
    except SQLAlchemyError as e:
        raise ConnectionError(
            f"Failed to connect to database at startup. "
            f"Database URL: {settings.database_url.split('@')[-1] if '@' in settings.database_url else '***'}. "
            f"Error: {str(e)}"
        ) from e


@contextmanager
def session_scope() -> Iterator[Session]:
    """Provide a transactional scope around a series of operations."""
    session: Session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db() -> Iterator[Session]:
    """FastAPI dependency that yields a DB session."""
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()


