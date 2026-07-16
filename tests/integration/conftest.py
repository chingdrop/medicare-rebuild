import os
import time

import pyodbc
import pytest

from medicare_rebuild.utils.db_utils import DatabaseManager

DB_HOST = os.environ.get("INTEGRATION_DB_HOST", "localhost")
DB_PORT = os.environ.get("INTEGRATION_DB_PORT", "14330")
DB_USER = os.environ.get("INTEGRATION_DB_USER", "sa")
DB_PASSWORD = os.environ.get("INTEGRATION_DB_PASSWORD", "IntegrationTest_Passw0rd!")
TEST_DB_NAME = "medicare_rebuild_integration_test"


def _connect_str() -> str:
    return (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={DB_HOST},{DB_PORT};UID={DB_USER};PWD={DB_PASSWORD};"
        "TrustServerCertificate=yes"
    )


def _wait_for_server(retries: int = 5, delay: float = 2.0) -> None:
    last_exc: Exception | None = None
    for _ in range(retries):
        try:
            conn = pyodbc.connect(_connect_str(), timeout=5)
            conn.close()
            return
        except Exception as exc:
            last_exc = exc
            time.sleep(delay)
    pytest.skip(
        f"Integration SQL Server not reachable at {DB_HOST}:{DB_PORT} "
        f"(run `docker compose up -d`): {last_exc}"
    )


def _connect(database: str) -> DatabaseManager:
    db = DatabaseManager()
    db.create_engine(
        username=DB_USER,
        password=DB_PASSWORD,
        host=f"{DB_HOST},{DB_PORT}",
        database=database,
    )
    return db


@pytest.fixture(scope="session")
def test_database():
    """Create a dedicated database on the test SQL Server for the session, drop it after."""
    _wait_for_server()

    conn = pyodbc.connect(_connect_str(), timeout=10)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"IF DB_ID('{TEST_DB_NAME}') IS NOT NULL DROP DATABASE {TEST_DB_NAME}")
    cur.execute(f"CREATE DATABASE {TEST_DB_NAME}")
    conn.close()

    yield TEST_DB_NAME

    conn = pyodbc.connect(_connect_str(), timeout=10)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        f"IF DB_ID('{TEST_DB_NAME}') IS NOT NULL "
        f"ALTER DATABASE {TEST_DB_NAME} SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
    )
    cur.execute(f"IF DB_ID('{TEST_DB_NAME}') IS NOT NULL DROP DATABASE {TEST_DB_NAME}")
    conn.close()


@pytest.fixture
def db_manager(test_database):
    """A DatabaseManager connected to the dedicated integration test database."""
    db = _connect(test_database)
    yield db
    db.close()


def execute_ddl(database: str, statements: list[str]) -> None:
    """Run DDL statements directly (bypassing DatabaseManager's swallowed exceptions),
    so schema setup failures raise loudly instead of silently logging and returning None.
    """
    conn = pyodbc.connect(_connect_str() + f";DATABASE={database}")
    conn.autocommit = True
    cur = conn.cursor()
    for statement in statements:
        cur.execute(statement)
    conn.close()
