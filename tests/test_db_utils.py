import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
from unittest.mock import MagicMock, patch
from utils.db_utils import DatabaseManager


@pytest.fixture
def db_manager():
    return DatabaseManager()

@patch("sqlalchemy.create_engine")
def test_create_engine(mock_create_engine, db_manager):
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    db_manager.create_engine("username", "password", "host", "database")
    assert db_manager.engine == mock_engine
    assert isinstance(db_manager.session, sessionmaker)

def test_get_session(db_manager):
    db_manager.session = sessionmaker()
    session = db_manager.get_session()
    assert isinstance(session, sessionmaker().__call__())

@patch("sqlalchemy.orm.Session.execute")
def test_execute_query(mock_execute, db_manager):
    db_manager.session = sessionmaker()
    mock_session = db_manager.session()
    mock_execute.return_value.returns_rows = True
    mock_execute.return_value.fetchall.return_value = ["result"]
    with patch.object(mock_session, "execute", mock_execute):
        result = db_manager.execute_query("SELECT 1")
        assert result == ["result"]

@patch("pandas.read_sql")
def test_read_sql(mock_read_sql, db_manager):
    mock_df = MagicMock()
    mock_read_sql.return_value = mock_df
    db_manager.engine = create_engine(URL.create("sqlite:///:memory:"))
    result = db_manager.read_sql("SELECT 1")
    assert result == mock_df

@patch("pandas.DataFrame.to_sql")
def test_to_sql(mock_to_sql, db_manager):
    mock_df = MagicMock()
    db_manager.engine = create_engine(URL.create("sqlite:///:memory:"))
    db_manager.to_sql(mock_df, "table")
    mock_to_sql.assert_called_once_with(
        "table", db_manager.engine, if_exists="fail", index=False
    )

@patch("sqlalchemy.engine.Engine.dispose")
def test_close(mock_dispose, db_manager):
    mock_engine = MagicMock()
    db_manager.engine = mock_engine
    db_manager.close()
    mock_dispose.assert_called_once()
