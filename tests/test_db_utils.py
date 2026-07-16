import logging
import pytest
from sqlalchemy.orm import sessionmaker, Session
from unittest.mock import MagicMock, patch

from medicare_rebuild.utils.db_utils import DatabaseManager


@pytest.fixture
def db_manager():
    return DatabaseManager()


def test_init_defaults(db_manager):
    assert isinstance(db_manager.logger, logging.Logger)
    assert db_manager.engine is None
    assert db_manager.session is None


@patch("medicare_rebuild.utils.db_utils.event.listen")
@patch("medicare_rebuild.utils.db_utils.create_engine")
def test_create_engine(mock_create_engine, mock_listen, db_manager):
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    db_manager.create_engine("username", "password", "host", "database")
    assert db_manager.engine == mock_engine
    assert isinstance(db_manager.session, sessionmaker)
    mock_listen.assert_called_once()


def test_get_session_without_engine_raises(db_manager):
    with pytest.raises(Exception, match="Database connection is not established"):
        db_manager.get_session()


def test_get_session(db_manager):
    db_manager.session = sessionmaker()
    session = db_manager.get_session()
    assert isinstance(session, Session)


def test_execute_query_returns_rows(db_manager):
    mock_session = MagicMock()
    mock_result = MagicMock()
    mock_result.returns_rows = True
    mock_result.fetchall.return_value = ["result"]
    mock_session.execute.return_value = mock_result
    db_manager.get_session = MagicMock(return_value=mock_session)

    result = db_manager.execute_query("SELECT 1")

    assert result == ["result"]
    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()


def test_execute_query_handles_error(db_manager):
    mock_session = MagicMock()
    mock_session.execute.side_effect = Exception("boom")
    db_manager.get_session = MagicMock(return_value=mock_session)
    db_manager.logger = MagicMock()

    result = db_manager.execute_query("SELECT 1")

    assert result is None
    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()
    db_manager.logger.error.assert_called_once()


@patch("medicare_rebuild.utils.db_utils.pd.read_sql")
def test_read_sql(mock_read_sql, db_manager):
    mock_df = MagicMock()
    mock_read_sql.return_value = mock_df
    db_manager.engine = MagicMock()

    result = db_manager.read_sql("SELECT 1")

    assert result == mock_df
    mock_read_sql.assert_called_once_with(
        "SELECT 1", db_manager.engine, params=None, parse_dates=None
    )


def test_to_sql(db_manager):
    mock_df = MagicMock()
    db_manager.engine = MagicMock()

    db_manager.to_sql(mock_df, "table")

    mock_df.to_sql.assert_called_once_with(
        "table", db_manager.engine, if_exists="fail", index=False
    )


def test_close(db_manager):
    mock_engine = MagicMock()
    db_manager.engine = mock_engine
    db_manager.close()
    mock_engine.dispose.assert_called_once()


def test_close_without_engine_is_noop(db_manager):
    db_manager.close()
