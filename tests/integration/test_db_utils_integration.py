import pandas as pd
import pytest

pytestmark = pytest.mark.integration


def test_execute_query_ddl_and_dml(db_manager):
    db_manager.execute_query(
        "CREATE TABLE widgets (id INT IDENTITY PRIMARY KEY, name VARCHAR(50))"
    )
    insert_result = db_manager.execute_query(
        "INSERT INTO widgets (name) VALUES (:name)", {"name": "sprocket"}
    )
    assert insert_result is None

    select_result = db_manager.execute_query("SELECT name FROM widgets")
    assert select_result == [("sprocket",)]


def test_execute_query_rolls_back_on_error(db_manager):
    db_manager.execute_query(
        "CREATE TABLE widgets_rollback (id INT IDENTITY PRIMARY KEY, name VARCHAR(50) NOT NULL)"
    )
    result = db_manager.execute_query(
        "INSERT INTO widgets_rollback (name) VALUES (NULL)"
    )
    assert result is None

    remaining = db_manager.execute_query("SELECT COUNT(*) FROM widgets_rollback")
    assert remaining == [(0,)]


def test_read_sql_and_to_sql_round_trip(db_manager):
    df = pd.DataFrame({"id": [1, 2, 3], "label": ["a", "b", "c"]})
    db_manager.to_sql(df, "gadgets", if_exists="replace", index=False)

    result_df = db_manager.read_sql("SELECT id, label FROM gadgets ORDER BY id")
    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), df.reset_index(drop=True)
    )


def test_read_sql_parse_dates(db_manager):
    db_manager.execute_query(
        "CREATE TABLE events (id INT IDENTITY PRIMARY KEY, event_date DATETIME2)"
    )
    db_manager.execute_query(
        "INSERT INTO events (event_date) VALUES ('2026-01-15T10:30:00')"
    )

    result_df = db_manager.read_sql(
        "SELECT event_date FROM events", parse_dates=["event_date"]
    )
    assert pd.api.types.is_datetime64_any_dtype(result_df["event_date"])
    assert result_df["event_date"].iloc[0] == pd.Timestamp("2026-01-15T10:30:00")


def test_get_session_without_create_engine_raises():
    from medicare_rebuild.utils.db_utils import DatabaseManager

    db = DatabaseManager()
    with pytest.raises(Exception, match="Database connection is not established"):
        db.get_session()
