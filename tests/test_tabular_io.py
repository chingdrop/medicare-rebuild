"""write_structured_file tests: extension dispatch, unsupported-type/write-failure ->
TabularIOError, and that text-format writes actually go through atomic_write (no
partial file, no temp leftovers). Round trips read back with plain pandas.
"""

import pandas as pd
import pytest

from medicare_rebuild.utils.tabular_io import TabularIOError, write_structured_file


@pytest.fixture
def df():
    return pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})


def test_write_then_read_csv_round_trips(tmp_path, df):
    path = tmp_path / "out.csv"
    write_structured_file(df, path, index=False)
    result = pd.read_csv(path)
    pd.testing.assert_frame_equal(result, df)


def test_write_then_read_json_round_trips(tmp_path, df):
    path = tmp_path / "out.json"
    write_structured_file(df, path)
    result = pd.read_json(path)
    pd.testing.assert_frame_equal(result, df)


def test_write_then_read_xlsx_round_trips(tmp_path, df):
    path = tmp_path / "out.xlsx"
    write_structured_file(df, path, index=False)
    result = pd.read_excel(path, engine="openpyxl")
    pd.testing.assert_frame_equal(result, df)


def test_file_type_override_ignores_extension(tmp_path, df):
    # Written as .dat but told explicitly it's CSV content.
    path = tmp_path / "out.dat"
    write_structured_file(df, path, file_type="csv", index=False)
    result = pd.read_csv(path)
    pd.testing.assert_frame_equal(result, df)


def test_write_unsupported_extension_raises(tmp_path, df):
    path = tmp_path / "out.parquet"
    with pytest.raises(TabularIOError, match="Unsupported file type for writing"):
        write_structured_file(df, path)


def test_csv_write_leaves_no_temp_file_behind(tmp_path, df):
    path = tmp_path / "out.csv"
    write_structured_file(df, path, index=False)
    leftovers = [p for p in tmp_path.iterdir() if p != path]
    assert leftovers == []


def test_csv_write_failure_preserves_original_and_cleans_up(tmp_path, df, monkeypatch):
    path = tmp_path / "out.csv"
    path.write_text("original content")

    import os

    def _boom(*args, **kwargs):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(os, "replace", _boom)
    with pytest.raises(TabularIOError, match="Failed to write"):
        write_structured_file(df, path, index=False)

    assert path.read_text() == "original content"
    leftovers = [p for p in tmp_path.iterdir() if p != path]
    assert leftovers == []
