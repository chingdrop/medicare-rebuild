from datetime import datetime
from pathlib import Path

from medicare_rebuild.helpers import (
    create_directory,
    create_file,
    get_files_in_dir,
    delete_files_in_dir,
    get_last_month_billing_cycle,
)


def test_create_directory_from_str(tmp_path):
    target = str(tmp_path / "new_dir")
    create_directory(target)
    assert Path(target).is_dir()


def test_create_directory_from_path_is_idempotent(tmp_path):
    target = tmp_path / "new_dir"
    create_directory(target)
    create_directory(target)
    assert target.is_dir()


def test_create_directory_creates_parents(tmp_path):
    target = tmp_path / "a" / "b" / "c"
    create_directory(target)
    assert target.is_dir()


def test_create_file_from_str(tmp_path):
    target = str(tmp_path / "file.txt")
    create_file(target)
    assert Path(target).is_file()


def test_create_file_from_path(tmp_path):
    target = tmp_path / "file.txt"
    create_file(target)
    assert target.is_file()


def test_get_files_in_dir_returns_only_files(tmp_path):
    (tmp_path / "a.txt").touch()
    (tmp_path / "b.txt").touch()
    (tmp_path / "subdir").mkdir()
    result = get_files_in_dir(tmp_path)
    assert sorted(p.name for p in result) == ["a.txt", "b.txt"]


def test_get_files_in_dir_accepts_str(tmp_path):
    (tmp_path / "a.txt").touch()
    result = get_files_in_dir(str(tmp_path))
    assert [p.name for p in result] == ["a.txt"]


def test_get_files_in_dir_returns_none_for_non_directory(tmp_path):
    missing = tmp_path / "does_not_exist"
    assert get_files_in_dir(missing) is None


def test_delete_files_in_dir_removes_only_files(tmp_path):
    (tmp_path / "a.txt").touch()
    (tmp_path / "b.txt").touch()
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    delete_files_in_dir(tmp_path)
    assert list(tmp_path.glob("*.txt")) == []
    assert subdir.exists()


def test_delete_files_in_dir_missing_dir_is_noop(tmp_path):
    missing = tmp_path / "does_not_exist"
    delete_files_in_dir(missing)


class _FixedMarchDateTime(datetime):
    @classmethod
    def today(cls):
        return cls(2024, 3, 15)


def test_get_last_month_billing_cycle(monkeypatch):
    monkeypatch.setattr("medicare_rebuild.helpers.datetime", _FixedMarchDateTime)
    first_day, last_day = get_last_month_billing_cycle()
    assert first_day == datetime(2024, 2, 1)
    assert last_day == datetime(2024, 2, 29)


class _FixedJanuaryDateTime(datetime):
    @classmethod
    def today(cls):
        return cls(2024, 1, 15)


def test_get_last_month_billing_cycle_crosses_year_boundary(monkeypatch):
    monkeypatch.setattr("medicare_rebuild.helpers.datetime", _FixedJanuaryDateTime)
    first_day, last_day = get_last_month_billing_cycle()
    assert first_day == datetime(2023, 12, 1)
    assert last_day == datetime(2023, 12, 31)
