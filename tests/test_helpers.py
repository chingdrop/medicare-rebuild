from datetime import datetime
from pathlib import Path

import time_machine

from medicare_rebuild.helpers import (
    create_file,
    delete_files_in_dir,
    get_files_in_dir,
    get_last_month_billing_cycle,
)


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


# The three tests below freeze at noon, not midnight: time_machine treats a naive
# datetime as UTC, and datetime.today() (what get_last_month_billing_cycle() calls)
# returns local time, so a midnight instant could land on the wrong calendar day on a
# machine whose local timezone isn't UTC. Noon leaves a 12-hour margin either way.


@time_machine.travel(datetime(2024, 3, 15, 12, 0))
def test_get_last_month_billing_cycle():
    """Today being in March gives February's cycle, including its leap-year 29th."""
    first_day, last_day = get_last_month_billing_cycle()
    assert first_day == datetime(2024, 2, 1)
    assert last_day == datetime(2024, 2, 29)


@time_machine.travel(datetime(2024, 1, 15, 12, 0))
def test_get_last_month_billing_cycle_crosses_year_boundary():
    first_day, last_day = get_last_month_billing_cycle()
    assert first_day == datetime(2023, 12, 1)
    assert last_day == datetime(2023, 12, 31)


@time_machine.travel(datetime(2024, 6, 1, 12, 0))
def test_get_last_month_billing_cycle_on_the_first_of_the_month():
    """Today being the 1st still gives the full prior month, not a zero-length one."""
    first_day, last_day = get_last_month_billing_cycle()
    assert first_day == datetime(2024, 5, 1)
    assert last_day == datetime(2024, 5, 31)
