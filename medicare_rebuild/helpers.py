import calendar
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple


def create_directory(path: Path | str) -> None:
    """Create a directory from a string or a Path object.

    Args:
        path (Path | str): Directory path
    """
    if isinstance(path, str):
        path = Path(path)
    path.mkdir(parents=True, exist_ok=True)


def create_file(path: Path | str) -> None:
    if isinstance(path, str):
        path = Path(path)
    path.touch()


def get_files_in_dir(path: Path | str) -> List[Path]:
    """Get all the files in a directory.

    Args:
        path (Path | str): Directory path

    Returns:
        List[Path]: List of path objects representing the files in the directory.
    """
    if isinstance(path, str):
        path = Path(path)
    if path.is_dir():
        return [item for item in path.iterdir() if item.is_file()]


def delete_files_in_dir(path: Path | str) -> None:
    """Delete all the files in a directory.

    Args:
        path (Path | str): Directory path
    """
    if isinstance(path, str):
        path = Path(path)
    if path.exists():
        for file in path.glob("*"):
            if file.is_file():
                file.unlink()


def get_last_month_billing_cycle() -> Tuple[datetime, datetime]:
    """Get the start and end of last month's billing cycle.

    Returns:
        (datetime, datetime): datetime objects representing the first and last day of the billing cycle.
    """
    today = datetime.today()
    first_day_current_month = datetime(today.year, today.month, 1)
    last_day_last_month = first_day_current_month - timedelta(days=1)

    year = last_day_last_month.year
    month = last_day_last_month.month
    first_day = datetime(year, month, 1)
    last_day = datetime(year, month, calendar.monthrange(year, month)[1])

    return first_day, last_day
