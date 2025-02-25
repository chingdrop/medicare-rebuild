import calendar
from datetime import datetime, timedelta
from pathlib import Path


def create_directory(path: Path | str) -> None:
    if isinstance(path, str):
        path = Path(path)
    if path.is_dir():
        path.mkdir(parents=True, exist_ok=True)


def get_files_in_dir(path: Path | str):
    if isinstance(path, str):
        path = Path(path)
    if path.is_dir():
        return [item for item in path.iterdir() if item.is_file()]


def delete_files_in_dir(path: Path | str) -> None:
    if isinstance(path, str):
        path = Path(path)
    if path.is_dir():
        for file in path.glob('*'):
            if file.is_file():
                file.unlink()


def get_last_month_billing_cycle():
    today = datetime.today()
    first_day_current_month = datetime(today.year, today.month, 1)
    last_day_last_month = first_day_current_month - timedelta(days=1)

    year = last_day_last_month.year
    month = last_day_last_month.month
    first_day = datetime(year, month, 1)
    last_day = datetime(year, month, calendar.monthrange(year, month)[1])
    
    return first_day, last_day