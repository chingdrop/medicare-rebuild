import calendar
from datetime import datetime, timedelta
from pathlib import Path


def read_file(filepath: Path, encoding: str=None) -> str:
    """Reads a file into text with given encoding.
        
        Args:
            - filename (Path): The filepath using pathlib.
            - encoding (str): The name of an encoding library.
        
        Returns:
            - String: The contents of the file.
        """
    with open(filepath, 'r', encoding=encoding) as f:
        file = f.read()
    return file


def get_last_month_billing_cycle():
    today = datetime.today()
    first_day_current_month = datetime(today.year, today.month, 1)
    last_day_last_month = first_day_current_month - timedelta(days=1)

    year = last_day_last_month.year
    month = last_day_last_month.month
    first_day = datetime(year, month, 1)
    last_day = datetime(year, month, calendar.monthrange(year, month)[1])
    
    return first_day.strftime("%Y-%m-%d"), last_day.strftime("%Y-%m-%d")