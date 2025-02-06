import calendar
from datetime import datetime, timedelta


def get_last_month_billing_cycle():
    today = datetime.today()
    first_day_current_month = datetime(today.year, today.month, 1)
    last_day_last_month = first_day_current_month - timedelta(days=1)

    year = last_day_last_month.year
    month = last_day_last_month.month
    first_day = datetime(year, month, 1)
    last_day = datetime(year, month, calendar.monthrange(year, month)[1])
    
    return first_day, last_day