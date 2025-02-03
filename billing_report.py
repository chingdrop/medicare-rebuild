import os
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import text

from db_utils import DatabaseManager
from helpers import read_sql_file


load_dotenv()
gps_db = DatabaseManager(
    username=os.getenv('LCH_SQL_GPS_USERNAME'),
    password=os.getenv('LCH_SQL_GPS_PASSWORD'),
    host=os.getenv('LCH_SQL_GPS_HOST'),
    database=os.getenv('LCH_SQL_GPS_DB')
)

with gps_db.begin() as conn:
    stmt = text("EXEC create_billing_report")
    df = pd.read_sql_query(stmt, conn)
    df.to_excel(Path.cwd() / 'data' / 'test_billing_report.xlsx', index=False, engine='openpyxl')