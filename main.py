import os
from pathlib import Path
from sqlalchemy import text
from dotenv import load_dotenv

from import_to_sql import import_patient_data, import_device_data, import_patient_note_data, \
    import_patient_reading_data
from db_utils import DatabaseManager
from helpers import read_sql_file
from logger import setup_logger


logger = setup_logger('main', level='info')

full_reset_stmt = read_sql_file(Path.cwd() / 'queries' / 'resets' / 'full_reset_patient_tables.sql', encoding="utf-8-sig")
load_dotenv()
gps_db = DatabaseManager(
    username=os.getenv('LCH_SQL_GPS_USERNAME'),
    password=os.getenv('LCH_SQL_GPS_PASSWORD'),
    host=os.getenv('LCH_SQL_GPS_HOST'),
    database=os.getenv('LCH_SQL_GPS_DB')
)
with gps_db.begin() as conn:
    conn.execute(text(full_reset_stmt))

import_patient_data(Path.cwd() / 'data' / 'Patient_Export.csv', logger=logger)
import_device_data(logger=logger)
import_patient_note_data(logger=logger)
import_patient_reading_data(logger=logger)