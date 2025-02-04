import os
import warnings
from pathlib import Path
from dotenv import load_dotenv

from import_to_sql import import_patient_data, import_device_data, import_patient_note_data, \
    import_patient_reading_data
from db_utils import DatabaseManager
from logger import setup_logger


warnings.filterwarnings("ignore")
load_dotenv()
dbm = DatabaseManager()
dbm.create_engine(
    'gps',
    username=os.getenv('LCH_SQL_GPS_USERNAME'),
    password=os.getenv('LCH_SQL_GPS_PASSWORD'),
    host=os.getenv('LCH_SQL_GPS_HOST'),
    database=os.getenv('LCH_SQL_GPS_DB')
)
with dbm.begin('gps') as conn:
    dbm.execute("EXEC reset_all_billing_tables", conn)

logger = setup_logger('main', level='debug')
import_patient_data(Path.cwd() / 'data' / 'Patient_Export.csv', snapshot=True, logger=logger)
import_device_data(snapshot=True, logger=logger)
import_patient_note_data(snapshot=True, logger=logger)
import_patient_reading_data(snapshot=True, logger=logger)