import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

from helpers import read_sql_file
from dataframe_utils import standardize_bp_readings, standardize_bg_readings, add_id_col
from db_utils import DatabaseManager


load_dotenv()
gps_db = DatabaseManager(
    username=os.getenv('LCH_SQL_GPS_USERNAME'),
    password=os.getenv('LCH_SQL_GPS_PASSWORD'),
    host=os.getenv('LCH_SQL_GPS_HOST'),
    database=os.getenv('LCH_SQL_GPS_DB')
)
gps_db.connect()
readings_db = DatabaseManager(
    username=os.getenv('LCH_SQL_USERNAME'),
    password=os.getenv('LCH_SQL_PASSWORD'),
    host=os.getenv('LCH_SQL_HOST'),
    database=os.getenv('LCH_SQL_SP_READINGS')
)
readings_db.connect()

get_queries_dir = Path.cwd() / 'queries' / 'gets'
bp_readings_stmt = read_sql_file(get_queries_dir / 'get_bp_readings.sql')
bg_readings_stmt = read_sql_file(get_queries_dir / 'get_bg_readings.sql')
patient_id_stmt = read_sql_file(get_queries_dir / 'get_patient_id.sql')
device_id_stmt = read_sql_file(get_queries_dir / 'get_device_id.sql')

bp_readings_df = readings_db.read_sql(bp_readings_stmt, parse_dates=['Time_Recorded', 'Time_Recieved'])
bg_readings_df = readings_db.read_sql(bg_readings_stmt, parse_dates=['Time_Recorded', 'Time_Recieved'])

bp_readings_df = standardize_bp_readings(bp_readings_df)
bg_readings_df = standardize_bg_readings(bg_readings_df)

patient_id_df = gps_db.read_sql(patient_id_stmt)
device_id_df = gps_db.read_sql(device_id_stmt)

bp_readings_df = add_id_col(df=bp_readings_df, id_df=patient_id_df, col='sharepoint_id')
bp_readings_df = add_id_col(df=bp_readings_df, id_df=device_id_df, col='patient_id')
bg_readings_df = add_id_col(df=bg_readings_df, id_df=patient_id_df, col='sharepoint_id')
bg_readings_df = add_id_col(df=bg_readings_df, id_df=device_id_df, col='patient_id')

gps_db.to_sql(bp_readings_df, 'blood_pressure_reading', if_exists='append')
gps_db.to_sql(bg_readings_df, 'glucose_reading', if_exists='append')