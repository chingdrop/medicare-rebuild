import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

from helpers import read_sql_file
from dataframe_utils import standardize_bp_readings, standardize_bg_readings, add_id_col
from sql_connect import create_alchemy_engine


load_dotenv()
gps_engine = create_alchemy_engine(
    username=os.getenv('LCH_SQL_GPS_USERNAME'),
    password=os.getenv('LCH_SQL_GPS_PASSWORD'),
    host=os.getenv('LCH_SQL_GPS_HOST'),
    database=os.getenv('LCH_SQL_GPS_DB')
)
readings_engine = create_alchemy_engine(
    username=os.getenv('LCH_SQL_USERNAME'),
    password=os.getenv('LCH_SQL_PASSWORD'),
    host=os.getenv('LCH_SQL_HOST'),
    database=os.getenv('LCH_SQL_SP_READINGS')
)

get_queries_dir = Path.cwd() / 'queries' / 'gets'
bp_readings_stmt = read_sql_file(get_queries_dir / 'get_bp_readings.sql')
bg_readings_stmt = read_sql_file(get_queries_dir / 'get_bg_readings.sql')
patient_id_stmt = read_sql_file(get_queries_dir / 'get_patient_id.sql')
device_id_stmt = read_sql_file(get_queries_dir / 'get_device_id.sql')

with readings_engine.begin() as conn:
    bp_readings_df = pd.read_sql(
        bp_readings_stmt,
        conn,
        parse_dates=['Time_Recorded', 'Time_Recieved']
    )
    bg_readings_df = pd.read_sql(
        bg_readings_stmt,
        conn,
        parse_dates=['Time_Recorded', 'Time_Recieved']
    )

bp_readings_df = standardize_bp_readings(bp_readings_df)
bg_readings_df = standardize_bg_readings(bg_readings_df)

with gps_engine.begin() as conn:
    patient_id_df = pd.read_sql(patient_id_stmt, conn)
    device_id_df = pd.read_sql(device_id_stmt, conn)

    bp_readings_df = add_id_col(df=bp_readings_df, id_df=patient_id_df, col='sharepoint_id')
    bp_readings_df = add_id_col(df=bp_readings_df, id_df=device_id_df, col='patient_id')
    bg_readings_df = add_id_col(df=bg_readings_df, id_df=patient_id_df, col='sharepoint_id')
    bg_readings_df = add_id_col(df=bg_readings_df, id_df=device_id_df, col='patient_id')

    bp_readings_df.to_sql('blood_pressure_reading', conn, if_exists='append', index=False)
    bg_readings_df.to_sql('glucose_reading', conn, if_exists='append', index=False)