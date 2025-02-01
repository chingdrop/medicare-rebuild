import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

from helpers import read_sql_file
from dataframe_utils import standardize_patient_notes, add_id_col
from db_utils import DatabaseManager


load_dotenv()
gps_db = DatabaseManager(
    username=os.getenv('LCH_SQL_GPS_USERNAME'),
    password=os.getenv('LCH_SQL_GPS_PASSWORD'),
    host=os.getenv('LCH_SQL_GPS_HOST'),
    database=os.getenv('LCH_SQL_GPS_DB')
)
gps_db.connect()
notes_db = DatabaseManager(
    username=os.getenv('LCH_SQL_USERNAME'),
    password=os.getenv('LCH_SQL_PASSWORD'),
    host=os.getenv('LCH_SQL_HOST'),
    database=os.getenv('LCH_SQL_SP_NOTES')
)
notes_db.connect()
time_db = DatabaseManager(
    username=os.getenv('LCH_SQL_USERNAME'),
    password=os.getenv('LCH_SQL_PASSWORD'),
    host=os.getenv('LCH_SQL_HOST'),
    database=os.getenv('LCH_SQL_SP_TIME')
)
time_db.connect()


def import_patient_note_data() -> None:
    get_queries_dir = Path.cwd() / 'queries' / 'gets'
    notes_stmt = read_sql_file(get_queries_dir / 'get_notes_log.sql')
    time_stmt = read_sql_file(get_queries_dir / 'get_time_log.sql')
    patient_id_stmt = read_sql_file(get_queries_dir / 'get_patient_id.sql')

    notes_df = notes_db.read_sql(notes_stmt, parse_dates=['TimeStamp'])
    time_df = time_db.read_sql(time_stmt, parse_dates=['Start_Time', 'End_Time'])

    time_df = time_df.rename(columns={
        'SharPoint_ID': 'SharePoint_ID',
        'Notes': 'Note_Type'
    })
    # Left join is needed for patient notes without any call time associated.
    patient_note_df = pd.merge(notes_df, time_df, on=['Note_ID', 'SharePoint_ID', 'LCH_UPN'], how='left')
    patient_note_df['Time_Note'] = patient_note_df['Time_Note'].fillna(patient_note_df['Note_Type'])
    patient_note_df.drop(columns=['Note_ID', 'Note_Type'], inplace=True)

    patient_note_df = standardize_patient_notes(patient_note_df)
    patient_id_df = gps_db.read_sql(patient_id_stmt)
    patient_note_df = add_id_col(df=patient_note_df, id_df=patient_id_df, col='sharepoint_id')
    gps_db.to_sql(patient_note_df, 'patient_note', if_exists='append')


import_patient_note_data()