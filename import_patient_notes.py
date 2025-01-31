import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

from helpers import read_sql_file
from dataframe_utils import standardize_patient_notes, add_id_col
from db_utils import create_alchemy_engine


load_dotenv()
gps_engine = create_alchemy_engine(
    username=os.getenv('LCH_SQL_GPS_USERNAME'),
    password=os.getenv('LCH_SQL_GPS_PASSWORD'),
    host=os.getenv('LCH_SQL_GPS_HOST'),
    database=os.getenv('LCH_SQL_GPS_DB')
)
notes_engine = create_alchemy_engine(
    username=os.getenv('LCH_SQL_USERNAME'),
    password=os.getenv('LCH_SQL_PASSWORD'),
    host=os.getenv('LCH_SQL_HOST'),
    database=os.getenv('LCH_SQL_SP_NOTES')
)
time_engine = create_alchemy_engine(
    username=os.getenv('LCH_SQL_USERNAME'),
    password=os.getenv('LCH_SQL_PASSWORD'),
    host=os.getenv('LCH_SQL_HOST'),
    database=os.getenv('LCH_SQL_SP_TIME')
)

get_queries_dir = Path.cwd() / 'queries' / 'gets'
notes_stmt = read_sql_file(get_queries_dir / 'get_notes_log.sql')
time_stmt = read_sql_file(get_queries_dir / 'get_time_log.sql')
patient_id_stmt = read_sql_file(get_queries_dir / 'get_patient_id.sql')

with notes_engine.begin() as conn:
    notes_df = pd.read_sql(
        notes_stmt,
        conn,
        parse_dates=['TimeStamp']
    )

with time_engine.begin() as conn:
    time_df = pd.read_sql(
        time_stmt,
        conn,
        parse_dates=['Start_Time', 'End_Time']
    )

time_df = time_df.rename(
    columns={
        'SharPoint_ID': 'SharePoint_ID',
        'Notes': 'Note_Type'
    }
)
# Left join is needed for patient notes without any call time associated.
patient_note_df = pd.merge(notes_df, time_df, on=['Note_ID', 'SharePoint_ID', 'LCH_UPN'], how='left')
patient_note_df['Time_Note'] = patient_note_df['Time_Note'].fillna(patient_note_df['Note_Type'])
patient_note_df.drop(columns=['Note_ID', 'Note_Type'], inplace=True)

patient_note_df = standardize_patient_notes(patient_note_df)

with gps_engine.begin() as conn:
    patient_id_df = pd.read_sql(patient_id_stmt, conn)

    patient_note_df = add_id_col(df=patient_note_df, id_df=patient_id_df, col='sharepoint_id')
    
    patient_note_df.to_sql('patient_note', conn, if_exists='append', index=False)