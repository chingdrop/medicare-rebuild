import os
import pandas as pd
from dotenv import load_dotenv

from standardize_funcs import standardize_patient_notes
from sql_connect import create_alchemy_engine


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

notes_stmt = '''
    SELECT SharePoint_ID, Notes, TimeStamp, LCH_UPN, Time_Note, Note_ID
    FROM Medical_Notes
    WHERE TimeStamp >= DATEADD(day, -60, GETDATE())
'''
time_stmt = '''
    SELECT SharPoint_ID, Recording_Time, LCH_UPN, Notes, Auto_Time, Start_Time, End_Time, Note_ID
    FROM Time_Log
    WHERE Start_Time >= DATEADD(day, -60, GETDATE())
'''

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
    patient_id_df = pd.read_sql('SELECT patient_id, sharepoint_id FROM patient', conn)

    patient_note_df = pd.merge(patient_note_df, patient_id_df, on='sharepoint_id')
    patient_note_df.drop(columns=['sharepoint_id'], inplace=True)
    
    patient_note_df.to_sql('patient_note', conn, if_exists='append', index=False)