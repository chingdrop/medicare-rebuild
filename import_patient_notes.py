import os
import html
import pandas as pd
from dotenv import load_dotenv

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

patient_note_df['SharePoint_ID'] = pd.to_numeric(patient_note_df['SharePoint_ID'], errors='coerce', downcast='integer')
patient_note_df.dropna(subset=['SharePoint_ID'], inplace=True)
# Boolean column is flipped because it's stored differently in the database.
patient_note_df['Auto_Time'] = patient_note_df['Auto_Time'].replace({True: 0, False: 1})
patient_note_df['Auto_Time'] = patient_note_df['Auto_Time'].astype('Int64')
patient_note_df['Recording_Time'] = patient_note_df['Recording_Time'].astype(str)
patient_note_df['Recording_Time'] = pd.to_timedelta(patient_note_df['Recording_Time']).dt.total_seconds()
patient_note_df['Notes'] = patient_note_df['Notes'].apply(html.unescape)
patient_note_df['Notes'] = patient_note_df['Notes'].str.replace(r'<.*?>', '', regex=True)
nurse_list = ['Joycelynn Harris', 'Melanie Coffey', 'Krista Lewin']
patient_note_df.loc[patient_note_df['LCH_UPN'].isin(nurse_list), 'Recording_Time'] = 900
patient_note_df['Recording_Time'] = patient_note_df['Recording_Time'].fillna(0)
patient_note_df.loc[patient_note_df['LCH_UPN'].isin(nurse_list), 'Time_Note'] = 'Initial Evaluation'
patient_note_df['Time_Note'] = patient_note_df['Time_Note'].str.replace('Initial Evaluation with APRN', 'Initial Evaluation')
alert_team_list = ['Christylyn Diosma', 'Maria Albingco', 'Mary Cortes', 'Richel Rodriguez', 'Rigel Sagayno']
patient_note_df.loc[patient_note_df['LCH_UPN'].isin(alert_team_list), 'Time_Note'] = 'Alert'
patient_note_df['Time_Note'] = patient_note_df['Time_Note'].str.split(',').str[0]
patient_note_df['Time_Note'] = patient_note_df['Time_Note'].replace('', None)
patient_note_df = patient_note_df.rename(
    columns={
        'SharePoint_ID': 'sharepoint_id',
        'Notes': 'note_content',
        'TimeStamp': 'note_datetime',
        'LCH_UPN': 'temp_user',
        'Time_Note': 'temp_note_type',
        'Recording_Time': 'call_time_seconds',
        'Auto_Time': 'is_manual',
        'Start_Time': 'start_call_datetime',
        'End_Time': 'end_call_datetime'
    }
)

with gps_engine.begin() as conn:
    patient_id_df = pd.read_sql('SELECT patient_id, sharepoint_id FROM patient', conn)

    patient_note_df = pd.merge(patient_note_df, patient_id_df, on='sharepoint_id')
    patient_note_df.drop(columns=['sharepoint_id'], inplace=True)
    
    patient_note_df.to_sql('patient_note', conn, if_exists='append', index=False)