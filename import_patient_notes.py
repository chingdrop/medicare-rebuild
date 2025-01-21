import os
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

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
    SELECT Recording_Time, Auto_Time, SharPoint_ID, LCH_UPN, Note_ID
    FROM Time_Log
    WHERE Start_Time >= DATEADD(day, -60, GETDATE())
'''

with notes_engine.begin() as conn:
    notes_df = pd.read_sql(notes_stmt, conn)

with time_engine.begin() as conn:
    time_df = pd.read_sql(time_stmt, conn)

print('Notes DF - ' + str(notes_df.shape))
print('Time DF - ' + str(time_df.shape))
merge_df = pd.merge(notes_df, time_df, on='Note_ID')
print('Merged DF - ' + str(merge_df.shape))

merge_df.to_csv(Path.cwd() / 'data' / 'test_merge.csv', index=False)