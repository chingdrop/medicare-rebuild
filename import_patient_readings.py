import os
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
readings_engine = create_alchemy_engine(
    username=os.getenv('LCH_SQL_USERNAME'),
    password=os.getenv('LCH_SQL_PASSWORD'),
    host=os.getenv('LCH_SQL_HOST'),
    database=os.getenv('LCH_SQL_SP_READINGS')
)

bp_readings_stmt = '''
    SELECT SharePoint_ID, Device_Model, Time_Recorded, Time_Recieved, BP_Reading_Systolic, BP_Reading_Diastolic, Manual_Reading
    FROM Blood_Pressure_Readings
    WHERE Time_Recorded >= DATEADD(day, -60, GETDATE())
'''
bg_readings_stmt = '''
    SELECT SharePoint_ID, Device_Model, Time_Recorded, Time_Recieved, BG_Reading, Manual_Reading
    FROM Glucose_Readings
    WHERE Time_Recorded >= DATEADD(day, -60, GETDATE())
'''

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

bp_readings_df['SharePoint_ID'] = bp_readings_df['SharePoint_ID'].astype('Int64')
bp_readings_df['Manual_Reading'] = bp_readings_df['Manual_Reading'].replace({True: 1, False: 0})
bp_readings_df['Manual_Reading'] = bp_readings_df['Manual_Reading'].astype('Int64')
bp_readings_df['BP_Reading_Systolic'] = bp_readings_df['BP_Reading_Systolic'].astype('Int64')
bp_readings_df['BP_Reading_Diastolic'] = bp_readings_df['BP_Reading_Diastolic'].astype('Int64')
bp_readings_df = bp_readings_df.rename(
    columns={
        'SharePoint_ID': 'sharepoint_id',
        'Device_Model': 'temp_device',
        'Time_Recorded': 'recorded_datetime',
        'Time_Received': 'received_datetime',
        'BP_Reading_Systolic': 'systolic_reading',
        'BP_Reading_Diastolic': 'diastolic_reading',
        'Manual_Reading': 'is_manual'
    }
)

bg_readings_df['SharePoint_ID'] = bg_readings_df['SharePoint_ID'].astype('Int64')
bg_readings_df['Manual_Reading'] = bg_readings_df['Manual_Reading'].replace({True: 1, False: 0})
bg_readings_df['Manual_Reading'] = bg_readings_df['Manual_Reading'].astype('Int64')
bp_readings_df['BG_Reading'] = bp_readings_df['BG_Reading'].astype('Int64')
bg_readings_df = bg_readings_df.rename(
    columns={
        'SharePoint_ID': 'sharepoint_id',
        'Device_Model': 'temp_device',
        'Time_Recorded': 'recorded_datetime',
        'Time_Received': 'received_datetime',
        'BG_Reading': 'glucose_reading',
        'Manual_Reading': 'is_manual'
    }
)
