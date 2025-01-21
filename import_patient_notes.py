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

with notes_engine.begin() as conn:
    notes_df = pd.read_sql('SELECT * FROM Medical_Notes WHERE TimeStamp >= DATEADD(day, -60, GETDATE())', conn)

with time_engine.begin() as conn:
    time_df = pd.read_sql('SELECT * FROM Time_Log WHERE Start_Time >= DATEADD(day, -60, GETDATE())')

