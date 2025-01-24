import os
import pandas as pd
from dotenv import load_dotenv

from sql_connect import create_alchemy_engine


load_dotenv()
gps_engine = create_alchemy_engine(
    username=os.getenv('GPS_SQL_USERNAME'),
    password=os.getenv('GPS_SQL_PASSWORD'),
    host=os.getenv('GPS_SQL_HOST'),
    database=os.getenv('GPS_SQL_DB')
)
fulfillment_engine = create_alchemy_engine(
    username=os.getenv('LEGACY_SQL_USERNAME'),
    password=os.getenv('LEGACY_SQL_PASSWORD'),
    host=os.getenv('LEGACY_SQL_HOST'),
    database=os.getenv('LEGACY_SQL_SP_FULFILLMENT')
)

device_stmt = '''
    SELECT Vendor, Device_ID, Device_Name, Patient_ID
    FROM Fulfillment_All
    WHERE Resupply = 0
'''

with fulfillment_engine.begin() as conn:
    device_df = pd.read_sql(device_stmt, conn)

