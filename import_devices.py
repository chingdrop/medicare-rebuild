import os
import pandas as pd
from dotenv import load_dotenv

from dataframe_utils import standardize_devices, add_id_col
from sql_connect import create_alchemy_engine


load_dotenv()
gps_engine = create_alchemy_engine(
    username=os.getenv('LCH_SQL_GPS_USERNAME'),
    password=os.getenv('LCH_SQL_GPS_PASSWORD'),
    host=os.getenv('LCH_SQL_GPS_HOST'),
    database=os.getenv('LCH_SQL_GPS_DB')
)
fulfillment_engine = create_alchemy_engine(
    username=os.getenv('LCH_SQL_USERNAME'),
    password=os.getenv('LCH_SQL_PASSWORD'),
    host=os.getenv('LCH_SQL_HOST'),
    database=os.getenv('LCH_SQL_SP_FULFILLMENT')
)

device_stmt = '''
    SELECT Vendor, Device_ID, Device_Name, Patient_ID
    FROM Fulfillment_All
    WHERE Resupply = 0 AND Vendor IN ('Tenovi', 'Omron')
'''

with fulfillment_engine.begin() as conn:
    device_df = pd.read_sql(device_stmt, conn)

device_df = standardize_devices(device_df)

with gps_engine.begin() as conn:
    patient_id_df = pd.read_sql('SELECT patient_id, sharepoint_id FROM patient', conn)
    vendor_id_df = pd.read_sql('SELECT vendor_id, name FROM vendor', conn)

    device_df = add_id_col(df=device_df, id_df=patient_id_df, col='sharepoint_id')
    vendor_id_df = vendor_id_df.rename(columns={'name': 'Vendor'})
    device_df = add_id_col(df=device_df, id_df=vendor_id_df, col='Vendor')

    device_df.to_sql('device', conn, if_exists='append', index=False)
