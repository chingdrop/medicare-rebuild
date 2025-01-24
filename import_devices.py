import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

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

# Filtered devices for either Tenovi or Omron.
def standardize_vendor(row):
    if not row['Vendor'] in row['Device_Name']:
        if 'Tenovi' in row['Device_Name']:
            return 'Tenovi'
        else:
            return 'Omron'
    return row['Vendor']

device_df['Patient_ID'] = device_df['Patient_ID'].astype('Int64')
device_df['Device_ID'] = device_df['Device_ID'].str.replace('-', '')
device_df['Vendor'] = device_df.apply(standardize_vendor, axis=1)
device_df = device_df.rename(
    columns={
        'Device_ID': 'model_number', 
        'Device_Name': 'name', 
        'Patient_ID': 'sharepoint_id'
    }
)

with gps_engine.begin() as conn:
    patient_id_df = pd.read_sql('SELECT patient_id, sharepoint_id FROM patient', conn)
    vendor_id_df = pd.read_sql('SELECT vendor_id, name FROM vendor', conn)

    device_df = pd.merge(device_df, patient_id_df, on='sharepoint_id')
    device_df.drop(columns=['sharepoint_id'], inplace=True)
    vendor_id_df = vendor_id_df.rename(columns={'name': 'Vendor'})
    device_df = pd.merge(device_df, vendor_id_df, on='Vendor')
    device_df.drop(columns=['Vendor'], inplace=True)

    device_df.to_sql('device', conn, if_exists='append', index=False)
