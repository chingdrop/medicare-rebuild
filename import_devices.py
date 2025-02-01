import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

from helpers import read_sql_file
from dataframe_utils import standardize_devices, add_id_col
from db_utils import DatabaseManager


load_dotenv()
gps_db = DatabaseManager(
    username=os.getenv('LCH_SQL_GPS_USERNAME'),
    password=os.getenv('LCH_SQL_GPS_PASSWORD'),
    host=os.getenv('LCH_SQL_GPS_HOST'),
    database=os.getenv('LCH_SQL_GPS_DB')
)
gps_db.connect()
fulfillment_db = DatabaseManager(
    username=os.getenv('LCH_SQL_USERNAME'),
    password=os.getenv('LCH_SQL_PASSWORD'),
    host=os.getenv('LCH_SQL_HOST'),
    database=os.getenv('LCH_SQL_SP_FULFILLMENT')
)
fulfillment_db


def import_device_data() -> None:
    get_queries_dir = Path.cwd() / 'queries' / 'gets'
    device_stmt = read_sql_file(get_queries_dir / 'get_fulfillment.sql')
    patient_id_stmt = read_sql_file(get_queries_dir / 'get_patient_id.sql')
    vendor_id_stmt = read_sql_file(get_queries_dir / 'get_vendor_id.sql')

    device_df = fulfillment_db.read_sql(device_stmt)
    device_df = standardize_devices(device_df)
    patient_id_df = gps_db.read_sql(patient_id_stmt)
    vendor_id_df = gps_db.read_sql(vendor_id_stmt)

    device_df = add_id_col(df=device_df, id_df=patient_id_df, col='sharepoint_id')
    vendor_id_df = vendor_id_df.rename(columns={'name': 'Vendor'})
    device_df = add_id_col(df=device_df, id_df=vendor_id_df, col='Vendor')
    
    gps_db.to_sql(device_df, 'device', if_exists='append')


import_device_data()