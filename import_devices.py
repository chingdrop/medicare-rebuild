import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

from helpers import read_sql_file
from dataframe_utils import standardize_devices, add_id_col
from db_utils import create_alchemy_engine


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

get_queries_dir = Path.cwd() / 'queries' / 'gets'
device_stmt = read_sql_file(get_queries_dir / 'get_fulfillment.sql')
patient_id_stmt = read_sql_file(get_queries_dir / 'get_patient_id.sql')
vendor_id_stmt = read_sql_file(get_queries_dir / 'get_vendor_id.sql')

with fulfillment_engine.begin() as conn:
    device_df = pd.read_sql(device_stmt, conn)

device_df = standardize_devices(device_df)

with gps_engine.begin() as conn:
    patient_id_df = pd.read_sql(patient_id_stmt, conn)
    vendor_id_df = pd.read_sql(vendor_id_stmt, conn)

    device_df = add_id_col(df=device_df, id_df=patient_id_df, col='sharepoint_id')
    vendor_id_df = vendor_id_df.rename(columns={'name': 'Vendor'})
    device_df = add_id_col(df=device_df, id_df=vendor_id_df, col='Vendor')

    device_df.to_sql('device', conn, if_exists='append', index=False)
