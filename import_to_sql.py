import os
import logging
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

from dataframe_utils import add_id_col, standardize_patients, standardize_patient_notes, \
    standardize_devices, standardize_bp_readings, standardize_bg_readings, \
    patient_check_db_constraints, patient_check_failed_data
from db_utils import DatabaseManager
from helpers import read_file
from logger import setup_logger


load_dotenv()


def import_patient_data(
        filename: Path,
        snapshot: bool=False,
        logger: logging.Logger=setup_logger('import_patients')
) -> None:
    dbm = DatabaseManager(logger=logger)
    dbm.create_engine(
        'gps',
        username=os.getenv('LCH_SQL_GPS_USERNAME'),
        password=os.getenv('LCH_SQL_GPS_PASSWORD'),
        host=os.getenv('LCH_SQL_GPS_HOST'),
        database=os.getenv('LCH_SQL_GPS_DB')
    )
    export_df = pd.read_csv(
        filename,
        dtype={
            'Phone Number': 'str',
            'Social Security': 'str',
            'Zip code': 'str'
        },
        parse_dates=['DOB', 'On-board Date']
    )
    logger.debug(f"Sharepoint Online Patient Export (rows: {export_df.shape[0]}, cols: {export_df.shape[1]})")
    
    patient_id_stmt = read_file(Path.cwd() / 'queries' / 'gets' / 'get_patient_id.sql')
    export_df = standardize_patients(export_df)
    failed_df = patient_check_failed_data(export_df)
    export_df = patient_check_db_constraints(export_df)
    patient_df = export_df[[
        'first_name',
        'last_name',
        'middle_name',
        'name_suffix',
        'full_name',
        'nick_name',
        'date_of_birth',
        'sex',
        'email',
        'phone_number',
        'social_security',
        'sharepoint_id'
    ]]
    address_df = export_df[['street_address', 'city', 'temp_state', 'zipcode', 'sharepoint_id']]
    insurance_df = export_df[[
        'medicare_beneficiary_id',
        'primary_payer_id',
        'primary_payer_name',
        'secondary_payer_id',
        'secondary_payer_name',
        'sharepoint_id'
    ]]
    med_nec_df = export_df[['evaluation_datetime', 'temp_dx_code', 'sharepoint_id']]
    med_nec_df.loc[:, 'temp_dx_code'] = med_nec_df['temp_dx_code'].str.split(',')
    med_nec_df = med_nec_df.explode('temp_dx_code', ignore_index=True)
    patient_status_df = export_df[['temp_status_type', 'sharepoint_id']]
    patient_status_df['modified_date'] = pd.Timestamp.now()
    patient_status_df['temp_user'] = 'ITHelp'

    data_dir = Path.cwd() / 'data'
    failed_df.to_csv(data_dir / 'failed_patient_export.csv', index=False)
    if snapshot:
        snapshot_dir = data_dir / 'snapshots'
        patient_df.to_csv(snapshot_dir / 'patient_snap.csv', index=False)
        address_df.to_csv(snapshot_dir / 'patient_address_snap.csv', index=False)
        insurance_df.to_csv(snapshot_dir / 'patient_insurance_snap.csv', index=False)
        med_nec_df.to_csv(snapshot_dir / 'medical_necessity_snap.csv', index=False)
        patient_status_df.to_csv(snapshot_dir / 'patient_status_snap.csv', index=False)
    
    # Patient data is imported first to get the patient_id.
    dbm.to_sql(patient_df, 'patient', 'gps', if_exists='append')
    patient_id_df = dbm.read_sql(patient_id_stmt, 'gps')
    
    address_df = add_id_col(df=address_df, id_df=patient_id_df, col='sharepoint_id')
    insurance_df = add_id_col(df=insurance_df, id_df=patient_id_df, col='sharepoint_id')
    med_nec_df = add_id_col(df=med_nec_df, id_df=patient_id_df, col='sharepoint_id')
    patient_status_df = add_id_col(df=patient_status_df, id_df=patient_id_df, col='sharepoint_id')
    
    dbm.to_sql(address_df, 'patient_address', 'gps', if_exists='append')
    dbm.to_sql(insurance_df, 'patient_insurance', 'gps', if_exists='append')
    dbm.to_sql(med_nec_df, 'medical_necessity', 'gps', if_exists='append')
    dbm.to_sql(patient_status_df, 'patient_status', 'gps', if_exists='append')
    
    dbm.dispose()


def import_patient_note_data(
        snapshot: bool=False,
        logger: logging.Logger=setup_logger('import_patient_notes')
):
    dbm = DatabaseManager(logger=logger)
    dbm.create_engine(
        'gps',
        username=os.getenv('LCH_SQL_GPS_USERNAME'),
        password=os.getenv('LCH_SQL_GPS_PASSWORD'),
        host=os.getenv('LCH_SQL_GPS_HOST'),
        database=os.getenv('LCH_SQL_GPS_DB')
    )
    dbm.create_engine(
        'notes',
        username=os.getenv('LCH_SQL_USERNAME'),
        password=os.getenv('LCH_SQL_PASSWORD'),
        host=os.getenv('LCH_SQL_HOST'),
        database=os.getenv('LCH_SQL_SP_NOTES')
    )
    dbm.create_engine(
        'time',
        username=os.getenv('LCH_SQL_USERNAME'),
        password=os.getenv('LCH_SQL_PASSWORD'),
        host=os.getenv('LCH_SQL_HOST'),
        database=os.getenv('LCH_SQL_SP_TIME')
    )

    get_queries_dir = Path.cwd() / 'queries' / 'gets'
    notes_stmt = read_file(get_queries_dir / 'get_notes_log.sql')
    time_stmt = read_file(get_queries_dir / 'get_time_log.sql')
    patient_id_stmt = read_file(get_queries_dir / 'get_patient_id.sql')
    
    notes_df = dbm.read_sql(notes_stmt, 'notes', parse_dates=['TimeStamp'])
    time_df = dbm.read_sql(time_stmt, 'time', parse_dates=['Start_Time', 'End_Time'])
    time_df = time_df.rename(columns={
        'SharPoint_ID': 'SharePoint_ID',
        'Notes': 'Note_Type'
    })

    # Left join is needed for patient notes without any call time associated.
    patient_note_df = pd.merge(notes_df, time_df, on=['SharePoint_ID', 'Note_ID', 'LCH_UPN'], how='left')
    patient_note_df['Time_Note'] = patient_note_df['Time_Note'].fillna(patient_note_df['Note_Type'])
    patient_note_df.drop(columns=['Note_ID', 'Note_Type'], inplace=True)
    patient_note_df = standardize_patient_notes(patient_note_df)
    if snapshot:
        snapshot_dir = Path.cwd() / 'data' / 'snapshots'
        patient_note_df.to_csv(snapshot_dir / 'patient_note_snap.csv', index=False)
    
    patient_id_df = dbm.read_sql(patient_id_stmt, 'gps')
    patient_note_df = add_id_col(df=patient_note_df, id_df=patient_id_df, col='sharepoint_id')

    dbm.to_sql(patient_note_df, 'patient_note', 'gps', if_exists='append')
    
    dbm.dispose()


def import_device_data(
        snapshot: bool=False,
        logger: logging.Logger=setup_logger('import_devices')
):
    dbm = DatabaseManager(logger=logger)
    dbm.create_engine(
        'gps',
        username=os.getenv('LCH_SQL_GPS_USERNAME'),
        password=os.getenv('LCH_SQL_GPS_PASSWORD'),
        host=os.getenv('LCH_SQL_GPS_HOST'),
        database=os.getenv('LCH_SQL_GPS_DB')
    )
    dbm.create_engine(
        'fulfillment',
        username=os.getenv('LCH_SQL_USERNAME'),
        password=os.getenv('LCH_SQL_PASSWORD'),
        host=os.getenv('LCH_SQL_HOST'),
        database=os.getenv('LCH_SQL_SP_FULFILLMENT')
    )

    get_queries_dir = Path.cwd() / 'queries' / 'gets'
    device_stmt = read_file(get_queries_dir / 'get_fulfillment.sql')
    patient_id_stmt = read_file(get_queries_dir / 'get_patient_id.sql')
    vendor_id_stmt = read_file(get_queries_dir / 'get_vendor_id.sql')
    
    device_df = dbm.read_sql(device_stmt, 'fulfillment')

    device_df = standardize_devices(device_df)
    if snapshot:
        snapshot_dir = Path.cwd() / 'data' / 'snapshots'
        device_df.to_csv(snapshot_dir / 'device_snap.csv', index=False)

    patient_id_df = dbm.read_sql(patient_id_stmt, 'gps')
    vendor_id_df = dbm.read_sql(vendor_id_stmt, 'gps')

    device_df = add_id_col(df=device_df, id_df=patient_id_df, col='sharepoint_id')
    vendor_id_df = vendor_id_df.rename(columns={'name': 'Vendor'})
    device_df = add_id_col(df=device_df, id_df=vendor_id_df, col='Vendor')
    
    dbm.to_sql(device_df, 'device', 'gps', if_exists='append')
    
    dbm.dispose()


def import_patient_reading_data(
        snapshot: bool=False,
        logger: logging.Logger=setup_logger('import_patient_readings')
):
    dbm = DatabaseManager(logger=logger)
    dbm.create_engine(
        'gps',
        username=os.getenv('LCH_SQL_GPS_USERNAME'),
        password=os.getenv('LCH_SQL_GPS_PASSWORD'),
        host=os.getenv('LCH_SQL_GPS_HOST'),
        database=os.getenv('LCH_SQL_GPS_DB')
    )
    dbm.create_engine(
        'readings',
        username=os.getenv('LCH_SQL_USERNAME'),
        password=os.getenv('LCH_SQL_PASSWORD'),
        host=os.getenv('LCH_SQL_HOST'),
        database=os.getenv('LCH_SQL_SP_READINGS')
    )

    get_queries_dir = Path.cwd() / 'queries' / 'gets'
    bp_readings_stmt = read_file(get_queries_dir / 'get_bp_readings.sql')
    bg_readings_stmt = read_file(get_queries_dir / 'get_bg_readings.sql')
    patient_id_stmt = read_file(get_queries_dir / 'get_patient_id.sql')
    device_id_stmt = read_file(get_queries_dir / 'get_device_id.sql')
    
    bp_readings_df = dbm.read_sql(bp_readings_stmt, 'readings', parse_dates=['Time_Recorded', 'Time_Recieved'])
    bg_readings_df = dbm.read_sql(bg_readings_stmt, 'readings', parse_dates=['Time_Recorded', 'Time_Recieved'])

    bp_readings_df = standardize_bp_readings(bp_readings_df)
    bg_readings_df = standardize_bg_readings(bg_readings_df)
    if snapshot:
        snapshot_dir = Path.cwd() / 'data' / 'snapshots'
        bp_readings_df.to_csv(snapshot_dir / 'blood_pressure_readings_snap.csv', index=False)
        bg_readings_df.to_csv(snapshot_dir / 'glucose_readings_snap.csv', index=False)

    patient_id_df = dbm.read_sql(patient_id_stmt, 'gps')
    device_id_df = dbm.read_sql(device_id_stmt, 'gps')
    
    bp_readings_df = add_id_col(df=bp_readings_df, id_df=patient_id_df, col='sharepoint_id')
    bp_readings_df = add_id_col(df=bp_readings_df, id_df=device_id_df, col='patient_id')
    bg_readings_df = add_id_col(df=bg_readings_df, id_df=patient_id_df, col='sharepoint_id')
    bg_readings_df = add_id_col(df=bg_readings_df, id_df=device_id_df, col='patient_id')
    
    dbm.to_sql(bp_readings_df, 'blood_pressure_reading', 'gps', if_exists='append')
    dbm.to_sql(bg_readings_df, 'glucose_reading', 'gps', if_exists='append')
    
    dbm.dispose()