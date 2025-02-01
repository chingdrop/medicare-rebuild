import os
import logging
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

from dataframe_utils import add_id_col, standardize_patients, standardize_patient_notes, \
    standardize_devices, standardize_bp_readings, standardize_bg_readings
from db_utils import DatabaseManager
from helpers import read_sql_file
from logger import setup_logger


load_dotenv()


def import_patient_data(filename: Path, logger: logging.Logger=setup_logger('import_patients')) -> None:
    gps_db = DatabaseManager(
        username=os.getenv('LCH_SQL_GPS_USERNAME'),
        password=os.getenv('LCH_SQL_GPS_PASSWORD'),
        host=os.getenv('LCH_SQL_GPS_HOST'),
        database=os.getenv('LCH_SQL_GPS_DB')
    )
    gps_db.create_engine()
    export_df = pd.read_csv(
        filename,
        dtype={
            'Phone Number': 'str',
            'Social Security': 'str',
            'Zip code': 'str'
        },
        parse_dates=['DOB', 'On-board Date']
    )
    logger.info(f"Sharepoint Online Patient Export (rows: {export_df.shape[0]}, cols: {export_df.shape[1]})")
    
    patient_id_stmt = read_sql_file(Path.cwd() / 'queries' / 'gets' / 'get_patient_id.sql')
    export_df = standardize_patients(export_df)
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
    
    # Patient data is imported first to get the patient_id.
    logger.info(f"Patient Dataframe (rows: {patient_df.shape[0]}, cols: {patient_df.shape[1]})")
    gps_db.to_sql(patient_df, 'patient', if_exists='append')
    patient_id_df = gps_db.read_sql(patient_id_stmt)
    
    address_df = add_id_col(df=address_df, id_df=patient_id_df, col='sharepoint_id')
    insurance_df = add_id_col(df=insurance_df, id_df=patient_id_df, col='sharepoint_id')
    med_nec_df = add_id_col(df=med_nec_df, id_df=patient_id_df, col='sharepoint_id')
    patient_status_df = add_id_col(df=patient_status_df, id_df=patient_id_df, col='sharepoint_id')
    
    logger.info(f"Patient Address Dataframe (rows: {address_df.shape[0]}, cols: {address_df.shape[1]})")
    logger.info(f"Patient Insurance Dataframe (rows: {insurance_df.shape[0]}, cols: {insurance_df.shape[1]})")
    logger.info(f"Medical Necessity Dataframe (rows: {insurance_df.shape[0]}, cols: {insurance_df.shape[1]})")
    logger.info(f"Patient Status Dataframe (rows: {patient_status_df.shape[0]}, cols: {patient_status_df.shape[1]})")
    gps_db.to_sql(address_df, 'patient_address', if_exists='append')
    gps_db.to_sql(insurance_df, 'patient_insurance', if_exists='append')
    gps_db.to_sql(med_nec_df, 'medical_necessity', if_exists='append')
    gps_db.to_sql(patient_status_df, 'patient_status', if_exists='append')
    
    gps_db.dispose()


def import_patient_note_data(logger: logging.Logger=setup_logger('import_patient_notes')):
    gps_db = DatabaseManager(
        username=os.getenv('LCH_SQL_GPS_USERNAME'),
        password=os.getenv('LCH_SQL_GPS_PASSWORD'),
        host=os.getenv('LCH_SQL_GPS_HOST'),
        database=os.getenv('LCH_SQL_GPS_DB')
    )
    gps_db.create_engine()
    notes_db = DatabaseManager(
        username=os.getenv('LCH_SQL_USERNAME'),
        password=os.getenv('LCH_SQL_PASSWORD'),
        host=os.getenv('LCH_SQL_HOST'),
        database=os.getenv('LCH_SQL_SP_NOTES')
    )
    notes_db.create_engine()
    time_db = DatabaseManager(
        username=os.getenv('LCH_SQL_USERNAME'),
        password=os.getenv('LCH_SQL_PASSWORD'),
        host=os.getenv('LCH_SQL_HOST'),
        database=os.getenv('LCH_SQL_SP_TIME')
    )
    time_db.create_engine()

    get_queries_dir = Path.cwd() / 'queries' / 'gets'
    notes_stmt = read_sql_file(get_queries_dir / 'get_notes_log.sql')
    time_stmt = read_sql_file(get_queries_dir / 'get_time_log.sql')
    patient_id_stmt = read_sql_file(get_queries_dir / 'get_patient_id.sql')
    
    notes_df = notes_db.read_sql(notes_stmt, parse_dates=['TimeStamp'])
    time_df = time_db.read_sql(time_stmt, parse_dates=['Start_Time', 'End_Time'])
    time_df = time_df.rename(columns={
        'SharPoint_ID': 'SharePoint_ID',
        'Notes': 'Note_Type'
    })
    logger.info(f"Notes Dataframe (rows: {notes_df.shape[0]}, cols: {notes_df.shape[1]})")
    logger.info(f"Time Dataframe (rows: {time_df.shape[0]}, cols: {time_df.shape[1]})")

    # Left join is needed for patient notes without any call time associated.
    patient_note_df = pd.merge(notes_df, time_df, on=['Note_ID', 'SharePoint_ID', 'LCH_UPN'], how='left')
    patient_note_df['Time_Note'] = patient_note_df['Time_Note'].fillna(patient_note_df['Note_Type'])
    patient_note_df.drop(columns=['Note_ID', 'Note_Type'], inplace=True)
    patient_note_df = standardize_patient_notes(patient_note_df)
    
    patient_id_df = gps_db.read_sql(patient_id_stmt)
    patient_note_df = add_id_col(df=patient_note_df, id_df=patient_id_df, col='sharepoint_id')

    logger.info(f"Patient Notes Dataframe (rows: {patient_note_df.shape[0]}, cols: {patient_note_df.shape[1]})")
    gps_db.to_sql(patient_note_df, 'patient_note', if_exists='append')
    
    gps_db.dispose()
    notes_db.dispose()
    time_db.dispose()


def import_device_data(logger: logging.Logger=setup_logger('import_devices')):
    gps_db = DatabaseManager(
        username=os.getenv('LCH_SQL_GPS_USERNAME'),
        password=os.getenv('LCH_SQL_GPS_PASSWORD'),
        host=os.getenv('LCH_SQL_GPS_HOST'),
        database=os.getenv('LCH_SQL_GPS_DB')
    )
    gps_db.create_engine()
    fulfillment_db = DatabaseManager(
        username=os.getenv('LCH_SQL_USERNAME'),
        password=os.getenv('LCH_SQL_PASSWORD'),
        host=os.getenv('LCH_SQL_HOST'),
        database=os.getenv('LCH_SQL_SP_FULFILLMENT')
    )
    fulfillment_db.create_engine()

    get_queries_dir = Path.cwd() / 'queries' / 'gets'
    device_stmt = read_sql_file(get_queries_dir / 'get_fulfillment.sql')
    patient_id_stmt = read_sql_file(get_queries_dir / 'get_patient_id.sql')
    vendor_id_stmt = read_sql_file(get_queries_dir / 'get_vendor_id.sql')
    
    device_df = fulfillment_db.read_sql(device_stmt)
    logger.info(f"Devices Dataframe (rows: {device_df.shape[0]}, cols: {device_df.shape[1]})")

    device_df = standardize_devices(device_df)
    patient_id_df = gps_db.read_sql(patient_id_stmt)
    vendor_id_df = gps_db.read_sql(vendor_id_stmt)

    device_df = add_id_col(df=device_df, id_df=patient_id_df, col='sharepoint_id')
    vendor_id_df = vendor_id_df.rename(columns={'name': 'Vendor'})
    device_df = add_id_col(df=device_df, id_df=vendor_id_df, col='Vendor')
    
    logger.info(f"Patient Devices Dataframe (rows: {device_df.shape[0]}, cols: {device_df.shape[1]})")
    gps_db.to_sql(device_df, 'device', if_exists='append')
    
    gps_db.dispose()
    fulfillment_db.dispose()


def import_patient_reading_data(logger: logging.Logger=setup_logger('import_patient_readings')):
    gps_db = DatabaseManager(
        username=os.getenv('LCH_SQL_GPS_USERNAME'),
        password=os.getenv('LCH_SQL_GPS_PASSWORD'),
        host=os.getenv('LCH_SQL_GPS_HOST'),
        database=os.getenv('LCH_SQL_GPS_DB')
    )
    gps_db.create_engine()
    readings_db = DatabaseManager(
        username=os.getenv('LCH_SQL_USERNAME'),
        password=os.getenv('LCH_SQL_PASSWORD'),
        host=os.getenv('LCH_SQL_HOST'),
        database=os.getenv('LCH_SQL_SP_READINGS')
    )
    readings_db.create_engine()

    get_queries_dir = Path.cwd() / 'queries' / 'gets'
    bp_readings_stmt = read_sql_file(get_queries_dir / 'get_bp_readings.sql')
    bg_readings_stmt = read_sql_file(get_queries_dir / 'get_bg_readings.sql')
    patient_id_stmt = read_sql_file(get_queries_dir / 'get_patient_id.sql')
    device_id_stmt = read_sql_file(get_queries_dir / 'get_device_id.sql')
    
    bp_readings_df = readings_db.read_sql(bp_readings_stmt, parse_dates=['Time_Recorded', 'Time_Recieved'])
    bg_readings_df = readings_db.read_sql(bg_readings_stmt, parse_dates=['Time_Recorded', 'Time_Recieved'])
    logger.info(f"Blood Pressure Readings Dataframe (rows: {bp_readings_df.shape[0]}, cols: {bp_readings_df.shape[1]})")
    logger.info(f"Glucose Readings Dataframe (rows: {bg_readings_df.shape[0]}, cols: {bg_readings_df.shape[1]})")

    bp_readings_df = standardize_bp_readings(bp_readings_df)
    bg_readings_df = standardize_bg_readings(bg_readings_df)
    patient_id_df = gps_db.read_sql(patient_id_stmt)
    device_id_df = gps_db.read_sql(device_id_stmt)
    
    bp_readings_df = add_id_col(df=bp_readings_df, id_df=patient_id_df, col='sharepoint_id')
    bp_readings_df = add_id_col(df=bp_readings_df, id_df=device_id_df, col='patient_id')
    bg_readings_df = add_id_col(df=bg_readings_df, id_df=patient_id_df, col='sharepoint_id')
    bg_readings_df = add_id_col(df=bg_readings_df, id_df=device_id_df, col='patient_id')
    
    logger.info(f"Patient Blood Pressure Readings Dataframe (rows: {bp_readings_df.shape[0]}, cols: {bp_readings_df.shape[1]})")
    logger.info(f"Patient Glucose Readings Dataframe (rows: {bg_readings_df.shape[0]}, cols: {bg_readings_df.shape[1]})")
    gps_db.to_sql(bp_readings_df, 'blood_pressure_reading', if_exists='append')
    gps_db.to_sql(bg_readings_df, 'glucose_reading', if_exists='append')
    
    gps_db.dispose()
    readings_db.dispose()