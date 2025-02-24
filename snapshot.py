import os
import pandas as pd
from datetime import datetime
from pathlib import Path

from db_utils import DatabaseManager
from helpers import get_last_month_billing_cycle
from dataframe_utils import standardize_patients, standardize_patient_notes, standardize_devices, \
    standardize_bg_readings, standardize_bp_readings, patient_check_db_constraints, \
    create_patient_df, create_patient_address_df, create_patient_insurance_df, \
    create_med_necessity_df, create_patient_status_df, create_emcontacts_df
from queries import get_notes_log_stmt, get_time_log_stmt, get_fulfillment_stmt, \
    get_bg_readings_stmt, get_bp_readings_stmt


def snap_patient_data(filename) -> None:
    export_df = pd.read_csv(
        filename,
        dtype={
            'Phone Number': 'str',
            'Social Security': 'str',
            'Zip code': 'str'
        },
        parse_dates=['DOB', 'On-board Date']
    )
    export_df = standardize_patients(export_df)
    export_df = patient_check_db_constraints(export_df)
    patient_df = create_patient_df(export_df)
    address_df = create_patient_address_df(export_df)
    insurance_df = create_patient_insurance_df(export_df)
    med_nec_df = create_med_necessity_df(export_df)
    patient_status_df = create_patient_status_df(export_df)
    emcontacts_df = create_emcontacts_df(export_df)

    data_dir = Path.cwd() / 'data'
    patient_df.to_excel(data_dir / 'snap_patient_df.xlsx', index=False, engine='openpyxl')
    address_df.to_excel(data_dir / 'snap_patient_address_df.xlsx', index=False, engine='openpyxl')
    insurance_df.to_excel(data_dir / 'snap_patient_insurance_df.xlsx', index=False, engine='openpyxl')
    med_nec_df.to_excel(data_dir / 'snap_med_necessity_df.xlsx', index=False, engine='openpyxl')
    patient_status_df.to_excel(data_dir / 'snap_patient_status_df.xlsx', index=False, engine='openpyxl')
    emcontacts_df.to_excel(data_dir / 'snap_emcontacts_df.xlsx', index=False, engine='openpyxl')


def snap_patient_note_data():
    notes_db = DatabaseManager()
    notes_db.create_engine(
        username=os.getenv('LCH_SQL_USERNAME'),
        password=os.getenv('LCH_SQL_PASSWORD'),
        host=os.getenv('LCH_SQL_HOST'),
        database=os.getenv('LCH_SQL_SP_NOTES')
    )
    time_db = DatabaseManager()
    time_db.create_engine(
        username=os.getenv('LCH_SQL_USERNAME'),
        password=os.getenv('LCH_SQL_PASSWORD'),
        host=os.getenv('LCH_SQL_HOST'),
        database=os.getenv('LCH_SQL_SP_TIME')
    )
    start_date, _ = get_last_month_billing_cycle()
    end_date = datetime.now()
    notes_df = notes_db.read_sql(get_notes_log_stmt,
                            params=(start_date, end_date),
                            parse_dates=['TimeStamp'])
    time_df = time_db.read_sql(get_time_log_stmt,
                           params=(start_date, end_date),
                           parse_dates=['Start_Time', 'End_Time'])
    time_df = time_df.rename(columns={
        'SharPoint_ID': 'SharePoint_ID',
        'Notes': 'Note_Type'
    })
    patient_note_df = pd.merge(notes_df, time_df, on=['SharePoint_ID', 'Note_ID', 'LCH_UPN'], how='left')
    patient_note_df['Time_Note'] = patient_note_df['Time_Note'].fillna(patient_note_df['Note_Type'])
    patient_note_df.drop(columns=['Note_ID', 'Note_Type'], inplace=True)
    patient_note_df = standardize_patient_notes(patient_note_df)
    patient_note_df.to_excel(Path.cwd() / 'data' / 'snap_patient_note_df.xlsx', index=False, engine='openpyxl')


def snap_device_data():
    fulfillment_db = DatabaseManager()
    fulfillment_db.create_engine(
        username=os.getenv('LCH_SQL_USERNAME'),
        password=os.getenv('LCH_SQL_PASSWORD'),
        host=os.getenv('LCH_SQL_HOST'),
        database=os.getenv('LCH_SQL_SP_FULFILLMENT')
    )
    device_df = fulfillment_db.read_sql(get_fulfillment_stmt)
    device_df = standardize_devices(device_df)
    device_df.to_excel(Path.cwd() / 'data' / 'snap_device_df.xlsx', index=False, engine='openpyxl')


def snap_reading_data():
    readings_db = DatabaseManager()
    readings_db.create_engine(
        username=os.getenv('LCH_SQL_USERNAME'),
        password=os.getenv('LCH_SQL_PASSWORD'),
        host=os.getenv('LCH_SQL_HOST'),
        database=os.getenv('LCH_SQL_SP_READINGS')
    )
    start_date, _ = get_last_month_billing_cycle()
    end_date = datetime.now()
    bp_readings_df = readings_db.read_sql(get_bp_readings_stmt,
                                  params=(start_date, end_date),
                                  parse_dates=['Time_Recorded', 'Time_Recieved'])
    bg_readings_df = readings_db.read_sql(get_bg_readings_stmt,
                                  params=(start_date, end_date),
                                  parse_dates=['Time_Recorded', 'Time_Recieved'])

    bp_readings_df = standardize_bp_readings(bp_readings_df)
    bg_readings_df = standardize_bg_readings(bg_readings_df)

    data_dir = Path.cwd() / 'data'
    bp_readings_df.to_excel(data_dir / 'snap_bp_reading_df.xlsx', index=False, engine='openpyxl')
    bg_readings_df.to_excel(data_dir / 'snap_bg_reading_df.xlsx', index=False, engine='openpyxl')


snap_patient_data(Path.cwd() / 'data' / 'Patient_Export.csv')
snap_patient_note_data()
snap_device_data()
snap_reading_data()