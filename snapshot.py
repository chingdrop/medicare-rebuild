import os
import logging
import warnings
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from api_utils import MSGraphApi, TenoviApi
from db_utils import DatabaseManager
from logger import setup_logger
from helpers import get_last_month_billing_cycle, \
    create_directory, delete_files_in_dir, get_files_in_dir
from dataframe_utils import standardize_patients, standardize_patient_notes, standardize_devices, \
    standardize_bg_readings, standardize_bp_readings, patient_check_db_constraints, \
    create_patient_df, create_patient_address_df, create_patient_insurance_df, \
    create_med_necessity_df, create_patient_status_df, create_emcontacts_df
from queries import get_notes_log_stmt, get_time_log_stmt, get_fulfillment_stmt, \
    get_bg_readings_stmt, get_bp_readings_stmt


def snap_user_data(logger=logging.getLogger()):
    msg = MSGraphApi(
        tenant_id=os.getenv('AZURE_TENANT_ID'),
        client_id=os.getenv('AZURE_CLIENT_ID'),
        client_secret=os.getenv('AZURE_CLIENT_SECRET'),
        logger=logger
    )
    msg.request_access_token()
    data = msg.get_group_members('4bbe3379-1250-4522-92e6-017f77517470')
    user_df = pd.DataFrame(data['value'])
    user_df = user_df[['givenName', 'surname', 'displayName', 'mail', 'id']]
    user_df = user_df.rename(columns={
        'givenName': 'first_name',
        'surname': 'last_name',
        'displayName': 'display_name',
        'mail': 'email',
        'id': 'ms_entra_id'
    })
    logger.debug(f'Writing User DataFrame (rows: {user_df.shape[0]}, cols: {user_df.shape[1]})...')
    user_df.to_excel(Path.cwd() / 'data' / 'snaps' / 'snap_user_df.xlsx', 
                     index=False, 
                     engine='openpyxl')


def snap_patient_data(filename, logger=logging.getLogger()) -> None:
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

    export_df = standardize_patients(export_df)
    export_df = patient_check_db_constraints(export_df)
    patient_df = create_patient_df(export_df)
    address_df = create_patient_address_df(export_df)
    insurance_df = create_patient_insurance_df(export_df)
    med_nec_df = create_med_necessity_df(export_df)
    patient_status_df = create_patient_status_df(export_df)
    emcontacts_df = create_emcontacts_df(export_df)

    snaps_dir = Path.cwd() / 'data' / 'snaps'
    logger.debug(f'Writing Patient DataFrame (rows: {patient_df.shape[0]}, cols: {patient_df.shape[1]})...')
    patient_df.to_excel(snaps_dir / 'snap_patient_df.xlsx', index=False, engine='openpyxl')
    logger.debug(f'Writing Patient Address DataFrame (rows: {address_df.shape[0]}, cols: {address_df.shape[1]})...')
    address_df.to_excel(snaps_dir / 'snap_patient_address_df.xlsx', index=False, engine='openpyxl')
    logger.debug(f'Writing Patient Insurance DataFrame (rows: {insurance_df.shape[0]}, cols: {insurance_df.shape[1]})...')
    insurance_df.to_excel(snaps_dir / 'snap_patient_insurance_df.xlsx', index=False, engine='openpyxl')
    logger.debug(f'Writing Medical Necessity DataFrame (rows: {med_nec_df.shape[0]}, cols: {med_nec_df.shape[1]})...')
    med_nec_df.to_excel(snaps_dir / 'snap_med_necessity_df.xlsx', index=False, engine='openpyxl')
    logger.debug(f'Writing Patient Status DataFrame (rows: {patient_status_df.shape[0]}, cols: {patient_status_df.shape[1]})...')
    patient_status_df.to_excel(snaps_dir / 'snap_patient_status_df.xlsx', index=False, engine='openpyxl')
    logger.debug(f'Writing Emergency Contacts DataFrame (rows: {emcontacts_df.shape[0]}, cols: {emcontacts_df.shape[1]})...')
    emcontacts_df.to_excel(snaps_dir / 'snap_emcontacts_df.xlsx', index=False, engine='openpyxl')


def snap_patient_note_data(logger=logging.getLogger()):
    notes_db = DatabaseManager(logger=logger)
    notes_db.create_engine(
        username=os.getenv('LCH_SQL_USERNAME'),
        password=os.getenv('LCH_SQL_PASSWORD'),
        host=os.getenv('LCH_SQL_HOST'),
        database=os.getenv('LCH_SQL_SP_NOTES')
    )
    time_db = DatabaseManager(logger=logger)
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

    logger.debug(f'Writing Patient Note DataFrame (rows: {patient_note_df.shape[0]}, cols: {patient_note_df.shape[1]})...')
    patient_note_df.to_excel(Path.cwd() / 'data' / 'snaps' / 'snap_patient_note_df.xlsx',
                             index=False, 
                             engine='openpyxl')


def snap_device_data(logger=logging.getLogger()):
    tenovi = TenoviApi(
        'livecare',
        api_key=os.getenv('TENOVI_API_KEY'),
        logger=logger
    )
    res_data = tenovi.get_devices()
    device_data = []
    for device in res_data:
        device_dict = {
            'hwi_id': device['id'],
            'name': device['device']['name'],
            'hardware_uuid': device['device']['hardware_uuid'],
            'status': device['status'],
            'connected_datetime': device['connected_on'],
            'unlinked_datetime': device['unlinked_on'],
            'last_measurement_datetime': device['last_measurement'],
            'created_datetime': device['device']['created'],
            'sharepoint_id': device['patient_id']
        }
        device_data.append(device_dict)
    device_df = pd.DataFrame(device_data)

    logger.debug(f'Writing Device DataFrame (rows: {device_df.shape[0]}, cols: {device_df.shape[1]})...')
    device_df.to_excel(Path.cwd() / 'data' / 'snaps' / 'snap_device_df.xlsx', 
                       index=False, 
                       engine='openpyxl')


def snap_reading_data(logger=logging.getLogger()):
    tenovi = TenoviApi(
        'livecare',
        api_key=os.getenv('TENOVI_API_KEY'),
        logger=logger
    )
    snaps_dir = Path.cwd() / 'data' / 'snaps'
    device_df = snaps_dir / 'snap_device_df.xlsx'
    if device_df.exists():
        device_df = pd.read_excel(device_df, engine='openpyxl')
        gluc_device_ids = device_df['hwi_id'].loc[device_df['name'].str.contains('Glucometer', na=False)].to_list()
        bp_device_ids = device_df['hwi_id'].loc[device_df['name'].str.contains('BPM', na=False)].to_list()
    else:
        res_data = tenovi.get_devices()
        gluc_device_ids = [device['id'] for device in res_data if 'Glucometer' in device['device']['name']]
        bp_device_ids = [device['id'] for device in res_data if 'BPM' in device['device']['name']]

    start_date, _ = get_last_month_billing_cycle()
    bg_total_readings = []
    for gluc_device_id in gluc_device_ids:
        bg_readings_data = tenovi.get_readings(
            gluc_device_id,
            metric='glucose',
            created_gte=start_date
        )
        if bg_readings_data:
            bg_readings_data = [
                {
                    'hwi_id': data['hwi_device_id'],
                    'glucose_reading': data['value_1'],
                    'recorded_datetime': data['timestamp'],
                    'received_datetime': data['created']
                } for data in bg_readings_data
            ]
            bg_total_readings.extend(bg_readings_data)

    bp_total_readings = []
    for bp_device_id in bp_device_ids:
        bp_readings_data = tenovi.get_readings(
            bp_device_id,
            metric='blood_pressure',
            created_gte=start_date
        )
        if bp_readings_data:
            bp_readings_data = [
                {
                    'hwi_id': data['hwi_device_id'],
                    'systolic_reading': data['value_1'],
                    'diastolic_reading': data['value_2'],
                    'recorded_datetime': data['timestamp'],
                    'received_datetime': data['created']
                } for data in bp_readings_data
            ]
            bp_total_readings.extend(bp_readings_data)

    bg_readings_df = pd.DataFrame(bg_total_readings)
    bp_readings_df = pd.DataFrame(bp_total_readings)
    logger.debug(f'Writing Glucose DataFrame (rows: {bg_readings_df.shape[0]}, cols: {bg_readings_df.shape[1]})...')
    bg_readings_df.to_excel(snaps_dir / 'snap_bg_readings_df.xlsx', index=False, engine='openpyxl')
    logger.debug(f'Writing Glucose DataFrame (rows: {bp_readings_df.shape[0]}, cols: {bp_readings_df.shape[1]})...')
    bp_readings_df.to_excel(snaps_dir / 'snap_bp_readings_df.xlsx', index=False, engine='openpyxl')


warnings.filterwarnings("ignore")
load_dotenv()
logger = setup_logger('snapshot', level='debug')

data_dir = Path.cwd() / 'data'
snaps_dir = data_dir / 'snaps'
if not snaps_dir.exists():
    create_directory(snaps_dir)
if get_files_in_dir(snaps_dir):
    delete_files_in_dir(snaps_dir)

snap_user_data(logger=logger)
snap_patient_data(data_dir / 'Patient_Export.csv', logger=logger)
snap_device_data(logger=logger)
snap_patient_note_data(logger=logger)
snap_reading_data(logger=logger)