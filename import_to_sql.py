import os
import pandas as pd
from datetime import datetime
from pathlib import Path

from api_utils import MSGraphApi, TenoviApi
from dataframe_utils import add_id_col, standardize_patients, standardize_patient_notes, \
    standardize_devices, standardize_bp_readings, standardize_bg_readings, \
    patient_check_db_constraints, \
    create_patient_df, create_patient_address_df, create_patient_insurance_df, \
    create_med_necessity_df, create_patient_status_df, create_emcontacts_df
from db_utils import DatabaseManager
from helpers import get_last_month_billing_cycle
from logger import setup_logger
from queries import get_patient_id_stmt, get_notes_log_stmt, get_time_log_stmt, \
    get_vendor_id_stmt, get_device_id_stmt, get_device_name_stmt


def import_user_data(logger=setup_logger('import_user_data')):
    msg = MSGraphApi(
        tenant_id=os.getenv('AZURE_TENANT_ID'),
        client_id=os.getenv('AZURE_CLIENT_ID'),
        client_secret=os.getenv('AZURE_CLIENT_SECRET'),
        logger=logger
    )
    msg.request_access_token()
    gps = DatabaseManager(logger=logger)
    gps.create_engine(
        username=os.getenv('LCH_SQL_GPS_USERNAME'),
        password=os.getenv('LCH_SQL_GPS_PASSWORD'),
        host=os.getenv('LCH_SQL_GPS_HOST'),
        database=os.getenv('LCH_SQL_GPS_DB')
    )

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
    gps.to_sql(user_df, 'user', if_exists='append')
    gps.close()


def import_patient_data(filename: Path, logger=setup_logger('import_patients')) -> None:
    gps = DatabaseManager(logger=logger)
    gps.create_engine(
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
    logger.debug(f"Reading patient export from SharePoint (rows: {export_df.shape[0]}, cols: {export_df.shape[1]})")
    
    export_df = standardize_patients(export_df)
    # failed_df = patient_check_failed_data(export_df)
    export_df = patient_check_db_constraints(export_df)
    patient_df = create_patient_df(export_df)
    address_df = create_patient_address_df(export_df)
    insurance_df = create_patient_insurance_df(export_df)
    med_nec_df = create_med_necessity_df(export_df)
    patient_status_df = create_patient_status_df(export_df)
    emcontacts_df = create_emcontacts_df(export_df)
    # failed_df.to_csv(data_dir / 'failed_patient_export.csv', index=False)
    
    # Patient data is imported first to get the patient_id.
    gps.to_sql(patient_df, 'patient', if_exists='append')
    patient_id_df = gps.read_sql(get_patient_id_stmt)
    
    address_df = add_id_col(df=address_df, id_df=patient_id_df, col='sharepoint_id')
    insurance_df = add_id_col(df=insurance_df, id_df=patient_id_df, col='sharepoint_id')
    med_nec_df = add_id_col(df=med_nec_df, id_df=patient_id_df, col='sharepoint_id')
    patient_status_df = add_id_col(df=patient_status_df, id_df=patient_id_df, col='sharepoint_id')
    emcontacts_df = add_id_col(df=emcontacts_df, id_df=patient_id_df, col='sharepoint_id')
    
    gps.to_sql(address_df, 'patient_address', if_exists='append')
    gps.to_sql(insurance_df, 'patient_insurance', if_exists='append')
    gps.to_sql(med_nec_df, 'medical_necessity', if_exists='append')
    gps.to_sql(patient_status_df, 'patient_status', if_exists='append')
    gps.to_sql(emcontacts_df, 'emergency_contact', if_exists='append')
    
    gps.close()


def import_patient_note_data(logger=setup_logger('import_patient_notes')):
    gps = DatabaseManager(logger=logger)
    gps.create_engine(
        username=os.getenv('LCH_SQL_GPS_USERNAME'),
        password=os.getenv('LCH_SQL_GPS_PASSWORD'),
        host=os.getenv('LCH_SQL_GPS_HOST'),
        database=os.getenv('LCH_SQL_GPS_DB')
    )
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

    # Left join is needed for patient notes without any call time associated.
    patient_note_df = pd.merge(notes_df, time_df, on=['SharePoint_ID', 'Note_ID', 'LCH_UPN'], how='left')
    patient_note_df['Time_Note'] = patient_note_df['Time_Note'].fillna(patient_note_df['Note_Type'])
    patient_note_df.drop(columns=['Note_ID', 'Note_Type'], inplace=True)
    patient_note_df = standardize_patient_notes(patient_note_df)
    
    patient_id_df = gps.read_sql(get_patient_id_stmt)
    patient_note_df = add_id_col(df=patient_note_df, id_df=patient_id_df, col='sharepoint_id')

    gps.to_sql(patient_note_df, 'patient_note', if_exists='append')
    gps.close()


def import_device_data(logger=setup_logger('import_devices')):
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
    device_df = standardize_devices(device_df)
    
    gps = DatabaseManager(logger=logger)
    gps.create_engine(
        username=os.getenv('LCH_SQL_GPS_USERNAME'),
        password=os.getenv('LCH_SQL_GPS_PASSWORD'),
        host=os.getenv('LCH_SQL_GPS_HOST'),
        database=os.getenv('LCH_SQL_GPS_DB')
    )
    patient_id_df = gps.read_sql(get_patient_id_stmt)
    vendor_id_df = gps.read_sql(get_vendor_id_stmt)

    device_df = add_id_col(df=device_df, id_df=patient_id_df, col='sharepoint_id')
    vendor_id_df = vendor_id_df.rename(columns={'name': 'vendor'})
    device_df = add_id_col(df=device_df, id_df=vendor_id_df, col='vendor')
    
    gps.to_sql(device_df, 'device', if_exists='append')
    gps.close()


def import_patient_reading_data(logger=setup_logger('import_patient_readings')):
    gps = DatabaseManager(logger=logger)
    gps.create_engine(
        username=os.getenv('LCH_SQL_GPS_USERNAME'),
        password=os.getenv('LCH_SQL_GPS_PASSWORD'),
        host=os.getenv('LCH_SQL_GPS_HOST'),
        database=os.getenv('LCH_SQL_GPS_DB')
    )
    device_name_df = gps.read_sql(get_device_name_stmt)
    gluc_device_ids = device_name_df['hwi_id'].loc[device_name_df['name'].str.contains('Glucometer', na=False)].to_list()
    bp_device_ids = device_name_df['hwi_id'].loc[device_name_df['name'].str.contains('BPM', na=False)].to_list()

    tenovi = TenoviApi(
        'livecare',
        api_key=os.getenv('TENOVI_API_KEY'),
        logger=logger
    )

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
                    'received_datetine': data['created']
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
                    'received_datetine': data['created']
                } for data in bp_readings_data
            ]
            bp_total_readings.extend(bp_readings_data)

    bg_readings_df = pd.DataFrame(bg_total_readings)
    bp_readings_df = pd.DataFrame(bp_total_readings)
    bp_readings_df = standardize_bp_readings(bp_readings_df)
    bg_readings_df = standardize_bg_readings(bg_readings_df)

    device_id_df = gps.read_sql(get_device_id_stmt)
    
    bp_readings_df = add_id_col(df=bp_readings_df, id_df=device_id_df, col='hwi_id')
    bg_readings_df = add_id_col(df=bg_readings_df, id_df=device_id_df, col='hwi_id')
    
    gps.to_sql(bp_readings_df, 'blood_pressure_reading', if_exists='append')
    gps.to_sql(bg_readings_df, 'glucose_reading', if_exists='append')
    gps.close()