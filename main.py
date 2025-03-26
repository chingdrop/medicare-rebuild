import os
import logging
import warnings
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from typing import Dict

from utils.api_utils import MSGraphApi
from utils.dataframe_utils import check_patient_db_constraints, add_id_col, \
    normalize_users, normalize_patients, normalize_patient_notes, normalize_devices, \
    normalize_bg_readings, normalize_bp_readings, \
    create_patient_df, create_patient_address_df, create_patient_insurance_df, create_med_necessity_df,\
    create_patient_status_df, create_emcontacts_df
from utils.db_utils import DatabaseManager
from medicare_rebuild.helpers import create_directory, get_files_in_dir, delete_files_in_dir
from medicare_rebuild.logger import setup_logger
from medicare_rebuild.queries import get_notes_log_stmt, get_time_log_stmt, get_fulfillment_stmt, get_patient_id_stmt, \
    get_device_id_stmt, get_vendor_id_stmt, get_bg_readings_stmt, get_bp_readings_stmt, \
    update_patient_note_stmt, update_patient_status_stmt, update_user_stmt, \
    update_user_note_stmt


class DataImporter:
    def __init__(self, start_date, end_date, logger=None):
        if isinstance(start_date, str):
            self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
        
        self.logger = logger or logging.getLogger(__name__)
        self.gps = DatabaseManager(logger=self.logger)
        self.gps.create_engine(username=os.getenv('LCH_SQL_GPS_USERNAME'),
                               password=os.getenv('LCH_SQL_GPS_PASSWORD'),
                               host=os.getenv('LCH_SQL_GPS_HOST'),
                               database=os.getenv('LCH_SQL_GPS_DB'))
        self.snaps_dir = Path.cwd() / 'data' / 'snaps'

    @staticmethod
    def snap_dataframe(df: pd.DataFrame, path: Path | str) -> None:
        df.to_excel(path, index=False, engine='openpyxl')

    def get_user_data(self, snap: bool=False) -> pd.DataFrame:
        msg = MSGraphApi(tenant_id=os.getenv('AZURE_TENANT_ID'),
                         client_id=os.getenv('AZURE_CLIENT_ID'),
                         client_secret=os.getenv('AZURE_CLIENT_SECRET'),
                         logger=self.logger)
        msg.request_access_token()
        data = msg.get_group_members('4bbe3379-1250-4522-92e6-017f77517470')
        df = pd.DataFrame(data['value'])
        df = normalize_users(df)
        if snap:
            self.snap_dataframe(df, self.snaps_dir / 'snap_user_df.xlsx')
        return df
    
    def get_patient_data(self, filename: Path | str, snap: bool=False) -> Dict[str, pd.DataFrame]:
        df = pd.read_csv(filename,
                         dtype={'Phone Number': 'str',
                                'Social Security': 'str',
                                'Zip code': 'str'},
                         parse_dates=['DOB', 'On-board Date'])
        self.logger.debug(f"Reading patient export from SharePoint (rows: {df.shape[0]}, cols: {df.shape[1]})")
        df = normalize_patients(df)
        # failed_df = patient_check_failed_data(export_df)
        df = check_patient_db_constraints(df)
        res = {'patient': create_patient_df(df),
               'address': create_patient_address_df(df),
               'insurance': create_patient_insurance_df(df),
               'med_nec': create_med_necessity_df(df),
               'status': create_patient_status_df(df),
               'emcontacts': create_emcontacts_df(df)}
        if snap:
            for name, df in res.items():
                self.snap_dataframe(df, self.snaps_dir / f'snap_{name}_df')
        return res
        

    def get_patient_note_data(self, snap: bool=False) -> pd.DataFrame:
        notes_db = DatabaseManager(logger=self.logger)
        notes_db.create_engine(
            username=os.getenv('LCH_SQL_USERNAME'),
            password=os.getenv('LCH_SQL_PASSWORD'),
            host=os.getenv('LCH_SQL_HOST'),
            database=os.getenv('LCH_SQL_SP_NOTES')
        )
        time_db = DatabaseManager(logger=self.logger)
        time_db.create_engine(
            username=os.getenv('LCH_SQL_USERNAME'),
            password=os.getenv('LCH_SQL_PASSWORD'),
            host=os.getenv('LCH_SQL_HOST'),
            database=os.getenv('LCH_SQL_SP_TIME')
        )
        notes_df = notes_db.read_sql(get_notes_log_stmt,
                                params=(self.start_date, self.end_date),
                                parse_dates=['TimeStamp'])
        time_df = time_db.read_sql(get_time_log_stmt,
                               params=(self.start_date, self.end_date),
                               parse_dates=['Start_Time', 'End_Time'])
        time_df = time_df.rename(
            columns={'SharPoint_ID': 
                     'SharePoint_ID',
                     'Notes': 
                     'Note_Type'})
        df = pd.merge(notes_df, time_df, on=['SharePoint_ID', 'Note_ID', 'LCH_UPN'], how='left')
        df['Time_Note'] = df['Time_Note'].fillna(df['Note_Type'])
        df.drop(columns=['Note_ID', 'Note_Type'], inplace=True)
        df = normalize_patient_notes(df)
        if snap:
            self.snap_dataframe(df, self.snaps_dir / 'snap_note_df.xlsx')
        time_db.close()
        notes_db.close()
        return df
    
    def get_device_data(self, snap: bool=False) -> pd.DataFrame:
        fulfillment_db = DatabaseManager(logger=logger)
        fulfillment_db.create_engine(
            username=os.getenv('LCH_SQL_USERNAME'),
            password=os.getenv('LCH_SQL_PASSWORD'),
            host=os.getenv('LCH_SQL_HOST'),
            database=os.getenv('LCH_SQL_SP_FULFILLMENT')
        )
        df = fulfillment_db.read_sql(get_fulfillment_stmt)
        df = normalize_devices(df)
        if snap:
            self.snap_dataframe(df, self.snaps_dir / 'snap_device_df.xlsx')
        return df
    
    def get_gluc_readings(self, snap: bool=False) -> pd.DataFrame:
        readings_db = DatabaseManager(logger=logger)
        readings_db.create_engine(
            username=os.getenv('LCH_SQL_USERNAME'),
            password=os.getenv('LCH_SQL_PASSWORD'),
            host=os.getenv('LCH_SQL_HOST'),
            database=os.getenv('LCH_SQL_SP_READINGS')
        )
        df = readings_db.read_sql(get_bg_readings_stmt,
                                  params=(self.start_date, self.end_date),
                                  parse_dates=['Time_Recorded', 'Time_Recieved'])
        if snap:
            self.snap_dataframe(df, self.snaps_dir / 'snap_glucose_df.xlsx')
        df = normalize_bg_readings(df)
        return df

    def get_bp_readings(self, snap: bool=False) -> pd.DataFrame:
        readings_db = DatabaseManager(logger=logger)
        readings_db.create_engine(
            username=os.getenv('LCH_SQL_USERNAME'),
            password=os.getenv('LCH_SQL_PASSWORD'),
            host=os.getenv('LCH_SQL_HOST'),
            database=os.getenv('LCH_SQL_SP_READINGS')
        )
        df = readings_db.read_sql(get_bp_readings_stmt,
                                  params=(self.start_date, self.end_date),
                                  parse_dates=['Time_Recorded', 'Time_Recieved'])
        if snap:
            self.snap_dataframe(df, self.snaps_dir / 'snap_blood_pressure_df.xlsx')
        df = normalize_bp_readings(df)
        return df

    def import_user_data(self, df: pd.DataFrame) -> None:
        self.gps.to_sql(df, 'user', if_exists='append')

    def import_patient_data(self, patient_data: Dict[str, pd.DataFrame]) -> None:
        self.gps.to_sql(patient_data['patient'], 'patient', if_exists='append')
        patient_id_df = self.gps.read_sql(get_patient_id_stmt)

        address_df = add_id_col(df=patient_data['address'], id_df=patient_id_df, col='sharepoint_id')
        insurance_df = add_id_col(df=patient_data['insurance'], id_df=patient_id_df, col='sharepoint_id')
        med_nec_df = add_id_col(df=patient_data['med_nec'], id_df=patient_id_df, col='sharepoint_id')
        patient_status_df = add_id_col(df=patient_data['status'], id_df=patient_id_df, col='sharepoint_id')
        emcontacts_df = add_id_col(df=patient_data['emcontacts'], id_df=patient_id_df, col='sharepoint_id')

        self.gps.to_sql(address_df, 'patient_address', if_exists='append')
        self.gps.to_sql(insurance_df, 'patient_insurance', if_exists='append')
        self.gps.to_sql(med_nec_df, 'medical_necessity', if_exists='append')
        self.gps.to_sql(patient_status_df, 'patient_status', if_exists='append')
        self.gps.to_sql(emcontacts_df, 'emergency_contact', if_exists='append')

    def import_patient_note_data(self, df: pd.DataFrame) -> None:
        patient_id_df = self.gps.read_sql(get_patient_id_stmt)
        df = add_id_col(df, id_df=patient_id_df, col='sharepoint_id')
        self.gps.to_sql(df, 'patient_note', if_exists='append')

    def import_device_data(self, df: pd.DataFrame) -> None:
        patient_id_df = self.gps.read_sql(get_patient_id_stmt)
        vendor_id_df = self.gps.read_sql(get_vendor_id_stmt)
        df = add_id_col(df=df, id_df=patient_id_df, col='sharepoint_id')
        vendor_id_df = vendor_id_df.rename(columns={'name': 'Vendor'})
        df = add_id_col(df=df, id_df=vendor_id_df, col='Vendor')
        self.gps.to_sql(df, 'device', if_exists='append')

    def import_gluc_readings_data(self, df: pd.DataFrame) -> None:
        patient_id_df = self.gps.read_sql(get_patient_id_stmt)
        device_id_df = self.gps.read_sql(get_device_id_stmt)
        df = add_id_col(df=df, id_df=patient_id_df, col='sharepoint_id')
        df = add_id_col(df=df, id_df=device_id_df, col='patient_id')
        self.gps.to_sql(df, 'glucose_reading', if_exists='append')

    def import_bp_readings_data(self, df: pd.DataFrame) -> None:
        patient_id_df = self.gps.read_sql(get_patient_id_stmt)
        device_id_df = self.gps.read_sql(get_device_id_stmt)
        df = add_id_col(df=df, id_df=patient_id_df, col='sharepoint_id')
        df = add_id_col(df=df, id_df=device_id_df, col='patient_id')
        self.gps.to_sql(df, 'blood_pressure_reading', if_exists='append')

    def close_db(self,) -> None:
        if self.gps:
            self.gps.close()


def import_all_data(start_date, end_date, snap=False, logger=logging.getLogger()):
    gps = DatabaseManager(logger=logger)
    gps.create_engine(username=os.getenv('LCH_SQL_GPS_USERNAME'),
                      password=os.getenv('LCH_SQL_GPS_PASSWORD'),
                      host=os.getenv('LCH_SQL_GPS_HOST'),
                      database=os.getenv('LCH_SQL_GPS_DB'))
    gps.execute_query("EXEC reset_all_billing_tables")

    data_dir = Path.cwd() / 'data'
    snaps_dir = data_dir / 'snaps'
    if not snaps_dir.exists():
        create_directory(snaps_dir)
    if get_files_in_dir(snaps_dir):
        delete_files_in_dir(snaps_dir)

    dim = DataImporter(start_date, end_date, logger=logger)
    user_df = dim.get_user_data(snap=snap)
    dim.import_user_data(user_df)
    patient_data = dim.get_patient_data(data_dir / 'Patient_Export.csv', snap=snap)
    dim.import_patient_data(patient_data)
    device_df = dim.get_device_data(snap=snap)
    dim.import_device_data(device_df)
    gluc_df = dim.get_gluc_readings(snap=snap)
    dim.import_gluc_readings_data(gluc_df)
    bp_df = dim.get_bp_readings(snap=snap)
    dim.import_bp_readings_data(bp_df)
    dim.close_db()

    gps.execute_query(update_patient_note_stmt)
    gps.execute_query(update_patient_status_stmt)
    gps.execute_query(update_user_stmt)
    gps.execute_query(update_user_note_stmt)
    gps.close()


def create_billing_report(start_date, end_date, logger=logging.getLogger()):
    if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
    gps = DatabaseManager(logger=logger)
    gps.create_engine(username=os.getenv('LCH_SQL_GPS_USERNAME'),
                      password=os.getenv('LCH_SQL_GPS_PASSWORD'),
                      host=os.getenv('LCH_SQL_GPS_HOST'),
                      database=os.getenv('LCH_SQL_GPS_DB'))
    medcode_params = {'today_date': end_date}

    gps.execute_query("EXEC reset_medical_code_tables")
    gps.execute_query("EXEC batch_medcode_99202")
    gps.execute_query("EXEC batch_medcode_99453_bg")
    gps.execute_query("EXEC batch_medcode_99453_bp")
    gps.execute_query("EXEC batch_medcode_99454_bg :today_date", medcode_params)
    gps.execute_query("EXEC batch_medcode_99454_bp :today_date", medcode_params)
    gps.execute_query("EXEC batch_medcode_99457 :today_date", medcode_params)
    gps.execute_query("EXEC batch_medcode_99458 :today_date", medcode_params)

    df = gps.read_sql("EXEC create_billing_report @start_date = ?, @end_date = ?", params=(start_date, end_date))
    df.to_excel(Path.cwd() / 'data' / 'LCH_Billing_Report.xlsx', index=False, engine='openpyxl')
    gps.close()


if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    load_dotenv()
    logger = setup_logger('main', level='debug')

    import_all_data("2025-01-01", "2025-02-28", logger=logger)
    create_billing_report("2025-02-01", "2025-02-28",logger=logger)