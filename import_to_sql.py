import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

from dataframe_utils import add_id_col, standardize_patients, standardize_patient_notes
from db_utils import DatabaseManager
from helpers import read_sql_file


class DataImporter:
    def __init__(self):
        load_dotenv()
        self.gps_db = None
        self.notes_db = None
        self.time_db = None
        self.fulfillment_db = None
        self.readings_db = None
        self.get_queries_dir = Path.cwd() / 'queries' / 'gets'

    def connect_gps_db(self,):
        self.gps_db = DatabaseManager(
            username=os.getenv('LCH_SQL_GPS_USERNAME'),
            password=os.getenv('LCH_SQL_GPS_PASSWORD'),
            host=os.getenv('LCH_SQL_GPS_HOST'),
            database=os.getenv('LCH_SQL_GPS_DB')
        )
        self.gps_db.connect()

    def connect_notes_db(self,):
        self.notes_db = DatabaseManager(
            username=os.getenv('LCH_SQL_USERNAME'),
            password=os.getenv('LCH_SQL_PASSWORD'),
            host=os.getenv('LCH_SQL_HOST'),
            database=os.getenv('LCH_SQL_SP_NOTES')
        )
        self.notes_db.connect()

    def connect_time_db(self,):
        self.time_db = DatabaseManager(
            username=os.getenv('LCH_SQL_USERNAME'),
            password=os.getenv('LCH_SQL_PASSWORD'),
            host=os.getenv('LCH_SQL_HOST'),
            database=os.getenv('LCH_SQL_SP_TIME')
        )
        self.time_db.connect()
        
    # Patient data MUST be exported from SharePoint first.
    def import_patient_data(self, filename: Path) -> None:
        export_df = pd.read_csv(
            filename,
            dtype={
                'Phone Number': 'str',
                'Social Security': 'str',
                'Zip code': 'str'
            },
            parse_dates=['DOB', 'On-board Date']
        )
        patient_id_stmt = read_sql_file(self.get_queries_dir / 'get_patient_id.sql')

        export_df = standardize_patients(export_df)

        # export_df, failed_df = check_database_constraints(export_df)
        # failed_df.to_csv(data_dir / 'failed_patient_export.csv', index=False)

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
        self.gps_db.to_sql(patient_df, 'patient', if_exists='append')
        patient_id_df = self.gps_db.read_sql(patient_id_stmt)

        address_df = add_id_col(df=address_df, id_df=patient_id_df, col='sharepoint_id')
        insurance_df = add_id_col(df=insurance_df, id_df=patient_id_df, col='sharepoint_id')
        med_nec_df = add_id_col(df=med_nec_df, id_df=patient_id_df, col='sharepoint_id')
        patient_status_df = add_id_col(df=patient_status_df, id_df=patient_id_df, col='sharepoint_id')

        self.gps_db.to_sql(address_df, 'patient_address', if_exists='append')
        self.gps_db.to_sql(insurance_df, 'patient_insurance', if_exists='append')
        self.gps_db.to_sql(med_nec_df, 'medical_necessity', if_exists='append')
        self.gps_db.to_sql(patient_status_df, 'patient_status', if_exists='append')

    def import_patient_note_data(self,):
        notes_stmt = read_sql_file(self.get_queries_dir / 'get_notes_log.sql')
        time_stmt = read_sql_file(self.get_queries_dir / 'get_time_log.sql')
        patient_id_stmt = read_sql_file(self.get_queries_dir / 'get_patient_id.sql')

        notes_df = self.notes_db.read_sql(notes_stmt, parse_dates=['TimeStamp'])
        time_df = self.time_db.read_sql(time_stmt, parse_dates=['Start_Time', 'End_Time'])

        time_df = time_df.rename(columns={
            'SharPoint_ID': 'SharePoint_ID',
            'Notes': 'Note_Type'
        })
        # Left join is needed for patient notes without any call time associated.
        patient_note_df = pd.merge(notes_df, time_df, on=['Note_ID', 'SharePoint_ID', 'LCH_UPN'], how='left')
        patient_note_df['Time_Note'] = patient_note_df['Time_Note'].fillna(patient_note_df['Note_Type'])
        patient_note_df.drop(columns=['Note_ID', 'Note_Type'], inplace=True)

        patient_note_df = standardize_patient_notes(patient_note_df)
        patient_id_df = self.gps_db.read_sql(patient_id_stmt)
        patient_note_df = add_id_col(df=patient_note_df, id_df=patient_id_df, col='sharepoint_id')
        self.gps_db.to_sql(patient_note_df, 'patient_note', if_exists='append')

    