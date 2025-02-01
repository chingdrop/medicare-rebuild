from import_to_sql import DataImporter


di = DataImporter()
di.connect_gps_db()
di.connect_readings_db()
di.import_patient_reading_data()