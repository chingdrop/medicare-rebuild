from pathlib import Path

from import_to_sql import DataImporter


di = DataImporter()
di.connect_gps_db()
di.connect_notes_db()
di.connect_time_db()
di.connect_fulfillment_db()
di.connect_readings_db()

di.import_patient_data(Path.cwd() / 'data' / 'Patient_Export.csv')
di.import_device_data()
di.import_patient_note_data()
di.import_patient_reading_data()