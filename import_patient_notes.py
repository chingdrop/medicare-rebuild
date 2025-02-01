from import_to_sql import DataImporter


di = DataImporter()
di.connect_gps_db()
di.connect_notes_db()
di.connect_time_db()
di.import_patient_note_data()