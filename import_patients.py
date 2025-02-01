from pathlib import Path

from import_to_sql import DataImporter


di = DataImporter()
di.connect_gps_db()
di.import_patient_data(Path.cwd() / 'data' / 'Patient_Export.csv')