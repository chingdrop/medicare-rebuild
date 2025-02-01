from pathlib import Path

from import_to_sql import import_patient_data, import_device_data, import_patient_note_data, \
    import_patient_reading_data


import_patient_data(Path.cwd() / 'data' / 'Patient_Export.csv')
import_device_data()
import_patient_note_data()
import_patient_reading_data()