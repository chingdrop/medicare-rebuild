from pathlib import Path

from import_to_sql import import_patient_data, import_device_data, import_patient_note_data, \
    import_patient_reading_data
from logger import setup_logger


logger = setup_logger('main', level='info')

import_patient_data(Path.cwd() / 'data' / 'Patient_Export.csv', logger=logger)
import_device_data(logger=logger)
import_patient_note_data(logger=logger)
import_patient_reading_data(logger=logger)