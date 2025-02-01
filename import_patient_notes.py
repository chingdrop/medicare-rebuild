import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

from helpers import read_sql_file
from dataframe_utils import standardize_patient_notes, add_id_col
from import_to_sql import DataImporter


di = DataImporter()
di.connect_gps_db()
di.connect_notes_db()
di.connect_time_db()
di.import_patient_note_data()