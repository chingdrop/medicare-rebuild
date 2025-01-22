import os
from dotenv import load_dotenv

from sql_connect import create_alchemy_engine


load_dotenv()
gps_engine = create_alchemy_engine(
    username=os.getenv('GPS_SQL_USERNAME'),
    password=os.getenv('GPS_SQL_PASSWORD'),
    host=os.getenv('GPS_SQL_HOST'),
    database=os.getenv('GPS_SQL_DB')
)
readings_engine = create_alchemy_engine(
    username=os.getenv('LEGACY_SQL_USERNAME'),
    password=os.getenv('LEGACY_SQL_PASSWORD'),
    host=os.getenv('LEGACY_SQL_HOST'),
    database=os.getenv('LEGACY_SQL_SP_READINGS')
)