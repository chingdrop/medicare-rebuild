from import_to_sql import DataImporter


di = DataImporter()
di.connect_gps_db()
di.fulfillment_db()
di.import_device_data()