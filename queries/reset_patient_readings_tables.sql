PRINT('Blood Pressure Reading');
DELETE FROM blood_pressure_reading;
DBCC CHECKIDENT ('blood_pressure_reading', RESEED, 0);

PRINT('Glucose Reading');
DELETE FROM glucose_reading;
DBCC CHECKIDENT ('glucose_reading', RESEED, 0);

PRINT('Device');
DELETE FROM device;
DBCC CHECKIDENT ('device', RESEED, 0);