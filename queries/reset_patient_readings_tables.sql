DELETE FROM blood_pressure_reading;
DBCC CHECKIDENT ('blood_pressure_reading', RESEED, 0);

DELETE FROM glucose_reading;
DBCC CHECKIDENT ('glucose_reading', RESEED, 0);