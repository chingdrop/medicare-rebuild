DELETE FROM patient_address;
DBCC CHECKIDENT ('patient_address', RESEED, 0);

DELETE FROM patient_insurance;
DBCC CHECKIDENT ('patient_insurance', RESEED, 0);

DELETE FROM medical_necessity;
DBCC CHECKIDENT ('medical_necessity', RESEED, 0);

DELETE FROM patient_note;
DBCC CHECKIDENT ('patient_note', RESEED, 0);

DELETE FROM blood_pressure_reading;
DBCC CHECKIDENT ('blood_pressure_reading', RESEED, 0);

DELETE FROM glucose_reading;
DBCC CHECKIDENT ('glucose_reading', RESEED, 0);

-- Patient records are deleted last due to foreign key constraints.
DELETE FROM patient;
DBCC CHECKIDENT ('patient', RESEED, 0);