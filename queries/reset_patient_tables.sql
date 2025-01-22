DELETE FROM patient_address;
DBCC CHECKIDENT ('patient_address', RESEED, 0);

DELETE FROM patient_insurance;
DBCC CHECKIDENT ('patient_insurance', RESEED, 0);

DELETE FROM medical_necessity;
DBCC CHECKIDENT ('medical_necessity', RESEED, 0);

DELETE FROM patient;
DBCC CHECKIDENT ('patient', RESEED, 0);