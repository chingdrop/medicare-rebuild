PRINT('Patient Address');
DELETE FROM patient_address;
DBCC CHECKIDENT ('patient_address', RESEED, 0);

PRINT('Patient Insurance');
DELETE FROM patient_insurance;
DBCC CHECKIDENT ('patient_insurance', RESEED, 0);

PRINT('Medical Necessity');
DELETE FROM medical_necessity;
DBCC CHECKIDENT ('medical_necessity', RESEED, 0);

PRINT('Patient Status');
DELETE FROM patient_status;
DBCC CHECKIDENT ('patient_status', RESEED, 0);

-- Patient records are deleted last due to foreign key constraints.
PRINT('Patient');
DELETE FROM patient;
DBCC CHECKIDENT ('patient', RESEED, 0);