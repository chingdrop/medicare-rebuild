-- Medical Code Device records are deleted sooner due to foreign key constraints.
PRINT('Medical Code Device');
DELETE FROM medical_code_device;
DBCC CHECKIDENT ('medical_code_device', RESEED, 0);

PRINT('Medical Code');
DELETE FROM medical_code;
DBCC CHECKIDENT ('medical_code', RESEED, 0);