PRINT('Medical Code');
DELETE FROM medical_code;
DBCC CHECKIDENT ('medical_code', RESEED, 0);