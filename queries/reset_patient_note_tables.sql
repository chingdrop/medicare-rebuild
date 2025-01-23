PRINT('Patient Note');
DELETE FROM patient_note;
DBCC CHECKIDENT ('patient_note', RESEED, 0);