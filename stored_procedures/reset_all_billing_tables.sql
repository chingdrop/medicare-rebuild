CREATE PROCEDURE [dbo].[reset_all_billing_tables]
AS
BEGIN
	
	SET NOCOUNT ON;

	PRINT('Patient Address');
	DELETE FROM patient_address;
	DBCC CHECKIDENT ('patient_address', RESEED, 0);

	PRINT('Patient Insurance');
	DELETE FROM patient_insurance;
	DBCC CHECKIDENT ('patient_insurance', RESEED, 0);

	PRINT('Medical Necessity');
	DELETE FROM medical_necessity;
	DBCC CHECKIDENT ('medical_necessity', RESEED, 0);

	PRINT('Patient Note');
	DELETE FROM patient_note;
	DBCC CHECKIDENT ('patient_note', RESEED, 0);

	PRINT('Blood Pressure Reading');
	DELETE FROM blood_pressure_reading;
	DBCC CHECKIDENT ('blood_pressure_reading', RESEED, 0);

	PRINT('Glucose Reading');
	DELETE FROM glucose_reading;
	DBCC CHECKIDENT ('glucose_reading', RESEED, 0);

	-- Medical Code Device records are deleted sooner due to foreign key constraints.
	PRINT('Medical Code Device');
	DELETE FROM medical_code_device;
	DBCC CHECKIDENT ('medical_code_device', RESEED, 0);

	PRINT('Device');
	DELETE FROM device;
	DBCC CHECKIDENT ('device', RESEED, 0);

	PRINT('Medical Code');
	DELETE FROM medical_code;
	DBCC CHECKIDENT ('medical_code', RESEED, 0);

	PRINT('Patient Status');
	DELETE FROM patient_status;
	DBCC CHECKIDENT ('patient_status', RESEED, 0);

	PRINT('Emergency Contacts');
	DELETE FROM emergency_contact;
	DBCC CHECKIDENT ('emergency_contact', RESEED, 0);

	-- Patient records are deleted last due to foreign key constraints.
	PRINT('Patient');
	DELETE FROM patient;
	DBCC CHECKIDENT ('patient', RESEED, 0);

	PRINT('User');
	DELETE FROM [user];
	DBCC CHECKIDENT ('user', RESEED, 0);

END