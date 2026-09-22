-- Generated from src/medicare_rebuild/models.py and legacy_models.py.
-- Do not hand-edit; run `make schema` to regenerate.
--
-- The GPS tables (below the first marker) are this repository's schema of record.
-- The legacy source tables (below the second marker) are reconstructed from the
-- columns queries.py reads; the real source system is not part of this repository,
-- and these table definitions are not authoritative for it. See
-- docs/decisions/0015-full-orm-schema-of-record.md.


-- GPS database (schema of record) --

CREATE TABLE medical_code_type (
	med_code_type_id INTEGER NOT NULL IDENTITY, 
	name VARCHAR(20) NULL, 
	PRIMARY KEY (med_code_type_id)
);

CREATE TABLE note_type (
	note_type_id INTEGER NOT NULL IDENTITY, 
	name VARCHAR(100) NULL, 
	PRIMARY KEY (note_type_id)
);

CREATE TABLE patient_status_type (
	patient_status_type_id INTEGER NOT NULL IDENTITY, 
	name VARCHAR(50) NULL, 
	PRIMARY KEY (patient_status_type_id)
);

CREATE TABLE [user] (
	user_id INTEGER NOT NULL IDENTITY, 
	first_name VARCHAR(100) NULL, 
	last_name VARCHAR(100) NULL, 
	display_name VARCHAR(200) NULL, 
	email VARCHAR(200) NULL, 
	ms_entra_id VARCHAR(100) NULL, 
	PRIMARY KEY (user_id)
);

CREATE TABLE vendor (
	vendor_id INTEGER NOT NULL IDENTITY, 
	name VARCHAR(50) NULL, 
	PRIMARY KEY (vendor_id)
);

CREATE TABLE patient (
	patient_id INTEGER NOT NULL IDENTITY, 
	first_name VARCHAR(100) NULL, 
	last_name VARCHAR(100) NULL, 
	middle_name VARCHAR(100) NULL, 
	name_suffix VARCHAR(20) NULL, 
	full_name VARCHAR(200) NULL, 
	nick_name VARCHAR(100) NULL, 
	date_of_birth DATETIME2 NULL, 
	sex VARCHAR(10) NULL, 
	email VARCHAR(200) NULL, 
	phone_number VARCHAR(20) NULL, 
	social_security VARCHAR(20) NULL, 
	temp_race VARCHAR(50) NULL, 
	temp_marital_status VARCHAR(50) NULL, 
	preferred_language VARCHAR(50) NULL, 
	weight_lbs INTEGER NULL, 
	height_in INTEGER NULL, 
	sharepoint_id INTEGER NULL, 
	temp_user VARCHAR(100) NULL, 
	user_id INTEGER NULL, 
	PRIMARY KEY (patient_id), 
	FOREIGN KEY(user_id) REFERENCES [user] (user_id)
);

CREATE TABLE device (
	device_id INTEGER NOT NULL IDENTITY, 
	hardware_uuid VARCHAR(100) NULL, 
	name VARCHAR(200) NULL, 
	patient_id INTEGER NULL, 
	vendor_id INTEGER NULL, 
	PRIMARY KEY (device_id), 
	FOREIGN KEY(patient_id) REFERENCES patient (patient_id), 
	FOREIGN KEY(vendor_id) REFERENCES vendor (vendor_id)
);

CREATE TABLE emergency_contact (
	emergency_contact_id INTEGER NOT NULL IDENTITY, 
	full_name VARCHAR(200) NULL, 
	phone_number VARCHAR(20) NULL, 
	relationship VARCHAR(50) NULL, 
	patient_id INTEGER NULL, 
	PRIMARY KEY (emergency_contact_id), 
	FOREIGN KEY(patient_id) REFERENCES patient (patient_id)
);

CREATE TABLE medical_code (
	med_code_id INTEGER NOT NULL IDENTITY, 
	patient_id INTEGER NULL, 
	med_code_type_id INTEGER NULL, 
	timestamp_applied DATETIME2 NULL, 
	PRIMARY KEY (med_code_id), 
	FOREIGN KEY(patient_id) REFERENCES patient (patient_id), 
	FOREIGN KEY(med_code_type_id) REFERENCES medical_code_type (med_code_type_id)
);

CREATE TABLE medical_necessity (
	medical_necessity_id INTEGER NOT NULL IDENTITY, 
	evaluation_datetime DATETIME2 NULL, 
	temp_dx_code VARCHAR(20) NULL, 
	patient_id INTEGER NULL, 
	PRIMARY KEY (medical_necessity_id), 
	FOREIGN KEY(patient_id) REFERENCES patient (patient_id)
);

CREATE TABLE patient_address (
	patient_address_id INTEGER NOT NULL IDENTITY, 
	street_address VARCHAR(200) NULL, 
	city VARCHAR(100) NULL, 
	temp_state VARCHAR(10) NULL, 
	zipcode VARCHAR(10) NULL, 
	patient_id INTEGER NULL, 
	PRIMARY KEY (patient_address_id), 
	FOREIGN KEY(patient_id) REFERENCES patient (patient_id)
);

CREATE TABLE patient_insurance (
	patient_insurance_id INTEGER NOT NULL IDENTITY, 
	medicare_beneficiary_id VARCHAR(20) NULL, 
	primary_payer_id VARCHAR(50) NULL, 
	primary_payer_name VARCHAR(100) NULL, 
	secondary_payer_id VARCHAR(50) NULL, 
	secondary_payer_name VARCHAR(100) NULL, 
	patient_id INTEGER NULL, 
	PRIMARY KEY (patient_insurance_id), 
	FOREIGN KEY(patient_id) REFERENCES patient (patient_id)
);

CREATE TABLE patient_note (
	patient_note_id INTEGER NOT NULL IDENTITY, 
	note_content NVARCHAR(max) NULL, 
	note_datetime DATETIME2 NULL, 
	temp_user VARCHAR(100) NULL, 
	temp_note_type VARCHAR(100) NULL, 
	call_time_seconds FLOAT NULL, 
	is_manual BIT NULL, 
	start_call_datetime DATETIME2 NULL, 
	end_call_datetime DATETIME2 NULL, 
	patient_id INTEGER NULL, 
	note_type_id INTEGER NULL, 
	user_id INTEGER NULL, 
	PRIMARY KEY (patient_note_id), 
	FOREIGN KEY(patient_id) REFERENCES patient (patient_id), 
	FOREIGN KEY(note_type_id) REFERENCES note_type (note_type_id), 
	FOREIGN KEY(user_id) REFERENCES [user] (user_id)
);

CREATE TABLE patient_status (
	patient_status_id INTEGER NOT NULL IDENTITY, 
	temp_status_type VARCHAR(50) NULL, 
	modified_date DATETIME2 NULL, 
	temp_user VARCHAR(100) NULL, 
	patient_id INTEGER NULL, 
	patient_status_type_id INTEGER NULL, 
	PRIMARY KEY (patient_status_id), 
	FOREIGN KEY(patient_id) REFERENCES patient (patient_id), 
	FOREIGN KEY(patient_status_type_id) REFERENCES patient_status_type (patient_status_type_id)
);

CREATE TABLE blood_pressure_reading (
	blood_pressure_reading_id INTEGER NOT NULL IDENTITY, 
	temp_device VARCHAR(100) NULL, 
	recorded_datetime DATETIME2 NULL, 
	received_datetime DATETIME2 NULL, 
	systolic_reading FLOAT NULL, 
	diastolic_reading FLOAT NULL, 
	is_manual BIT NULL, 
	device_id INTEGER NULL, 
	PRIMARY KEY (blood_pressure_reading_id), 
	FOREIGN KEY(device_id) REFERENCES device (device_id)
);

CREATE TABLE glucose_reading (
	glucose_reading_id INTEGER NOT NULL IDENTITY, 
	temp_device VARCHAR(100) NULL, 
	recorded_datetime DATETIME2 NULL, 
	received_datetime DATETIME2 NULL, 
	glucose_reading FLOAT NULL, 
	is_manual BIT NULL, 
	device_id INTEGER NULL, 
	PRIMARY KEY (glucose_reading_id), 
	FOREIGN KEY(device_id) REFERENCES device (device_id)
);

CREATE TABLE medical_code_device (
	medical_code_device_id INTEGER NOT NULL IDENTITY, 
	med_code_id INTEGER NULL, 
	device_id INTEGER NULL, 
	PRIMARY KEY (medical_code_device_id), 
	FOREIGN KEY(med_code_id) REFERENCES medical_code (med_code_id), 
	FOREIGN KEY(device_id) REFERENCES device (device_id)
);


-- Legacy source tables (reconstructed, not authoritative) --

CREATE TABLE [Blood_Pressure_Readings] (
	[SharePoint_ID] INTEGER NULL, 
	[Device_Model] VARCHAR(100) NULL, 
	[Time_Recorded] DATETIME2 NULL, 
	[Time_Recieved] DATETIME2 NULL, 
	[BP_Reading_Systolic] FLOAT NULL, 
	[BP_Reading_Diastolic] FLOAT NULL, 
	[Manual_Reading] BIT NULL
);

CREATE TABLE [Fulfillment_All] (
	[Vendor] VARCHAR(50) NULL, 
	[Device_ID] VARCHAR(100) NULL, 
	[Device_Name] VARCHAR(200) NULL, 
	[Patient_ID] INTEGER NULL, 
	[Resupply] BIT NULL
);

CREATE TABLE [Glucose_Readings] (
	[SharePoint_ID] INTEGER NULL, 
	[Device_Model] VARCHAR(100) NULL, 
	[Time_Recorded] DATETIME2 NULL, 
	[Time_Recieved] DATETIME2 NULL, 
	[BG_Reading] FLOAT NULL, 
	[Manual_Reading] BIT NULL
);

CREATE TABLE [Medical_Notes] (
	[SharePoint_ID] INTEGER NULL, 
	[Notes] NVARCHAR(max) NULL, 
	[TimeStamp] DATETIME2 NULL, 
	[AZURE_UPN] VARCHAR(100) NULL, 
	[Time_Note] VARCHAR(100) NULL, 
	[Note_ID] INTEGER NULL
);

CREATE TABLE [Time_Log] (
	[SharPoint_ID] INTEGER NULL, 
	[Recording_Time] VARCHAR(8) NULL, 
	[AZURE_UPN] VARCHAR(100) NULL, 
	[Notes] VARCHAR(100) NULL, 
	[Auto_Time] BIT NULL, 
	[Start_Time] DATETIME2 NULL, 
	[End_Time] DATETIME2 NULL, 
	[Note_ID] INTEGER NULL
);

