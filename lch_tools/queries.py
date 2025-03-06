# --- GET Queries --- #
get_bg_readings_stmt = """
SELECT SharePoint_ID, Device_Model, Time_Recorded, Time_Recieved, BG_Reading, Manual_Reading
FROM Glucose_Readings
WHERE Time_Recorded >= ? AND Time_Recorded <= ?
"""

get_bp_readings_stmt = """
SELECT SharePoint_ID, Device_Model, Time_Recorded, Time_Recieved, BP_Reading_Systolic, BP_Reading_Diastolic, Manual_Reading
FROM Blood_Pressure_Readings
WHERE Time_Recorded >= ? AND Time_Recorded <= ?
"""

get_device_id_stmt = """
SELECT device_id, patient_id
FROM device
"""

get_fulfillment_stmt = """
SELECT Vendor, Device_ID, Device_Name, Patient_ID
FROM Fulfillment_All
WHERE Resupply = 0 AND Vendor IN ('Tenovi', 'Omron')
"""

get_notes_log_stmt = """
SELECT SharePoint_ID, Notes, TimeStamp, LCH_UPN, Time_Note, Note_ID
FROM Medical_Notes
WHERE TimeStamp >= ? AND TimeStamp <= ?
"""

get_patient_id_stmt = """
SELECT patient_id, sharepoint_id
FROM patient
"""

get_time_log_stmt = """
SELECT SharPoint_ID, Recording_Time, LCH_UPN, Notes, Auto_Time, Start_Time, End_Time, Note_ID
FROM Time_Log
WHERE End_Time >= ? AND End_Time <= ?
"""

get_vendor_id_stmt = """
SELECT vendor_id, name
FROM vendor
"""

# --- UPDATE Queries --- #
update_patient_note_stmt = """
UPDATE patient_note
SET patient_note.note_type_id = (
	SELECT nt.note_type_id
	FROM note_type nt
	WHERE nt.name = patient_note.temp_note_type
)
WHERE patient_note.temp_note_type IS NOT NULL;
"""

update_patient_status_stmt = """
UPDATE patient_status
SET patient_status.patient_status_type_id = (
	SELECT pst.patient_status_type_id
	FROM patient_status_type pst
	WHERE pst.name = patient_status.temp_status_type
)
WHERE patient_status.temp_status_type IS NOT NULL;
"""

update_user_stmt = """
UPDATE patient
SET patient.user_id = (
	SELECT u.user_id
	FROM [user] u
	WHERE u.display_name = patient.temp_user
)
WHERE patient.temp_user IS NOT NULL;
"""

update_user_note_stmt = """
UPDATE patient_note
SET patient_note.user_id = (
	SELECT u.user_id
	FROM [user] u
	WHERE u.display_name = patient_note.temp_user
)
WHERE patient_note.temp_user IS NOT NULL;
"""