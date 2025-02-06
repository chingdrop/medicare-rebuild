UPDATE patient_status
SET patient_status.patient_status_type_id = (
	SELECT pst.patient_status_type_id
	FROM patient_status_type pst
	WHERE pst.name = patient_status.temp_status_type
)
WHERE patient_status.temp_status_type IS NOT NULL;