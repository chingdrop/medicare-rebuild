SELECT pn.patient_id,
	MAX(pn.note_datetime) AS last_note
FROM patient_note pn
WHERE pn.note_datetime >= DATEADD(MONTH, -1, GETDATE())
AND NOT EXISTS (
	SELECT 1
	FROM medical_code mc
	JOIN medical_code_type mct 
		ON mc.med_code_type_id = mct.med_code_type_id
		AND mct.name = '99457'
	WHERE mc.patient_id = pn.patient_id
		AND mc.timestamp_applied >= DATEADD(MONTH, -1, GETDATE())
)
GROUP BY pn.patient_id
HAVING SUM(pn.call_time_seconds) / 60 >= 20;