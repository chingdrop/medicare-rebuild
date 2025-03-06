SELECT pn.patient_id,
	MAX(pn.note_datetime) AS last_note
FROM patient_note pn
JOIN note_type nt
	ON pn.note_type_id = nt.note_type_id
	AND nt.name = 'Initial Evaluation'
WHERE NOT EXISTS (
	SELECT 1
	FROM medical_code mc
	JOIN medical_code_type mct 
		ON mc.med_code_type_id = mct.med_code_type_id
		AND mct.name IN ('99202', '99203', '99204', '99205')
	WHERE mc.patient_id = pn.patient_id
)
GROUP BY pn.patient_id
HAVING FLOOR(SUM(pn.call_time_seconds)) / 60 >= 15
	AND FLOOR(SUM(pn.call_time_seconds)) / 60 < 30;