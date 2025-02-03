SELECT pn.patient_id,
	CASE
		WHEN COALESCE(FLOOR(SUM(pn.call_time_seconds) /1200), 0) > 4 THEN 4
		ELSE COALESCE(FLOOR(SUM(pn.call_time_seconds) /1200), 0)
	END AS rpm_20_mins_blocks,
	COALESCE(COUNT(DISTINCT mc.med_code_id), 0) AS code_count,
	MAX(pn.note_datetime) AS last_note
FROM patient_note pn
JOIN medical_code mc
	ON pn.patient_id = mc.patient_id
	AND mc.timestamp_applied >= DATEADD(MONTH, -1, GETDATE())
LEFT JOIN medical_code_type mct
	ON mc.med_code_type_id = mct.med_code_type_id
	AND mct.name = '99458'
WHERE pn.note_datetime >= DATEADD(MONTH, -1, GETDATE())
AND EXISTS (
	SELECT 1
	FROM medical_code mc
	JOIN medical_code_type mct 
		ON mc.med_code_type_id = mct.med_code_type_id
		AND mct.name = '99457'
	WHERE mc.patient_id = pn.patient_id
		AND mc.timestamp_applied >= DATEADD(MONTH, -1, GETDATE())
)
GROUP BY pn.patient_id;