SELECT d.patient_id,
	CASE
		WHEN MAX(gr.received_datetime) > MAX(bpr.received_datetime)
		THEN MAX(gr.received_datetime)
		ELSE MAX(bpr.received_datetime)
	END AS last_reading
FROM device d
LEFT JOIN glucose_reading gr
	ON d.device_id = gr.device_id
LEFT JOIN blood_pressure_reading bpr
	ON d.device_id = bpr.device_id
WHERE NOT EXISTS (
	SELECT 1
	FROM medical_code mc
	JOIN medical_code_type mct 
		ON mc.med_code_type_id = mct.med_code_type_id
		AND mct.name = '99453'
	JOIN medical_code_device mcd
		ON mc.med_code_id = mcd.med_code_id
	WHERE mc.patient_id = d.patient_id
		AND mcd.device_id = d.device_id
)
GROUP BY d.patient_id, d.device_id
HAVING COUNT(DISTINCT CAST(gr.received_datetime AS DATE)) >= 16
	OR COUNT(DISTINCT CAST(bpr.received_datetime AS DATE)) >= 16;