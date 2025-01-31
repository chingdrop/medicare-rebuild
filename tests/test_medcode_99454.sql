SELECT d.patient_id,
	CASE
		WHEN MAX(gr.received_datetime) > MAX(bpr.received_datetime)
		THEN MAX(gr.received_datetime)
		ELSE MAX(bpr.received_datetime)
	END AS last_reading
FROM device d
LEFT JOIN glucose_reading gr
	ON d.device_id = gr.device_id
	AND gr.received_datetime >= DATEADD(day, -30, GETDATE())
LEFT JOIN blood_pressure_reading bpr
	ON d.device_id = bpr.device_id
	AND bpr.received_datetime >= DATEADD(day, -30, GETDATE())
WHERE NOT EXISTS (
	SELECT 1
	FROM medical_code mc
	JOIN medical_code_type mct 
		ON mc.med_code_type_id = mct.med_code_type_id
		AND mct.name = '99454'
	WHERE mc.patient_id = d.patient_id
		AND mc.timestamp_applied >= DATEADD(day, -30, GETDATE())
)
GROUP BY d.patient_id
HAVING COUNT(DISTINCT CAST(gr.received_datetime AS DATE)) >= 16
	OR COUNT(DISTINCT CAST(bpr.received_datetime AS DATE)) >= 16;