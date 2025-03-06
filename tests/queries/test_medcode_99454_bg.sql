DECLARE @today_date DATE = '2025-02-28';

SELECT d.patient_id,
	MAX(gr.received_datetime) latest_reading
FROM device d
JOIN glucose_reading gr
ON d.device_id = gr.device_id
AND gr.received_datetime >= DATEADD(day, -30, @today_date)
WHERE NOT EXISTS (
	SELECT 1
	FROM medical_code mc
	JOIN medical_code_type mct 
	ON mc.med_code_type_id = mct.med_code_type_id
	AND mct.name = '99454'
	WHERE mc.patient_id = d.patient_id
	AND mc.timestamp_applied >= DATEADD(day, -30, @today_date)
)
GROUP BY d.patient_id
HAVING COUNT(DISTINCT CAST(gr.received_datetime AS DATE)) >= 16;