DECLARE @today_date DATE = '2025-02-28';

SELECT d.patient_id,
	MAX(bpr.received_datetime) latest_reading
FROM device d
JOIN blood_pressure_reading bpr
ON d.device_id = bpr.device_id
AND bpr.received_datetime >= DATEADD(day, -30, @today_date)
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
HAVING COUNT(DISTINCT CAST(bpr.received_datetime AS DATE)) >= 16;