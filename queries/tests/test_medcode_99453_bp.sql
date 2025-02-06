SELECT d.patient_id,
	MAX(bpr.received_datetime) AS latest_reading
FROM device d
JOIN blood_pressure_reading bpr
ON d.device_id = bpr.device_id
GROUP BY d.patient_id
HAVING COUNT(DISTINCT CAST(bpr.received_datetime AS DATE)) >= 16;