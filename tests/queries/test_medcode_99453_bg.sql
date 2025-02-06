SELECT d.patient_id,
	MAX(gr.received_datetime) AS latest_reading
FROM device d
JOIN glucose_reading gr
ON d.device_id = gr.device_id
GROUP BY d.patient_id
HAVING COUNT(DISTINCT CAST(gr.received_datetime AS DATE)) >= 16;