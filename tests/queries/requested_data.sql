WITH dx_code_agg AS (
	SELECT mn.patient_id,
		STRING_AGG(mn.temp_dx_code, ',') AS dx_codes
	FROM medical_necessity mn
	WHERE mn.temp_dx_code = 'E119'
	GROUP BY mn.patient_id
),
test_16_list AS (
	SELECT p.patient_id,
		COUNT(DISTINCT CAST(gr.received_datetime AS DATE)) AS gr_count,
		COUNT(DISTINCT CAST(bpr.received_datetime AS DATE)) AS bpr_count
	FROM patient p
	JOIN device d
	ON p.patient_id = d.patient_id
	LEFT JOIN glucose_reading gr
	ON d.device_id = gr.device_id
	LEFT JOIN blood_pressure_reading bpr
	ON d.device_id = bpr.device_id
	WHERE gr.received_datetime >= DATEADD(day, -30, GETDATE())
	OR bpr.received_datetime >= DATEADD(day, -30, GETDATE())
	GROUP BY p.patient_id
	HAVING COUNT(DISTINCT CAST(gr.received_datetime AS DATE)) >= 16
	OR COUNT(DISTINCT CAST(bpr.received_datetime AS DATE)) >= 16
)

SELECT p.patient_id,
	p.first_name,
	p.last_name,
	p.date_of_birth,
	p.sex,
	p.phone_number,
	pa.temp_state,
	pin.medicare_beneficiary_id,
	dca.dx_codes,
	tsl.gr_count,
	tsl.bpr_count
FROM patient p
JOIN patient_address pa
ON p.patient_id = pa.patient_id
JOIN patient_insurance pin
ON p.patient_id = pin.patient_id
JOIN dx_code_agg dca
ON p.patient_id = dca.patient_id
JOIN test_16_list tsl
ON p.patient_id = tsl.patient_id
ORDER BY p.patient_id