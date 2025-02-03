CREATE PROCEDURE create_billing_report
AS
BEGIN

	SET NOCOUNT ON;

	DECLARE @dx_codes_concat TABLE (
		patient_id INT,
		dx_codes varchar(MAX)
	);
	DECLARE @med_code_count TABLE(
		patient_id INT,
		count_99202 INT,
		count_99453 INT,
		count_99454 INT,
		count_99457 INT,
		count_99458 INT
	);

	INSERT INTO @dx_codes_concat(patient_id, dx_codes)
	SELECT patient_id,
		STRING_AGG(temp_dx_code, ',') AS dx_codes
	FROM medical_necessity
	GROUP BY patient_id;

	INSERT INTO @med_code_count(
		patient_id,
		count_99202,
		count_99453,
		count_99454,
		count_99457,
		count_99458
	)
	SELECT mc.patient_id,
		COUNT(CASE WHEN mct.name = '99202' THEN 1 END) AS count_99202,
		COUNT(CASE WHEN mct.name = '99453' THEN 1 END) AS count_99453,
		COUNT(CASE WHEN mct.name = '99454' THEN 1 END) AS count_99454,
		COUNT(CASE WHEN mct.name = '99457' THEN 1 END) AS count_99457,
		COUNT(CASE WHEN mct.name = '99458' THEN 1 END) AS count_99458
	FROM medical_code mc
	JOIN medical_code_type mct
		ON mc.med_code_type_id = mct.med_code_type_id
	GROUP BY mc.patient_id;

	SELECT GETDATE() AS now_now,
		p.patient_id,
		p.sharepoint_id,
		p.first_name,
		p.last_name,
		p.name_suffix,
		p.date_of_birth,
		p.sex,
		p.phone_number,
		pa.street_address,
		pa.city,
		pa.temp_state,
		pa.zipcode,
		pin.medicare_beneficiary_id,
		pin.primary_payer_name,
		pin.primary_payer_id,
		pin.secondary_payer_name,
		pin.secondary_payer_id,
		dcc.dx_codes,
		mcc.count_99202,
		mcc.count_99453,
		mcc.count_99454,
		mcc.count_99457,
		mcc.count_99458
	FROM patient p
	JOIN patient_address pa
		ON p.patient_id = pa.patient_id
	JOIN patient_insurance pin
		ON p.patient_id = pin.patient_id
	JOIN @dx_codes_concat dcc
		ON p.patient_id = dcc.patient_id
	JOIN @med_code_count mcc
		ON p.patient_id = mcc.patient_id
	RETURN;
END
