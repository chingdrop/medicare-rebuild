CREATE PROCEDURE [dbo].[create_billing_report]
	@start_date date,
	@end_date date
AS
BEGIN

	SET NOCOUNT ON;

	WITH daily_patient_interactions AS (
		SELECT mc.patient_id,
			CAST(mc.timestamp_applied AS DATE) AS date_of_service,
			SUM(CASE WHEN mct.name = '99202' THEN 1 ELSE 0 END) AS count_99202,
			SUM(CASE WHEN mct.name = '99453' THEN 1 ELSE 0 END) AS count_99453,
			SUM(CASE WHEN mct.name = '99454' THEN 1 ELSE 0 END) AS count_99454,
			SUM(CASE WHEN mct.name = '99457' THEN 1 ELSE 0 END) AS count_99457,
			SUM(CASE WHEN mct.name = '99458' THEN 1 ELSE 0 END) AS count_99458
		FROM medical_code mc
		JOIN medical_code_type mct
		ON mc.med_code_type_id = mct.med_code_type_id
		WHERE mc.timestamp_applied >= @start_date
		AND mc.timestamp_applied <= @end_date
		GROUP BY CAST(mc.timestamp_applied AS DATE), mc.patient_id
	),
	patient_dx_codes AS (
		SELECT patient_id,
			STRING_AGG(temp_dx_code, ', ') AS dx_codes
		FROM medical_necessity
		GROUP BY patient_id
	)

	SELECT dpi.date_of_service AS DateOfService,
		p.sharepoint_id AS ID,
		p.first_name AS FirstName,
		p.middle_name AS MiddleName,
		p.last_name AS LastName,
		p.name_suffix AS Suffix,
		p.date_of_birth AS DOB,
		p.phone_number AS PhoneNumber,
		pa.street_address AS Address,
		pa.city AS City,
		pa.temp_state AS State,
		pa.zipcode AS Zipcode,
		p.sex AS Gender,
		pin.medicare_beneficiary_id AS MedicareNumber,
		pin.primary_payer_name AS PrimaryPayer,
		pin.primary_payer_id AS PrimaryPayerID,
		pin.secondary_payer_name AS SecondaryPayer,
		pin.secondary_payer_id AS SecondaryPayerID,
		pdc.dx_codes AS DXCodes,
		dpi.count_99202 AS '99202',
		dpi.count_99453 AS '99453',
		dpi.count_99454 AS '99454',
		dpi.count_99457 AS '99457',
		dpi.count_99458 AS '99458'
	FROM patient p
	JOIN patient_address pa
	ON p.patient_id = pa.patient_id
	JOIN patient_insurance pin
	ON p.patient_id = pin.patient_id
	JOIN patient_dx_codes pdc
	ON p.patient_id = pdc.patient_id
	JOIN daily_patient_interactions dpi
	ON p.patient_id = dpi.patient_id
	ORDER BY p.patient_id
	RETURN;
END