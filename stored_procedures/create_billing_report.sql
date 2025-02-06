CREATE PROCEDURE [dbo].[create_billing_report]
	@start_date date,
	@end_date date
AS
BEGIN

	SET NOCOUNT ON;

	SELECT MAX(mc.timestamp_applied) AS date_of_service,
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
		STRING_AGG(temp_dx_code, ',') AS dx_codes,
		COUNT(CASE WHEN mct.name = '99202' THEN 1 END) AS count_99202,
		COUNT(CASE WHEN mct.name = '99453' THEN 1 END) AS count_99453,
		COUNT(CASE WHEN mct.name = '99454' THEN 1 END) AS count_99454,
		COUNT(CASE WHEN mct.name = '99457' THEN 1 END) AS count_99457,
		COUNT(CASE WHEN mct.name = '99458' THEN 1 END) AS count_99458
	FROM patient p
	JOIN patient_address pa
	ON p.patient_id = pa.patient_id
	JOIN patient_insurance pin
	ON p.patient_id = pin.patient_id
	JOIN medical_necessity mn
	ON p.patient_id = mn.patient_id
	JOIN patient_status ps
	ON p.patient_id = ps.patient_id
	JOIN patient_status_type pst
	ON ps.patient_status_type_id = pst.patient_status_type_id
	AND pst.name IN ('New', 'Rx', 'Onboard', 'Active', 'Retention Pending')
	JOIN medical_code mc
	ON p.patient_id = mc.patient_id
	AND mc.timestamp_applied <= @end_date
	AND mc.timestamp_applied >= @start_date
	JOIN medical_code_type mct
	ON mc.med_code_type_id = mct.med_code_type_id
	GROUP BY mc.med_code_id, 
		p.patient_id, p.sharepoint_id, p.first_name, p.last_name, p.name_suffix, p.date_of_birth, p.sex, p.phone_number, 
		pa.street_address, pa.city, pa.temp_state, pa.zipcode,
		pin.medicare_beneficiary_id, pin.primary_payer_name, pin.primary_payer_id, pin.secondary_payer_name, pin.secondary_payer_id
	ORDER BY p.patient_id
	RETURN;
END