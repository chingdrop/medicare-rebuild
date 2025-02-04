-- =============================================
-- Author:		Craig Hurley
-- Create date: 12/20/24
-- Revision date: 01/27/25
-- Description:	Adds 99453 code to medical code for patients with 16 distinct days of testing.
-- =============================================
CREATE PROCEDURE [dbo].[batch_medcode_99453]

AS
BEGIN

	SET NOCOUNT ON;
	DROP TABLE IF EXISTS #99453;

	DECLARE @inserted_codes TABLE (
		med_code_id INT, patient_id INT
	);

	-- Create a temporary table #99453.
	-- Select patient_id and the latest reading blood pressure or glucose reading date.
	-- Selecting from the patients, devices. Left join on both readings tables, this ensures you get all patients.
	-- Where a patient_id and device_id in the medical code table with a 99453 code doesn't exist.
	-- Group by patient_id and count the distinct dates of eith blood pressure or glucose recevied readings.
	SELECT d.patient_id,
		CASE
			WHEN MAX(gr.received_datetime) > MAX(bpr.received_datetime)
			THEN MAX(gr.received_datetime)
			ELSE MAX(bpr.received_datetime)
		END AS last_reading
	INTO #99453
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
	GROUP BY d.patient_id
	HAVING COUNT(DISTINCT CAST(gr.received_datetime AS DATE)) >= 16
		OR COUNT(DISTINCT CAST(bpr.received_datetime AS DATE)) >= 16;

	-- Using the #99453 temporary table.
	-- Insert patient_id, medical_code_type and latest reading datetime into medical code table.
	-- Return the med_code_id and patient_id.
	INSERT INTO medical_code (patient_id, med_code_type_id, timestamp_applied)
	OUTPUT INSERTED.med_code_id, INSERTED.patient_id 
		INTO @inserted_codes (med_code_id, patient_id)
	SELECT t.patient_id,
		(
		SELECT mct.med_code_type_id
		FROM medical_code_type mct
		WHERE mct.name = '99453'
		),
		t.last_reading
	FROM #99453 t;
	
	-- Insert the variable @inserted_codes into medical code device table.
	INSERT INTO medical_code_device (med_code_id, device_id)
	SELECT ic.med_code_id, d.device_id
	FROM @inserted_codes ic
	JOIN device d
		ON ic.patient_id = d.patient_id;

END