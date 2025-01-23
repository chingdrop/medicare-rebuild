-- =============================================
-- Author:		Craig Hurley
-- Create date: 12/23/24
-- Description:	Adds 99454 code to medical code for patients with glucometers.
-- =============================================
CREATE PROCEDURE [dbo].[batch_medcode_99454_gluc]

AS
BEGIN

	SET NOCOUNT ON;
	DROP TABLE IF EXISTS #99454_gluc

	SELECT gr.patient_id,
		MAX(gr.received_datetime) AS latest_reading
	INTO #99454_gluc
	FROM glucose_reading gr
	WHERE gr.received_datetime >= DATEADD(day, -30, GETDATE())
	AND gr.patient_id IN (
		SELECT mc.patient_id
		FROM medical_code mc
		JOIN medical_code_type mct 
			ON mc.med_code_type_id = mct.med_code_type_id
			AND mct.name = '99453'
		JOIN medical_code_device mcd
			ON mct.med_code_type_id = mcd.med_code_type_id
		JOIN device d
			ON mcd.device_id = d.device_id
			AND d.name LIKE '%glucometer%'
	) AND gr.patient_id NOT IN (
		SELECT mc.patient_id
		FROM medical_code mc
		JOIN medical_code_type mct 
			ON mc.med_code_type_id = mct.med_code_type_id
			AND mct.name = '99454'
			AND mc.timestamp_applied >= DATEADD(day, -30, GETDATE())
		JOIN medical_code_device mcd
			ON mct.med_code_type_id = mcd.med_code_type_id
		JOIN device d
			ON mcd.device_id = d.device_id
			AND d.name LIKE '%glucometer%'
	)
	GROUP BY gr.patient_id
		HAVING COUNT(DISTINCT CAST(gr.received_datetime AS DATE)) >= 16

	INSERT INTO medical_code (patient_id, med_code_type_id, timestamp_applied)
	SELECT t.patient_id,
		(
		SELECT mct.med_code_type_id
		FROM medical_code_type mct
		WHERE mct.name = '99454'
		),
		t.latest_reading
	FROM #99454_gluc t

END