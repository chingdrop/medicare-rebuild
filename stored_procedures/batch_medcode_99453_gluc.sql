-- =============================================
-- Author:		Craig Hurley
-- Create date: 12/19/24
-- Description:	Adds 99453 code to medical code for patients with glucometers.
-- =============================================
CREATE PROCEDURE [dbo].[batch_medcode_99453_gluc]

AS
BEGIN

	SET NOCOUNT ON;
	DROP TABLE IF EXISTS #99453_gluc

	SELECT gr.patient_id,
		MAX(gr.received_datetime) AS latest_reading
	INTO #99453_gluc
	FROM glucose_reading gr
	WHERE gr.patient_id NOT IN (
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
	)
	GROUP BY gr.patient_id
		HAVING COUNT(DISTINCT CAST(gr.received_datetime AS DATE)) >= 16

	INSERT INTO medical_code (patient_id, med_code_type_id, timestamp_applied)
	SELECT t.patient_id,
		(
		SELECT mct.med_code_type_id
		FROM medical_code_type mct
		WHERE mct.name = '99453'
		),
		t.latest_reading
	FROM #99453_gluc t

END