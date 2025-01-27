-- =============================================
-- Author:		Craig Hurley
-- Create date: 12/23/24
-- Revision date: 01/27/25
-- Description:	Adds 99454 code to medical code for patients with 16 distinct days of testing.
-- =============================================
CREATE PROCEDURE batch_medcode_99454

AS
BEGIN

	SET NOCOUNT ON;
	DROP TABLE IF EXISTS #99454;

	SELECT d.patient_id
	INTO #99454
	FROM device d
	LEFT JOIN glucose_reading gr
		ON d.device_id = gr.device_id
		AND gr.received_datetime >= DATEADD(day, -30, GETDATE())
	LEFT JOIN blood_pressure_reading bpr
		ON d.device_id = bpr.device_id
		AND bpr.received_datetime >= DATEADD(day, -30, GETDATE())
	WHERE NOT EXISTS (
		SELECT 1
		FROM medical_code mc
		JOIN medical_code_type mct 
			ON mc.med_code_type_id = mct.med_code_type_id
			AND mct.name = '99454'
		WHERE mc.patient_id = d.patient_id
			AND mc.timestamp_applied >= DATEADD(day, -30, GETDATE())
	)
	GROUP BY d.patient_id
		HAVING COUNT(DISTINCT CAST(gr.received_datetime AS DATE)) >= 16
			OR COUNT(DISTINCT CAST(bpr.received_datetime AS DATE)) >= 16;

	INSERT INTO medical_code (patient_id, med_code_type_id, timestamp_applied)
	SELECT t.patient_id,
		(
		SELECT mct.med_code_type_id
		FROM medical_code_type mct
		WHERE mct.name = '99454'
		),
		GETDATE()
	FROM #99454 t

END