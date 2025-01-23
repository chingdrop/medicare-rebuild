-- =============================================
-- Author:		Craig Hurley
-- Create date: 12/20/24
-- Description:	Adds 99453 code to medical code for patients with blood pressure monitor.
-- =============================================
CREATE PROCEDURE [dbo].[batch_medcode_99453_bp]

AS
BEGIN

	SET NOCOUNT ON;
	DROP TABLE IF EXISTS #99453_bp

	SELECT bpr.patient_id,
		MAX(bpr.received_datetime) AS latest_reading
	INTO #99453_bp
	FROM blood_pressure_reading bpr
	WHERE bpr.patient_id NOT IN (
		SELECT mc.patient_id
		FROM medical_code mc
		JOIN medical_code_type mct 
			ON mc.med_code_type_id = mct.med_code_type_id
			AND mct.name = '99453'
		JOIN medical_code_device mcd
			ON mct.med_code_type_id = mcd.med_code_type_id
		JOIN device d
			ON mcd.device_id = d.device_id
			AND d.name LIKE '%blood pressure monitor%'
	)
	GROUP BY bpr.patient_id
		HAVING COUNT(DISTINCT CAST(bpr.received_datetime AS DATE)) >= 16

	INSERT INTO medical_code (patient_id, med_code_type_id, timestamp_applied)
	SELECT t.patient_id,
		(
		SELECT mct.med_code_type_id
		FROM medical_code_type mct
		WHERE mct.name = '99453'
		),
		t.latest_reading
	FROM #99453_bp t
END