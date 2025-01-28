-- =============================================
-- Author:		Craig Hurley
-- Create date: 12/26/24
-- Description:	Adds 99458 to medical code for patients with 20 mins of RPM.
-- =============================================
CREATE PROCEDURE [dbo].[batch_medcode_99458]	
AS
BEGIN

	SET NOCOUNT ON;

	DROP TABLE IF EXISTS #99458;

	DECLARE @numbers TABLE (n INT);

	INSERT INTO @numbers (n)
	VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10); 

	WITH patient_rpm AS (
		SELECT pn.patient_id,
			FLOOR(SUM(pn.call_time_seconds) /1200) AS rpm_20_mins_blocks
		FROM patient_note pn
		WHERE pn.note_datetime >= DATEADD(MONTH, -1, GETDATE())
		GROUP BY pn.patient_id
	),
	patient_code AS (
		SELECT mc.patient_id,
			COALESCE(COUNT(CASE WHEN mct.name = '99458' THEN mc.patient_id END), 0) AS code_count
		FROM medical_code mc
		LEFT JOIN medical_code_type mct
			ON mc.med_code_type_id = mct.med_code_type_id
		WHERE mc.timestamp_applied >= DATEADD(MONTH, -1, GETDATE())
		GROUP BY mc.patient_id
	)

	SELECT pn.patient_id
	INTO #99458
	FROM patient_note pn
	JOIN patient_rpm pr
		ON pn.patient_id = pr.patient_id
	JOIN patient_code pc
		ON pn.patient_id = pc.patient_id
		AND pr.rpm_20_mins_blocks - pc.code_count > 0
		AND pc.code_count < 3
	JOIN @numbers n
		ON n.n <= (pr.rpm_20_mins_blocks - pc.code_count - 1)
	WHERE EXISTS (
		SELECT 1
		FROM medical_code mc
		JOIN medical_code_type mct 
			ON mc.med_code_type_id = mct.med_code_type_id
			AND mct.name = '99457'
		WHERE mc.patient_id = pn.patient_id
			AND mc.timestamp_applied >= DATEADD(MONTH, -1, GETDATE())
	)
	GROUP BY pn.patient_id;

	INSERT INTO medical_code (patient_id, med_code_type_id, timestamp_applied)
	SELECT t.patient_id,
		(
		SELECT mct.med_code_type_id
		FROM medical_code_type mct
		WHERE mct.name = '99458'
		),
		GETDATE()
	FROM #99458 t;

END