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
	VALUES (1), (2), (3);

	SELECT pn.patient_id,
		CASE
			WHEN COALESCE(FLOOR(SUM(pn.call_time_seconds) /1200), 0) > 4 THEN 4
			ELSE COALESCE(FLOOR(SUM(pn.call_time_seconds) /1200), 0)
		END AS rpm_20_mins_blocks,
		COALESCE(COUNT(DISTINCT mc.med_code_id), 0) AS code_count,
		MAX(pn.note_datetime) AS last_note
	INTO #99458
	FROM patient_note pn
	JOIN medical_code mc
		ON pn.patient_id = mc.patient_id
		AND mc.timestamp_applied >= DATEADD(MONTH, -1, GETDATE())
	LEFT JOIN medical_code_type mct
		ON mc.med_code_type_id = mct.med_code_type_id
		AND mct.name = '99458'
	WHERE pn.note_datetime >= DATEADD(MONTH, -1, GETDATE())
	AND EXISTS (
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
		t.last_note
	FROM #99458 t
	CROSS JOIN @numbers as n
	WHERE (t.rpm_20_mins_blocks - t.code_count) > 1
	AND n.n >= (t.rpm_20_mins_blocks - t.code_count - 1)

END