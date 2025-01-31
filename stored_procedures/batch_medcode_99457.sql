-- =============================================
-- Author:		Craig Hurley
-- Create date: 12/23/24
-- Description:	Adds 99457 to medical code for patients with 20 mins of RPM.
-- =============================================
CREATE PROCEDURE batch_medcode_99457 

AS
BEGIN

	SET NOCOUNT ON;
	DROP TABLE IF EXISTS #99457;

    SELECT pn.patient_id,
		MAX(pn.note_datetime) AS last_note
	INTO #99457
	FROM patient_note pn
	WHERE pn.note_datetime >= DATEADD(MONTH, -1, GETDATE())
	AND NOT EXISTS (
		SELECT 1
		FROM medical_code mc
		JOIN medical_code_type mct 
			ON mc.med_code_type_id = mct.med_code_type_id
			AND mct.name = '99457'
		WHERE mc.patient_id = pn.patient_id
			AND mc.timestamp_applied >= DATEADD(MONTH, -1, GETDATE())
	)
	GROUP BY pn.patient_id
	HAVING SUM(pn.call_time_seconds) / 60 >= 20;

	INSERT INTO medical_code (patient_id, med_code_type_id, timestamp_applied)
	SELECT t.patient_id,
		(
		SELECT mct.med_code_type_id
		FROM medical_code_type mct
		WHERE mct.name = '99457'
		),
		t.last_note
	FROM #99457 t;
END