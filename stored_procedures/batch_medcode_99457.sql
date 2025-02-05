-- =============================================
-- Author:		Craig Hurley
-- Create date: 12/23/24
-- Description:	Adds 99457 to medical code for patients with 20 mins of RPM.
-- =============================================
CREATE PROCEDURE batch_medcode_99457
	@today_date date
AS
BEGIN

	SET NOCOUNT ON;
	DROP TABLE IF EXISTS #99457;

	-- Create a temporary table #99457.
	-- Select patient_id and latest note datetime from the patient notes and note types tables.
	-- Where a patient_id doesn't exist in the medical code table with a 99454 code and within the last month.
    -- Group by patient_id, only include call time minutes 20 or above.
	SELECT pn.patient_id,
	MAX(pn.note_datetime) AS last_note
	INTO #99457
	FROM patient_note pn
	WHERE pn.note_datetime >= DATEADD(MONTH, -1, @today_date)
	AND NOT EXISTS (
		SELECT 1
		FROM medical_code mc
		JOIN medical_code_type mct 
		ON mc.med_code_type_id = mct.med_code_type_id
		AND mct.name = '99457'
		WHERE mc.patient_id = pn.patient_id
		AND mc.timestamp_applied >= DATEADD(MONTH, -1, @today_date)
	)
	GROUP BY pn.patient_id
	HAVING SUM(pn.call_time_seconds) / 60 >= 20;

	-- Using the #99457 temporary table.
	-- Insert patient_id, medical_code_type and latest note datetime into medical code table.
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