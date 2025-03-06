-- =============================================
-- Author:		Craig Hurley
-- Create date: 12/17/24
-- Description:	Adds 99202 code to medical code for patients.
-- =============================================
CREATE PROCEDURE [dbo].[batch_medcode_99202]
AS
BEGIN

	SET NOCOUNT ON;
	DROP TABLE IF EXISTS #99202;

	-- Create a temporary table #99202.
	-- Select patient_id and latest note datetime from the patient notes and note types tables.
	-- Only give me notes with the type of 'Initial Evaluation'.
	-- Where a patient_id in the medical code table with a 9920X code doesn't exist.
	-- Group by patient_id, only include call time minutes 15 or above and less than 30.
	SELECT pn.patient_id,
	MAX(pn.note_datetime) AS last_note
	INTO #99202
	FROM patient_note pn
	JOIN note_type nt
	ON pn.note_type_id = nt.note_type_id
	AND nt.name = 'Initial Evaluation'
	WHERE NOT EXISTS (
		SELECT 1
		FROM medical_code mc
		JOIN medical_code_type mct 
		ON mc.med_code_type_id = mct.med_code_type_id
		AND mct.name IN ('99202', '99203', '99204', '99205')
		WHERE mc.patient_id = pn.patient_id
	)
	GROUP BY pn.patient_id
	HAVING FLOOR(SUM(pn.call_time_seconds)) / 60 >= 15
	AND FLOOR(SUM(pn.call_time_seconds)) / 60 < 30;

	-- Using the #99202 temporary table.
	-- Insert patient_id, medical_code_type and latest note datetime into medical code table.
	INSERT INTO medical_code (patient_id, med_code_type_id, timestamp_applied)
	SELECT t.patient_id,
	(
		SELECT mct.med_code_type_id
		FROM medical_code_type mct
		WHERE mct.name = '99202'
	),
	t.last_note
	FROM #99202 t;

END