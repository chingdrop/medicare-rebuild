﻿-- =============================================
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

	-- Create and insert the numbers 1, 2, 3.
	INSERT INTO @numbers (n)
	VALUES (1), (2), (3);

	-- Select patient_id and latest note from the medical code and medical code type tables.
	-- Left join on medical code and medical code types to include patients with 0 codes.
	-- Calculate the number of RPM 20 min blocks, round down and coalesce nulls with zero. 
		-- If a patient has more than 4 RPM 20 min blocks, then override value with 4.
	-- Calculate the number of 99458 codes already recorded for a patient.
	-- Where a patient_id exists in the medical code table with a 99457 code and within the last month.
	-- Group by patient_id
	SELECT pn.patient_id,
		CASE
			WHEN COALESCE(FLOOR(SUM(pn.call_time_seconds) /1200), 0) > 4 THEN 4
			ELSE COALESCE(FLOOR(SUM(pn.call_time_seconds) /1200), 0)
		END AS rpm_20_mins_blocks,
		COALESCE(SUM(CASE WHEN mct.name = '99458' THEN 1 ELSE 0 END), 0) AS code_count,
		MAX(pn.note_datetime) AS last_note
	INTO #99458
	FROM patient_note pn
	LEFT JOIN medical_code mc
		ON pn.patient_id = mc.patient_id
		AND mc.timestamp_applied >= DATEADD(MONTH, -1, GETDATE())
	LEFT JOIN medical_code_type mct
		ON mc.med_code_type_id = mct.med_code_type_id
	WHERE pn.note_datetime >= DATEADD(MONTH, -1, GETDATE())
	GROUP BY pn.patient_id;

	-- Using the #99458 temporary table.
	-- Insert patient_id, medical_code_type and latest note datetime into medical code table.
	-- Cross Join the temporary table #99458 with the numbers table.
	-- Where the number of RPM 20 min blocks minus 99458 code counts are larger than 1.
	-- And where the number of RPM 20 min blocks minus 99458 code counts minus 1 is 3 or less.
	-- The cross join and the last where statement should multiply the rows by the result. 
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
	AND n.n <= (t.rpm_20_mins_blocks - t.code_count - 1)

END