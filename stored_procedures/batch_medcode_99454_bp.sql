-- =============================================
-- Author:		Craig Hurley
-- Create date: 12/23/24
-- Revision date: 01/27/25
-- Description:	Adds 99454 to medical code for patients with 16 distinct days of blood pressure testing.
-- =============================================
CREATE PROCEDURE [dbo].[batch_medcode_99454_bp]
	@today_date date
AS
BEGIN

	SET NOCOUNT ON;
	DROP TABLE IF EXISTS #99454_bp;

	-- Create a temporary table #99454_bp.
	-- Select patient_id and the latest blood pressure reading date.
	-- Selecting from the patients, devices. Join on blood pressure readings tables within the last 30 days.
	-- Where a patient_id doesn't exist in the medical code table with a 99454_bp code and in the last 30 days.
	-- Group by patient_id and count the distinct dates of blood pressure recevied readings.
	SELECT d.patient_id,
		MAX(bpr.received_datetime) latest_reading
	INTO #99454_bp
	FROM device d
	JOIN blood_pressure_reading bpr
	ON d.device_id = bpr.device_id
	AND bpr.received_datetime >= DATEADD(day, -30, @today_date)
	WHERE NOT EXISTS (
		SELECT 1
		FROM medical_code mc
		JOIN medical_code_type mct 
		ON mc.med_code_type_id = mct.med_code_type_id
		AND mct.name = '99454'
		WHERE mc.patient_id = d.patient_id
		AND mc.timestamp_applied >= DATEADD(day, -30, @today_date)
	)
	GROUP BY d.patient_id
	HAVING COUNT(DISTINCT CAST(bpr.received_datetime AS DATE)) >= 16;

	-- Using the #99454_bp temporary table.
	-- Insert patient_id, medical_code_type and latest reading datetime into medical code table.
	INSERT INTO medical_code (patient_id, med_code_type_id, timestamp_applied)
	SELECT t.patient_id,
	(
		SELECT mct.med_code_type_id
		FROM medical_code_type mct
		WHERE mct.name = '99454'
	),
	t.latest_reading
	FROM #99454_bp t;

END