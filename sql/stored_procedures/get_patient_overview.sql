
CREATE PROCEDURE [dbo].[get_patient_overview] 
	@patient_id int
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @first_of_month DATE;
	SET @first_of_month = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0);

	WITH monthly_minutes AS (
		SELECT p.patient_id,
			COALESCE(SUM(pn.call_time_seconds) / 60, 0) AS mon_min
		FROM patient p
		LEFT JOIN patient_note pn
		ON p.patient_id = pn.patient_id
		AND pn.note_datetime >= @first_of_month
		AND pn.note_datetime <= GETDATE()
		WHERE p.patient_id = @patient_id
		GROUP BY p.patient_id
	),
	monthly_count AS (
		SELECT p.patient_id,
			COUNT(DISTINCT CAST(gr.received_datetime AS DATE)) +
			COUNT(DISTINCT CAST(bpr.received_datetime AS DATE)) AS mon_count
		FROM patient p
		LEFT JOIN device d
		ON p.patient_id = d.patient_id
		LEFT JOIN glucose_reading gr
		ON d.device_id = gr.device_id
		AND gr.received_datetime >= @first_of_month
		AND gr.received_datetime <= GETDATE()
		LEFT JOIN blood_pressure_reading bpr
		ON d.device_id = bpr.device_id
		AND bpr.received_datetime >= @first_of_month
		AND bpr.received_datetime <= GETDATE()
		WHERE p.patient_id = @patient_id
		GROUP BY p.patient_id, d.device_id
	),
	patient_devices AS (
		SELECT d.patient_id,
			COUNT(d.device_id) AS owned_devices
		FROM device d
		GROUP BY d.patient_id
	)

	SELECT p.patient_id,
		p.full_name,
		pa.temp_state,
		p.phone_number,
		p.date_of_birth,
		DATEDIFF(YEAR, p.date_of_birth, GETDATE()) -
			CASE
				WHEN MONTH(p.date_of_birth) > MONTH(GETDATE()) OR
				(MONTH(p.date_of_birth) = MONTH(GETDATE()) AND DAY(p.date_of_birth) > DAY(GETDATE()))
				THEN 1 ELSE 0
			END AS age,
		ps.temp_status_type,
		mm.mon_min,
		mc.mon_count,
		pd.owned_devices
	FROM patient p
	JOIN patient_status ps
	ON p.patient_id = ps.patient_id
	JOIN patient_address pa
	ON p.patient_id = pa.patient_id
	JOIN monthly_minutes mm
	ON p.patient_id = mm.patient_id
	JOIN monthly_count mc
	ON p.patient_id = mc.patient_id
	JOIN patient_devices pd
	ON p.patient_id = pd.patient_id
	GROUP BY p.patient_id, p.full_name, pa.temp_state, p.phone_number, p.date_of_birth, ps.temp_status_type, mm.mon_min, mc.mon_count, pd.owned_devices
	RETURN;
END