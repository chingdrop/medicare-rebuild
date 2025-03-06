
CREATE PROCEDURE [dbo].[search_patient_list] 
	@first_name varchar(50) = NULL,
	@last_name varchar(75) = NULL,
	@phone_number varchar(10) = NULL,
	@patient_id int = NULL
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @first_of_month DATE;
	SET @first_of_month = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0);

	WITH monthly_minutes AS (
		SELECT p.patient_id,
			COALESCE(SUM(pn.call_time_seconds) / 60, 0) AS mon_min,
			MAX(pn.note_datetime) AS last_note_date
		FROM patient p
		LEFT JOIN patient_note pn
		ON p.patient_id = pn.patient_id
		AND pn.note_datetime >= @first_of_month
		AND pn.note_datetime <= GETDATE()
		WHERE (@first_name IS NULL OR p.first_name = @first_name)
		AND (@last_name IS NULL OR p.last_name = @last_name)
		AND (@phone_number IS NULL OR p.phone_number = @phone_number)
		AND (@patient_id IS NULL OR p.patient_id = @patient_id)
		GROUP BY p.patient_id
	),
	monthly_count AS (
		SELECT p.patient_id,
			COUNT(DISTINCT CAST(gr.received_datetime AS DATE)) +
			COUNT(DISTINCT CAST(bpr.received_datetime AS DATE)) AS mon_count,
			(
				SELECT MAX(received_datetime)
				FROM (
					SELECT gr.received_datetime
					FROM glucose_reading gr
					WHERE gr.device_id = d.device_id
					UNION
					SELECT bpr.received_datetime
					FROM blood_pressure_reading bpr
					WHERE bpr.device_id = d.device_id
				) AS combined_dates
			) AS last_reading_date
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
		WHERE (@first_name IS NULL OR p.first_name = @first_name)
		AND (@last_name IS NULL OR p.last_name = @last_name)
		AND (@phone_number IS NULL OR p.phone_number = @phone_number)
		AND (@patient_id IS NULL OR p.patient_id = @patient_id)
		GROUP BY p.patient_id, d.device_id
	)

	SELECT p.patient_id,
		p.first_name,
		p.last_name,
		ps.temp_status_type,
		pa.temp_state,
		mm.mon_min,
		mm.last_note_date,
		mc.mon_count,
		mc.last_reading_date
	FROM patient p
	JOIN patient_status ps
	ON p.patient_id = ps.patient_id
	JOIN patient_address pa
	ON p.patient_id = pa.patient_id
	JOIN monthly_minutes mm
	ON p.patient_id = mm.patient_id
	JOIN monthly_count mc
	ON p.patient_id = mc.patient_id
	GROUP BY p.patient_id, p.first_name, p.last_name, ps.temp_status_type, pa.temp_state, mm.mon_min, mm.last_note_date, mc.mon_count, mc.last_reading_date
	RETURN;
END