SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[get_my_queue] 
	@user_email varchar(150)
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @first_of_month DATE;
	SET @first_of_month = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0);

	WITH monthly_minutes AS (
		SELECT pn.patient_id,
			SUM(pn.call_time_seconds) / 60 AS mon_min
		FROM patient_note pn
		WHERE pn.note_datetime <= @first_of_month
			AND pn.note_datetime >= DATEADD(MONTH, -1, GETDATE())
		GROUP BY pn.patient_id
	)

	SELECT p.first_name,
		p.last_name,
		ps.temp_status_type,
		pa.temp_state,
		mm.
	FROM patient p
	JOIN patient_status ps
	ON p.patient_id = ps.patient_id
	JOIN patient_address pa
	ON p.patient_id = pa.patient_id
	JOIN monthly_minutes mm
	ON p.patient_id = mm.patient_id
END
GO
