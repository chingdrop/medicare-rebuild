DROP TABLE IF EXISTS #99458;

DECLARE @today_date DATE = '2025-02-28';
DECLARE @numbers TABLE (n INT);

-- Create and insert the numbers 1, 2, 3.
INSERT INTO @numbers (n)
VALUES (1), (2), (3);

WITH rpm_time_blocks AS (
	SELECT pn.patient_id,
		CASE
			WHEN COALESCE(FLOOR(SUM(pn.call_time_seconds) / 1200), 0) > 4 THEN 4
			ELSE COALESCE(FLOOR(SUM(pn.call_time_seconds) / 1200), 0)
		END AS rpm_20_mins_blocks
	FROM patient_note pn
	WHERE pn.note_datetime >= DATEADD(MONTH, -1, @today_date)
	GROUP BY pn.patient_id
),
med_code_count AS (
	SELECT mc.patient_id,
		COALESCE(SUM(CASE WHEN mct.name = '99458' THEN 1 ELSE 0 END), 0) AS code_count
	FROM medical_code mc
	JOIN medical_code_type mct
	ON mc.med_code_type_id = mct.med_code_type_id
	WHERE mc.timestamp_applied >= DATEADD(MONTH, -1, @today_date)
	GROUP BY mc.patient_id
)

SELECT pn.patient_id,
	rtb.rpm_20_mins_blocks,
	mcc.code_count,
	MAX(pn.note_datetime) AS latest_note
INTO #99458
FROM patient_note pn
JOIN rpm_time_blocks rtb
ON pn.patient_id = rtb.patient_id
JOIN med_code_count mcc
ON pn.patient_id = mcc.patient_id
GROUP BY pn.patient_id, rtb.rpm_20_mins_blocks, mcc.code_count

SELECT t.patient_id,
	t.rpm_20_mins_blocks,
	t.code_count
FROM #99458 t
CROSS JOIN @numbers as n
WHERE (t.rpm_20_mins_blocks - t.code_count) > 1
AND n.n <= (t.rpm_20_mins_blocks - t.code_count - 1)
ORDER BY t.patient_id;