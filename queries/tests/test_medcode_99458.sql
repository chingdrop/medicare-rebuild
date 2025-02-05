DROP TABLE IF EXISTS #99458;

DECLARE @numbers TABLE (n INT);

-- Create and insert the numbers 1, 2, 3.
INSERT INTO @numbers (n)
VALUES (1), (2), (3);

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

SELECT t.patient_id,
	(
	SELECT mct.med_code_type_id
	FROM medical_code_type mct
	WHERE mct.name = '99458'
	),
	t.rpm_20_mins_blocks,
	t.code_count
FROM #99458 t
CROSS JOIN @numbers as n
WHERE (t.rpm_20_mins_blocks - t.code_count) > 1
AND n.n <= (t.rpm_20_mins_blocks - t.code_count - 1)
ORDER BY t.patient_id