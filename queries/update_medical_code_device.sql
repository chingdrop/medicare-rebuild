INSERT INTO medical_code_device(med_code_type_id, device_id)
SELECT mct.med_code_type_id, d.device_id
FROM medical_code_type mct
CROSS JOIN device d
WHERE mct.name in ('99453', '99454')