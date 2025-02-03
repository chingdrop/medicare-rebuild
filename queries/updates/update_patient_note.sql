UPDATE patient_note
SET patient_note.note_type_id = (
	SELECT nt.note_type_id
	FROM note_type nt
	WHERE nt.name = patient_note.temp_note_type
)
WHERE patient_note.temp_note_type IS NOT NULL;