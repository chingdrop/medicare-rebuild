SELECT SharePoint_ID, Device_Model, Time_Recorded, Time_Recieved, BG_Reading, Manual_Reading
FROM Glucose_Readings
WHERE Time_Recorded >= DATEADD(day, -60, GETDATE())