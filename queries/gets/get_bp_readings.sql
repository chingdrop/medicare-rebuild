SELECT SharePoint_ID, Device_Model, Time_Recorded, Time_Recieved, BP_Reading_Systolic, BP_Reading_Diastolic, Manual_Reading
FROM Blood_Pressure_Readings
WHERE Time_Recorded >= DATEADD(day, -60, GETDATE())