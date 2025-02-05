SELECT SharPoint_ID, Recording_Time, LCH_UPN, Notes, Auto_Time, Start_Time, End_Time, Note_ID
FROM Time_Log
WHERE End_Time >= DATEADD(day, -35, GETDATE())