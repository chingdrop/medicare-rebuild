SELECT SharePoint_ID, Notes, TimeStamp, AZURE_UPN, Time_Note, Note_ID
FROM Medical_Notes
WHERE TimeStamp >= DATEADD(day, -35, GETDATE())