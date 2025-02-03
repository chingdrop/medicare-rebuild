SELECT Vendor, Device_ID, Device_Name, Patient_ID
FROM Fulfillment_All
WHERE Resupply = 0 AND Vendor IN ('Tenovi', 'Omron')