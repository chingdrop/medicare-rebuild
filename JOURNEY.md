## Notes
- I got more accurate results in the billing report when I only imported data from the last month.
	- Is there a way to dynamically search for the current billing cycle? i.e., Jan 1st to Jan 31st.

## To-Do List
### Infrastructure
- [ ] Clean up the LCH-GPS database.
	- [ ] Remove unrelated Stored Procedures
	- [ ] Remove unrelated Tables
- [ ] Clean up the Microsoft SQL Server
	- [ ] Lock down user permissions
	- [ ] Configure SQL backups
	- [ ] Configure server firewall
- [ ] Add SSL certificate to Microsoft SQL Server.
### Code
- [ ] Separate medical codes and retain their date of service.
	- [ ] Fix the bug with some readings not having dates.
- [ ] Abstract the failed data check function.
- [ ] Fix the anomaly of more bp/bg readings being written than read.
- [ ] Improve the billing report function.
	- [ ] Add delete medical code table to function.
- [ ] Provide a more precise method for deleting data in tables.
	- [ ] Add queries to check if certain tables have data.
	- [ ] Add a method to DatabaseManager for deleting and resetting identity.
- [ ] Refactor the import functions to use a class for state dependence.
	- [ ] (Or) Refactor the import data functions to be cleaner.
- [ ] Automate the exporting of the Patient data from SharePoint Online.

## Completed
- [x] Add parameters to the stored procedures for more accurate billing.
- [x] Add ability to record and export failed data.
- [x] Add snapshot switch for import functions.
- [x] Add the ability for DatabaseManager to handle multiple connections.
- [x] Overhaul code repository to make code more abstract.