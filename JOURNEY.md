## To-Do List
### Infrastructure
- [ ] Clean up the GPS database.
	- [ ] Remove unrelated Stored Procedures
	- [ ] Remove unrelated Tables
### Code
- [ ] Add snapshot switch for import functions.
	- [ ] Save the dataframes in their states throughout the process.
- [ ] Provide a more precise method for deleting data in tables.
	- [ ] Add queries to check if certain tables have data.
	- [ ] Add a method to DatabaseManager for deleting and resetting identity.
- [ ] Automate the exporting of the Patient data from SharePoint Online.

## Completed
- [x] Add the ability for DatabaseManager to handle multiple connections.
- [x] Overhaul code repository to make code more abstract.