# Journey

## Notes

- Eventually, we will import all the data from SharePoint and the other databases.
- Queries that have multiple statements must be a stored procedure due to SQLAlchemy.
- I got more accurate results in the billing report when I only imported data from the last month.
- It might be a good idea to combine the glucose readings and blood pressure readings table.
  - It might prove difficult to manage multiple types of devices, but it works with two different kinds of devices.
- Getting readings data from Tenovi takes a very long time.
  - I could use Celery to distribute the workload of this task.

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

- [ ] Add Days Since calculation to Last Reading Date and Last Note Date.
- [ ] Add Pydantic for JSON schema checking on API classes.
- [ ] Add mechanism for importing readings directly from Tenovi into the DB.
  - [ ] Add Tenovi API class.
  - [ ] Add Tenovi data standardization with Pandas.
  - [ ] Pick pieces of data to send to the readings tables.
- [ ] Add mechanism for importing PVerify data into the DB.
  - [ ] Add PVerify API class.
  - [ ] Add Pverify data standardization with Pandas.
  - [ ] Pick pieces of data to add to patient insurance.
  - [ ] Store the rest of the JSON in holder field in DB.
- [ ] Add docstring for setup_logger function.
- [ ] Alter the stored procedures to find the current billing cycle on their own.
- [ ] Add ability to filter patients by member_status in the billing report.
- [ ] Fix the anomaly of more bp/bg readings being written than read.
- [ ] Provide a more precise method for deleting data in tables.
  - [ ] Add queries to check if certain tables have data.
  - [ ] Add a method to DatabaseManager for deleting and resetting identity.
- [ ] Automate the exporting of the Patient data from SharePoint Online.

## Completed

- [x] Add snapshot functions that will save standardized data to files for viewing.
- [x] Abstract the keyword search function used in dataframe_utils.
- [x] Create functions for splitting the dataframes into their respective tables.
- [x] Change format of docstrings for RestAdapter.
- [x] Add docstring to MSGraphApi.
- [x] Update docstring DatabaseManager.
- [x] Update the CSV snapshot option in the import data functions.
- [x] Refine the fill_insurance info methods.
- [x] Add ability to dynamically find the current billing cycle.
  - [x] Add start_date and end_date to the data pulling queries.
  - [x] Add start_date and end_date to the billing code sprocs as parameters.
- [x] Improve the billing report function.
  - [x] Add delete medical code table to function.
- [x] Separate medical codes and retain their date of service.
  - [x] Fix the bug with some readings not having dates.
- [x] Add parameters to the stored procedures for more accurate billing.
- [x] Add ability to record and export failed data.
- [x] Add snapshot switch for import functions.
- [x] Add the ability for DatabaseManager to handle multiple connections.
- [x] Overhaul code repository to make code more abstract.

## Rejected

- [ ] Refactor the import functions to use a class for state dependence.
  - [ ] (Or) Refactor the import data functions to be cleaner.
- [ ] Abstract the failed data check function and database constraint function.
