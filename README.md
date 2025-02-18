# LCH-Tools
Toolkit of Python scripts used for large(ish)-scale data manipulation.

## Scope
The current scope of LCH-Tools is to standardize data for roughly ~22,000 Medicare patients and import to a SQL database. LiveCare Health is upgrading the quality of their reports to Medicare for billing. 
## Process
Here is a list of technologies used by LCH-Tools.
- Microsoft SQL Server
	- ODBC Driver 18 - Driver used to interface with the SQL server.
- SQLAlchemy - Micro Framework ORM for SQL execution.
	- pyodbc - Library used for interfacing with the ODBC driver.
- Pandas - Structured data manipulation tool.
	- openpyxl - Excel engine used by Pandas to create spreadsheets.
- Requests - Library used for HTTP request handling.
