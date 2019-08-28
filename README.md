## Migration from Google Apps Script to Python Script

This repository contains all the code for migration of the .gs scripts to .py scripts.  


### Current work completed:    
- Created a local Postgres DB 'cp_test'  
- Established a connection with the database  
- Able to retrieve the data stored in 'Credentials' table    
- Converting the retrieved data into a format acceptable for the SOAP call  
- Making a SOAP request using the parsed data  
- Parsing the response object  
- Storing the response data in the appropriate tables (Upsert operation accomplished using Delete followed by Insert queries on the Database tables)
- Updated the 'Credentials' table columns 'Status', 'RowsAffected' and 'TimeCompleted'

### Migrations:
- Made Database migrations from PostgreSQL to MySQL
