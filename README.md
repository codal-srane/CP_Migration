## Migration from Google Apps Script to Python Script

This repository contains all the code for migration of the .gs scripts to .py scripts.  


### Current work completed:    
- Created a local Postgres DB 'cp_test'  
- Established a connection with the database  
- Able to retrieve the data stored in 'Credentials' table  


### Upcoming work:  
- Converting the retrieved data into a format acceptable for the SOAP call  
- Make a SOAP request using the parsed data  
- Parse the response object  
- Store the response data in the appropriate tables     
