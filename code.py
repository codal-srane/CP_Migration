import local
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime
import xmltodict
from psycopg2.extensions import AsIs
from variables import table_cols
import json

def lambda_function(credential):
	"""
	Connects to the database using configurations in local.py.
	Makes SOAP request using the read credentials.
	Stores the SOAP response into different tables in the database.
	"""
	conn = None
	
	try:
		print('\nLambda Function connecting to the database...')

		# Establish a connection to the database 
		conn = psycopg2.connect(
			host = local.db_config['host'],
			database = local.db_config['db'],
			user = local.db_config['user'], 
			password = local.db_config['password']
			)

		# Cursor for DB operations
		cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

		# Default status flag for a credential
		status = 'Completed'

		# Delete all existing records for this credential
		for entry in table_cols:
			cur.execute('DELETE FROM {0} WHERE CorpCode = \'{1}\' AND ' 
				'LocationCode = \'{2}\';'.format(
					entry, 
					credential['sCorpCode'.lower()], 
					credential['sLocationCode'.lower()]
					)
				)
			conn.commit()

		# Get today's date
		today = datetime.now()
		end_date = today.strftime('%Y-%m-%d')

		# Variables for making SOAP request
		soap_url = local.soap_url
		soap_headers = {'content-type':'text/xml;charset=UTF-8'}
		soap_body = """
			<Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">
    		<Body>
        		<InquiryTracking xmlns = 
        		"http://tempuri.org/CallCenterWs/ReportingWs">
					<sCorpCode>%s</sCorpCode>
					<sLocationCode>%s</sLocationCode>
					<sCorpUserName>%s</sCorpUserName>
					<sCorpPassword>%s</sCorpPassword>
					<dReportDateStart>2013-01-01</dReportDateStart>
					<dReportDateEnd>%s</dReportDateEnd>
				</InquiryTracking>
			</Body>
			</Envelope>
			""" %(
				credential['sCorpCode'.lower()],
				credential['sLocationCode'.lower()],
				credential['Username'.lower()],
				credential['Password'.lower()],
				end_date,
				)

		# Post the SOAP request and receive response	
		soap_response = requests.post(
			soap_url, 
			data = soap_body, 
			headers = soap_headers
			)	

		soap_json = xmltodict.parse(soap_response.content) 

		# Gather contents for all the tables
		main_tab = soap_json['soap:Envelope']['soap:Body']\
		['InquiryTrackingResponse']['InquiryTrackingResult']\
		['diffgr:diffgram']['NewDataSet']
		
		# JSON to keep track of number of writes to DB
		rows_json = {'Insertions' : {
			'Activity': 0,
			'Summary': 0,
			'Cancelled': 0,
			'Marketing': 0,
			'InquirySource': 0,
			'Employees': 0,
			'Sites': 0
			}
		}

		# Write data to the corresponding tables
		tables = [
			'Activity',
			'Summary',
			'Cancelled', 
			'Marketing', 
			'InquirySource',
			'Employees',
			'Sites',
		]

		for entry in tables:
			columns = table_cols[entry]
			itemlist = []
			if entry == 'Sites':
				row = main_tab[entry]
				row.pop('@diffgr:id', None)
				row.pop('@msdata:rowOrder', None)
				row['LocationName'] = credential['LocationName'.lower()]	 
				row['AccountName'] = credential['AccountName'.lower()]
				row['StartDate'] = credential['StartDate'.lower()]
				row['CorpCode'] = credential['sCorpCode'.lower()]
				row['LocationCode'] = credential['sLocationCode'.lower()]
				itemlist.append([row.get(c) for c in columns])
			else:
				for row in main_tab[entry]:
					row.pop('@diffgr:id', None)
					row.pop('@msdata:rowOrder', None)
					row['CorpCode'] = credential['sCorpCode'.lower()]
					row['LocationCode'] = credential['sLocationCode'.lower()]
					itemlist.append([row.get(c) for c in columns])
			cols = ','.join((t for t in columns))
			values = ','.join(('{}'.format("%s") for t in columns))
			
			cur.executemany('INSERT INTO {0} ({1}) VALUES ({2});'.format(entry, 
			cols, values), itemlist)
			conn.commit()
			rows_json['Insertions'][entry] = len(itemlist)	
	
	except Exception as error:
		status = 'Error'
		print('Lambda Function Error: ' + str(error))
	
	finally:
		if conn is not None:
			# Update the Credentials table with the status of the soap request
			time_completed = datetime.now()
			cur.execute('UPDATE Credentials SET Status = \'{0}\', ' 
				'Rows = \'{1}\', TimeCompleted = \'{2}\' WHERE '
				'sCorpCode = \'{3}\' AND sLocationCode = \'{4}\';'.format(
					status,
					json.dumps(rows_json),
					time_completed,
					credential['sCorpCode'.lower()], 
					credential['sLocationCode'.lower()]
					)
				)
			conn.commit()

			# Close the cursor
			cur.close()
			
			#Close DB connection
			conn.close()
			print('\nLambda function to database connection closed.')

