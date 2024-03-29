import local
import mysql.connector
import requests
from variables import table_cols
from datetime import datetime
import xmltodict
import json
import string


def lambda_function(credential):
	"""
	Input: A credentials' table row
	Reads input credentials and makes a SOAP request
	Inserts the SOAP response data into different tables 
	Updates the credentials table with the status information  
	"""
	start_time = datetime.now()
	conn = None

	try:
		print('\nLambda Function connecting to the database...')

		# Establish a connection to the database 
		conn = mysql.connector.connect(
			host = local.db_config['host'],
			database = local.db_config['db'],
			user = local.db_config['user'], 
			password = local.db_config['password'],
			auth_plugin = local.db_config['auth_plugin']
			)

		# Cursor for DB operations
		cur = conn.cursor()

		# Default status flag for a credential
		status = 'Completed'

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
				credential['sCorpCode'],
				credential['sLocationCode'],
				credential['Username'],
				credential['Password'],
				end_date,
				)

		# Post the SOAP request and receive response	
		soap_response = requests.post(
			soap_url, 
			data = soap_body, 
			headers = soap_headers
			)	

		print('API called for {0} :: {1}'.format(credential['sCorpCode'], 
			credential['sLocationCode']))

		# Delete all existing records for this credential
		for entry in table_cols:
			cur.execute('DELETE FROM {0} WHERE sCorpCode = \'{1}\' AND ' 
				'sLocationCode = \'{2}\';'.format(
					entry.lower(), 
					credential['sCorpCode'], 
					credential['sLocationCode']
					)
				)
			print("Existing data deleted from {} table".format(entry.lower()))
			conn.commit()


		soap_json = xmltodict.parse(soap_response.content) 

		# Gather contents for all the tables
		main_tab = soap_json['soap:Envelope']['soap:Body']\
		['InquiryTrackingResponse']['InquiryTrackingResult']\
		['diffgr:diffgram']['NewDataSet']
		ret_code = None
		ret_msg = None

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

		# JSON to keep track of number of writes to DB
		rows_json = {'Insertions' : {
			'activity': 0,
			'summary': 0,
			'cancelled': 0,
			'marketing': 0,
			'inquirysource': 0,
			'employees': 0,
			'sites': 0
			}
		}

		printable = set(string.printable)

		for entry in tables:
			columns = table_cols[entry]
			itemlist = []
			rows = []
			if type(main_tab[entry]) is not list:
				rows.append(main_tab[entry])
				#print(rows)
			else:
				rows = main_tab[entry]
			#print(rows)	
			for row in rows:
				row.pop('@diffgr:id', None)
				row['rowOrder'] = row.pop('@msdata:rowOrder', None)
				row['sCorpCode'] = credential['sCorpCode']
				row['sLocationCode'] = credential['sLocationCode']
				if entry == 'Sites':
					row['LocationName'] = credential['LocationName']	 
					row['AccountName'] = credential['AccountName']
					row['StartDate'] = credential['StartDate']

				# itemlist.append([str(row.get(c))[:250] if row.get(c) 
				# 	else '' for c in columns])
				itemlist.append([''.join(filter(lambda x : x in printable, 
					str(row.get(c))))[:250] if row.get(c) else '' 
					for c in columns])
				
			cols = ','.join((t for t in columns))
			values = ','.join(('{}'.format("%s") for t in columns))
			
			#print("Initialized cursor execution for {}".format(entry))
			cur.executemany('INSERT INTO {0} ({1}) VALUES ({2});'.format(
				entry.lower(), cols, values), itemlist)
			conn.commit()
			print("Rows inserted into {} table".format(entry.lower()))
			
			rows_json['Insertions'][entry.lower()] = len(itemlist)

		print('All insertions done')

	except mysql.connector.Error as error:
		# Handle DB related errors
		status = 'Error'
		print('Lambda Function Error for {0} :: {1} - {2}'.format(
			credential['sCorpCode'], 
			credential['sLocationCode'], 
			str(error)
			)
		)

	except requests.exceptions.RequestException as error:
		# Handle SOAP request related errors
		status = 'Error'
		print('Lambda Function Error for {0} :: {1} - {2}'.format(
			credential['sCorpCode'], 
			credential['sLocationCode'], 
			str(error)
			)
		)		
	
	except Exception as error:	
		# Handle Invalid Credentials Error
		status = 'Error'
		err_obj = main_tab.get('RT', None)
		if err_obj is not None:
			ret_code = err_obj.get('Ret_Code', None)
			ret_msg = err_obj.get('Ret_Msg', None)
		print('Lambda Function Error for {0} :: {1} - {2}'.format(
			credential['sCorpCode'], 
			credential['sLocationCode'], 
			str(error)
			)
		)

	finally:
		if conn is not None:
			# Update the Credentials table with the status of the soap request
			time_completed = datetime.now()
			cur.execute('UPDATE credentials SET Status = \'{0}\', ' 
				'RowsAffected = \'{1}\', TimeCompleted = \'{2}\', '
				'Ret_Code = \'{3}\', Ret_Msg = \'{4}\' WHERE '
				'sCorpCode = \'{5}\' AND sLocationCode = \'{6}\';'.format(
					status,
					json.dumps(rows_json),
					time_completed,
					ret_code,
					ret_msg,
					credential['sCorpCode'], 
					credential['sLocationCode']
					)
				)
			conn.commit()

			# Close the cursor
			cur.close()
			
			#Close DB connection
			conn.close()
			print('Lambda function to database connection closed.')
			total_run_time = datetime.now() - start_time
			print('Time for execution = {}'.format(total_run_time))

if __name__ == '__main__':
	lambda_function()
