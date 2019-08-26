import local
import psycopg2
import psycopg2.extras
import requests
import datetime
import xmltodict
from psycopg2.extensions import AsIs


def lambda_function():
	"""
	Connects to the database using credentials in local.py.
	Makes SOAP request using the read credentials.
	To do : Stores the SOAP response into different tables in the database.
	"""
	conn = None
	try:
		print('Connecting to the Database...')

		# Establish a connection to the database 
		conn = psycopg2.connect(
			host = local.db_config['host'],
			database = local.db_config['db'],
			user = local.db_config['user'], 
			password = local.db_config['password']
			)

		# Cursor for DB operations
		cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
		print('Cursor created')

		# Fetch all records from 'Credentials' table
		cur.execute('SELECT * FROM Credentials;')
		credential = cur.fetchone()

		# Get today's date
		today = datetime.datetime.now()
		end_date = today.strftime('%Y-%m-%d')

		# Variables for making SOAP request
		soap_url = 'https://www.smdservers.net/CCWs_3.5/ReportingWs.asmx'
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
				credential['s_corp_code'],
				credential['s_location_code'],
				credential['username'],
				credential['password'],
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

		#activity_tab = ['Activity']
		
		summary_tab = soap_json['soap:Envelope']['soap:Body']\
		['InquiryTrackingResponse']['InquiryTrackingResult']\
		['diffgr:diffgram']['NewDataSet']['Summary']
		
		employees_tab = soap_json['soap:Envelope']['soap:Body']\
		['InquiryTrackingResponse']['InquiryTrackingResult']\
		['diffgr:diffgram']['NewDataSet']['Employees']
		
		sites_tab = soap_json['soap:Envelope']['soap:Body']\
		['InquiryTrackingResponse']['InquiryTrackingResult']\
		['diffgr:diffgram']['NewDataSet']['Sites']
		
		# Write data to the corresponding tables
		tables = [
			'Summary',
			# 'Cancelled', 
			# 'Marketing', 
			# 'InquirySource',
		]

		for entry in tables:
			if entry == 'Sites':
				pass
			for row in main_tab[entry]:
				row.pop('@diffgr:id', None)
				row.pop('@msdata:rowOrder', None)
				#print(row)
				row_list = [(c, v) for c, v in row.items()]
				#print(row_list)
				columns = ','.join([t[0] for t in row_list])
				#print(columns)
				values = ','.join(['%({})s'.format(t[0]) for t in row_list])
				print(values)
			cur.executemany('INSERT INTO {0} ({1}) VALUES ({2});'.format(entry, 
				columns, values),  main_tab[entry])
			conn.commit()


		#print(sites_tab)

		cur.close()
	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		# Close DB connection
		if conn is not None:
			conn.close()
			print('Database connection closed.')


if __name__ == '__main__':
	lambda_function()