import local
import mysql.connector
from code_ms import lambda_function

def lambda_trigger():
	"""
	Connects to the database using configurations in local.py.
	Fetches all the valid credential records (Include = TRUE) from the 
	'Credentials' table.
	For each valid record, makes a separate call to the lambda function.
	"""
	conn = None
	
	try:
		print('Trigger connecting to the database...')

		# Establish a connection to the database 
		conn = mysql.connector.connect(
			host = local.db_config['host'],
			database = local.db_config['db'],
			user = local.db_config['user'], 
			password = local.db_config['password'],
			auth_plugin = local.db_config['auth_plugin']
			)

		# Cursor for DB operations
		cur = conn.cursor(dictionary = True)
		
		# Fetch all records from 'Credentials' table
		cur.execute('SELECT * FROM credentials WHERE Include = TRUE;')
		credentials = cur.fetchall()

		for a_credential in credentials:
			lambda_function(a_credential)

		# Close the cursor
		cur.close()	
		
	except Exception as error:
		print('Lambda Trigger Error: ' + str(error))
	
	finally:
		#Close DB connection
		if conn:
			conn.close()
			print('\nTrigger to database connection closed.')


if __name__ == '__main__':
	lambda_trigger()