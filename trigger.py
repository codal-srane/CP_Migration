import local
import psycopg2
import psycopg2.extras
from code import lambda_function

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
		conn = psycopg2.connect(
			host = local.db_config['host'],
			database = local.db_config['db'],
			user = local.db_config['user'], 
			password = local.db_config['password']
			)

		# Cursor for DB operations
		cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
		
		# Fetch all records from 'Credentials' table
		cur.execute('SELECT * FROM Credentials WHERE Include = TRUE;')
		credentials = cur.fetchall()

		for a_credential in credentials:
			lambda_function(a_credential)

	except Exception as error:
		print('Lambda Trigger Error: ' + str(error))
	
	finally:
		# Close the cursor
		cur.close()
		
		#Close DB connection
		conn.close()
		print('\nTrigger to database connection closed.')


if __name__ == '__main__':
	lambda_trigger()