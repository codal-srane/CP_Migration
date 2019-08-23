import local
import psycopg2


def lambda_function():
	"""
	Connects to the database using credentials in local.py.
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
		cur = conn.cursor()
		print('Cursor created')

		# Fetch all records from 'Credentials' table
		cur.execute("SELECT * FROM Credentials;")
		cred_all = cur.fetchall() 

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