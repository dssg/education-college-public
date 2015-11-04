''' Create empty schemas in Postgres database that will subsequently be needed to store tables. '''

from util import cred # load SQL credentials
from util.SQL_helpers import connect_to_db

def main():
	# Create empty schemas
	sqlcmd_createschemas = '''
		CREATE SCHEMA lookup;
		CREATE SCHEMA noble;
		CREATE SCHEMA kipp_nj;
		CREATE SCHEMA common;'''

	with connect_to_db(cred.host, cred.user, cred.pw, cred.dbname) as conn: # use context managed db connection
	    with conn.cursor() as cur:
	        cur.execute(sqlcmd_createschemas)
	        cur.connection.commit()

if __name__ == '__main__':
	main()