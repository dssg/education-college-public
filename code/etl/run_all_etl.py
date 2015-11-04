''' Perform all ETL and load all data into common schema in Postgres database.

Inputs:
- an empty database
- raw CSV files from partners

Outputs:
- a database filled with cleaned tables in common schema
'''

import etl.uploaders.create_schemas as create_schemas
import etl.uploaders.load_lookuptables as load_lookuptables
import etl.uploaders.load_noble as load_noble
import etl.uploaders.load_kippnj as load_kippnj
import etl.uploaders.combine_common as combine_common

def main():

	# Step 0: Create empty schemas
	create_schemas.main()

	# Step 1: Create lookup tables
	load_lookuptables.main()

	# Step 2: Load individual partners' data
	load_noble.main()
	load_kippnj.main()

	# Step 3: Combine all partners' data in common schema
	combine_common.main()

if __name__ == '__main__':
	main()