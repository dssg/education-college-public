from etl.pipeline.tableuploader import UploadTable
import pandas as pd
import config

def main():

	###################
	### Common Bits ###
	###################

	lookup_cols = {
		'commonids': '''
			''',
		'partnerids': '''
			''',
		'data': 'name VARCHAR PRIMARY KEY'
	}

	def prep_lookup_cleandf(rawdf, lookupcol):
		return pd.DataFrame(rawdf[lookupcol]).rename(columns = {lookupcol:'name'}).drop_duplicates().dropna().sort(columns='name')

	##################
	### Load to DB ###
	##################

	# College Degree lookup
	UploadTable(rawcsv_dict = { 'rawdf': config.PERSISTENCE_PATH + '/code/etl/mappers/degreetranslation.csv' },
				rawtoclean_fn = lambda rawdf: prep_lookup_cleandf(rawdf, 'StandardDegree'),
				sqltable_name = 'lookup.collegedegrees',
				sqltable_cols = lookup_cols,
				overwrite=True
	)

	# College Major lookup
	UploadTable(rawcsv_dict = { 'rawdf': config.PERSISTENCE_PATH + '/code/etl/mappers/majortranslation.csv' },
				rawtoclean_fn = lambda rawdf: prep_lookup_cleandf(rawdf, 'StandardMajor'),
				sqltable_name = 'lookup.majors',
				sqltable_cols = lookup_cols,
				overwrite=True
	)

	# AP Subjects lookup
	UploadTable(rawcsv_dict = { 'rawdf': config.PERSISTENCE_PATH + '/code/etl/mappers/APsubjecttranslation.csv' },
				rawtoclean_fn = lambda rawdf: prep_lookup_cleandf(rawdf, 'StandardAP'),
				sqltable_name = 'lookup.apsubjects',
				sqltable_cols = lookup_cols,
				overwrite=True
	)

	# Discipline Types lookup
	UploadTable(rawcsv_dict = { 'rawdf': config.PERSISTENCE_PATH + '/code/etl/mappers/disciplinetranslation.csv' },
				rawtoclean_fn = lambda rawdf: prep_lookup_cleandf(rawdf, 'StandardDiscipline'),
				sqltable_name = 'lookup.disciplinetypes',
				sqltable_cols = lookup_cols,
				overwrite=True
	)

if __name__ == '__main__':
	main()