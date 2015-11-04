from etl.pipeline.tableuploader import UploadTable
import etl.cleaners.clean_noble as clean_noble
import etl.db_schema.SQLtables_cols as SQLtables_cols
import config

def main():

	# Student IDs
	UploadTable(rawcsv_dict = { 'contacts': config.DATA_PATH + 'noble/raw_data/jun_24/Contact_06_19_2015.csv' },
				rawtoclean_fn = clean_noble.studentid,
				sqltable_name = 'noble.studentid',
				sqltable_cols = SQLtables_cols.studentid,
				overwrite=True
	)


	# College IDs
	UploadTable(rawcsv_dict = { 'colleges': config.DATA_PATH + 'noble/raw_data/feb_26/CollegeDatabase.csv' },
				rawtoclean_fn = clean_noble.collegeid,
				sqltable_name = 'noble.collegeid',
				sqltable_cols = SQLtables_cols.collegeid,
				overwrite=True
	)

	# Students
	UploadTable(rawcsv_dict = { 'noble_students': config.DATA_PATH + 'noble/raw_data/jun_24/Contact_06_19_2015.csv' },
				rawtoclean_fn = clean_noble.students,
				sqltable_name = 'noble.students',
				sqltable_cols = SQLtables_cols.students,
				overwrite=True
	)

	# ACTs
	UploadTable(rawcsv_dict = { 'noble_tests': config.DATA_PATH + 'noble/raw_data/jul_9/explore-plan-act_dumpjul11.csv' },
				rawtoclean_fn = clean_noble.acttests,
				sqltable_name = 'noble.acttests',
	            sqltable_cols = SQLtables_cols.acttests,
	            overwrite=True
	)

	# AP
	UploadTable(rawcsv_dict = { 'noble_aps': config.DATA_PATH + 'noble/raw_data/jul_9/AP_test_dump_jul9.csv' },
				rawtoclean_fn = clean_noble.ap,
				sqltable_name = 'noble.aptests',
	            sqltable_cols = SQLtables_cols.aptests,
	            overwrite=True
	)

	# Attendance
	UploadTable(rawcsv_dict = { 'noble_att': config.DATA_PATH + 'noble/raw_data/jul_9/attendance_dump_jul9.csv' },
				rawtoclean_fn = clean_noble.attendance,
				sqltable_name = 'noble.attendance',
	            sqltable_cols = SQLtables_cols.attendance,
	            overwrite=True
	)

	# Contacts
	UploadTable(rawcsv_dict = { 'contacts': config.DATA_PATH + 'noble/raw_data/jul_8/ContactNotes.csv' , 
								'alumni': config.DATA_PATH + 'noble/raw_data/feb_26/Alumni.csv'},
				rawtoclean_fn = clean_noble.contacts,
				sqltable_name = 'noble.contacts',
	            sqltable_cols = SQLtables_cols.contacts,
	            overwrite=True
	)


	# Enrollments
	UploadTable(rawcsv_dict = { 'enrollment': config.DATA_PATH + 'noble/raw_data/jun_24/Enrollment__c_06_19_2015.csv' },
				rawtoclean_fn = clean_noble.enrollments,
				sqltable_name = 'noble.enrollments',
	            sqltable_cols = SQLtables_cols.enrollments,
	            overwrite=True
	)

	# Colleges
	UploadTable(rawcsv_dict = { 'colleges': config.DATA_PATH + 'noble/raw_data/feb_26/CollegeDatabase.csv', 
								'extra_college_features':  config.DATA_PATH + 'noble/raw_data/jun_24/extra_college_features.csv' },
				rawtoclean_fn = clean_noble.colleges,
				sqltable_name = 'noble.colleges',
	            sqltable_cols = SQLtables_cols.colleges,
	            overwrite=True
	)

	# Schools
	UploadTable(rawcsv_dict = { 'accounts': config.DATA_PATH + 'noble/raw_data/feb_26/Account_02_23_2015.csv',
								'alumni': config.DATA_PATH + 'noble/raw_data/feb_26/Alumni.csv',
								'founding_years': config.DATA_PATH + 'noble/clean_data/founding_years.csv' },
				rawtoclean_fn = clean_noble.schools,
				sqltable_name = 'noble.schools',
	            sqltable_cols = SQLtables_cols.schools,
	            overwrite=True
	)

	# School IDs
	UploadTable(rawcsv_dict = { 'accounts': config.DATA_PATH + 'noble/raw_data/feb_26/Account_02_23_2015.csv',
								'alumni': config.DATA_PATH + 'noble/raw_data/jun_24/Contact_06_19_2015.csv',
								'powerschool_ids': config.DATA_PATH + 'noble/raw_data/jul_9/course_dump_schoolids.csv'},
				rawtoclean_fn = clean_noble.schoolid,
				sqltable_name = 'noble.schoolid',
	            sqltable_cols = SQLtables_cols.schoolid,
	            overwrite=True
	)

	# GPA by year 
	UploadTable(rawcsv_dict = { 'courses': config.DATA_PATH + 'noble/raw_data/jul_9/courses_dump_jul9.csv' },
				rawtoclean_fn = clean_noble.gpa_by_year,
				sqltable_name = 'noble.gpa_by_year',
	            sqltable_cols = SQLtables_cols.gpa_by_year,
	            overwrite=True
	)

	# GPA cumulative 
	UploadTable(rawcsv_dict = { 'alumni': config.DATA_PATH + 'noble/raw_data/feb_26/Alumni.csv',
								'accounts': config.DATA_PATH + 'noble/raw_data/feb_26/Account_02_23_2015.csv' },
				rawtoclean_fn = clean_noble.gpa_cumulative,
				sqltable_name = 'noble.gpa_cumulative',
	            sqltable_cols = SQLtables_cols.gpa_cumulative,
	            overwrite=True
	)

	# Courses
	UploadTable(rawcsv_dict = { 'courses': config.DATA_PATH + 'noble/raw_data/jul_9/courses_dump_jul9.csv'},
				rawtoclean_fn = clean_noble.courses,
				sqltable_name = 'noble.courses',
	            sqltable_cols = SQLtables_cols.courses,
	            overwrite=True
	)


	# HS Enrollment
	UploadTable(rawcsv_dict = { 'contact': config.DATA_PATH + 'noble/raw_data/jun_24/Contact_06_19_2015.csv',
								'accounts': config.DATA_PATH + 'noble/raw_data/feb_26/Account_02_23_2015.csv'},
				rawtoclean_fn = clean_noble.hs_enrollment,
				sqltable_name = 'noble.hs_enrollment',
	            sqltable_cols = SQLtables_cols.hs_enrollment,
	            overwrite=True
	)

if __name__ == '__main__':
	main()