from etl.pipeline.tableuploader import UploadTable
import etl.cleaners.clean_kippnj as clean_kippnj
import etl.db_schema.SQLtables_cols as SQLtables_cols
import config

def main():

	# Student IDs

	UploadTable(rawcsv_dict = { 'kipp_nj_students': config.DATA_PATH + 'kipp_nj/raw_data/students_db_jul20.csv' },
				rawtoclean_fn = clean_kippnj.student_id_table,
				sqltable_name = 'kipp_nj.studentid',
				sqltable_cols = SQLtables_cols.studentid,
				overwrite=True
				)

	# School IDs

	UploadTable(rawcsv_dict = { 'kipp_nj_schools': config.DATA_PATH + 'kipp_nj/raw_data/schools_db_jul23.csv' },
				rawtoclean_fn = clean_kippnj.school_id_table,
				sqltable_name = 'kipp_nj.schoolid',
				sqltable_cols = SQLtables_cols.schoolid,
				overwrite=True
				)

	# Students

	UploadTable(rawcsv_dict = { 'kipp_nj_students': config.DATA_PATH + 'kipp_nj/raw_data/students_db_jul20.csv' },
				rawtoclean_fn = clean_kippnj.students_table,
				sqltable_name = 'kipp_nj.students',
				sqltable_cols = SQLtables_cols.students,
				overwrite=True
				)

	# Schools

	UploadTable(rawcsv_dict = { 'kipp_nj_schools': config.DATA_PATH + 'kipp_nj/raw_data/schools_db_jul23.csv' },
				rawtoclean_fn = clean_kippnj.schools_table,
				sqltable_name = 'kipp_nj.schools',
				sqltable_cols = SQLtables_cols.schools,
				overwrite=True
				)

	# Enrollments

	UploadTable(rawcsv_dict = { 'kipp_nj_enrollments': config.DATA_PATH + 'kipp_nj/raw_data/college_enr_db_aug13.csv' },
				rawtoclean_fn = clean_kippnj.enrollments_table,
				sqltable_name = 'kipp_nj.enrollments',
				sqltable_cols = SQLtables_cols.enrollments,
				overwrite=True
				)

	# Contacts

	UploadTable(rawcsv_dict = { 'kipp_nj_contacts': config.DATA_PATH + 'kipp_nj/raw_data/contacts_db_aug5.csv' },
				rawtoclean_fn = clean_kippnj.contacts_table,
				sqltable_name = 'kipp_nj.contacts',
				sqltable_cols = SQLtables_cols.contacts,
				overwrite=True
				)

	# ACT

	UploadTable(rawcsv_dict = { 'kipp_nj_act': config.DATA_PATH + 'kipp_nj/raw_data/act_db_aug5.csv' },
				rawtoclean_fn = clean_kippnj.act_table,
				sqltable_name = 'kipp_nj.acttests',
				sqltable_cols = SQLtables_cols.acttests,
				overwrite=True
				)

	# Courses

	UploadTable(rawcsv_dict = { 'kipp_nj_courses': config.DATA_PATH + 'kipp_nj/raw_data/courses_db_aug5.csv' },
				rawtoclean_fn = clean_kippnj.courses_table,
				sqltable_name = 'kipp_nj.courses',
				sqltable_cols = SQLtables_cols.courses,
				overwrite=True
				)

	# Attendance

	UploadTable(rawcsv_dict = { 'kipp_nj_attendance': config.DATA_PATH + 'kipp_nj/raw_data/att_db_aug11.csv' },
				rawtoclean_fn = clean_kippnj.attendance_table,
				sqltable_name = 'kipp_nj.attendance',
				sqltable_cols = SQLtables_cols.attendance,
				overwrite=True
				)

	# GPA by year

	UploadTable(rawcsv_dict = { 'kipp_nj_gpa': config.DATA_PATH + 'kipp_nj/raw_data/gpa_db_aug13.csv' },
				rawtoclean_fn = clean_kippnj.gpa_by_year,
				sqltable_name = 'kipp_nj.gpa_by_year',
				sqltable_cols = SQLtables_cols.gpa_by_year,
				overwrite=True
				)

	# Cumulative GPA

	UploadTable(rawcsv_dict = { 'kipp_nj_gpa': config.DATA_PATH + 'kipp_nj/raw_data/gpa_db_aug13.csv' },
				rawtoclean_fn = clean_kippnj.gpa_cumulative,
				sqltable_name = 'kipp_nj.gpa_cumulative',
				sqltable_cols = SQLtables_cols.gpa_cumulative,
				overwrite=True
				)

	# HS enrollment

	UploadTable(rawcsv_dict = {  'kipp_nj_enrollments': config.DATA_PATH + 'kipp_nj/raw_data/college_enr_db_aug13.csv' },
				rawtoclean_fn = clean_kippnj.hs_enrollment_table,
				sqltable_name = 'kipp_nj.hs_enrollment',
				sqltable_cols = SQLtables_cols.hs_enrollment,
				overwrite=True
				)

if __name__ == '__main__':
	main()