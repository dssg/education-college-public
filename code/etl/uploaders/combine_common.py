from etl.pipeline.tableuploader import CombineTable, UploadTable
import etl.db_schema.SQLtables_cols as SQLtables_cols
import etl.cleaners.gen_persistence_dummies as gen_persistence_dummies

def main():

	# Student IDs
	CombineTable(sqltable_name = 'common.studentid',
	 			sqltable_cols = SQLtables_cols.studentid,
				sqlcmd_populate = '''
					INSERT INTO {commontable} ({datacolnames}) (
						SELECT * from noble.studentid
						UNION
						SELECT * from kipp_nj.studentid
					);
				''',
				overwrite=True
	)

	# School IDs
	CombineTable(sqltable_name = 'common.schoolid',
	 			sqltable_cols = SQLtables_cols.schoolid,
				sqlcmd_populate = '''
					INSERT INTO {commontable} ({datacolnames}) (
						SELECT * from noble.schoolid
						UNION
						SELECT * from kipp_nj.schoolid
					);
				''',
				overwrite=True
	)

	# College IDs
	CombineTable(sqltable_name = 'common.collegeid',
	 			sqltable_cols = SQLtables_cols.collegeid,
				sqlcmd_populate = '''
					INSERT INTO {commontable} ({datacolnames}) (
						SELECT * from noble.collegeid
					);
				''',
				overwrite=True
	)

	# Students
	CombineTable(sqltable_name = 'common.students',
	 			sqltable_cols = SQLtables_cols.students,
				sqlcmd_populate = '''
					INSERT INTO {commontable} (
					    SELECT a.studentid, {partnerdatacols} FROM (
					    	noble.students as partnerdata
					    	LEFT JOIN common.studentid as a
					            ON partnerdata.noble_sf_id = a.noble_sf_id
					    )
					    UNION
					    SELECT a.studentid, {partnerdatacols} FROM (
					    	kipp_nj.students as partnerdata
					    	LEFT JOIN common.studentid as a
					            ON partnerdata.kipp_nj_sf_id = a.kipp_nj_sf_id
					    )
					);
				''',
				overwrite=True
	)

	# Enrollments
	CombineTable(sqltable_name = 'common.enrollments',
	 			sqltable_cols = SQLtables_cols.enrollments,
				cols_toindex = ['studentid','collegeid'],
				sqlcmd_populate = '''
					INSERT INTO {commontable} (studentid, collegeid, {datacolnames}) (
						SELECT a.studentid, b.collegeid, {partnerdatacols} FROM (
							noble.enrollments as partnerdata
							LEFT JOIN common.studentid as a
								ON partnerdata.noble_student_sf_id = a.noble_sf_id
							LEFT JOIN common.collegeid as b
								ON partnerdata.noble_college_sf_id = b.noble_sf_college_id
						)
						UNION
						SELECT a.studentid, b.collegeid, {partnerdatacols} FROM (
							kipp_nj.enrollments as partnerdata
							LEFT JOIN common.studentid as a
								ON partnerdata.kipp_nj_sf_id = a.kipp_nj_sf_id
							LEFT JOIN common.collegeid as b
								on partnerdata.ipedsid = b.ipedsid
						)
					);
				''',
				overwrite=True
	)

	# ACTs
	# Note: the use of INNER JOINs is because of student_sf_IDs in acttests that aren't in studentid/students
	CombineTable(sqltable_name = 'common.acttests',
	 			sqltable_cols = SQLtables_cols.acttests,
				cols_toindex = ['studentid'],
				sqlcmd_populate = '''
					INSERT INTO {commontable} (
						SELECT a.studentid, {partnerdatacols} FROM (
							common.studentid as a INNER JOIN noble.acttests as partnerdata
								ON a.cps_id = partnerdata.cps_id
						)
						UNION
						SELECT a.studentid, {partnerdatacols} FROM (
							common.studentid as a INNER JOIN kipp_nj.acttests as partnerdata
								ON a.kipp_nj_sf_id = partnerdata.kipp_nj_sf_id
						)
					);
				''',
				overwrite=True
	)

	# APs
	# Note: the use of INNER JOINs is because of student_sf_IDs in aptests that aren't in studentid/students
	CombineTable(sqltable_name = 'common.aptests',
	 			sqltable_cols = SQLtables_cols.aptests,
				cols_toindex = ['studentid'],
				sqlcmd_populate = '''
					INSERT INTO {commontable} (
						SELECT a.studentid, {partnerdatacols} FROM (
							noble.aptests as partnerdata
							INNER JOIN common.studentid as a
								ON partnerdata.noble_powerschool_id = a.cps_id
						)
						-- TODO: add KIPP NJ if they send us AP test data
					);
				''',
				overwrite=True
	)

	# GPA - Cumulative
	CombineTable(sqltable_name = 'common.gpa_cumulative',
	 			sqltable_cols = SQLtables_cols.gpa_cumulative,
				cols_toindex = ['schoolid'],
				sqlcmd_populate = '''
					INSERT INTO {commontable} (
						SELECT a.studentid, b.schoolid, {partnerdatacols} FROM (
							noble.gpa_cumulative as partnerdata
							LEFT JOIN common.studentid as a
								ON partnerdata.noble_student_sf_id = a.noble_sf_id
							LEFT JOIN common.schoolid as b
								ON partnerdata.noble_sf_school_id = b.noble_sf_school_id
						)
						UNION
						SELECT a.studentid, b.schoolid, {partnerdatacols} FROM (
							kipp_nj.gpa_cumulative as partnerdata
							LEFT JOIN common.studentid as a
								ON partnerdata.kipp_nj_sf_id = a.kipp_nj_sf_id
							LEFT JOIN common.schoolid as b
								ON partnerdata.kipp_nj_sf_school_id = b.kipp_nj_sf_school_id
						)
					);
				''',
				overwrite=True
	)

	# GPA - by year
	CombineTable(sqltable_name = 'common.gpa_by_year',
	 			sqltable_cols = SQLtables_cols.gpa_by_year,
				cols_toindex = ['studentid','schoolid'],
				sqlcmd_populate = '''
					INSERT INTO {commontable} (
						SELECT a.studentid, b.schoolid, {partnerdatacols} FROM (
							noble.gpa_by_year as partnerdata
							LEFT JOIN common.studentid as a
								ON partnerdata.cps_id = a.cps_id
							LEFT JOIN common.schoolid as b
								ON partnerdata.noble_powerschool_school_id = b.noble_powerschool_id
						)
						UNION
						SELECT a.studentid, b.schoolid, {partnerdatacols} FROM (
							kipp_nj.gpa_by_year as partnerdata
							LEFT JOIN common.studentid as a
								ON partnerdata.kipp_nj_sf_id = a.kipp_nj_sf_id
							LEFT JOIN common.schoolid as b
								ON partnerdata.kipp_nj_sf_school_id = b.kipp_nj_sf_school_id
						)
					);
				''',
				overwrite=True
	)

	# Attendance
	CombineTable(sqltable_name = 'common.attendance',
	 			sqltable_cols = SQLtables_cols.attendance,
				cols_toindex = ['studentid','schoolid'],
				sqlcmd_populate = '''
					INSERT INTO {commontable} (studentid, schoolid, {datacolnames}) (
						SELECT a.studentid, b.schoolid, {partnerdatacols} FROM (
							noble.attendance as partnerdata
							LEFT JOIN common.studentid as a
								ON partnerdata.student_cps_id = a.cps_id
							LEFT JOIN common.schoolid as b
								ON partnerdata.school_noble_powerschoolid = b.noble_powerschool_id
						)
						UNION
						SELECT a.studentid, Null as schoolid, {partnerdatacols} FROM (
							kipp_nj.attendance as partnerdata
							LEFT JOIN common.studentid as a
								ON partnerdata.kipp_nj_sf_id = a.kipp_nj_sf_id
							-- TODO: join schoolids, once KIPP NJ sends us middle school enrollment data
						)
					);
				''',
				overwrite=True
	)

	# HS enrollment
	CombineTable(sqltable_name = 'common.hs_enrollment',
	 			sqltable_cols = SQLtables_cols.hs_enrollment,
				cols_toindex = ['studentid','schoolid'],
				sqlcmd_populate = '''
					INSERT INTO {commontable} (studentid, schoolid, {datacolnames}) (
						SELECT a.studentid, b.schoolid, {partnerdatacols} FROM (
							noble.hs_enrollment as partnerdata
							LEFT JOIN common.studentid as a
								ON partnerdata.noble_sf_id = a.noble_sf_id
							LEFT JOIN common.schoolid as b
								ON partnerdata.noble_sf_school_id = b.noble_sf_school_id
						)
						UNION
						SELECT a.studentid, b.schoolid, {partnerdatacols} FROM (
							kipp_nj.hs_enrollment as partnerdata
							LEFT JOIN common.studentid as a
								ON partnerdata.kipp_nj_sf_id = a.kipp_nj_sf_id
							LEFT JOIN common.schoolid as b
								ON partnerdata.kipp_nj_sf_school_id = b.kipp_nj_sf_school_id
						)
					);
				''',
				overwrite=True
	)

	# Enrollment dummies
	UploadTable(rawcsv_dict = {},
				rawtoclean_fn = gen_persistence_dummies.gen_persistence_dummies_df,
				sqltable_name = 'common.enrollment_dummies',
	            sqltable_cols = SQLtables_cols.enrollment_dummies,
				cols_toindex = ['studentid','collegeid'],
	            overwrite=True
	)

	# Colleges
	CombineTable(sqltable_name = 'common.colleges',
	 			sqltable_cols = SQLtables_cols.colleges,
				sqlcmd_populate = '''
					INSERT INTO {commontable} (
						SELECT a.collegeid, {partnerdatacols} FROM (
							noble.colleges as partnerdata
							LEFT JOIN common.collegeid as a
								ON partnerdata.noble_sf_college_id = a.noble_sf_college_id
						)
					);
				''',
				overwrite=True
	)

	# Schools
	CombineTable(sqltable_name = 'common.schools',
	 			sqltable_cols = SQLtables_cols.schools,
				sqlcmd_populate = '''
					INSERT INTO {commontable} (
					    SELECT a.schoolid, {partnerdatacols} FROM (
					    	noble.schools as partnerdata
					    	LEFT JOIN common.schoolid as a
					    		ON partnerdata.noble_sf_school_id = a.noble_sf_school_id
					    )
					    UNION
					    SELECT a.schoolid, {partnerdatacols} FROM (
					    	kipp_nj.schools as partnerdata
					    	LEFT JOIN common.schoolid as a
					    		ON partnerdata.kipp_nj_sf_school_id = a.kipp_nj_sf_school_id
					    )
					);
				''',
				overwrite=True
	)

	# Contacts
	# Note: the use of INNER JOINs is because both Noble and KIPP NJ have student sf IDs in contacts that aren't in studentid/students
	CombineTable(sqltable_name = 'common.contacts',
	 			sqltable_cols = SQLtables_cols.contacts,
				sqlcmd_populate = '''
					INSERT INTO {commontable} (
					    SELECT a.studentid, {partnerdatacols} FROM (
					    	noble.contacts as partnerdata
					    	INNER JOIN common.studentid as a
					    		ON partnerdata.noble_sf_id = a.noble_sf_id
					    )
					    UNION
					    SELECT a.studentid, {partnerdatacols} FROM (
					    	kipp_nj.contacts as partnerdata
					    	INNER JOIN common.studentid as a
					    		ON partnerdata.kipp_nj_sf_id = a.kipp_nj_sf_id
					    )
					);
				''',
				overwrite=True
	)

	# Courses
	CombineTable(sqltable_name = 'common.courses',
	 			sqltable_cols = SQLtables_cols.courses,
				cols_toindex = ['studentid','schoolid'],
				sqlcmd_populate = '''
					INSERT INTO {commontable} (
						SELECT a.studentid, b.schoolid, {partnerdatacols} FROM (
							noble.courses as partnerdata
							LEFT JOIN common.studentid as a
								ON partnerdata.cps_id = a.cps_id
							LEFT JOIN common.schoolid as b
								ON partnerdata.noble_powerschool_school_id = b.noble_powerschool_id
						)
						UNION
						SELECT a.studentid, b.schoolid, {partnerdatacols} FROM (
							kipp_nj.courses as partnerdata
							LEFT JOIN common.studentid as a
								ON partnerdata.kipp_nj_sf_id = a.kipp_nj_sf_id
							LEFT JOIN common.schoolid as b
								ON partnerdata.kipp_nj_sf_school_id = b.kipp_nj_sf_school_id
						)
					);
				''',
				overwrite=True
	)

if __name__ == '__main__':
	main()