''' This file defines the columns for each table in the Postgres database.

We store our data in multiple schemas.  We create one schema for each partner,
(ex: 'noble' and 'kipp_nj'), where each table contains the data from that
individual partner.  We also create a 'common' schema where we combine the data
from all partners.

Each table has a standardized set of columns:

- Every table in a partner data schema contain the columns defined in 'partnerids' and
  'data' of that table's dictionary below.

  This means that corresponding tables in partner data schemas (ex: noble.students and
  kipp_nj.students) contain identical columns.

- Every table in the 'common' schema contains the set of columns defined in 'commonids'
  and 'data' of that table's dictionary below.
'''

template = {
	'commonids': '''
		''',
	'partnerids': '''
		''',
	'data': '''
		'''
}

studentid = {
	'commonids': '''
		studentid SERIAL PRIMARY KEY,''',
	'partnerids': '',
	'data': '''
		noble_sf_id VARCHAR(18) UNIQUE,
			CONSTRAINT check_length_id CHECK (length(noble_sf_id) = 18),
		kipp_nj_sf_id VARCHAR(18) UNIQUE,
			CONSTRAINT check_length CHECK (length(kipp_nj_sf_id) = 18),
		kipp_nj_powerschool_id VARCHAR UNIQUE,			
		cps_id VARCHAR(18) UNIQUE,
			CONSTRAINT check_cps_id CHECK (length(cps_id) = 8)'''
}

schoolid = {
	'commonids': '''
		schoolid SERIAL PRIMARY KEY,''',
	'partnerids': '''
		''',
	'data': '''
		kipp_nj_sf_school_id VARCHAR(18) UNIQUE CHECK (length(kipp_nj_sf_school_id) = 18),
		noble_sf_school_id VARCHAR(18) UNIQUE CHECK (length(kipp_nj_sf_school_id) = 18),
		noble_powerschool_id VARCHAR(8) UNIQUE'''		
}

collegeid = {
	'commonids': '''
		collegeid SERIAL PRIMARY KEY,''',
	'partnerids': '''
		''',
	'data': '''
		"noble_sf_college_id" CHAR(18) UNIQUE NOT NULL CHECK(length(noble_sf_college_id) = 18),
		"ipedsid" INTEGER UNIQUE CHECK(ipedsid>=0 and ipedsid<=999999)'''
}

acttests = {
	'commonids': '''
		studentid INTEGER NOT NULL REFERENCES common.studentid(studentid),''',
	'partnerids': '''
	    "kipp_nj_sf_id" VARCHAR(18) CHECK (length(kipp_nj_sf_id) = 18),
	    "cps_id" CHAR(8) CHECK(length(cps_id) = 8),''',
	'data': '''
	    "date" DATE CHECK(date <= now()::date),
	    "test_level" VARCHAR,
	    	CONSTRAINT valid_level CHECK (test_level IN ('PLAN', 'EXPLORE', 'ACT')), 
	    "test_semester" VARCHAR,
	    	CONSTRAINT valid_semester CHECK (test_semester in ('Fall', 'Spring')),
	    "score_composite" INTEGER CHECK(score_composite>=1 and score_composite<=36 and score_composite=round((score_english+score_math+score_reading+score_science+0.0001)/4)),
	    "score_english" INTEGER CHECK(score_english>=1 and score_english<=36),
	    "score_math" INTEGER CHECK(score_math>=1 and score_math<=36),
	    "score_reading" INTEGER CHECK(score_reading>=1 and score_reading<=36),
	    "score_science" INTEGER CHECK(score_science>=1 and score_science<=36),
	    "score_writing" INTEGER CHECK(score_writing>=1 and score_writing<=36),
	    "onlyhighestavailable" BOOLEAN NOT NULL'''
}

aptests = {
	'commonids': '''
		studentid INTEGER NOT NULL REFERENCES common.studentid(studentid),''',
	'partnerids': '''
		"noble_powerschool_id" CHAR(18) CHECK(length(noble_powerschool_id) = 8),''',
	'data': '''
		"date" DATE CHECK(date <= now()::date),
		"score" INTEGER CHECK(score>=1 and score<=5),
		"subject" VARCHAR(37) REFERENCES lookup.apsubjects(name)'''		
}

attendance = {
	'commonids': '''
		studentid INTEGER REFERENCES common.studentid(studentid),
		schoolid INTEGER REFERENCES common.schoolid(schoolid),''',
	'partnerids': '''
		student_cps_id CHAR(8) CHECK(length(student_cps_id) = 8),
		school_noble_powerschoolid VARCHAR,
		kipp_nj_sf_id VARCHAR(18) CHECK(length(kipp_nj_sf_id) = 18),
		kipp_nj_sf_school_id VARCHAR(18) CHECK(length(kipp_nj_sf_school_id) = 18),''',
	'data': '''
		attendance_date DATE CHECK (attendance_date > DATE('1900-01-01') and attendance_date <= now()::date), 
		school_year INTEGER CHECK (school_year > 1950 and school_year < extract(year from now()) + 1),
		attendance_type VARCHAR(15),
			CONSTRAINT "valid_type" CHECK (attendance_type IN ('excused','unexcused','suspended','tardy','early_dismissal'))'''		
}

students = {
	'commonids': '''
		studentid INTEGER UNIQUE NOT NULL REFERENCES common.studentid(studentid),''',
	'partnerids': '''
		kipp_nj_sf_id CHAR(18) CHECK (length(kipp_nj_sf_id) = 18),
		noble_sf_id CHAR(18) CHECK (length(noble_sf_id) = 18),
		cps_id CHAR(8) CHECK (length(cps_id) = 8),	
		kipp_nj_powerschool_id VARCHAR(20),''',
	'data': '''
		network VARCHAR,
		date_of_birth DATE CHECK (date_of_birth > DATE('1970-01-01')),
		ethnicity VARCHAR(25),
		--constrain the categories of ethnicity to the standard set of nine
			CONSTRAINT valid_ethnicity CHECK (ethnicity IN ('Hispanic', 'African American', 'Caucasian', 'Multicultural', 'Asian', 'American Indian', 'Pacific Islander', 'Unknown', 'Other')), 
		is_female BOOLEAN,
		ever_special_ed BOOLEAN,
		ever_free_lunch BOOLEAN,
		family_income_bracket VARCHAR(15),
		number_in_household INTEGER,
		is_first_gen BOOLEAN,
		street VARCHAR,
		zip VARCHAR(5),
		-- force zip to be five digits long
			CONSTRAINT check_length_zip CHECK (length(zip) = 5),
		fafsa_efc FLOAT'''
}

enrollments = {
	'commonids': '''
		enrollid SERIAL PRIMARY KEY,
		studentid INTEGER REFERENCES common.studentid(studentid),
		collegeid INTEGER REFERENCES common.collegeid(collegeid),''',
	'partnerids': '''
		noble_student_sf_id CHAR(18) CHECK (length(noble_student_sf_id) = 18),
		kipp_nj_sf_id CHAR(18) CHECK (length(kipp_nj_sf_id) = 18),
		"ipedsid" INTEGER CHECK(ipedsid>=0 and ipedsid<=999999),
		noble_college_sf_id CHAR(18),
			CONSTRAINT check_id CHECK (length(noble_college_sf_id) = 18),''',
	'data': '''
		"start_date" DATE,
		"end_date" DATE,
		"date_last_verified" DATE,
		"status" VARCHAR(20) CHECK(status IN ('Transferred out', 'Matriculating', 'Attending', 'Did not matriculate', 'Withdrew', 'Graduated')),
		"data_source" VARCHAR,
		"living_on_campus" BOOLEAN,
		"degree_type" VARCHAR CHECK(degree_type IN ('Associates','Associates or Certificate (TBD)', 'Masters', 'Bachelors', 'Certificate', 'Trade/Vocational', 'Employment')),
		"degree_subject" VARCHAR REFERENCES lookup.collegedegrees (name),
		"major" VARCHAR REFERENCES lookup.majors (name),
		"withdrawal_reason_financial" BOOLEAN,
		"withdrawal_reason_academic" BOOLEAN,
		"withdrawal_reason_motivational" BOOLEAN,
		"withdrawal_reason_family" BOOLEAN,
		"withdrawal_reason_health" BOOLEAN,
		"withdrawal_reason_social" BOOLEAN,
		"withdrawal_reason_racial" BOOLEAN'''
}

enrollment_dummies = {
	'commonids': '''
		''',
	'partnerids': '''
		''',
	'data': '''
        "enrollid" INTEGER UNIQUE REFERENCES common.enrollments(enrollid),
        "studentid" INTEGER REFERENCES common.studentid(studentid),
        "collegeid" INTEGER REFERENCES common.collegeid(collegeid),
        "persist_1_halfyear" BOOLEAN,
        "persist_2_halfyear" BOOLEAN,
        "persist_3_halfyear" BOOLEAN,
        "persist_4_halfyear" BOOLEAN,
        "persist_5_halfyear" BOOLEAN,
        "persist_6_halfyear" BOOLEAN,
        "persist_7_halfyear" BOOLEAN,
        "persist_8_halfyear" BOOLEAN'''		
}

contacts = {
	'commonids': '''
		studentid INTEGER NOT NULL REFERENCES common.studentid(studentid),''',
	'partnerids': '''
		kipp_nj_sf_id CHAR(18) CHECK (length(kipp_nj_sf_id) = 18),
		noble_sf_id CHAR(18) CHECK (length(noble_sf_id) = 18),''',
	'data': '''
		contact_date DATE CHECK (contact_date > DATE('1900-01-01') and contact_date <= now()::date), 
		counselor_id VARCHAR, 
		contact_medium VARCHAR,
			CONSTRAINT "valid_medium" CHECK (
			  	contact_medium IN ('Call', 'Email', 'In Person', 'Text', 'Social Networking', 'School Visit', 'Mail', 'IM', 'Parent Contact', 'College Contact')),
		initiated_by_student BOOLEAN, 
		was_outreach BOOLEAN, 
		was_successful BOOLEAN'''		
}

schools = {
	'commonids': '''
		schoolid INTEGER UNIQUE NOT NULL REFERENCES common.schoolid(schoolid),''',
	'partnerids': '''
	   "noble_sf_school_id" CHAR(18) CHECK (length(noble_sf_school_id) = 18),
	   "kipp_nj_sf_school_id" CHAR(18) CHECK (length(kipp_nj_sf_school_id) = 18),''',
	'data': '''
	  	"name" VARCHAR(120) NOT NULL,
	   	"level" VARCHAR(15) NOT NULL,
	   		CONSTRAINT "valid_level" CHECK (
				"level" IN ('high school', 'middle school', '6 through 12', 'elementary')),
	  	"kind" VARCHAR(10),
			CONSTRAINT "valid_kind" CHECK (
				"kind" IN ('charter', 'magnet', 'private', 'public', 'contract', 'military', 'selective', 'special', 'boarding')),
	   	"parent_organization" VARCHAR(20),
	   	"no_excuses" BOOLEAN,
	   	"zip" INTEGER CHECK ("zip" >= 00501 AND "zip" <= 99950),
	   	"year_of_first_graduating_class" DATE CHECK("year_of_first_graduating_class" > DATE('1000-01-01')),
	   	"dropout_rate" DECIMAL(5,2) CHECK("dropout_rate" >= 0.00 AND "dropout_rate" <= 100.00 ),
	   	"number_of_students" INTEGER CHECK("number_of_students" >= 0),
	   	"avg_class_size" DECIMAL(6,2) CHECK("avg_class_size" >= 0.00),
	   	"number_of_staff" INTEGER CHECK("number_of_staff" >= 0),
	   	"number_of_counselors" INTEGER CHECK("number_of_counselors" >= 0),
	   	"annual_budget" DECIMAL(11,2)'''		
}

courses = {
	'commonids': '''
		studentid INTEGER REFERENCES common.studentid(studentid),
		schoolid INTEGER REFERENCES common.schoolid(schoolid),''',
	'partnerids': '''
		cps_id VARCHAR(8), 
		noble_powerschool_school_id VARCHAR(18),
		kipp_nj_sf_id VARCHAR(18) CHECK(length(kipp_nj_sf_id) = 18),
		kipp_nj_sf_school_id VARCHAR(18) CHECK(length(kipp_nj_sf_school_id) = 18),''',
	'data': '''
		course_name VARCHAR,
		course_number VARCHAR,
		teacher_id VARCHAR, 
		year_taken INTEGER CHECK (year_taken > 1000 and year_taken < extract(year from now())),
		semester_taken INTEGER CHECK (semester_taken >= 1 and semester_taken <=3), 
		grade_available DATE CHECK (grade_available > DATE('1900-01-01') and grade_available < (now() + INTERVAL '6 months')::date), 
		course_length VARCHAR,
			CONSTRAINT valid_length CHECK (course_length IN ('year', 'quarter', 'semester', 'summer')),
		course_level INTEGER, 
		--subject_type VARCHAR REFERENCES lookup.subjecttypes (name),
		credit_hours NUMERIC,
		was_honors BOOLEAN, 
		was_ap BOOLEAN, 
		percent_grade NUMERIC CHECK (percent_grade >= 0.0 and percent_grade <= 120.0), 
		letter_grade VARCHAR CHECK (letter_grade ~* '^[PAaBbCcDdFfIiWw][+-]?$')'''		
}

gpa_by_year = {
	'commonids': '''
		studentid INTEGER REFERENCES common.studentid(studentid),
		schoolid INTEGER REFERENCES common.schoolid(schoolid),''',
	'partnerids': '''
		cps_id VARCHAR(8) CHECK(length(cps_id) = 8),
		noble_powerschool_school_id VARCHAR(4) CHECK(length(noble_powerschool_school_id)=4),
		kipp_nj_sf_id VARCHAR CHECK(length(kipp_nj_sf_id) = 18),
		kipp_nj_sf_school_id VARCHAR CHECK(length(kipp_nj_sf_school_id) = 18),''',
	'data': '''
		school_year INT, 
		weighted_gpa NUMERIC CHECK(weighted_gpa >= 0.0), 
		unweighted_gpa NUMERIC CHECK(unweighted_gpa >= 0.0)'''
}

gpa_cumulative = {
	'commonids': '''
		studentid INTEGER UNIQUE REFERENCES common.studentid(studentid),
		schoolid INTEGER REFERENCES common.schoolid(schoolid),''',
	'partnerids': '''
		noble_student_sf_id VARCHAR CHECK(length(noble_student_sf_id) = 18),
		noble_sf_school_id VARCHAR(18) CHECK(length(noble_sf_school_id)=18),
		kipp_nj_sf_id VARCHAR CHECK(length(kipp_nj_sf_id) = 18),
		kipp_nj_sf_school_id VARCHAR CHECK(length(kipp_nj_sf_school_id) = 18),''',
	'data': '''
		cumulative_weighted_gpa NUMERIC CHECK(cumulative_weighted_gpa >= 0.0), 
		cumulative_unweighted_gpa NUMERIC CHECK(cumulative_unweighted_gpa >= 0.0)'''
	
}

colleges = {
	'commonids': '''
		collegeid INTEGER UNIQUE NOT NULL REFERENCES common.collegeid(collegeid),''',
	'partnerids': '''
		"noble_sf_college_id" CHAR(18) CHECK (length(noble_sf_college_id) = 18),
		"ipedid" CHAR(6) CHECK(length(ipedid)=6),''',
	'data': '''
		"isprivate" BOOLEAN,
	    "isforprofit" BOOLEAN,
		"is4year" BOOLEAN,
	   	"zip" INTEGER CHECK ("zip" >= 00501 AND "zip" <= 99950),
		"name" VARCHAR(70),
		"isrural"	BOOLEAN,
		"allmale" BOOLEAN,
		"allfemale" BOOLEAN,
		"graduationrate_6yr" FLOAT CHECK(graduationrate_6yr>=0 and graduationrate_6yr<=100),
		"graduationrate_minority_6yr" FLOAT CHECK(graduationrate_minority_6yr>=0 and graduationrate_minority_6yr<=100),
		"transferrate_6yr" FLOAT CHECK(transferrate_6yr>=0 and transferrate_6yr<=100),
		"transferrate_minority_6yr" FLOAT CHECK(transferrate_minority_6yr>=0 and transferrate_minority_6yr<=100),
		"historicallyblack" BOOLEAN,
		"state" VARCHAR(2), 
		"longitude" NUMERIC, 
		"latitude" NUMERIC, 
		"dist_from_chicago" NUMERIC, 
		"barrons_rating" VARCHAR(30),
		"perc_accepted" FLOAT, 
		"perc_accepted_enroll" FLOAT,
		"perc_male" FLOAT, 
		"perc_female" FLOAT,
		"perc_african_american" FLOAT, 
		"perc_hispanic" FLOAT,
		"percentinstate" FLOAT, 
		"percentoutofstate" FLOAT, 
		"percentpellgrant" FLOAT, 
		"avgnetprice" FLOAT, 
		"netprice0_30" FLOAT, 
		"netprice30_48" FLOAT, 
		"netprice48_75" FLOAT, 
		"locale" VARCHAR, 
		"size_range" VARCHAR
		'''		
}

hs_enrollment = {
	'commonids': '''
		studentid INTEGER REFERENCES common.studentid(studentid),
		schoolid INTEGER REFERENCES common.schoolid(schoolid),''',
	'partnerids': '''
		noble_sf_id CHAR(18) CHECK (length(noble_sf_id) = 18),
		noble_sf_school_id CHAR(18) CHECK (length(noble_sf_school_id) = 18),
		kipp_nj_sf_id CHAR(18) CHECK (length(kipp_nj_sf_id) = 18),
		kipp_nj_sf_school_id CHAR(18) CHECK (length(kipp_nj_sf_school_id) = 18),''',
	'data': '''
		start_date DATE CHECK (start_date > DATE('1900-01-01') and start_date < now()::date), 
		end_date DATE CHECK (start_date > DATE('1900-01-01')),
		exit_type VARCHAR,
			CONSTRAINT valid_exit CHECK (exit_type IN ( 'Graduated', 'Attending', 'Transferred out', 'Withdrew', 'Matriculating', 'Deferred', 'Other')), 
		high_school_class INTEGER,
			CONSTRAINT valid_class CHECK (high_school_class > 1950 AND high_school_class < extract(year from now()) + 10)'''		
}

discipline = {
	'commonids': '''
	   "studentid" INTEGER REFERENCES common.studentid(studentid) NOT NULL,
	   "schoolid" INTEGER REFERENCES common.schoolid(schoolid) NOT NULL,''',
	'partnerids': '''
		''',
	'data': '''
	   "date" DATE,
	   		CONSTRAINT valid_date CHECK(
	   			"date" >= DATE('1980-01-01') AND "date" < CURRENT_DATE+1),
		"type" VARCHAR REFERENCES lookup.disciplinetypes (name),
		"severity" INTEGER,
			CONSTRAINT valid_severity CHECK(
				"severity" >= 0 AND "severity" <= 5),
		"consequence_severity" INTEGER,
			CONSTRAINT valid_consequence_severity CHECK(
				"consequence_severity" >= 0 AND "consequence_severity" <= 5)
		'''
}

maptests = {
	'commonids': '''
		''',
	'partnerids': '''
		''',
	'data': '''
		"academicyear" INTEGER CHECK(academicyear <= extract(year from now())),
		"season" VARCHAR(6) CHECK (season IN ( 'FALL', 'WINTER', 'SPRING' ),
		"subject" VARCHAR(27) CHECK (subject IN ('Reading','Mathematics','Language','Science General','Science Concepts and Processes')),
	    "score" INTEGER,
	    "nationalpercentileranking" INTEGER CHECK(nationalpercentileranking>0 and nationalpercentileranking<=100)'''
}

sattests = {
	'commonids': '''
		''',
	'partnerids': '''
		''',
	'data': '''
		"date" DATE CHECK(date <= now()::date),
		"score_reading" INTEGER CHECK(score_reading>=200 and score_reading<=800 and score_reading%10=0),
		"score_math" INTEGER CHECK(score_math>=200 and score_math<=800 and score_math%10=0),
		"score_writing" INTEGER CHECK(score_writing>=200 and score_writing<=800 and score_writing%10=0),
		"score_total" INTEGER CHECK(score_total = score_reading + score_math + score_writing),
	    "onlyhighestavailable" BOOLEAN NOT NULL'''
}

applications = {
	'commonids': '''
		studentid INTEGER NOT NULL REFERENCES common.studentid(studentid) ON DELETE RESTRICT,
		collegeid INTEGER NOT NULL REFERENCES common.collegeid(collegeid) ON DELETE RESTRICT,''',
	'partnerids': '''
		''',
	'data': '''
		date_of_initial_interest DATE CHECK (date_of_initial_interest > DATE('1900-01-01') and date_of_initial_interest <= now()::date), 
		was_early_decision BOOLEAN, 
		was_accepted BOOLEAN, --this will blur over things like accepted, matriculated, deferred, etc
		application_status VARCHAR, 
			CONSTRAINT "valid_status" CHECK (application_status IN ('Accepted', 'Denied', 'Submitted', 'Matriculated', 'Withdrew Application', 'In progress', 'Waitlist', 'Unknown', 'Deferred'))'''
}