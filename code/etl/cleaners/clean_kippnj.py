''' Contains all the functions to clean KIPP NJ data.

Inputs:
Each cleaning function takes a dataframe (or many dataframes) as inputs.
These input dataframes are "raw", loaded directly from CSVs provided by partners.

Output:
Each cleaning function returns a "cleaned" dataframe, which is in the correct
format to be uploaded directly into the database.
'''

import pandas as pd
from util.ETL_helpers import map_value_from_dict, ethnicity_fixed_mapping, medium_fixed_mapping, create_conversion_dict, convert_free_text
import numpy as np
from util.integer_withNA_tostr import int_with_NaN_tostr
import config

###################################################################
# HELPER FUNCTIONS
# small functions called by the larger cleaners
# note: some more general helper functions are in util/ETL_helpers
###################################################################

def fix_zip(zip):
	'''Deal with missing zeros on zip codes'''
	if len(zip) < 5:
	    return '0' + zip
	else: return zip

def valid_zip(zip):
	'''Deal with invalid zips by setting them to null'''
	if zip == 'nan':
		return np.nan
	elif int(zip) < 00501 or int(zip) > 99950:
		return np.nan
	else: return zip

def code_degree_subject(degree, degree_subject):
	'''To match the mapping we've set up, need to add the degree type in uppercase
	to the beginning of the degree subject'''
	degree_subject_mapping = {'Education':'EDUCATION',
	'Engineering': 'ENGINEERING',
	'Life Sciences': 'SCIENCE',
	'Social Sciences': 'ARTS', 
	'Computer Sciences' : 'SCIENCE', 
	'Mathematics': 'SCIENCE',
	'Fine Arts': 'FINE ARTS'
	}

	if degree == 'Certificate':
		return 'CERTIFICATE'
	else:
		if degree_subject in degree_subject_mapping:
			return degree[:-1].upper() + ' ' + degree_subject_mapping[degree_subject]	
		else: return np.nan

def extract_grad_year(status, end_date):
	'Get the year of graduation for students who have graduated'
	if status == 'Graduated':
		return end_date.year - 1 # to account for the fact that years are counted from the fall, not the spring
	else: return np.nan


def map_months_to_semester(date):
	if date.month in [2,3,4,5,6]:
		return 'Spring'
	elif date.month in [9,10,11,12]:
		return 'Fall'
	else: return np.nan

def map_school_types(school_type):
	if 'Charter' in school_type or 'KIPP' in school_type:
		return 'charter'
	elif 'Magnet' in school_type:
		return 'magnet'
	elif 'Private' in school_type:
		return 'private'
	elif 'Public' in school_type:
		return 'public'
	elif 'Boarding' in school_type:
		return 'boarding'
	else: return np.nan

def code_school_level(level):
	if level == 'Elementary School':
		return 'elementary'
	else: return level.lower()	

def map_attendance_types(att_type):
	if 'Tardy' in att_type:
		return 'tardy'
	elif 'Suspension' in att_type:
		return 'suspended'
	elif 'Documented' in att_type or 'Excused' in att_type:
		return 'excused'
	elif 'Undocumented' in att_type or 'Absent' in att_type:
		return 'unexcused'
	elif 'Early' in att_type:
		return 'early_dismissal'
	else: return np.nan	

###################################################################
# MAPPING DICTIONARIES
# map from partner values to clean values
###################################################################  

status_fixed_mapping = {'Matriculated': 'Matriculating', 
	'Withdrawn': 'Withdrew',
	'Did Not Enroll': 'Did not matriculate', 
	'Deferred': 'Did not matriculate'
	}

degree_fixed_mapping = {"Bachelor's (4-year)" : "Bachelors",
	"Associate's (2 year)": "Associates", 
	"Master's": 'Masters'} 

###################################################################
# CLEANER FUNCTIONS
# cleaners for every table in the database
###################################################################        

def students_table(kipp_nj_students):	

	'''Cleans KIPP NJ student data to match our database schema'''

	# we don't load names into our DB
	clean_students = kipp_nj_students.drop('name', axis =1)
	#rename columns correctly
	clean_students.rename(columns={'contact_id':'kipp_nj_sf_id', 'powerschool_id':'kipp_nj_powerschool_id'}, inplace=True)
	#make DOB a date
	clean_students.date_of_birth = pd.to_datetime(clean_students.date_of_birth)
	#remap ethnicities
	clean_students.ethnicity = clean_students.ethnicity.apply(lambda x: map_value_from_dict(ethnicity_fixed_mapping, x))
	# name of partner organization
	clean_students['network'] = 'KIPP_NJ'
	#get rid of commas in income values (confuses psycopg2)
	clean_students.family_income_bracket = clean_students.family_income_bracket.str.replace(',','')
	# change number in household to int
	clean_students.number_in_household = clean_students.dropna().number_in_household.astype(int)
	clean_students.number_in_household = clean_students.number_in_household.apply(int_with_NaN_tostr)
	# deal with missing 0 on zip codes
	clean_students.zip = clean_students.zip.dropna().astype(int).astype(str).apply(fix_zip)
	#drop the student with duplicated salesforce ids
	clean_students = clean_students.drop_duplicates(subset = ['kipp_nj_powerschool_id'])
	#reorder columns
	clean_students = clean_students[['kipp_nj_sf_id', 'cps_id', 'kipp_nj_powerschool_id', 'network', 'date_of_birth', 'ethnicity', 'is_female', 'ever_special_ed', 'ever_free_lunch', 'family_income_bracket', 'number_in_household', 'is_first_gen', 'zip', 'fafsa_efc']]
	#return clean df
	return clean_students

def student_id_table(kipp_nj_students):
	'''Add student ids to the id table'''	
	# initialize empty dataframe
	student_ids = pd.DataFrame()


	# load necessary variables
	student_ids['kipp_nj_sf_id'] = kipp_nj_students['contact_id'] #salesforce id
	student_ids['kipp_nj_powerschool_id'] = kipp_nj_students['powerschool_id']

	#drop the student with duplicated salesforce ids
	student_ids = student_ids.drop_duplicates(subset = ['kipp_nj_powerschool_id'])

	# return clean dataframe
	return student_ids

def school_id_table(kipp_nj_schools):
	# initialize empty dataframe
	school_ids = pd.DataFrame()

	# load necessary variables
	school_ids['kipp_nj_sf_school_id'] = kipp_nj_schools['school_salesforce_id'] #salesforce id

	# return clean dataframe
	return school_ids

def enrollments_table(kipp_nj_enrollments):

	'''Cleans KIPP NJ enrollment data to match our database schema'''

	# remove the did not enroll, other, and deferred enrollment types
	clean_enrollments = kipp_nj_enrollments[~kipp_nj_enrollments.status.isin(['Other', 'Did Not Enroll', 'Deferred'])]
	# rename Withdrawn to withdrew, matriculated to matriculating

	clean_enrollments.status = clean_enrollments.status.apply(lambda x: map_value_from_dict(status_fixed_mapping, x))
	# made dates into dates
	clean_enrollments.start_date = pd.to_datetime(clean_enrollments.start_date)
	clean_enrollments.end_date = pd.to_datetime(clean_enrollments.end_date)
	clean_enrollments.date_last_verified = pd.to_datetime(clean_enrollments.date_last_verified)
	# deal with degree type
	#remove high school diploma and GED, only interested in college enrollments
	clean_enrollments = clean_enrollments[~clean_enrollments.degree_type.isin(['High School Diploma', 'GED'])]
	clean_enrollments.degree_type = clean_enrollments.degree_type.apply(lambda x: map_value_from_dict(degree_fixed_mapping, x))
	# clean up degree subject
	clean_enrollments.degree_subject= clean_enrollments.degree_type.combine(clean_enrollments.degree_subject, func = code_degree_subject)
	# clean up major
	major_dict = create_conversion_dict(config.PERSISTENCE_PATH + '/code/etl/mappers/majortranslation.csv')
	# do conversions from freetext to coded options
	clean_enrollments['major'] = clean_enrollments.major.fillna('missing').apply(lambda x: convert_free_text(major_dict, x))
	#map transfer reasons to the reasons we keep track of
	withdrawal_reasons = pd.get_dummies(clean_enrollments.transfer_reason__c).astype(bool)
	withdrawal_reasons.columns = ['withdrawal_reason_academic', 'withdrawal_reason_career', 'withdrawal_reason_financial', 'withdrawal_reason_other', 'withdrawal_reason_placement', 'withdrawal_reason_relocation', 'withdrawal_reason_social']
	withdrawal_reasons.drop(['withdrawal_reason_relocation', 'withdrawal_reason_placement', 'withdrawal_reason_other', 'withdrawal_reason_career'], axis = 1,inplace = True)
	# join the columns back into the original 
	clean_enrollments = clean_enrollments.join(withdrawal_reasons)
	clean_enrollments['withdrawal_reason_motivational'] = np.nan
	clean_enrollments['withdrawal_reason_family'] = np.nan 
	clean_enrollments['withdrawal_reason_health'] = np.nan
	clean_enrollments['withdrawal_reason_racial'] = np.nan

	# Drop invalid IPEDS id
	clean_enrollments['college_ncesid'] = clean_enrollments['college_ncesid'].convert_objects(convert_numeric=True)
	clean_enrollments.loc[clean_enrollments['college_ncesid'] > 999999, 'college_ncesid'] = np.nan
	clean_enrollments['college_ncesid'] = clean_enrollments['college_ncesid'].apply(int_with_NaN_tostr)

	clean_enrollments.drop(['transfer_reason__c', 'college_salesforce_id'],axis = 1,inplace = True)
	clean_enrollments.rename(columns={'student_salesforce_id':'kipp_nj_sf_id', 'college_ncesid': 'ipedsid'}, inplace=True)	
	return clean_enrollments

def contacts_table(kipp_nj_contacts):
	'''Cleans KIPP NJ enrollment data to match our database schema'''
	# correct the id column
	kipp_nj_contacts.rename(columns={'student_salesforce_id':'kipp_nj_sf_id'}, inplace=True)
	# map the unix timestamps to dates
	kipp_nj_contacts.contact_date = pd.to_datetime(kipp_nj_contacts.contact_date, unit = 's')
	#remove time
	kipp_nj_contacts.contact_date = kipp_nj_contacts.contact_date.apply(lambda x: x.date())
	#map the contact mediums
	kipp_nj_contacts.contact_medium = kipp_nj_contacts.contact_medium.apply(lambda x: map_value_from_dict(medium_fixed_mapping, x))
	# kipp nj doesn't keep track of mass email outreach, so was_outreach is always false
	kipp_nj_contacts.was_outreach = False 
	# was_successful is coded as true or nan, replace nans with False
	kipp_nj_contacts.was_successful = kipp_nj_contacts.was_successful.fillna(False)
	# reorder columns
	clean_contacts = kipp_nj_contacts[['kipp_nj_sf_id', 'contact_date', 'counselor_id', 'contact_medium', 'initiated_by_student', 'was_outreach', 'was_successful']]
	return clean_contacts

def act_table(kipp_nj_act):

	'''Cleans KIPP NJ act data to match our database schema'''
	kipp_nj_act.date = pd.to_datetime(kipp_nj_act.date)
	#map dates to semesters
	kipp_nj_act['test_semester'] = kipp_nj_act.date.apply(map_months_to_semester)
	# multiple ACT tests per student, so only_highest_available = False
	kipp_nj_act['onlyhighestavailable'] = False
	#deal with scores being integers with nulls (confuses sql)
	kipp_nj_act.score_composite = kipp_nj_act.score_composite.dropna().astype(int)
	kipp_nj_act.score_composite = kipp_nj_act.score_composite.apply(int_with_NaN_tostr)
	kipp_nj_act.score_english = kipp_nj_act.score_english.dropna().astype(int)
	kipp_nj_act.score_english = kipp_nj_act.score_english.apply(int_with_NaN_tostr)
	kipp_nj_act.score_math = kipp_nj_act.score_math.dropna().astype(int)
	kipp_nj_act.score_math = kipp_nj_act.score_math.apply(int_with_NaN_tostr)
	kipp_nj_act.score_reading = kipp_nj_act.score_reading.dropna().astype(int)
	kipp_nj_act.score_reading = kipp_nj_act.score_reading.apply(int_with_NaN_tostr)
	kipp_nj_act.score_science = kipp_nj_act.score_science.dropna().astype(int)
	kipp_nj_act.score_science = kipp_nj_act.score_science.apply(int_with_NaN_tostr)
	kipp_nj_act.score_writing = kipp_nj_act.score_writing.dropna().astype(int)
	kipp_nj_act.score_writing = kipp_nj_act.score_writing.apply(int_with_NaN_tostr)
	clean_act = kipp_nj_act[['student_salesforce_id', 'date', 'test_level', 'test_semester', 'score_composite', 'score_english', 'score_math', 'score_reading', 'score_science', 'score_writing', 'onlyhighestavailable']]
	# correct the id columns
	clean_act.rename(columns={'student_salesforce_id':'kipp_nj_sf_id'}, inplace=True)
	return clean_act

def schools_table(kipp_nj_schools):
	# make parent organization KIPP when the kind is KIPP (prior to overwriting kinds)
	kipp_nj_schools.parent_organization[kipp_nj_schools.kind.str.contains('KIPP')==True] = 'KIPP'
	kipp_nj_schools.parent_organization[kipp_nj_schools.kind.str.contains('KIPP')==False] = np.nan
	# deal with commas in names
	kipp_nj_schools.name = kipp_nj_schools.name.str.replace(',','')
	#map school types
	kipp_nj_schools.kind = kipp_nj_schools.kind.dropna().apply(map_school_types)
	# map school level
	kipp_nj_schools.level = kipp_nj_schools.level.dropna().apply(code_school_level)
	# get only first 5 digits of zip code
	kipp_nj_schools.zip = kipp_nj_schools.zip.dropna().astype(str).apply(lambda x: x[:5])
	# get rid of incorrect zip codes
	kipp_nj_schools.zip = kipp_nj_schools.zip.dropna().apply(valid_zip)
	#no dropout rate
	kipp_nj_schools['dropout_rate'] = np.nan
	kipp_nj_schools.rename(columns={'school_salesforce_id':'kipp_nj_sf_school_id'}, inplace=True)
	#reorder columns
	clean_schools = kipp_nj_schools[['name', 'kipp_nj_sf_school_id', 'level', 'kind', 'parent_organization', 'no_excuses', 'zip', 'year_of_first_graduating_class', 'dropout_rate', 'number_of_students', 'avg_class_size', 'number_of_staff', 'number_of_counselors', 'annual_budget']]
	return clean_schools

def courses_table(kipp_nj_courses):
	# table is already clean (many values are missing)
	#add null course number
	kipp_nj_courses['course_number'] = np.nan
	kipp_nj_courses['grade_available'] = np.nan
	# deal with spaces in names
	kipp_nj_courses.letter_grade = kipp_nj_courses.letter_grade.str.replace(' ','')
	#remap highest percentage to 100
	kipp_nj_courses.percent_grade[kipp_nj_courses.percent_grade > 100.0] = 100.0
	# correct the id columns
	kipp_nj_courses.rename(columns={'student_salesforce_id':'kipp_nj_sf_id', 'school_salesforce_id': 'kipp_nj_sf_school_id'}, inplace=True)
	# reorganize columns
	clean_courses = kipp_nj_courses[['course_name', 'course_number', 'kipp_nj_sf_id', 'kipp_nj_sf_school_id', 'teacher_id', 'year_taken', 'semester_taken', 'grade_available', 'course_length', 'course_level', 'credit_hours', 'was_honors', 'was_ap', 'percent_grade', 'letter_grade']]
	return clean_courses
	
def attendance_table(kipp_nj_attendance):
	# drop powerschool id, we can use sf id
	clean_att = kipp_nj_attendance.drop('powerschool_id', axis =1)
	clean_att.rename(columns={'student_salesforce_id':'kipp_nj_sf_id'}, inplace=True)
	# make date a date
	clean_att.attendance_date = pd.to_datetime(clean_att.attendance_date)
	# map attendance types correctly
	clean_att.attendance_type = clean_att.attendance_type.fillna('missing').apply(map_attendance_types)
	# reorder columns
	clean_att = clean_att[['kipp_nj_sf_id', 'attendance_date', 'school_year', 'attendance_type']]
	return clean_att

def gpa_by_year(kipp_nj_gpa):
	#only need to rename id columns
	kipp_nj_gpa.rename(columns={'student_salesforce_id':'kipp_nj_sf_id', 'school_salesforce_id': 'kipp_nj_sf_school_id'}, inplace=True)
	return kipp_nj_gpa

def gpa_cumulative(kipp_nj_gpa):
	# groupby students to get average gpa over all years
	gpa_cumulative = kipp_nj_gpa[['student_salesforce_id', 'weighted_gpa', 'unweighted_gpa']].groupby('student_salesforce_id').mean()
	gpa_cumulative = gpa_cumulative.reset_index()

	# join school id back in afterwards

	#school ids per student

	school_ids = kipp_nj_gpa[['student_salesforce_id', 'school_salesforce_id']].drop_duplicates(subset= ['student_salesforce_id'])

	gpa_cumulative = pd.merge(gpa_cumulative, school_ids, how = 'left', left_on = 'student_salesforce_id', right_on = 'student_salesforce_id')

	#rename some id columns
	gpa_cumulative.rename(columns={'student_salesforce_id':'kipp_nj_sf_id', 'school_salesforce_id': 'kipp_nj_sf_school_id', 'weighted_gpa': 'cumulative_weighted_gpa', 'unweighted_gpa': 'cumulative_unweighted_gpa'}, inplace=True)

	return gpa_cumulative


def hs_enrollment_table(kipp_nj_enrollments):
	# get only high school enrollments
	high_school_enr = kipp_nj_enrollments[kipp_nj_enrollments.degree_type == 'High School Diploma']
	#clean up statuses
	high_school_enr = high_school_enr[~high_school_enr.status.isin(['Other', 'Did Not Enroll', 'Deferred'])]
	high_school_enr.status = high_school_enr.status.apply(lambda x: map_value_from_dict(status_fixed_mapping, x))
	# make dates into dates
	high_school_enr.start_date = pd.to_datetime(high_school_enr.start_date)
	high_school_enr.end_date = pd.to_datetime(high_school_enr.end_date)
	#to get high school class, get end years of graduated students
	high_school_enr['high_school_class']= high_school_enr.status.combine(high_school_enr.end_date, func = extract_grad_year)
	high_school_enr.rename(columns={'student_salesforce_id':'kipp_nj_sf_id', 'college_salesforce_id': 'kipp_nj_sf_school_id', 'status': 'exit_type'}, inplace=True)
	# drop irrelevant columns
	high_school_enr = high_school_enr.drop(['college_ncesid','data_source', 'date_last_verified', 'degree_subject', 'degree_type', 'living_on_campus', 'major', 'transfer_reason__c'], axis = 1)
	return high_school_enr









	


