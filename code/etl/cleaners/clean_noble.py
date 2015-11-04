''' Contains all the functions to clean Noble data.

Inputs:
Each cleaning function takes a dataframe (or many dataframes) as inputs.
These input dataframes are "raw", loaded directly from CSVs provided by partners.

Output:
Each cleaning function returns a "cleaned" dataframe, which is in the correct
format to be uploaded directly into the database.
'''

import pandas as pd
import numpy as np
import datetime
import config
import csv
from util.ETL_helpers import fill_empty_cols, map_value_from_dict, ethnicity_fixed_mapping, create_conversion_dict, convert_free_text
from util.integer_withNA_tostr import int_with_NaN_tostr

###################################################################
# HELPER FUNCTIONS
# small functions called by the larger cleaners
# note: some more general helper functions are in util/ETL_helpers
###################################################################
def code_gender(gender):
    if gender == 'Female' or gender == 'F':
        return 'True'
    elif gender == 'Male' or gender == 'M':
        return 'False'
    else: return np.nan

def wrong_birthdates_to_null(date):
    '''Sets date to null if it is before 1970'''
    if date < pd.to_datetime(1970):
        return np.nan
    else:
        return date

def wrong_dates_to_null(date):
    '''sets date to null if it is after today'''
    date_today = pd.to_datetime(datetime.date.today())    
    if date > date_today:
        return np.nan
    else:
        return date  

def code_contact_status(status):
    if not (type(status) == float and np.isnan(status)):
        if status == 'Outreach only' or status == 'Outeach only':
            return False
        else: return True
    else: return np.nan 

def overwrite_ids(row, school_counselors,student_counselors, students_with_unique_ids_missing):
    # need to handle the schools with multiple counselors differently
    #for schools with only one counselor, map all contact notes to that counselor unless we don't have an alumni entry for that student (i.e. HS is null)
    if row['High School'] not in ['Chicago Bulls','Gary Comer']:
        if type(row['High School']) == float and np.isnan(row['High School']):
            return np.nan
        else: 
            return str(school_counselors[school_counselors['High School'] == row['High School']]['CreatedById'].values[0])
    #for schools with multiple counselors, map to the mode of counselors for that student, unless they have no other contact notes in which case map to max of school
    elif row['CreatedById'] == 'missing': 
        if row['Contact__c'] in students_with_unique_ids_missing.values:
            if type(row['High School']) == float and np.isnan(row['High School']):
                return np.nan
            else: 
                return str(school_counselors[school_counselors['High School'] == row['High School']]['CreatedById'].values[0])

        else: return str(student_counselors[student_counselors['Contact__c'] == row['Contact__c']]['CreatedById'].values[0])
    else: return row['CreatedById'] 

def code_isprivate(collegetype):
    if "Private" in collegetype:
        return True
    elif "Public" in collegetype:
        return False
    else:
        return np.nan 

def code_isforprofit(collegetype):
    if "not-for-profit" in collegetype:
        return False
    elif "for-profit" in collegetype:
        return True
    else:
        return np.nan  

def code_is4year(collegetype):
    if "less than 2-year" in collegetype:
        return np.nan
    elif "4-year" in collegetype:
        return True
    elif "2-year" in collegetype:
        return False
    else:
        return np.nan  

def map_gpas(gpa_mapping, grade):
    if grade in gpa_mapping:
        return gpa_mapping[grade]
    else: return np.nan
                              

###################################################################
# MAPPING DICTIONARIES
# map from partner values to clean values
###################################################################  
medium_fixed_mapping = {'Phone': 'Call',
'Mail (letter/postcard)': 'Mail',
'College staff contact': 'College Contact',
'Parent contact (any medium)': 'Parent Contact' 
}

gpa_mapping ={'A+': 4.3, 
'A': 4.0,
'A-': 3.7,
'B+': 3.3,
'B': 3.0,
'B-': 2.7,
'C+': 2.3,
'C': 2.0, 
'C-': 1.7,
'D+': 1.3,
'D': 1.0,
'D-': 0.7,
'F': 0.0, 
'P': np.nan} 

selectivity_mapping = {'Not Available': np.nan,
'2 year (Noncompetitive)': np.nan,
'2 year (Competitive)': np.nan

}

size_range_mapping = {'Not applicable': np.nan, 'Not reported': np.nan}

###################################################################
# CLEANER FUNCTIONS
# cleaners for every table in the database
###################################################################        

def studentid(contacts):

    # initialize empty dataframe
    student_ids = pd.DataFrame()

    # load necessary variables
    student_ids['noble_sf_id'] = contacts['Id'] #salesforce id
    student_ids['cps_id'] = contacts['Network_Student_ID__c']

    # return clean dataframe
    return student_ids

def collegeid(colleges):

    # Create dataframe with just Noble's Salesforce college IDs
    dfsql_collegeid=pd.DataFrame(colleges['College ID'])
    dfsql_collegeid.columns = ['noble_sf_college_id']

    # Add ipedsid
    # Note: IPEDS IDs are 6-digits for colleges, NCES IDs are 12 digits for grade schools.
    # Manually checked a couple rows using the name of college to confirm Noble's "NCESid" is actually IPEDs ID.
    dfsql_collegeid['ipedsid'] = colleges['NCESid__c'].apply(int_with_NaN_tostr)
    # drop two randomly duplicated colleges found through manual inspection
    dfsql_collegeid = dfsql_collegeid[~dfsql_collegeid.noble_sf_college_id.isin(['001E000000Sg2wPIAR', '001E000000Sg2wQIAR'])]
    return dfsql_collegeid

def schoolid(accounts, alumni, powerschool_ids):
    # get the relevant high schools

    names = alumni['High_School__c'].unique()

    # get the ids only for high schools

    id_df = pd.DataFrame(accounts[accounts.Name.isin(names)].drop_duplicates()[['Name','Id']])
    all_ids = pd.merge(id_df, powerschool_ids, left_on = 'Name', right_on = 'Campus')

    school_ids = all_ids.drop(['Campus','Name'], axis = 1)
    school_ids.columns = ['noble_sf_school_id', 'noble_powerschool_id']

    return school_ids


def colleges(colleges, extra_college_features):

    # Store CollegeID_Noble
    df_collegesql=pd.DataFrame(colleges['College ID'])
    df_collegesql.columns = ['noble_sf_college_id']
    df_collegesql['ipedid'] = colleges['NCESid__c']
    df_collegesql['isprivate'] = colleges['College_Type__c'].fillna('').apply(code_isprivate)
    df_collegesql['isforprofit'] = colleges['College_Type__c'].fillna('').apply(code_isforprofit)
    df_collegesql['is4year'] = colleges['College_Type__c'].fillna('').apply(code_is4year)
    df_collegesql['zip'] = colleges.ShippingPostalCode.str.extract('^([0-9]{5})(?:\D|$)')
    # Store Name
    df_collegesql['name'] = colleges.Name.str.replace(',','')

    # Store isrural
    df_collegesql['isrural'] = np.nan

    # Store allmale & allfemale
    df_collegesql['allmale'] = np.nan
    df_collegesql['allfemale'] = np.nan

    # Store graduation rates and transfer rates
    df_collegesql['graduationrate_6yr'] = colleges['6_yr_completion_rate__c'].apply(int_with_NaN_tostr)
    df_collegesql['graduationrate_minority_6yr'] = colleges['6_yr_minority_completion_rate__c'].apply(int_with_NaN_tostr)

    df_collegesql['transferrate_6yr'] = colleges['6_yr_transfer_rate__c'].apply(int_with_NaN_tostr)
    df_collegesql['transferrate_minority_6yr'] = colleges['6_yr_minority_transfer_rate__c'].apply(int_with_NaN_tostr)

    # Store historicallyblack
    df_collegesql['historicallyblack'] = colleges['HBCU__c']

    # add in extra college features
    # first make new dataframe (will need to merge later)

    extra_college = pd.DataFrame(extra_college_features['UNITID'])
    extra_college['state'] = extra_college_features['STABBR']
    extra_college['longitude'] = extra_college_features['Longitude']
    extra_college['latitude'] = extra_college_features['Latitude']
    extra_college['dist_from_chicago'] = extra_college_features['DistFromChicago']
    # set missings and 2 year colleges to null
    extra_college['barrons_rating'] = extra_college_features.SimpleBarrons.apply(lambda x: map_value_from_dict(selectivity_mapping, x))
    # convert percentages to numbers
    extra_college['perc_accepted'] = extra_college_features['%Apply_Accepted'].str.replace('%', '')
    extra_college['perc_accepted_enroll'] = extra_college_features['%Accepted_Enroll'].str.replace('%', '')
    extra_college['perc_male'] = extra_college_features['% male'].str.replace('%', '')
    extra_college['perc_female'] = extra_college_features['% female'].str.replace('%', '')
    extra_college['perc_african_american'] = extra_college_features['% AA'].str.replace('%', '')
    extra_college['perc_hispanic'] = extra_college_features['% Hispanic'].str.replace('%', '')
    extra_college['percentinstate'] = extra_college_features['PercentInState']
    extra_college['percentoutofstate'] = extra_college_features['PercentOutOfState']
    extra_college['percentpellgrant'] = extra_college_features['PercentPellGrant']
    extra_college[['avgnetprice', 'netprice0_30', 'netprice30_48', 'netprice48_75']] = extra_college_features[['AvgNetPrice', 'NetPrice0-30', 'NetPrice30-48', 'NetPrice48-75']]
    extra_college['locale'] = extra_college_features['Locale']
    extra_college['size_range'] = extra_college_features['Size Range'].str.replace(',','')
    # set not reported and not applicable to null
    extra_college['size_range'] = extra_college['size_range'].apply(lambda x: map_value_from_dict(size_range_mapping, x))

    # then join on ids

    clean_colleges = pd.merge(df_collegesql, extra_college, how = 'left', left_on = 'ipedid', right_on = 'UNITID')
    clean_colleges = clean_colleges.drop(['UNITID'], axis = 1)
    #fix nulls for upload to sql
    clean_colleges.ipedid = clean_colleges.ipedid.apply(int_with_NaN_tostr)
    # drop two randomly duplicated colleges found through manual inspection
    clean_colleges = clean_colleges[~clean_colleges.noble_sf_college_id.isin(['001E000000Sg2wPIAR', '001E000000Sg2wQIAR'])]
    return clean_colleges

def acttests(noble_tests):

    # initialize empty dataframe
    dfsql_act = pd.DataFrame()

    # Add student id
    dfsql_act['cps_id'] = noble_tests['student_number']

    # add date
    dfsql_act['date'] = noble_tests['test_date']

    # Add test level
    dfsql_act['test_level'] = noble_tests.testname_clean.apply(lambda x: x.upper())

    # Add test semester
    def prepost_map(s):
        if s=='Pre':
            return 'Fall'
        elif s=='Post':
            return 'Spring'
        else:
            return ''

    dfsql_act['test_semester'] = noble_tests['timeperiod'].apply(prepost_map)

    #add composite
    dfsql_act['score_composite'] = noble_tests['composite']

    # add subject scores
    dfsql_act['score_english'] = noble_tests['english']
    dfsql_act['score_math'] = noble_tests['math']
    dfsql_act['score_reading'] = noble_tests['reading']
    dfsql_act['score_science'] = noble_tests['science']
    dfsql_act['score_writing'] = np.nan

    # Prepare onlyhighestavailable
    dfsql_act['onlyhighestavailable'] = False

    # Return dataframe
    return dfsql_act

def attendance(noble_att):

    # initialize clean dataframe 
    attendance = pd.DataFrame()

    # IDs
    attendance['student_cps_id'] = noble_att.STUDENT_NUMBER
    attendance['school_noble_powerschoolid'] = noble_att.SCHOOLID

    # Dates
    attendance['attendance_date'] = pd.to_datetime(noble_att.ATT_DATE)
    attendance['school_year'] = noble_att.SY

    # Attendance codes
    excused_codes = ['X','S','M']
    codedict = {'X':'excused',
                'S': 'excused',
                'M': 'excused',
                'T': 'tardy',
                'T0': 'tardy',
                'T5': 'tardy',
                'L0': 'tardy',
                'L5': 'tardy',
                'A': 'unexcused',
                'R': 'unexcused',
                'E': 'early_dismissal',
                'E0': 'early_dismissal',
                'E5': 'early_dismissal',
                'S': 'suspended'
                }

    attendance['attendance_type'] = noble_att.ATT_CODE.replace(to_replace=codedict)
    
    # Manually fix some attendance codes
    attendance.ix[noble_att.DESCRIPTION=='Late Arrival Excused','attendance_type'] = 'excused'
    attendance.ix[noble_att.DESCRIPTION=='Late','attendance_type'] = 'tardy'

    # Drop some attendance codes
    droplist = ['F','G','H5','H1']
    attendance = attendance[~attendance.attendance_type.isin(droplist)]

    # Return clean df
    return attendance

def enrollments(enrollment):

    # initialize clean dataframe 
    clean_enrollment = pd.DataFrame()

    # add student and college ids
    clean_enrollment['noble_student_sf_id'] = enrollment['Student__c']
    clean_enrollment['noble_college_sf_id'] = enrollment['College__c']

    # convert date columns to datetime to do date cleaning
    enrollment['Start_Date__c'] = pd.to_datetime(enrollment['Start_Date__c'])
    enrollment['End_Date__c'] = pd.to_datetime(enrollment['End_Date__c'])
    enrollment['Date_Last_Verified__c'] = pd.to_datetime(enrollment['Date_Last_Verified__c'])

    #remove weird dates, add to new df
    clean_enrollment['start_date'] = enrollment.Start_Date__c.apply(wrong_dates_to_null)
    clean_enrollment['end_date'] = enrollment.End_Date__c.apply(wrong_dates_to_null)
    clean_enrollment['date_last_verified'] = enrollment.Date_Last_Verified__c.apply(wrong_dates_to_null)

    # add status, data source, and degree type as is
    clean_enrollment['status'] = enrollment.Status__c
    clean_enrollment['data_source'] = enrollment.Data_Source__c

    # Living on campus
    clean_enrollment['living_on_campus'] = np.nan

    # Degree type, without apostrophes
    clean_enrollment['degree_type'] = enrollment.Degree_Type__c.dropna().apply(lambda x: str(x).replace("'",""))

    # load degree and major dictionaries from manually coded csvs
    degree_dict = create_conversion_dict(config.PERSISTENCE_PATH + '/code/etl/mappers/degreetranslation.csv')
    major_dict = create_conversion_dict(config.PERSISTENCE_PATH + '/code/etl/mappers/majortranslation.csv')

    # do conversions from freetext to coded options
    clean_enrollment['degree_subject'] = enrollment.Degree_Text__c.fillna('missing').apply(lambda x: convert_free_text(degree_dict, x))
    clean_enrollment['major'] = enrollment.Major_Text__c.fillna('missing').apply(lambda x: convert_free_text(major_dict, x))

    # convert withdrawal reasons to several boolean categories
    def boolean_withdrawal(dummy_code, input_code):
        if input_code == 'missing':
            return np.nan
        else:
            codes = input_code.split(';')
            dummy_booleans = [True if code == dummy_code else False for code in codes]
            if sum(dummy_booleans) > 0:
                return True
            else: return False

    withdrawal_codes = ['Financial', 'Academic', 'Motivational', 'Family', 'Health', 'Social', 'Racial Conflict']
    for dummy_code in withdrawal_codes:
        clean_enrollment['withdrawal_reason_' + "_".join(dummy_code.lower().split())] = enrollment.Withdrawal_code__c.fillna('missing').apply(lambda x: boolean_withdrawal(dummy_code, x))
    clean_enrollment.rename(columns={'withdrawal_reason_racial_conflict': 'withdrawal_reason_racial'}, inplace=True)

    # Return clean df
    return clean_enrollment

def students(noble_students):
    # initialize clean dataframe
    clean_students = pd.DataFrame()
    clean_students['noble_sf_id'] = noble_students.Id
    clean_students['cps_id'] = noble_students.Network_Student_ID__c
    clean_students['network'] = 'Noble'
    # add DOB
    noble_students['Birthdate'] = pd.to_datetime(noble_students['Birthdate'], format = '%m/%d/%Y')
    clean_students['date_of_birth'] = noble_students.Birthdate.apply(wrong_birthdates_to_null)
    # add gender as a boolean 
    clean_students['is_female'] = noble_students.Gender__c.fillna('').apply(code_gender)
    # add fixed ethnicity
    clean_students['ethnicity'] = noble_students['Ethnicity__c'].apply(lambda x: map_value_from_dict(ethnicity_fixed_mapping, x))
    
    ## add ever_special need and free lunch as booleans
    clean_students['ever_special_ed'] = noble_students['Special_Education__c']
    clean_students['ever_free_lunch'] = noble_students['Low_Income__c']
    clean_students['is_first_gen'] = noble_students['First_Generation_College_Student__c']
    # add EFC
    clean_students['fafsa_efc'] = noble_students.EFC_from_FAFSA__c
    # add all the empty columns for now
    missing_cols = ['family_income_bracket','number_in_household','zip']
    fill_empty_cols(clean_students,missing_cols)
    # reorder columns
    return clean_students

def contacts(contacts, alumni):
    #initialize clean df
    clean_contacts = pd.DataFrame()
    # add student id, initiated by student
    clean_contacts['noble_sf_id'] = contacts.Contact__c
    clean_contacts['initiated_by_student'] = contacts.Initiated_by_alum__c
    # convert contact date to datetime
    contacts.Date_of_Contact__c = pd.to_datetime(contacts.Date_of_Contact__c)
    # set weird dates to null
    clean_contacts['contact_date'] = contacts.Date_of_Contact__c.apply(wrong_dates_to_null)
    # clean medium to fit table constraints
    clean_contacts['contact_medium'] = contacts.Mode_of_Communication__c.apply(lambda x: map_value_from_dict(medium_fixed_mapping,x))
    # convert communication status to successful boolean
    #note Noble doesn't keep track of outreach mass email, so was_outreach is always False
    clean_contacts['was_outreach'] = False
    clean_contacts['was_successful'] = contacts.Comm_Status__c.apply(code_contact_status)
    # clean up counselor IDs, because many contact events are batch uploaded by two people, need to fix these CreatedByIds to be the relevant counselor
    # merge contacts with alums to match students to the schools they went to
    merged_contacts = pd.merge(contacts, alumni, how = 'left', left_on = 'Contact__c', right_on = 'Id')
    # ids we want to overwrite are for Matt and a data manager, and some other random IDs
    irrelevant_ids = ['005E0000000GphFIAS', '005E00000048sScIAI']
    #recode these ids as missing
    merged_contacts['CreatedById'] = merged_contacts['CreatedById'].apply(lambda x:'missing' if x in irrelevant_ids else x)
    # get the counselors for every school that aren't the irrelevant ones
    school_counselors = merged_contacts[merged_contacts['CreatedById'] != 'missing'].groupby(['High School', 'CreatedById']).size()
    # get the most common counselor for every school
    max_mask = school_counselors.groupby(level=0).agg('idxmax')
    school_counselors = school_counselors.loc[max_mask]
    school_counselors = school_counselors.reset_index()
    #get the most common counselors for every student
    student_counselors = merged_contacts[merged_contacts['CreatedById'] != 'missing'].groupby(['Contact__c', 'CreatedById']).size()
    student_max_mask = student_counselors.groupby(level=0).agg('idxmax')
    student_counselors = student_counselors.loc[student_max_mask]
    student_counselors = student_counselors.reset_index()
    #figure out whether there are students who have only ever been loaded by those missing IDs
    students_with_unique_ids_missing  = merged_contacts.groupby('Contact__c').filter(lambda x: x.CreatedById.nunique() ==1)
    students_with_unique_ids_missing = students_with_unique_ids_missing[students_with_unique_ids_missing['CreatedById'] == 'missing']
    students_with_unique_ids_missing = students_with_unique_ids_missing.groupby(['Contact__c', 'CreatedById']).size().reset_index()['Contact__c']
    #save the new ids
    clean_contacts['counselor_id'] = merged_contacts.apply(lambda x: overwrite_ids(x,school_counselors, student_counselors, students_with_unique_ids_missing), axis =1)
    #reorder columns 
    clean_contacts = clean_contacts[['noble_sf_id','contact_date', 'counselor_id', 'contact_medium', 'initiated_by_student', 'was_outreach', 'was_successful']]
    return clean_contacts

def ap(noble_aps):
    #initialize new df
    ap_tests = pd.DataFrame()
    # add id
    ap_tests['noble_powerschool_id'] = noble_aps['student_number']
    # add date
    ap_tests['date'] = pd.to_datetime(noble_aps['test_date'])
    # add score 
    ap_tests['score'] = noble_aps['numscore']
    # load ap dictionary from manually coded csv
    subject_dict = create_conversion_dict(config.PERSISTENCE_PATH + '/code/etl/mappers/APsubjecttranslation.csv')
    # do conversions from freetext to coded options
    noble_aps.testname = noble_aps.testname.apply(lambda x: x.replace('AP ', '')) #remove the 'AP '
    ap_tests['subject'] = noble_aps.testname.fillna('missing').apply(lambda x: convert_free_text(subject_dict, x))
    return ap_tests

def schools(accounts, alumni,founding_years):
    founding_years['year_of_first_graduating_class'] = pd.to_datetime(founding_years['year_of_first_graduating_class'], format='%Y')
    # extract the schools that have alumni
    names = alumni['High School'].unique()

    schools = pd.DataFrame(data=[], index=names)
    schools.index.name = 'name'
    # add in Noble IDs
    schools['noble_sf_school_id'] = accounts[accounts.Name.isin(names)].drop_duplicates()[['Id','Name']].set_index('Name')

    # add in the information that is the same across schools
    schools['level'] = 'high school'
    schools['kind'] = 'charter'
    schools['parent_organization'] = 'Noble'
    schools['no_excuses'] = True

        # get zipcodes of schools from accounts file
    schools['zip'] = accounts[accounts.Name.isin(names)].drop_duplicates()[
            ['BillingPostalCode','Name']].set_index('Name').astype(np.integer)

    # get year of first class
    schools['year_of_first_graduating_class'] = founding_years['year_of_first_graduating_class']

    # fill missing columns with NaNs
    schools['dropout_rate'] = np.nan
    schools['number_of_students'] = np.nan
    schools['avg_class_size'] = np.nan
    schools['number_of_staff'] = np.nan
    schools['number_of_counselors'] = np.nan
    schools['annual_budget'] = np.nan

    schools = schools.reset_index()
    return schools
   

def gpa_by_year(courses):

    #clean courses in the same way that they're cleaned for courses (below), but without dropping columns

    courses.columns = map(str.lower, courses.columns)
    courses.honors = courses.honors.astype(bool)  
    courses.ap = courses.ap.astype(bool)

    # fix the semester codes - TODO: is this the right way to do it?
    courses.storecode.replace('s3',3,inplace=True)
    courses.storecode.replace('Q1',1,inplace=True)
    courses.storecode.replace('Q3',2,inplace=True)
    courses.storecode.replace('S1',1,inplace=True)
    courses.storecode.replace('S2',2,inplace=True)
    courses.storecode.replace('S3',3,inplace=True)
    courses.storecode = courses.storecode.astype(int)

    # get the school year of the grade
    courses['grade_available'] = courses.loc[:,'sy']
    # if the semester is the first, the actual date of the grade
    # is in the previous calendar year
    courses.loc[courses.storecode==1,'grade_available'] -= 1
    # add in the approximate end dates of the semester
    courses.grade_available = courses.grade_available.astype(str) + (
                                courses.storecode.replace({1: '-12-31',
                                                      2: '-06-15',
                                                      3: '-08-19'
                                                      }))
    courses.grade_available = pd.to_datetime(courses.grade_available)

    # typo fix
    courses.loc[courses.course_name=='Comuter Programming','course_name'] = 'Computer Programming'
    #remove commas
    courses.course_name = courses.course_name.str.replace(',', '')
    # cut off percent grades that seems to be errors
    courses.loc[courses.percent > 120.0,'percent'] = pd.np.nan
    #co.loc[co.percent > 100.0,'percent'] = 100.0
    courses.student_number = courses.student_number.astype(str)

    # cast the Noble schoolid to str - that's how it's in our ID table
    courses.schoolid = courses.schoolid.astype(str)

    # for 'exclusion from GPA', we have to assume that missing values mean 'No'
    courses.excludefromgpa = courses.excludefromgpa=='Yes'

    # fix some of the grades' spelling, remove invalid values
    courses.grade.replace('p','P',inplace=True)
    courses.grade.replace(' P','P',inplace=True)
    courses.grade.replace('c+','C+',inplace=True)
    courses.grade.replace('c-','C-',inplace=True)
    courses.grade.replace('d','D',inplace=True)

    # NOTE: should the credit hours for courses with invalid values be counted as part of the GPA?

    courses.loc[courses.grade.str.contains('^[PAaBbCcDdFfIiWw][+-]?$')==False,'grade'] = pd.np.nan

    # then map between letter grades and GPA points

    courses['gpa_conversion'] = courses.grade.apply(lambda x: map_gpas(gpa_mapping, x))

    # get rid of courses that are excluded from gpa (i.e. pass/fail courses)
    courses = courses[courses.excludefromgpa == False]

    # calculate honors (0.5) and ap bonus (1) (this info from the Noble student handbook)

    courses['ap_bonus'] = courses.ap.apply(lambda x: 1.0 if x == True else 0)
    courses['honors_bonus'] = courses.honors.apply(lambda x: 0.5 if x == True else 0)

    #then multiply gpa conversion by credit hours to get the gpa points for the class

    courses['gpa_points'] = courses.gpa_conversion * courses.potentialcrhrs 

    # add in bonuses for AP and honors courses
    courses['weighted_gpa_points'] = (courses.gpa_conversion + courses.ap_bonus + courses.honors_bonus) * courses.potentialcrhrs

    # then group by student by year, sum points and divide by number of credit hours

    gpa_by_year = courses.groupby(['student_number', 'sy']).agg({'gpa_points':'sum', 'weighted_gpa_points':'sum', 'potentialcrhrs':'sum', 'schoolid': lambda x: x.value_counts().index[0]})
    gpa_by_year['unweighted_gpa']  = gpa_by_year['gpa_points']/gpa_by_year['potentialcrhrs']
    gpa_by_year['weighted_gpa'] = gpa_by_year['weighted_gpa_points'] /gpa_by_year['potentialcrhrs']
    gpa_by_year = gpa_by_year.reset_index()

    # drop credit hours

    gpa_by_year = gpa_by_year.drop(['potentialcrhrs', 'gpa_points', 'weighted_gpa_points'], axis = 1)

    # then rename the relevant columns

    gpa_by_year.rename(columns={'student_number': 'cps_id', 'schoolid': 'noble_powerschool_school_id', 'sy': 'school_year'}, inplace=True)

    return gpa_by_year

def gpa_cumulative(alumni, accounts):

    # initialize clean dataframe
    dfsql_gpa = pd.DataFrame()

    # Prepare nobleid
    dfsql_gpa['noble_student_sf_id']=alumni['Id']

    # go from high school name to high school id

    # get the unique names

    names = alumni['High School'].unique()

    # get the id/name pairing from accounts
    id_df = pd.DataFrame(accounts[accounts.Name.isin(names)].drop_duplicates()[['Name','Id']])
    id_df.rename(columns = {'Id': 'noble_sf_school_id'}, inplace = True)

    # merge the ids and names

    alumni_ids = pd.merge(alumni, id_df, how = 'left', left_on = 'High School', right_on = 'Name')

    dfsql_gpa['noble_sf_school_id'] = alumni_ids['noble_sf_school_id']

    # Prepare unweighted_gpa
    dfsql_gpa['cumulative_unweighted_gpa'] = alumni_ids['HS GPA']

    # Return clean df
    return dfsql_gpa

def courses(courses):

    courses.columns = map(str.lower, courses.columns)
    courses.honors = courses.honors.astype(bool)  
    courses.ap = courses.ap.astype(bool)

    # fix the semester codes - TODO: is this the right way to do it?
    courses.storecode.replace('s3',3,inplace=True)
    courses.storecode.replace('Q1',1,inplace=True)
    courses.storecode.replace('Q3',2,inplace=True)
    courses.storecode.replace('S1',1,inplace=True)
    courses.storecode.replace('S2',2,inplace=True)
    courses.storecode.replace('S3',3,inplace=True)
    courses.storecode = courses.storecode.astype(int)

    # get the school year of the grade
    courses['grade_available'] = courses.loc[:,'sy']
    # if the semester is the first, the actual date of the grade
    # is in the previous calendar year
    courses.loc[courses.storecode==1,'grade_available'] -= 1
    # add in the approximate end dates of the semester
    courses.grade_available = courses.grade_available.astype(str) + (
                                courses.storecode.replace({1: '-12-31',
                                                      2: '-06-15',
                                                      3: '-08-19'
                                                      }))
    courses.grade_available = pd.to_datetime(courses.grade_available)

    # typo fix
    courses.loc[courses.course_name=='Comuter Programming','course_name'] = 'Computer Programming'
    #remove commas
    courses.course_name = courses.course_name.str.replace(',', '')
    # cut off percent grades that seems to be errors
    courses.loc[courses.percent > 120.0,'percent'] = pd.np.nan
    #co.loc[co.percent > 100.0,'percent'] = 100.0
    courses.student_number = courses.student_number.astype(str)

    # cast the Noble schoolid to str - that's how it's in our ID table
    courses.schoolid = courses.schoolid.astype(str)

    # for 'exclusion from GPA', we have to assume that missing values mean 'No'
    courses.excludefromgpa = courses.excludefromgpa=='Yes'

    # fix some of the grades' spelling, remove invalid values
    courses.grade.replace('p','P',inplace=True)
    courses.grade.replace(' P','P',inplace=True)
    courses.loc[courses.grade.str.contains('^[PAaBbCcDdFfIiWw][+-]?$')==False,'grade'] = pd.np.nan

    #drop random unnecessary columns

    courses = courses.drop(['id', 'termid', 'yearid', 'credit_type', 'excludefromgpa', 'earnedcrhrs', 'gpa_points', 'additional _weighted_ gpa_points'], axis = 1)
    # rename id columns
    courses.rename(columns={'student_number': 'cps_id', 'schoolid': 'noble_powerschool_school_id', 'sy': 'year_taken',
        'storecode': 'semester_taken', 'grade level': 'course_level', 'ap': 'was_ap', 'honors': 'was_honors',
        'percent': 'percent_grade', 'grade': 'letter_grade', 'potentialcrhrs': 'credit_hours'}, inplace=True)

    # deal with int with nans

    courses.course_level = courses.course_level.apply(int_with_NaN_tostr)
    return courses

def hs_enrollment(contact, accounts):

    # get high school id
    id_df = pd.DataFrame(accounts[accounts.Name.isin(contact.High_School__c)].drop_duplicates()[['Name','Id']])
    # merge ids into contact
    hs_enroll_with_ids = pd.merge(contact, id_df, how = 'left', left_on = 'High_School__c', right_on = 'Name')

    clean_hs_enrollment = pd.DataFrame()
    clean_hs_enrollment['noble_sf_id'] = hs_enroll_with_ids.Id_x
    clean_hs_enrollment['noble_sf_school_id'] = hs_enroll_with_ids.Id_y
    clean_hs_enrollment['high_school_class'] = hs_enroll_with_ids.HS_Class__c

    return clean_hs_enrollment



