from modeling.featurepipeline.abstractfeature import *
from modeling.featurepipeline.abstractboundedfeature import *
from modeling.featurepipeline.abstractpandasfeature import *
from modeling.featurepipeline.abstractboundedpandasfeature import *
from modeling.featurepipeline.abstracttargetfeature import *
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import modeling.featurepipeline.postprocessors as pp
import pandas as pd
from util.pcheck_date_functions import find_nth_pcheck_date


# =======================================
#  Student Features   
# =======================================

class HSClass(AbstractFeature):
    name = "high school graduating class of the student"
    sql_query = "select studentid, high_school_class from hs_enrollment"
    index_col = "studentid"
    feature_type = "numerical"
    feature_col = "high_school_class"
    postprocessors = {}
    index_level = "studentid"

class StudentAgeAtEnrollment(AbstractFeature):
    name= "Age of student at time of enrollment"
    sql_query= '''
        select enrollments.enrollid, extract(year from age(enrollments.start_date, students.date_of_birth)) as age_at_enrollment from
        enrollments left join students
            on enrollments.studentid = students.studentid
            '''
    index_col="enrollid"
    feature_col="age_at_enrollment"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "enrollid"

class StudentGender(AbstractFeature):
    name= "StudentGender"
    sql_query= "select studentid, is_female from students"
    index_col="studentid"
    feature_col="is_female"
    feature_type = "boolean"
    postprocessors = {}
    index_level = "studentid"

class StudentEthnicity(AbstractFeature):
    name= "StudentEthnicity"
    sql_query= "select studentid, ethnicity from students"
    index_col="studentid"
    feature_col="ethnicity"
    feature_type = "categorical"
    postprocessors = {pp.getdummies:
    {}}
    index_level = "studentid"

class finalUnweightedGPA(AbstractFeature): 
    name= "Student's Final Graduating Unweighted GPA as an average over all years"
    sql_query= '''SELECT studentid, cumulative_unweighted_gpa from gpa_cumulative
    '''
    index_col = 'studentid'
    feature_col = 'cumulative_unweighted_gpa'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = "studentid" 

class year12GPA(AbstractFeature): 
    name= "Student's year eleven Unweighted GPA"
    sql_query= '''SELECT studentid, year12_unweightedgpa from
    (select studentid, avg(unweighted_gpa) as year12_unweightedgpa, count(unweighted_gpa) as count_gpa from
    (select gpa_by_year.studentid, unweighted_gpa, high_school_class - school_year as year_diff
    from gpa_by_year
    left join hs_enrollment
    on gpa_by_year.studentid = hs_enrollment.studentid) as foo
    where foo.year_diff = 0
    group by studentid) as bar
    where count_gpa = 1
    '''
    index_col = 'studentid'
    feature_col = 'year12_unweightedgpa'
    feature_type = 'numerical'
    # postprocessors = {pp.dummyCodeNull: {}}
    postprocessors = {}
    index_level = "studentid"  

class year11GPA(AbstractFeature): 
    name= "Student's year eleven Unweighted GPA"
    # NOTE: this query makes sure that there are no repeated GPAs for the same year (i.e. students who repeat a grade)
    # so that's why there's an average where count = 1...same goes for all similar features
    # also makes sure that we're getting GPA from the correct year (in this case 1 away from graduation year)
    sql_query= '''SELECT studentid, year11_unweightedgpa from
    (select studentid, avg(unweighted_gpa) as year11_unweightedgpa, count(unweighted_gpa) as count_gpa from
    (select gpa_by_year.studentid, unweighted_gpa, high_school_class - school_year as year_diff
    from gpa_by_year
    left join hs_enrollment
    on gpa_by_year.studentid = hs_enrollment.studentid) as foo
    where foo.year_diff = 1
    group by studentid) as bar
    where count_gpa = 1
    '''
    index_col = 'studentid'
    feature_col = 'year11_unweightedgpa'
    feature_type = 'numerical'
    # postprocessors = {pp.dummyCodeNull: {}}
    postprocessors = {}
    index_level = "studentid"     

class year10GPA(AbstractFeature): 
    name= "Student's year ten Unweighted GPA"
    sql_query= '''SELECT studentid, year10_unweightedgpa from
    (select studentid, avg(unweighted_gpa) as year10_unweightedgpa, count(unweighted_gpa) as count_gpa from
    (select gpa_by_year.studentid, unweighted_gpa, high_school_class - school_year as year_diff
    from gpa_by_year
    left join hs_enrollment
    on gpa_by_year.studentid = hs_enrollment.studentid) as foo
    where foo.year_diff = 2
    group by studentid) as bar
    where count_gpa = 1
    '''
    index_col = 'studentid'
    feature_col = 'year10_unweightedgpa'
    feature_type = 'numerical'
    # postprocessors = {pp.dummyCodeNull: {}}
    postprocessors = {}
    index_level = "studentid" 

class year9GPA(AbstractFeature): 
    name= "Student's year nine Unweighted GPA"

    sql_query= '''SELECT studentid, year9_unweightedgpa from
(select studentid, avg(unweighted_gpa) as year9_unweightedgpa, count(unweighted_gpa) as count_gpa from
    (select gpa_by_year.studentid, unweighted_gpa, high_school_class - school_year as year_diff
    from gpa_by_year
    left join hs_enrollment
    on gpa_by_year.studentid = hs_enrollment.studentid) as foo
    where foo.year_diff = 3
    group by studentid) as bar
    where count_gpa = 1
    '''
    index_col = 'studentid'
    feature_col = 'year9_unweightedgpa'
    feature_type = 'numerical'
    # postprocessors = {pp.dummyCodeNull: {}}
    postprocessors = {}
    index_level = "studentid"              

class avgYear11_12UnweightedGPA(AbstractFeature): 
    name= "Student's average unweighted GPA in years 11 and 12"
    sql_query= '''SELECT studentid, avg_y11_y12_unweightedgpa from
        (select studentid, avg(unweighted_gpa) as avg_y11_y12_unweightedgpa, count(unweighted_gpa) as count_gpa from
            (select gpa_by_year.studentid, unweighted_gpa, high_school_class - school_year as year_diff
            from gpa_by_year
            left join hs_enrollment
            on gpa_by_year.studentid = hs_enrollment.studentid) as foo
        where foo.year_diff <= 1
        group by studentid) as bar
    where count_gpa =2
    '''
    index_col = 'studentid'
    feature_col = 'avg_y11_y12_unweightedgpa'
    feature_type = 'numerical'
    # postprocessors = {pp.dummyCodeNull: {}}
    postprocessors = {}
    index_level = "studentid" 

class avgYear9_10UnweightedGPA(AbstractFeature): 
    name= "Student's average unweighted GPA in years 9 and 10"
    sql_query= '''SELECT studentid, avg_y9_y10_unweightedgpa from
        (select studentid, avg(unweighted_gpa) as avg_y9_y10_unweightedgpa, count(unweighted_gpa) as count_gpa from
            (select gpa_by_year.studentid, unweighted_gpa, high_school_class - school_year as year_diff
            from gpa_by_year
            left join hs_enrollment
            on gpa_by_year.studentid = hs_enrollment.studentid) as foo
        where foo.year_diff > 1 and foo.year_diff < 4
        group by studentid) as bar
    where count_gpa =2
    '''
    index_col = 'studentid'
    feature_col = 'avg_y9_y10_unweightedgpa'
    feature_type = 'numerical'
    # postprocessors = {pp.dummyCodeNull: {}}
    postprocessors= {}
    index_level = "studentid"

class ChangeInUnweightedGPA(AbstractFeature): 
    name= "Difference in avg unweighted gpa in years 9 and 10, and 11 and 12"
    sql_query= '''SELECT later.studentid, (later.avg_y11_y12_unweightedgpa - earlier.avg_y9_y10_unweightedgpa) as unweighted_gpa_diff FROM    
        (SELECT studentid, avg_y11_y12_unweightedgpa FROM
            (select studentid, avg(unweighted_gpa) as avg_y11_y12_unweightedgpa, count(unweighted_gpa) as count_gpa from
                (select gpa_by_year.studentid, unweighted_gpa, high_school_class - school_year as year_diff
                from gpa_by_year
                left join hs_enrollment
                on gpa_by_year.studentid = hs_enrollment.studentid) as foo
            where foo.year_diff <= 1
            group by studentid) as bar
        where count_gpa =2) as later
        left join (select studentid, avg_y9_y10_unweightedgpa from
            (select studentid, avg(unweighted_gpa) as avg_y9_y10_unweightedgpa, count(unweighted_gpa) as count_gpa from
                (select gpa_by_year.studentid, unweighted_gpa, high_school_class - school_year as year_diff
                from gpa_by_year
                left join hs_enrollment
                on gpa_by_year.studentid = hs_enrollment.studentid) as foo
            where foo.year_diff > 1 and foo.year_diff < 4
            group by studentid) as bar
        where count_gpa =2) as earlier
    on earlier.studentid = later.studentid
    '''
    index_col = 'studentid'
    feature_col = 'unweighted_gpa_diff'
    feature_type = 'numerical'
    # postprocessors = {pp.dummyCodeNull: {}}
    postprocessors = {}
    index_level = "studentid"           


class HighSchoolMostRecentlyAttended(AbstractFeature): #TODO MAKE THIS FEATURE BOUNDABLE 
    name = "high school attended, most recent if multiple enrollments"
    sql_query= "select distinct on (studentid) studentid, name from hs_enrollment left join schools on hs_enrollment.schoolid = schools.schoolid order by studentid, end_date desc"
    index_col="studentid"
    feature_col="name"
    feature_type = "categorical"
    postprocessors = {pp.getdummies:
                        {}}
    index_level = "studentid"

class everSpecialEd(AbstractFeature): 
    name= "whether a student was ever special ed"
    sql_query= "select studentid, ever_special_ed from students"
    index_col = 'studentid'
    feature_col = 'ever_special_ed'
    feature_type = 'boolean'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = "studentid"   

class everFreeLunch(AbstractFeature): 
    name= "whether a student was ever on FRPL"
    sql_query= "select studentid, ever_free_lunch from students"
    index_col = 'studentid'
    feature_col = 'ever_free_lunch'
    feature_type = 'boolean'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = "studentid"

class isFirstGen(AbstractFeature): 
    name= "whether a student is a first generation student"
    sql_query= "select studentid, is_first_gen from students"
    index_col = 'studentid'
    feature_col = 'is_first_gen'
    feature_type = 'boolean'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = "studentid"              


# =======================================
#  College Features
# =======================================

class CollegeName(AbstractFeature):
    name = 'name of the college'
    sql_query =  "select colleges.collegeid, colleges.name as college_name from colleges"
    index_col = "collegeid"
    feature_col = "college_name"
    feature_type = "categorical"
    postprocessors = {pp.getdummies:{}}
    index_level = "collegeid"

class AlumsEverAttendedSpecificCollege(AbstractBoundedFeature):
    name = "Number of alums ever attending a college"
    sql_query = "select count(*) as alum_count, collegeid from enrollments where status not in ('Matriculating', 'Did not matriculate') {} group by collegeid"
    index_col = 'collegeid'
    feature_col = 'alum_count'
    feature_type = 'numerical'
    postprocessors = {pp.fillNullWithZero: {}}
    index_level = 'collegeid'
    bound_col = 'start_date'

class isPrivate(AbstractFeature):
    name = "whether college is private or public"
    sql_query = "select collegeid, isprivate from colleges"
    index_col = 'collegeid'
    feature_col = 'isprivate'
    feature_type = 'boolean'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid'  

class isForProfit(AbstractFeature):
    name = "whether college is for profit"
    sql_query = "select collegeid, isforprofit from colleges"
    index_col = 'collegeid'
    feature_col = 'isforprofit'
    feature_type = 'boolean'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid'      

class is4year(AbstractFeature):
    name = "whether college is four year"
    sql_query = "select collegeid, is4year from colleges"
    index_col = 'collegeid'
    feature_col = 'is4year'
    feature_type = 'boolean'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid'    

class sixYearGraduationRate(AbstractFeature):
    name = "six year graduation rate"
    sql_query = "select collegeid, graduationrate_6yr from colleges"
    index_col = 'collegeid'
    feature_col = 'graduationrate_6yr'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid'

class sixYearMinorityGraduationRate(AbstractFeature):
    name = "six year minority graduation rate"
    sql_query = "select collegeid, graduationrate_minority_6yr from colleges"
    index_col = 'collegeid'
    feature_col = 'graduationrate_minority_6yr'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid' 

class sixYearTransferRate(AbstractFeature):
    name = "six year transfer rate"
    sql_query = "select collegeid, transferrate_6yr from colleges"
    index_col = 'collegeid'
    feature_col = 'transferrate_6yr'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid' 

class sixYearMinorityTransferRate(AbstractFeature):
    name = "six year minority transfer rate"
    sql_query = "select collegeid, transferrate_minority_6yr from colleges"
    index_col = 'collegeid'
    feature_col = 'transferrate_minority_6yr'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid'                     

class historicallyBlackCollege(AbstractFeature):
    name = "if college is an HBCU"
    sql_query = "select collegeid, historicallyblack from colleges"
    index_col = 'collegeid'
    feature_col = 'historicallyblack'
    feature_type = 'boolean'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid' 

class barronsSelectivity(AbstractFeature):
    name = "barrons selectivity score for the college"
    sql_query = "select collegeid, barrons_rating from colleges"
    index_col = 'collegeid'
    feature_col = 'barrons_rating'
    feature_type = 'categorical'
    postprocessors = {pp.getdummies:{}}
    index_level = 'collegeid'  

class distanceFromChicago(AbstractFeature):
    name = "college's distance from chicago in miles"
    sql_query = "select collegeid, dist_from_chicago from colleges"
    index_col = 'collegeid'
    feature_col = 'dist_from_chicago'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid' 

class percentAccepted(AbstractFeature):
    name = "number of students who apply who are accepted"
    sql_query = "select collegeid, perc_accepted from colleges"
    index_col = 'collegeid'
    feature_col = 'perc_accepted'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid' 

class percentEnroll(AbstractFeature):
    name = "percent accepted students who enroll"
    sql_query = "select collegeid, perc_accepted_enroll from colleges"
    index_col = 'collegeid'
    feature_col = 'perc_accepted_enroll'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid'    

class percentFemale(AbstractFeature):
    name = "percent students who are female"
    sql_query = "select collegeid, perc_female from colleges"
    index_col = 'collegeid'
    feature_col = 'perc_female'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid' 

class percentAfricanAmerican(AbstractFeature):
    name = "percent students at the college who are african american"
    sql_query = "select collegeid, perc_african_american from colleges"
    index_col = 'collegeid'
    feature_col = 'perc_african_american'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid'      

class percentHispanic(AbstractFeature):
    name = "percent students at the college who are hispanic"
    sql_query = "select collegeid, perc_hispanic from colleges"
    index_col = 'collegeid'
    feature_col = 'perc_hispanic'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid'

class percentInState(AbstractFeature):
    name = "percent students at the college who from the state"
    sql_query = "select collegeid, percentinstate from colleges"
    index_col = 'collegeid'
    feature_col = 'percentinstate'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid'         

class percentPellGrant(AbstractFeature):
    name = "percent students at the college who are on pell grants"
    sql_query = "select collegeid, percentpellgrant from colleges"
    index_col = 'collegeid'
    feature_col = 'percentpellgrant'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid' 

class avgNetPrice(AbstractFeature):
    name = "average net price paid "
    sql_query = "select collegeid, avgnetprice from colleges"
    index_col = 'collegeid'
    feature_col = 'avgnetprice'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid'                                 

class netPrice0_30(AbstractFeature):
    name = "net price paid by incomes between 0 and 30 k"
    sql_query = "select collegeid, netprice0_30 from colleges"
    index_col = 'collegeid'
    feature_col = 'netprice0_30'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid' 

class netPrice30_48(AbstractFeature):
    name = "net price paid by incomes between 30 and 48 k"
    sql_query = "select collegeid, netprice30_48 from colleges"
    index_col = 'collegeid'
    feature_col = 'netprice30_48'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid'

class netPrice48_75(AbstractFeature):
    name = "net price paid by incomes between 48 and 75 k"
    sql_query = "select collegeid, netprice48_75 from colleges"
    index_col = 'collegeid'
    feature_col = 'netprice48_75'
    feature_type = 'numerical'
    postprocessors = {pp.dummyCodeNull: {}}
    index_level = 'collegeid'    

class locale(AbstractFeature):
    name = "setting of the college, i.e. city, large"
    sql_query = "select collegeid, locale from colleges"
    index_col = 'collegeid'
    feature_col = 'locale'
    feature_type = 'categorical'
    postprocessors = {pp.getdummies:{}}
    index_level = 'collegeid'

class sizeRange(AbstractFeature):
    name = "size of the college"
    sql_query = "select collegeid, size_range from colleges"
    index_col = 'collegeid'
    feature_col = 'size_range'
    feature_type = 'categorical'
    postprocessors = {pp.getdummies:{}}
    index_level = 'collegeid'    

# =======================================
#  Persistence from Semester to Semester (targets)   
# =======================================

class PersistOneSemester(AbstractTargetFeature):

    def __init__(self, lower_bound=None,upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
        if self.lower_bound != None:
            self.lower_bound = (find_nth_pcheck_date(datetime.strptime(lower_bound,'%Y-%m-%d'), -1)).strftime('%Y-%m-%d')
        if self.upper_bound != None:
            self.upper_bound = (find_nth_pcheck_date(datetime.strptime(upper_bound,'%Y-%m-%d'), -1)).strftime('%Y-%m-%d')

    name = "Has a non-null one semester label"
    sql_query='''
        select enrollments.enrollid as enrollid, studentid, collegeid, persist_1_halfyear
        from enrollment_dummies left join (select enrollid, start_date from enrollments) as enrollments
            on enrollment_dummies.enrollid = enrollments.enrollid
        where persist_1_halfyear is not null {}
    '''
    index_col="enrollid"
    feature_col="persist_1_halfyear"
    feature_type = "boolean"
    postprocessors = {}
    # index_level = "enrollid" # set by superclass
    bound_col ='start_date'
    studentid_col="studentid"
    collegeid_col="collegeid"

class PersistTwoSemesters(AbstractTargetFeature):

    def __init__(self, lower_bound=None,upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
        if self.lower_bound != None:
            self.lower_bound = (find_nth_pcheck_date(datetime.strptime(lower_bound,'%Y-%m-%d'), -2)).strftime('%Y-%m-%d')
        if self.upper_bound != None:
            self.upper_bound = (find_nth_pcheck_date(datetime.strptime(upper_bound,'%Y-%m-%d'), -2)).strftime('%Y-%m-%d')

    name = "Has a non-null two semester label"
    sql_query='''
        select enrollments.enrollid as enrollid, studentid, collegeid, persist_2_halfyear
        from enrollment_dummies left join (select enrollid, start_date from enrollments) as enrollments
            on enrollment_dummies.enrollid = enrollments.enrollid
        where persist_2_halfyear is not null {}
    '''
    index_col="enrollid"
    feature_col="persist_2_halfyear"
    feature_type = "boolean"
    postprocessors = {}
    # index_level = "enrollid" # set by superclass
    bound_col ='start_date'
    studentid_col="studentid"
    collegeid_col="collegeid"

class PersistThreeSemesters(AbstractTargetFeature):

    def __init__(self, lower_bound=None,upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
        if self.lower_bound != None:
            self.lower_bound = (find_nth_pcheck_date(datetime.strptime(lower_bound,'%Y-%m-%d'), -3)).strftime('%Y-%m-%d')
        if self.upper_bound != None:
            self.upper_bound = (find_nth_pcheck_date(datetime.strptime(upper_bound,'%Y-%m-%d'), -3)).strftime('%Y-%m-%d')

    name = "Has a non-null three semester label"
    sql_query='''
        select enrollments.enrollid as enrollid, studentid, collegeid, persist_3_halfyear
        from enrollment_dummies left join (select enrollid, start_date from enrollments) as enrollments
            on enrollment_dummies.enrollid = enrollments.enrollid
        where persist_3_halfyear is not null {}
    '''
    index_col="enrollid"
    feature_col="persist_3_halfyear"
    feature_type = "boolean"
    postprocessors = {}
    # index_level = "enrollid" # set by superclass
    bound_col = "start_date"
    studentid_col="studentid"
    collegeid_col="collegeid"

class PersistFourSemesters(AbstractTargetFeature):

    def __init__(self, lower_bound=None,upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
        if self.lower_bound != None:
            self.lower_bound = (find_nth_pcheck_date(datetime.strptime(lower_bound,'%Y-%m-%d'), -4)).strftime('%Y-%m-%d')
        if self.upper_bound != None:
            self.upper_bound = (find_nth_pcheck_date(datetime.strptime(upper_bound,'%Y-%m-%d'), -4)).strftime('%Y-%m-%d')

    name = "Has a non-null four semester label"
    sql_query='''
        select enrollments.enrollid as enrollid, studentid, collegeid, persist_4_halfyear
        from enrollment_dummies left join (select enrollid, start_date from enrollments) as enrollments
            on enrollment_dummies.enrollid = enrollments.enrollid
        where persist_4_halfyear is not null {}
    '''
    index_col="enrollid"
    feature_col="persist_4_halfyear"
    feature_type = "boolean"
    postprocessors = {}
    # index_level = "enrollid" # set by superclass
    bound_col = "start_date"
    studentid_col="studentid"
    collegeid_col="collegeid"

class PersistFiveSemesters(AbstractTargetFeature):

    def __init__(self, lower_bound=None,upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
        if self.lower_bound != None:
            self.lower_bound = (find_nth_pcheck_date(datetime.strptime(lower_bound,'%Y-%m-%d'), -5)).strftime('%Y-%m-%d')
        if self.upper_bound != None:
            self.upper_bound = (find_nth_pcheck_date(datetime.strptime(upper_bound,'%Y-%m-%d'), -5)).strftime('%Y-%m-%d')

    name = "Has a non-null five semester label"
    sql_query='''
        select enrollments.enrollid as enrollid, studentid, collegeid, persist_5_halfyear
        from enrollment_dummies left join (select enrollid, start_date from enrollments) as enrollments
            on enrollment_dummies.enrollid = enrollments.enrollid
        where persist_5_halfyear is not null {}
    '''
    index_col="enrollid"
    feature_col="persist_5_halfyear"
    feature_type = "boolean"
    postprocessors = {}
    # index_level = "enrollid" # set by superclass
    bound_col = "start_date"
    studentid_col="studentid"
    collegeid_col="collegeid"

class PersistSixSemesters(AbstractTargetFeature):

    def __init__(self, lower_bound=None,upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
        if self.lower_bound != None:
            self.lower_bound = (find_nth_pcheck_date(datetime.strptime(lower_bound,'%Y-%m-%d'), -6)).strftime('%Y-%m-%d')
        if self.upper_bound != None:
            self.upper_bound = (find_nth_pcheck_date(datetime.strptime(upper_bound,'%Y-%m-%d'), -6)).strftime('%Y-%m-%d')

    name = "Has a non-null six semester label"
    sql_query='''
        select enrollments.enrollid as enrollid, studentid, collegeid, persist_6_halfyear
        from enrollment_dummies left join (select enrollid, start_date from enrollments) as enrollments
            on enrollment_dummies.enrollid = enrollments.enrollid
        where persist_6_halfyear is not null {}
    '''
    index_col="enrollid"
    feature_col="persist_6_halfyear"
    feature_type = "boolean"
    postprocessors = {}
    # index_level = "enrollid" # set by superclass
    bound_col = "start_date"
    studentid_col="studentid"
    collegeid_col="collegeid"

class PersistSevenSemesters(AbstractTargetFeature):

    def __init__(self, lower_bound=None,upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
        if self.lower_bound != None:
            self.lower_bound = (find_nth_pcheck_date(datetime.strptime(lower_bound,'%Y-%m-%d'), -7)).strftime('%Y-%m-%d')
        if self.upper_bound != None:
            self.upper_bound = (find_nth_pcheck_date(datetime.strptime(upper_bound,'%Y-%m-%d'), -7)).strftime('%Y-%m-%d')

    name = "Has a non-null seven semester label"
    sql_query='''
        select enrollments.enrollid as enrollid, studentid, collegeid, persist_7_halfyear
        from enrollment_dummies left join (select enrollid, start_date from enrollments) as enrollments
            on enrollment_dummies.enrollid = enrollments.enrollid
        where persist_7_halfyear is not null {}
    '''
    index_col="enrollid"
    feature_col="persist_7_halfyear"
    feature_type = "boolean"
    postprocessors = {}
    # index_level = "enrollid" # set by superclass
    bound_col = "start_date"
    studentid_col="studentid"
    collegeid_col="collegeid"

class PersistEightSemesters(AbstractTargetFeature):

    def __init__(self, lower_bound=None,upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
        if self.lower_bound != None:
            self.lower_bound = (find_nth_pcheck_date(datetime.strptime(lower_bound,'%Y-%m-%d'), -8)).strftime('%Y-%m-%d')
        if self.upper_bound != None:
            self.upper_bound = (find_nth_pcheck_date(datetime.strptime(upper_bound,'%Y-%m-%d'), -8)).strftime('%Y-%m-%d')

    name = "Has a non-null eight semester label"
    sql_query='''
        select enrollments.enrollid as enrollid, studentid, collegeid, persist_8_halfyear
        from enrollment_dummies left join (select enrollid, start_date from enrollments) as enrollments
            on enrollment_dummies.enrollid = enrollments.enrollid
        where persist_8_halfyear is not null {}
    '''
    index_col="enrollid"
    feature_col="persist_8_halfyear"
    feature_type = "boolean"
    postprocessors = {}
    # index_level = "enrollid" # set by superclass
    bound_col = "start_date"
    studentid_col="studentid"
    collegeid_col="collegeid"

class EnrollmentStartDate(AbstractFeature):
    name = "The start date of an enrollment."
    sql_query='''
        select enrollid as enrollid, studentid, collegeid, start_date
        from enrollments
    '''
    index_col="enrollid"
    feature_col="start_date"
    feature_type = "date"
    postprocessors = {}
    index_level = "enrollid"

class SpringEnrollment(AbstractFeature):
    name = "If the enrollment happened Dec-April."
    sql_query='''
        select enrollid as enrollid, EXTRACT('month' FROM start_date) in (12,1,2,3,4) as is_spring
        from enrollments
    '''
    index_col="enrollid"
    feature_col="is_spring"
    feature_type = "boolean"
    postprocessors = {}
    index_level = "enrollid"

class NetworkNoble(AbstractTargetFeature):
    name = "Student comes from Noble"
    sql_query='''
        select enrollid as enrollid, network in ('Noble') as is_noble from enrollments
        left join students ON enrollments.studentid = students.studentid {}
    ''' # hacky: this feature doesn't use its bounds ever, as it's practically unbounded
    index_col="enrollid"
    feature_col="is_noble"
    feature_type = "boolean"
    postprocessors = {}
    index_level = "enrollid"
    studentid_col="studentid"
    collegeid_col="collegeid"
    bound_col = None # this is hacky, but works - this feature doesn't need a bound

class NetworkKIPPNJ(AbstractTargetFeature):
    name = "Student comes from KIPP NJ"
    sql_query='''
        select enrollid as enrollid, network in ('KIPP_NJ') as is_kippnj from enrollments
        left join students ON enrollments.studentid = students.studentid {}
    ''' # hacky: this feature doesn't use its bounds ever, as it's practically unbounded
    index_col="enrollid"
    feature_col="is_kippnj"
    feature_type = "boolean"
    postprocessors = {}
    index_level = "enrollid"    
    studentid_col="studentid"
    collegeid_col="collegeid"
    bound_col = None # this is hacky, but works - this feature doesn't need a bound


# =======================================
#  Performance of Students per Year
# =======================================


# number of APs, number of honors, number of courses
class numberAPsYear12(AbstractBoundedFeature):
    name = 'number of AP courses in the last year of HS'
    sql_query='''
        WITH tmp as(
            SELECT
                courses.studentid,
                grade_available,
                course_number,
                was_ap,
                high_school_class-year_taken as yearsbeforegrad
            FROM courses
            LEFT JOIN hs_enrollment ON courses.studentid=hs_enrollment.studentid
        )
        SELECT studentid as studentid, sum(was_ap::int) as numberapsyear12
        FROM tmp
        where yearsbeforegrad=0 {}
        GROUP BY studentid
    '''
    index_col = "studentid"
    feature_col = "numberapsyear12"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col = "grade_available"

class numberAPsYear11(AbstractBoundedFeature):
    name = 'number of AP courses in the second to last year of HS'
    sql_query='''
        WITH tmp as(
            SELECT
                courses.studentid,
                grade_available,
                course_number,
                was_ap,
                high_school_class-year_taken as yearsbeforegrad
            FROM courses
            LEFT JOIN hs_enrollment ON courses.studentid=hs_enrollment.studentid
        )
        SELECT studentid as studentid, sum(was_ap::int) as numberapsyear11
        FROM tmp
        where yearsbeforegrad=1 {}
        GROUP BY studentid
    '''
    index_col = "studentid"
    feature_col = "numberapsyear11"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col = "grade_available"

class numberAPsYear10(AbstractBoundedFeature):
    name = 'number of AP courses in the third to last year of HS'
    sql_query='''
        WITH tmp as(
            SELECT
                courses.studentid,
                grade_available,
                course_number,
                was_ap,
                high_school_class-year_taken as yearsbeforegrad
            FROM courses
            LEFT JOIN hs_enrollment ON courses.studentid=hs_enrollment.studentid
        )
        SELECT studentid as studentid, sum(was_ap::int) as numberapsyear10
        FROM tmp
        where yearsbeforegrad=2 {}
        GROUP BY studentid
    '''
    index_col = "studentid"
    feature_col = "numberapsyear10"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col = "grade_available"

class numberHonsYear12(AbstractBoundedFeature):
    name = 'number of honor courses in the last year of HS'
    sql_query='''
        WITH tmp as(
            SELECT
                courses.studentid,
                grade_available,
                course_number,
                was_honors,
                high_school_class-year_taken as yearsbeforegrad
            FROM courses
            LEFT JOIN hs_enrollment ON courses.studentid=hs_enrollment.studentid
        )
        SELECT studentid as studentid, sum(was_honors::int) as numberhonsyear12
        FROM tmp
        where yearsbeforegrad=0 {}
        GROUP BY studentid
    '''
    index_col = "studentid"
    feature_col = "numberhonsyear12"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col = "grade_available"

class numberHonsYear11(AbstractBoundedFeature):
    name = 'number of honor courses in the second to last year of HS'
    sql_query='''
        WITH tmp as(
            SELECT
                courses.studentid,
                grade_available,
                course_number,
                was_honors,
                high_school_class-year_taken as yearsbeforegrad
            FROM courses
            LEFT JOIN hs_enrollment ON courses.studentid=hs_enrollment.studentid
        )
        SELECT studentid as studentid, sum(was_honors::int) as numberhonsyear11
        FROM tmp
        where yearsbeforegrad=1 {}
        GROUP BY studentid
    '''
    index_col = "studentid"
    feature_col = "numberhonsyear11"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col = "grade_available"

class numberHonsYear10(AbstractBoundedFeature):
    name = 'number of honor courses in the third to last year of HS'
    sql_query='''
        WITH tmp as(
            SELECT
                courses.studentid,
                grade_available,
                course_number,
                was_honors,
                high_school_class-year_taken as yearsbeforegrad
            FROM courses
            LEFT JOIN hs_enrollment ON courses.studentid=hs_enrollment.studentid
        )
        SELECT studentid as studentid, sum(was_honors::int) as numberhonsyear10
        FROM tmp
        where yearsbeforegrad=2 {}
        GROUP BY studentid
    '''
    index_col = "studentid"
    feature_col = "numberhonsyear10"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col = "grade_available"

class maxRankYear12(AbstractBoundedFeature):

    def __init__(self, lower_bound = None, upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
            
    name = "maximum percentile rank of percent grade per student for the last year of HS"
    sql_query = '''
        WITH tmp as(
            SELECT
                courses.studentid,
                semester_taken,
                grade_available,
                course_number,
                high_school_class-year_taken as yearsbeforegrad,
                percent_rank() OVER (PARTITION BY course_number ORDER BY percent_grade) AS percrank
            FROM courses
            LEFT JOIN hs_enrollment ON courses.studentid=hs_enrollment.studentid
        )
        SELECT studentid as studentid, max(percrank) as maxrankyear12
        FROM tmp
        where yearsbeforegrad=0 and semester_taken!=3 {}
        GROUP BY studentid
    '''
    index_col = "studentid"
    feature_col = "maxrankyear12"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col = "grade_available"


class minRankYear12(AbstractBoundedFeature):

    def __init__(self, lower_bound = None, upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
            
    name = "minimum percentile rank of percent grade per student for the last year of HS"
    sql_query = '''
        WITH tmp as(
            SELECT
                courses.studentid,
                semester_taken,
                grade_available,
                course_number,
                high_school_class-year_taken as yearsbeforegrad,
                percent_rank() OVER (PARTITION BY course_number ORDER BY percent_grade) AS percrank
            FROM courses
            LEFT JOIN hs_enrollment ON courses.studentid=hs_enrollment.studentid
        )
        SELECT studentid as studentid, min(percrank) as minrankyear12
        FROM tmp
        where yearsbeforegrad=0 and semester_taken!=3 {}
        GROUP BY studentid
    '''
    index_col = "studentid"
    feature_col = "minrankyear12"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col = "grade_available"

class maxRankYear11(AbstractBoundedFeature):

    def __init__(self, lower_bound = None, upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
            
    name = "maximum percentile rank of percent grade per student for the second to last year of HS"
    sql_query = '''
        WITH tmp as(
            SELECT
                courses.studentid,
                semester_taken,
                grade_available,
                course_number,
                high_school_class-year_taken as yearsbeforegrad,
                percent_rank() OVER (PARTITION BY course_number ORDER BY percent_grade) AS percrank
            FROM courses
            LEFT JOIN hs_enrollment ON courses.studentid=hs_enrollment.studentid
        )
        SELECT studentid as studentid, max(percrank) as maxrankyear11
        FROM tmp
        where yearsbeforegrad=1 and semester_taken!=3  {}
        GROUP BY studentid
    '''
    index_col = "studentid"
    feature_col = "maxrankyear11"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col = "grade_available"


class minRankYear11(AbstractBoundedFeature):

    def __init__(self, lower_bound = None, upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
            
    name = "minimum percentile rank of percent grade per student for the second to last year of HS"
    sql_query = '''
        WITH tmp as(
            SELECT
                courses.studentid,
                semester_taken,
                grade_available,
                course_number,
                high_school_class-year_taken as yearsbeforegrad,
                percent_rank() OVER (PARTITION BY course_number ORDER BY percent_grade) AS percrank
            FROM courses
            LEFT JOIN hs_enrollment ON courses.studentid=hs_enrollment.studentid
        )
        SELECT studentid as studentid, min(percrank) as minrankyear11
        FROM tmp
        where yearsbeforegrad=1 and semester_taken!=3 {}
        GROUP BY studentid
    '''
    index_col = "studentid"
    feature_col = "minrankyear11"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col = "grade_available"

class maxRankYear10(AbstractBoundedFeature):

    def __init__(self, lower_bound = None, upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
            
    name = "maximum percentile rank of percent grade per student for the third to last year of HS"
    sql_query = '''
        WITH tmp as(
            SELECT
                courses.studentid,
                semester_taken,
                grade_available,
                course_number,
                high_school_class-year_taken as yearsbeforegrad,
                percent_rank() OVER (PARTITION BY course_number ORDER BY percent_grade) AS percrank
            FROM courses
            LEFT JOIN hs_enrollment ON courses.studentid=hs_enrollment.studentid
        )
        SELECT studentid as studentid, max(percrank) as maxrankyear10
        FROM tmp
        where yearsbeforegrad=2 and semester_taken!=3 {}
        GROUP BY studentid
    '''
    index_col = "studentid"
    feature_col = "maxrankyear10"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col = "grade_available"


class minRankYear10(AbstractBoundedFeature):

    def __init__(self, lower_bound = None, upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
            
    name = "minimum percentile rank of percent grade per student for the third to last year of HS"
    sql_query = '''
        WITH tmp as(
            SELECT
                courses.studentid,
                semester_taken,
                grade_available,
                course_number,
                high_school_class-year_taken as yearsbeforegrad,
                percent_rank() OVER (PARTITION BY course_number ORDER BY percent_grade) AS percrank
            FROM courses
            LEFT JOIN hs_enrollment ON courses.studentid=hs_enrollment.studentid
        )
        SELECT studentid as studentid, min(percrank) as minrankyear10
        FROM tmp
        where yearsbeforegrad=2 and semester_taken!=3 {}
        GROUP BY studentid
    '''
    index_col = "studentid"
    feature_col = "minrankyear10"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col = "grade_available"

class avgRankYear12(AbstractBoundedFeature):

    def __init__(self, lower_bound = None, upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
            
    name = "average percent rank in percent grade per student for the last year of HS"
    sql_query = '''
        WITH tmp as(
            SELECT
                courses.studentid,
                semester_taken,
                grade_available,
                course_number,
                high_school_class-year_taken as yearsbeforegrad,
                percent_rank() OVER (PARTITION BY course_number ORDER BY percent_grade) AS percrank
            FROM courses
            LEFT JOIN hs_enrollment ON courses.studentid=hs_enrollment.studentid
        )
        SELECT studentid as studentid, avg(percrank) as avgrankyear12
        FROM tmp
        where yearsbeforegrad=0 and semester_taken!=3 {}
        GROUP BY studentid
    '''
    index_col = "studentid"
    feature_col = "avgrankyear12"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col = "grade_available"

class avgRankYear11(AbstractBoundedFeature):

    def __init__(self, lower_bound = None, upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
            
    name = "average percent rank in percent grade per student for second to last year of HS"
    sql_query = '''
        WITH tmp as(
            SELECT
                courses.studentid,
                semester_taken,
                grade_available,
                course_number,
                high_school_class-year_taken as yearsbeforegrad,
                percent_rank() OVER (PARTITION BY course_number ORDER BY percent_grade) AS percrank
            FROM courses
            LEFT JOIN hs_enrollment ON courses.studentid=hs_enrollment.studentid
        )
        SELECT studentid as studentid, avg(percrank) as avgrankyear11
        FROM tmp
        where yearsbeforegrad=1 and semester_taken!=3 {}
        GROUP BY studentid
    '''
    index_col = "studentid"
    feature_col = "avgrankyear11"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col = "grade_available"

class avgRankYear10(AbstractBoundedFeature):

    def __init__(self, lower_bound = None, upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
            
    name = "average percent rank in percent grade per student for the third to last year of HS"
    sql_query = '''
        WITH tmp as(
            SELECT
                courses.studentid,
                semester_taken,
                grade_available,
                course_number,
                high_school_class-year_taken as yearsbeforegrad,
                percent_rank() OVER (PARTITION BY course_number ORDER BY percent_grade) AS percrank
            FROM courses
            LEFT JOIN hs_enrollment ON courses.studentid=hs_enrollment.studentid
        )
        SELECT studentid as studentid, avg(percrank) as avgrankyear10
        FROM tmp
        where yearsbeforegrad=2 and semester_taken!=3 {}
        GROUP BY studentid
    '''
    index_col = "studentid"
    feature_col = "avgrankyear10"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col = "grade_available"


# =======================================
#  Standardized Tests
# =======================================

class highestCompositeACT(AbstractBoundedFeature):
    def __init__(self, lower_bound=None,upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)

    name = "highest act score achieved"
    sql_query= "select studentid, max(score_composite) as max_composite_act_score from acttests where test_level = 'ACT' {} group by studentid"
    index_col="studentid"
    feature_col="max_composite_act_score"
    feature_type = "numerical"
    postprocessors = {pp.dummyCodeNull:
                        {}}
    index_level = "studentid"
    bound_col ='date'

# =======================================
#  Contact Features
# =======================================

class totalNumberofContacts(AbstractBoundedFeature):
    def __init__(self, lower_bound=None,upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)

    name = "total number of counselor contact events"
    sql_query= "select studentid , count(*) as num_contacts from contacts {} group by studentid"
    index_col="studentid"
    feature_col="num_contacts"
    feature_type = "numerical"
    postprocessors = {pp.fillNullWithZero:
                        {}}
    index_level = "studentid"
    bound_col ='contact_date'

class numberContactsInitiatedByStudent(AbstractBoundedFeature):
    def __init__(self, lower_bound=None,upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)

    name = "total number of contact events initiated by student"
    sql_query= "select studentid , count(*) as student_initiated_contacts from contacts where initiated_by_student = True {} group by studentid"
    index_col="studentid"
    feature_col="student_initiated_contacts"
    feature_type = "numerical"
    postprocessors = {pp.fillNullWithZero:
                        {}}
    index_level = "studentid"
    bound_col ='contact_date'

class numberUnsuccessfulContacts(AbstractBoundedFeature):
    def __init__(self, lower_bound=None,upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)

    name = "total number of unsuccessful contact events"
    sql_query= "select studentid , count(*) as total_unsuccessful_contacts from contacts where was_successful = False {} group by studentid"
    index_col="studentid"
    feature_col="total_unsuccessful_contacts"
    feature_type = "numerical"
    postprocessors = {pp.fillNullWithZero:
                        {}}
    index_level = "studentid"
    bound_col ='contact_date'

class mediumOfLastSuccessfulContact(AbstractBoundedFeature):
    def __init__(self, lower_bound=None,upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)

    name = "contact medium of last successful contact"
    sql_query= "select distinct on (studentid) studentid, contact_medium as last_contact_medium from contacts where was_successful = True {} order by studentid, contact_date desc"
    index_col="studentid"
    feature_col="last_contact_medium"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col ='contact_date'

class numberOfCounselors(AbstractBoundedFeature):
    def __init__(self, lower_bound=None,upper_bound = None):
        AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)

    name = "number of different people contacting a student"
    sql_query= "select count(distinct(counselor_id)) as number_counselors, studentid from contacts {} group by studentid"
    index_col="studentid"
    feature_col="number_counselors"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col ='contact_date'


class ContactMediumPercentages(AbstractBoundedPandasFeature):

    def __init__(self, lower_bound = None, upper_bound = None):
        AbstractBoundedPandasFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)

    def process(self):
        counts = self.rows.groupby(['studentid', 'contact_medium']).size()
        percentages = counts.groupby(level=0).apply(lambda x:100*x/float(x.sum()))
        percentages_by_student = percentages.unstack().fillna(0).reset_index()
        self.rows = percentages_by_student

    name = "Has a non-null two semester label"
    sql_query= "select studentid, contact_medium from contacts {}"
    index_col="studentid"
    feature_col = "contact_medium"
    feature_type = "boolean"
    postprocessors = {pp.fillNullWithZero:
                        {}}
    index_level = "studentid"
    bound_col ='contact_date'

# TODO GET THIS FEATURE TO WORK
# class daysSinceLastSuccessfulContact(AbstractBoundedFeature):
#     def __init__(self, lower_bound=None,upper_bound = None):
#         AbstractBoundedFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
#         self.sql_query = "select distinct on (studentid) studentid,(%s::date - contact_date::date) from contacts where was_successful = True and {} order by studentid, contact_date desc"%upper_bound


#     name = "days since last successful contact with student"
#     index_col="studentid"
#     feature_col="days_since_successful_contact"
#     feature_type = "numerical"
#     postprocessors = {pp.fillNullWithZero:
#                         {'column':feature_col}}
#     index_level = "studentid"
#     bound_col ='contact_date'

# =======================================
#  Enrollment Features
# =======================================

class degreeSubject(AbstractBoundedFeature):
    name = "type of degree pursued, ex: Bachelor of Arts"
    sql_query = "select enrollid, degree_subject from enrollments {}"
    index_col = 'enrollid'
    feature_col = 'degree_subject'
    feature_type = 'categorical'
    postprocessors = {pp.getdummies:{}}
    index_level = 'enrollid'
    bound_col = 'start_date'

class Major(AbstractBoundedFeature):
    name = "type of major pursued"
    sql_query = "select enrollid, major from enrollments {}"
    index_col = 'enrollid'
    feature_col = 'major'
    feature_type = 'categorical'
    postprocessors = {pp.getdummies:{}}
    index_level = 'enrollid'
    bound_col = 'start_date'

class DegreeType(AbstractTargetFeature):
    name = "level of degree pursued"
    sql_query = 'select enrollid as enrollid, studentid, collegeid, degree_type from enrollments {}'
    index_col = 'enrollid'
    # index_level = "enrollid" # set by superclass
    studentid_col='studentid'
    collegeid_col='collegeid'
    feature_col = 'degree_type'
    feature_type = 'categorical'
    postprocessors = {pp.getdummies:{}}
    bound_col = 'start_date'

class IsFirstEnrollment(AbstractFeature):

    name = 'true if this is the students first enrollment'
    index_col = 'enrollid'
    feature_col = 'is_first_enrollment'
    feature_type = 'boolean'
    postprocessors = {}
    index_level = 'enrollid'
    sql_query = '''
                WITH firstenroll AS(
                SELECT min(start_date) as firstEnrollDate, studentid as studentid
                FROM enrollments
                WHERE status != 'Did not matriculate'
                GROUP BY studentid
            )
            SELECT
               enrollid AS enrollid,
                CASE WHEN (firstenroll.firstEnrollDate < start_date)
                    THEN FALSE
                    ELSE TRUE
                END AS is_first_enrollment
            FROM enrollments
            LEFT JOIN firstenroll ON firstenroll.studentid = enrollments.studentid
            '''


# =======================================
#  Attendance Features
# =======================================       

# count of tardy events per student per year
# count of excused and unexcused absences per student per year
# per school: avg. number of tardy events for one student


class YearlyAvgAttendance(AbstractBoundedPandasFeature):

    sql_query = 'this will be overwritten in a second - hackyhacky'
    index_col="studentid"
    feature_col="count, yearsbeforegrad, attendance_type"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col ='attendance_date'
    name = "avgerage yearly absences per student"
    postprocessors = {pp.fillNullWithZero:
                    {}}

    def __init__(self, lower_bound=None,upper_bound = None,
                attendance_type_list = ['tardy','unexcused','suspended','excused','early_dismissal'],
                yearsbeforegrad_list = [0,1,2,3]):

        if len(set(attendance_type_list)-set(
            ['tardy','unexcused','suspended','excused','early_dismissal'])) > 0:
            raise ValueError("The attendance_types need to be in ['tardy','unexcused','suspended','excused','early_dismissal']")

        if len(set(yearsbeforegrad_list)-set(range(7)))>0:
            raise ValueError("yearsbeforegrad_list needs to be in [0,1,2,3,4,5,6]")

        self.attendance_type_list = attendance_type_list
        self.yearsbeforegrad_list = yearsbeforegrad_list

        self.sql_query = '''
            with tmp as (
                select attendance.studentid,
                       count(attendance_type),
                       attendance_type,
                       high_school_class-school_year as yearsbeforegrad
                from attendance
                left join hs_enrollment ON hs_enrollment.studentid = attendance.studentid
                where attendance_type in (%s)
                and high_school_class-school_year in (%s)
                {}
                group by attendance.studentid, yearsbeforegrad, attendance_type
            )
            select studentid, avg(count)
            from tmp
            group by studentid
            '''%( ','.join("'{0}'".format(x) for x in self.attendance_type_list),
                  ','.join(map(str,self.yearsbeforegrad_list))
                 )

        AbstractBoundedPandasFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)
        
    def process(self):

        # rearrange values into columns
        # self.rows = self.rows.set_index(['studentid','attendance_type'])
        self.rows = self.rows.set_index(['studentid'])
        # self.rows = self.rows.unstack(['attendance_type']).fillna(0)
        self.rows = self.rows.fillna(0)
        self.rows = self.rows.rename(columns={'avg':self.__class__.__name__})

        # flatten and rename the hierarchical columns
        # self.rows.columns = ['_'.join(map(str,col)).strip('_ ') for col in self.rows.columns.values]
        self.rows = self.rows.reset_index()


class TotalAttendance(AbstractBoundedPandasFeature):

    sql_query = 'this will be overwritten in a second - hackyhacky'
    name = "total HS absences per student"
    index_col="studentid"
    feature_col="count, attendance_type"
    feature_type = "numerical"
    postprocessors = {}
    index_level = "studentid"
    bound_col ='attendance_date'
    postprocessors = {pp.fillNullWithZero:
                    {}}

    def __init__(self, lower_bound=None,upper_bound = None,
                attendance_type_list = ['tardy','unexcused','suspended','excused','early_dismissal'],
                yearsbeforegrad_list = [0,1,2,3]):

        if len(set(yearsbeforegrad_list)-set(range(7)))>0:
            raise ValueError("yearsbeforegrad_list needs to be in [0,1,2,3,4,5,6]")

        if len(set(attendance_type_list)-set(
            ['tardy','unexcused','suspended','excused','early_dismissal'])) > 0:
            raise ValueError("The attendance_types need to be in ['tardy','unexcused','suspended','excused','early_dismissal']")

        self.yearsbeforegrad_list = yearsbeforegrad_list
        self.attendance_type_list = attendance_type_list

        self.sql_query = '''
            select attendance.studentid,
                   count(attendance_type),
                   attendance_type
            from attendance
            left join hs_enrollment ON hs_enrollment.studentid = attendance.studentid
            where attendance_type in (%s)
            and high_school_class-school_year in (%s)
            {}
            group by attendance.studentid, attendance_type
            '''%( ','.join("'{0}'".format(x) for x in self.attendance_type_list),
                  ','.join(map(str,self.yearsbeforegrad_list))
                 )
        
        AbstractBoundedPandasFeature.__init__(self, lower_bound=lower_bound, upper_bound=upper_bound)

    def process(self):

        # rearrange values into columns
        self.rows = self.rows.set_index(['studentid','attendance_type'])
        self.rows = self.rows.unstack(['attendance_type']).fillna(0)
        self.rows = self.rows.rename(columns={'count':'HSTotal'})

        # flatten and rename the hierarchical columns
        self.rows.columns = ['_'.join(map(str,col)).strip('_ ') for col in self.rows.columns.values]
        self.rows = self.rows.reset_index()


# TODO: should missing values really be filled with zeros??

class AvgHSTardy(YearlyAvgAttendance):
    name = "avg HS tardy events per student"
    def __init__(self, lower_bound=None,upper_bound = None):
        YearlyAvgAttendance.__init__(self,lower_bound=None,upper_bound = None,
                attendance_type_list = ['tardy'],
                yearsbeforegrad_list = [0,1,2,3])

class AvgHSSuspended(YearlyAvgAttendance):
    name = "avg HS tardy events per student"
    def __init__(self, lower_bound=None,upper_bound = None):
        YearlyAvgAttendance.__init__(self,lower_bound=None,upper_bound = None,
                attendance_type_list = ['suspended'],
                yearsbeforegrad_list = [0,1,2,3])

class AvgHSExcused(YearlyAvgAttendance):
    name = "avg HS tardy events per student"
    def __init__(self, lower_bound=None,upper_bound = None):
        YearlyAvgAttendance.__init__(self,lower_bound=None,upper_bound = None,
                attendance_type_list = ['excused'],
                yearsbeforegrad_list = [0,1,2,3])

class AvgHSUnexcused(YearlyAvgAttendance):
    name = "avg HS tardy events per student"
    def __init__(self, lower_bound=None,upper_bound = None):
        YearlyAvgAttendance.__init__(self,lower_bound=None,upper_bound = None,
                attendance_type_list = ['unexcused'],
                yearsbeforegrad_list = [0,1,2,3])

class AvgHSEarlyDismissal(YearlyAvgAttendance):
    name = "avg HS tardy events per student"
    def __init__(self, lower_bound=None,upper_bound = None):
        YearlyAvgAttendance.__init__(self,lower_bound=None,upper_bound = None,
                attendance_type_list = ['early_dismissal'],
                yearsbeforegrad_list = [0,1,2,3])

class AvgHSAway(YearlyAvgAttendance):
    name = "avg HS tardy events per student"
    def __init__(self, lower_bound=None,upper_bound = None):
        YearlyAvgAttendance.__init__(self,lower_bound=None,upper_bound = None,
                attendance_type_list = ['tardy','unexcused', 'early_dismissal'],
                yearsbeforegrad_list = [0,1,2,3])


class AvgMSTardy(YearlyAvgAttendance):
    name = "avg MS tardy events per student"
    def __init__(self, lower_bound=None,upper_bound = None):
        YearlyAvgAttendance.__init__(self,lower_bound=None,upper_bound = None,
                attendance_type_list = ['tardy'],
                yearsbeforegrad_list = [4,5,6])

class AvgMSSuspended(YearlyAvgAttendance):
    name = "avg MS tardy events per student"
    def __init__(self, lower_bound=None,upper_bound = None):
        YearlyAvgAttendance.__init__(self,lower_bound=None,upper_bound = None,
                attendance_type_list = ['suspended'],
                yearsbeforegrad_list = [4,5,6])

class AvgMSExcused(YearlyAvgAttendance):
    name = "avg MS tardy events per student"
    def __init__(self, lower_bound=None,upper_bound = None):
        YearlyAvgAttendance.__init__(self,lower_bound=None,upper_bound = None,
                attendance_type_list = ['excused'],
                yearsbeforegrad_list = [4,5,6])

class AvgMSUnexcused(YearlyAvgAttendance):
    name = "avg MS tardy events per student"
    def __init__(self, lower_bound=None,upper_bound = None):
        YearlyAvgAttendance.__init__(self,lower_bound=None,upper_bound = None,
                attendance_type_list = ['unexcused'],
                yearsbeforegrad_list = [4,5,6])

class AvgMSEarlyDismissal(YearlyAvgAttendance):
    name = "avg MS tardy events per student"
    def __init__(self, lower_bound=None,upper_bound = None):
        YearlyAvgAttendance.__init__(self,lower_bound=None,upper_bound = None,
                attendance_type_list = ['early_dismissal'],
                yearsbeforegrad_list = [4,5,6])

class AvgMSAway(YearlyAvgAttendance):
    name = "total HS tardy events per student"
    def __init__(self, lower_bound=None,upper_bound = None):
        YearlyAvgAttendance.__init__(self,lower_bound=None,upper_bound = None,
                attendance_type_list = ['tardy','unexcused', 'early_dismissal'],
                yearsbeforegrad_list = [4,5,6])


        #['tardy','unexcused','suspended','excused','early_dismissal']
