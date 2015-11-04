''' Contains functions to clean KIPP Foundation data.

These were not used during the Summer 2015 project.

Inputs:
Each cleaning function takes a dataframe (or many dataframes) as inputs.
These input dataframes are "raw", loaded directly from CSVs provided by partners.

Output:
Each cleaning function returns a "cleaned" dataframe, which is in the correct
format to be uploaded directly into the database.
'''

import pandas as pd
import numpy as np
import config
import csv
from util.integer_withNA_tostr import int_with_NaN_tostr

###################################################################
# CLEANER FUNCTIONS
###################################################################        


def acttests(df_tests):
    ''' Clean KIPP Foundation ACT data
    '''

    # Load KIPP Foundation's Standardized Test data into Pandas
    # df_tests = pd.read_csv(config.PERSISTENCE_PATH + '/data/kipp_foundation/raw_data/may_21/College Persistence/Standardized_Tests.csv')

    # Extract ACT tests, and drop empty columns
    kipp_act = df_tests[df_tests.Test_Type__c == "ACT"].dropna(axis=1,how='all')


    # Prepare kippid
    sql_act=pd.DataFrame(kipp_act['Id'])
    sql_act.columns = ['kippid']

    # Prepare date
    sql_act['date'] = kipp_act['Date__c']


    ## Prepare score_composite

    # Details: http://www.actstudent.org/scores/understand/
    # The Composite Score is the average of your four test scores, rounded to the nearest whole number. Fractions less than one-half are rounded down; fractions one-half or more are rounded up.

    # KIPP has two values for some reason: Overall_Score and ACT_Composite. Explore when they're different
    scorediscrepancy = (kipp_act.Overall_Score__c != kipp_act.ACT_Composite__c)
    scorediscrepancy.value_counts()
    kipp_act[scorediscrepancy][['Overall_Score__c','ACT_Composite__c']]
    # --> Only have discrepancies when ACT_Composite is missing. Let's use ACT Composite variable

    # Note: Valid ACT scores are 1 to 36. Replace 0s with missing.

    # Load into sql dataframe
    sql_act['score_composite']=kipp_act['ACT_Composite__c'].replace(0,np.nan).apply(int_with_NaN_tostr)


    ## Prepare score_english
    sql_act['score_english']=kipp_act['ACT_English__c'].replace(0,np.nan).apply(int_with_NaN_tostr)

    ## Prepare score_math
    sql_act['score_math']=kipp_act['ACT_Math__c'].replace(0,np.nan).apply(int_with_NaN_tostr)

    ## Prepare score_reading
    sql_act['score_reading']=kipp_act['ACT_Reading__c'].replace(0,np.nan).apply(int_with_NaN_tostr)

    ## Prepare score_science
    sql_act['score_science']=kipp_act['ACT_Science__c'].replace(0,np.nan).apply(int_with_NaN_tostr)

    ## Prepare score_writing
    # http://www.actstudent.org/writing/writing-scores.html
    sql_act['score_writing']=kipp_act['ACT_Writing__c'].replace(0,np.nan).apply(int_with_NaN_tostr)

    ## Prepare onlyhighestavailable
    sql_act['onlyhighestavailable']=False # KIPP Foundation provides repeated ACT scores, not just highest one.


    ## Return clean dataframe
    return sql_act
