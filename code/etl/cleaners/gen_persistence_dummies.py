''' Contains function that fetches enrollment records from SQL database and returns persistence dummies.
'''

from util import cred # import credentials
from util.SQL_helpers import connect_to_db
import pandas as pd
import math
import config
from datetime import date, datetime
from util.integer_withNA_tostr import *
from util.pcheck_date_functions import find_nth_pcheck_date
import tempfile

def persist_n_halfyear(enrollrow,n):
    '''Given a row from the enrollments table, returns whether or not
    the student persisted n semesters.'''

    # Throw error if missing start_date
    if isinstance(enrollrow['start_date'], pd.tslib.Timestamp)==False:
        raise ValueError('start_date is missing')

    # Find "magic date" date to check whether student is enrolled in nth semester
    checkdate = find_nth_pcheck_date(enrollrow['start_date'],n)

    # Find cutoff date of student's enrollment
    if enrollrow['status']=='Matriculating':
        return np.nan
    elif enrollrow['status']=='Did not matriculate':
        return False
    elif enrollrow['status'] in ['Transferred out','Withdrew','Graduated']:
        if enrollrow['end_date'] is pd.NaT:
            return np.nan
        elif enrollrow['status']=='Transferred out':
            return True if checkdate < enrollrow['end_date'] else False
        elif enrollrow['status']=='Withdrew':
                return True if checkdate < enrollrow['end_date'] else False
        elif enrollrow['status']=='Graduated':
            return True if checkdate < enrollrow['end_date'] else np.nan
    elif enrollrow['status']=='Attending':
        if enrollrow['date_last_verified'] is pd.NaT:
            return np.nan
        return True if checkdate < enrollrow['date_last_verified'] else np.nan
    else:
        raise ValueError('enrollment status %s is not handled in persist_n_semesters()' % enrollrow['status'])

def gen_persistence_dummies_df():
    ''' Fetches enrollments table from SQL database and returns enrollment dummies dataframe
    '''

    # Load data
    with connect_to_db(cred.host, cred.user, cred.pw,cred.dbname) as connection:
        enrollments = pd.read_sql('select enrollid, studentid, collegeid, start_date, end_date, date_last_verified, status from common.enrollments;',connection)

    # Convert date vars to datetime
    datevars = ['start_date','end_date', 'date_last_verified']
    for dtcol in datevars:
        enrollments[dtcol] = pd.to_datetime(enrollments[dtcol])

    # Drop rows missing necessary vars
    mandatoryvars = ['studentid','collegeid','start_date','status']
    irows = enrollments.shape[0]
    for mcol in mandatoryvars:
        enrollments = enrollments[ enrollments[mcol].notnull() ]    
    frows = enrollments.shape[0]
    print 'Dropped enrollment rows with missing ' + ', '.join(mandatoryvars)
    print '--> Row count went from %d to %d.' %(irows,frows)

    # Change studentid & collegeid from float to int
    enrollments['studentid'] = enrollments['studentid'].astype(int)
    enrollments['collegeid'] = enrollments['collegeid'].astype(int)

    # Create persistence dummies
    for n in range(1,9):
        enrollments['persist_'+str(n)+'_halfyear'] = enrollments.apply(lambda row: persist_n_halfyear(row,n), axis=1)

    # Return dataframe
    return enrollments.drop(['start_date','end_date','date_last_verified','status'],axis=1)
