''' Defines two classes that create tables & load data to our Postgres database.

These are used to create and populate all the tables that are in our database.
'''

import pandas as pd
import psycopg2
import re
import os
import tempfile
from util import cred # load SQL credentials
from util.SQL_helpers import connect_to_db

def CreateSQLTable(cur,sqlcmd_createtable,sqltable_name):
    try:
        cur.execute(sqlcmd_createtable)
    except psycopg2.ProgrammingError, e:
        # give more informative error message if table already exists
        tableexists_pattern = re.compile('^relation \".+\" already exists$')
        if tableexists_pattern.match(str(e))!=None:
            raise IOError('Table ' + sqltable_name + ' already exists in the database. Use overwrite=True if desired.')
        else:
            raise e

def CreateTable_to_ColList(sqlcols):
    '''
    @description: Given column section of a CREATE TABLE statement, returns the list of column names.
    @param sqlcols: String containing the column stub of a CREATE TABLE statement.
    '''

    sqlcmd_createtemptable = '''
        CREATE TABLE public.temp_colparser (
            {cols}
        );
        '''.format(cols=sqlcols)

    sqlcmd_fetchcols = '''
    SELECT column_name
    FROM information_schema.columns
      WHERE table_name = \'temp_colparser\'
      AND table_schema = \'public\';'''

    with connect_to_db(cred.host, cred.user, cred.pw, cred.dbname) as conn: # use context managed db connection
        with conn.cursor() as cur:
            cur.execute(sqlcmd_createtemptable)

            cur.execute(sqlcmd_fetchcols)
            queried_columns = cur.fetchall()

    return [c[0] for c in queried_columns]

class UploadTable(object):

    def __init__(self,rawcsv_dict,rawtoclean_fn,sqltable_name,sqltable_cols,cols_toindex='',overwrite=False,upload=True):
        ''' Loads a table into our Postgres database from local data. Performs 4 steps:

                1. Load raw CSVs into dataframes
                2. Map raw dataframes to 1 clean dataframe.
                3. Export clean dataframe to clean CSV.
                4. Load clean CSV to Postgres database.
        '''

        # Create dictionary of dataframes
        self.rawdf_dict = {key:pd.read_csv(val) for key,val in rawcsv_dict.items()}

        # Create clean dataframe
        self.cleandf = rawtoclean_fn(**self.rawdf_dict)

        # Write clean dataframe to CSV
        cleancsv = tempfile.NamedTemporaryFile()
        self.cleandf.to_csv(cleancsv.name, na_rep='NaN', header=False, index=False)

        # Generate the CREATE TABLE statement
        self.sqlcmd_createtable = '''
            {drop}

            CREATE TABLE {name}(
                {cols}
            );
            '''.format(drop='DROP TABLE IF EXISTS ' + sqltable_name + ';' if overwrite==True else '',
                       name=sqltable_name,
                       cols=sqltable_cols['partnerids']+sqltable_cols['data'])

        # Generate CREATE INDEX statements
        self.sqlcmd_createindex = '\n'.join( ['CREATE INDEX on ' + sqltable_name + '(' + col + ');' for col in cols_toindex] )

        # Upload clean CSV to SQL database
        if upload==True:
            with connect_to_db(cred.host, cred.user, cred.pw, cred.dbname) as conn: # use context managed db connection
                with conn.cursor() as cur:
                    with open(cleancsv.name,"r") as f:

                        # create the temporary table
                        CreateSQLTable(cur=cur,sqlcmd_createtable=self.sqlcmd_createtable,sqltable_name=sqltable_name)

                        # create indices if specified
                        if self.sqlcmd_createindex: cur.execute(self.sqlcmd_createindex)

                        # load the data
                        cur.copy_from(f, sqltable_name, sep=',', null = 'NaN', columns = list(self.cleandf.columns.values))
                        cur.connection.commit()

            print '\nUploaded table: ' + sqltable_name
        else:
            print 'As requested, DID NOT UPLOAD ' + sqltable_name

class CombineTable(object):

    def __init__(self,sqltable_name,sqltable_cols,sqlcmd_populate,cols_toindex='',overwrite=False,upload=True):
        ''' Creates and populates a table in our Postgres database using data already in the database. Performs 2 steps:

            1. Create an empty table.
            2. Populate the table from data in the database.
        '''

        # Generate the CREATE TABLE statement
        self.sqlcmd_createtable = '''
            {drop}

            CREATE TABLE {name}(
                {cols}
            );
            '''.format(drop='DROP TABLE IF EXISTS ' + sqltable_name + ';' if overwrite==True else '',
                       name=sqltable_name,
                       cols=sqltable_cols['commonids']+sqltable_cols['data'])

        # Generate CREATE INDEX statements
        self.sqlcmd_createindex = '\n'.join( ['CREATE INDEX on ' + sqltable_name + '(' + col + ');' for col in cols_toindex] )

        # Parse list of data columns
        datacol_list = CreateTable_to_ColList(sqltable_cols['data'])

        # Generate the INSERT INTO statement that populates the common table
        self.sqlcmd_populate = sqlcmd_populate.format(commontable=sqltable_name,
                                                      datacolnames=', '.join(datacol_list),
                                                      partnerdatacols=', '.join(['partnerdata.'+x for x in datacol_list]))

        # Create and populate the table in the SQL database
        if upload==True:
            with connect_to_db(cred.host, cred.user, cred.pw, cred.dbname) as conn: # use context managed db connection
                with conn.cursor() as cur:

                    # create the empty table
                    CreateSQLTable(cur=cur,sqlcmd_createtable=self.sqlcmd_createtable,sqltable_name=sqltable_name)

                    # create indices if specified
                    if self.sqlcmd_createindex: cur.execute(self.sqlcmd_createindex)

                    # populate the table with data
                    cur.execute(self.sqlcmd_populate)
                    cur.connection.commit()

            print 'Created and populated table: ' + sqltable_name
        else:
            print 'As requested, DID NOT UPLOAD ' + sqltable_name


