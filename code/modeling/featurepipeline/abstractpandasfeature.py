__author__ = 'Masha'

from abstractpipeline import *
import pandas as pd
import postprocessors as pp
from dataloader import get_feature_object

class AbstractPandasFeature(AbstractPipelineConfig):
    '''
    Abstract representation of a pandas feature. A pandas feature checks out columns from the database
    and is then processed in pandas before being joined back in to the larger dataframe. It needs to have
    an implemented 'process' method that specifies how it is processed in pandas. Index_level is used to determine
    how to join it in after processing
    '''

    def __init__(self):

        #test to make sure subclass overwrote fields
        self.name
        self.sql_query
        self.index_col
        self.feature_col
        self.index_level
        self.feature_type
        self.postprocessors

        # def mypost(df,param):
        #     return new_df

        if self.feature_type not in ['boolean', 'numerical', 'categorical','date']:
            raise ValueError("Feature type must be 'boolean', 'numerical','date', or 'categorical'.")
        elif self.feature_type == 'categorical' and pp.getdummies not in self.postprocessors:
            raise ValueError("Categorical features must have a getdummies postprocessor")

        if self.index_level not in ['enrollid','studentid','collegeid']:
            raise ValueError("The index_level must be enrollid, studentid, or collegeid")


        self.rows = None


    def summary(self,prefix=''):
        p = prefix
        summary = p+self.name + '\n'
        # summary += p+'Type: '+self.feature_type +'\n'
        summary += p+'SQL query: '+self.sql_query +'\n'
        summary += p+'Index column: ' + self.index_col+'\n'
        summary += p+'Feature column: '+self.feature_col+'\n'

        return summary

    def load_rows(self,connection,restrictors, split):
        '''
        Load the database rows that comprise this field either from the database
        :return: nothing. Loads rows into rows instance variable
        '''

        restricted_query = self.generate_restricted_query(restrictors, split)

        self.read_sql_into_rows(restricted_query,connection)

    def read_sql_into_rows(self,query,connection):

        self.rows = pd.read_sql(query,connection)
        self.rows.reindex(copy=False)


    def generate_query(self):

        return self.sql_query

    def generate_restricted_query(self, restrictors, split):

        # example target query

        # select enrollid, t.studentid, contact_medium from
        # (select studentid, contact_medium from contacts where contact_date < '2013-06-01') as t
        # left join (select id as enrollid, studentid, collegeid, persist_1_halfyear
        # from features.enrollment_dummies where persist_1_halfyear is not null ) as r0
        # on t.studentid = r0.studentid 
        # WHERE r0.persist_1_halfyear=True;
        
        # get restrictor objects

        orestrictors = list(get_feature_object(x,split) for x in restrictors)
        restrictstrings = list(x['restriction'] for x in restrictors)


        # get the indexlevel of the feature
        # check that it is enrollid, collegeid, or studentid
        # then depending on that, choose the restrictor's .collegeid_col, .studentid_col, or .index_col to merge on


        query = 'select f.%s, f.%s' %(self.index_col, self.feature_col)
        # for i in range(0,len(orestrictors)):
        #     query += ', r%s.%s' %(str(i), orestrictors[i].index_col)
        query += '\nfrom \n  (%s) as f\n' %(self.generate_query())

        for i in range(0,len(orestrictors)):

            # we need to know what column we can join the restrictors on
            if self.index_level=='enrollid':
                r_index = orestrictors[i].index_col
            elif self.index_level=='studentid':
                r_index = orestrictors[i].studentid_col
            elif self.index_level=='collegeid':
                r_index = orestrictors[i].collegeid_col

            query += 'left join (%s) as r%s\n' %(orestrictors[i].generate_query(), str(i))
            query += '   on f.%s = r%s.%s\n' %(self.index_col, str(i), r_index)

        query += ' WHERE \n' if len(restrictors) > 0 else ''
        query += ' AND '.join('r'+str(i)+'.'+r.feature_col+restrictstrings[i] for i,r in enumerate(orestrictors))
        query += ';'

        return query

    def process(self):
        raise NotImplementedError

    def update_cols(self):
        '''this method handles the fact that once a pandas feature has been processed, it has new column names
            that need to be stored for subsetting. Updates the columns to reflect the processing
        '''
        # drop the index column that was used to merge

        self.feature_col = [column for column in self.rows.columns.values if column is not self.index_col]

    # A subclass must override these class fields
    # to be instantiated without an error
    @property
    def name(self):
        raise NotImplementedError
    @property
    def sql_query(self):
        raise NotImplementedError
    @property
    def index_col(self):
        raise NotImplementedError
    # @property
    # def feature_col(self):
    #     raise NotImplementedError
    @property
    def feature_type(self):
        raise NotImplementedError
    @property
    def postprocessors(self):
        raise NotImplementedError
    @property
    def index_level(self):
        raise NotImplementedError
    