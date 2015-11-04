__author__ = 'College Persistence Team'

from abstractpipeline import *
import pandas as pd
import postprocessors as pp

class AbstractFeature(AbstractPipelineConfig):
    '''
    Abstract representation of an feature. Should be extended by classes representing
    individual features, not instantiated.
    '''

    def __init__(self):

        #test to make sure subclass overwrote fields
        self.name
        self.sql_query
        self.index_col
        self.feature_col
        self.index_level
        self.feature_type
        # self.default_value
        self.postprocessors

        # def mypost(df,param):
        #     return new_df

        if self.feature_type not in ['boolean', 'numerical', 'categorical','date']:
            raise ValueError("Feature type must be 'boolean', 'numerical', 'date' or 'categorical'.")
        elif self.feature_type == 'categorical' and pp.getdummies not in self.postprocessors:
            raise ValueError("Categorical features must have a getdummies postprocessor")

        self.rows = None


    def summary(self,prefix=''):
        p = prefix
        summary = p+self.name + '\n'
        # summary += p+'Type: '+self.feature_type +'\n'
        summary += p+'SQL query: '+self.sql_query +'\n'
        summary += p+'Index column: ' + self.index_col+'\n'
        summary += p+'Feature column: '+self.feature_col+'\n'

        return summary

    def load_rows(self,connection,read_from_cache=False,write_to_cache=False):
        '''
        Load the database rows that comprise this field either from the database or from the local
        cache if told to do so. If instructed, write to the cache if it's not already there.
        :param read_from_cache: whether to look for an read from a local cache if it is there
        :param write_to_cache: whether to write loaded results to local cache if it isn't there
        :return: nothing. Loads rows into rows instance variable
        '''

        self.read_sql_into_rows(self.sql_query,connection)

    def read_sql_into_rows(self,query,connection):
        print query
        self.rows = pd.read_sql(query,connection)
        self.rows.set_index(self.index_col,inplace=True)
        # self.rows.sort(inplace=True)
        # print self.rows.head(50)

    def generate_query(self):
        return self.sql_query

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
    @property
    def feature_col(self):
        raise NotImplementedError
    @property
    def feature_type(self):
        raise NotImplementedError
    # @property
    # def default_value(self):
    #     raise NotImplementedError
    @property
    def postprocessors(self):
        raise NotImplementedError
    @property
    def index_level(self):
        raise NotImplementedError
    