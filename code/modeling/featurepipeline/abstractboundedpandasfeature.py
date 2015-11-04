__author__ = 'College Persistence Team'

from abstractpandasfeature import *

class AbstractBoundedPandasFeature(AbstractPandasFeature):
    '''
    This represents a feature that takes in an optional start value and end value on one of its index columns
    A temporal feature where you need to bound on a start time and an end time would be an example
    of a bounded featured
    '''
    def __init__(self, lower_bound=None,upper_bound = None):
        AbstractPandasFeature.__init__(self)

        self.bound_col # will raise NotImplementedError if this is not dealt with in child class
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        if not '{}' in self.sql_query:
            raise NotImplementedError('Bad bounded feature class definition; SQL query does not have a {} in which to place a bounding where clause.')

    def load_rows(self,connection,read_from_cache=False,write_to_cache=False):
        '''Overwritten version of this function that handles
        the presence/expectation of bounds on a column'''

        #Format sql query to accommodate bounds
        f_query = self.generate_query()
        self.read_sql_into_rows(f_query,connection)
        print self.rows.head()

    def generate_query(self):
        if self.lower_bound == None and self.upper_bound == None:
            f_query = self.sql_query.format('')
        else:
            if 'where' in self.sql_query:
                add = 'and '
            else:
                add = 'where '

            if self.lower_bound != None:
                add += self.bound_col +' >= \''+self.lower_bound+'\''

            if self.upper_bound != None:
                if self.lower_bound != None:
                    add += ' and '
                add += self.bound_col +' < \''+self.upper_bound+'\''

            f_query = self.sql_query.format(add)

        return f_query


    @property
    def bound_col(self):
        raise NotImplementedError
    
