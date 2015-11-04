'''
@description: Create plots that show how many students start in the fall term and in the spring term.
'''

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from sklearn import metrics
import yaml
import os
import shutil
import datetime
import time
import itertools

from config import PERSISTENCE_PATH
from modeling.featurepipeline.dataloader import DataLoader
from modeling.featurepipeline.experiment import Experiment
from util.SQL_helpers import connect_to_db
from util import cred # import credentials
from modeling.features.all_features import IsFirstEnrollment


#### Set Output File ##########################################
outputFolder = 'visualization_logs/'
###############################################################


if __name__ == '__main__':
	
	with connect_to_db(cred.host, cred.user, cred.pw, cred.dbname) as conn:
		df = pd.read_sql("select id, start_date from college_persistence.enrollments where degree_type='Bachelors'",conn)
		df = df.set_index('id')
		df.loc[:,'start_date'] = pd.to_datetime(df.loc[:,'start_date'])
		ife = IsFirstEnrollment()
		ife.load_rows(conn)

	df = df.merge(ife.rows,how='left',left_index=True,right_index=True)

	df['spring'] = df.start_date.apply(lambda x: x.month in [12,1,2,3,4])
	df.loc[df.start_date.isnull(),'spring'] = pd.NaT


	pd.crosstab(index=df.spring,columns=df.is_first_enrollment)



