'''
@description: Create plots that show how different variables are predictive 
			  of persistence in different semester models (i.e., how well they predictive
			  persisting from semester 1 to 2, 2 to 3, and so on).
			  All necessary parameters are read from '[...]/code/visualizations/configs/semesters.yaml'
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
from matplotlib import colors

from config import PERSISTENCE_PATH
from modeling.featurepipeline.dataloader import DataLoader
from modeling.featurepipeline.experiment import Experiment
from visualizations.parameters_vs_time import tableau20, make_log_folder, auc_plot

sns.set(font_scale=1.5)

#### Input File ###############################################
configFile = 'code/visualizations/configs/semesters.yaml'
###############################################################


if __name__ == '__main__':

	# get yaml for this visualization
	with open(os.path.join(PERSISTENCE_PATH, configFile), 'r') as f:
			cfg = yaml.load(f)

	outputFolder = cfg['logFolder']
	
	logpath = make_log_folder(outputFolder) # also creates the folder

	# copy the config yaml to the output folder
	shutil.copyfile(os.path.join(PERSISTENCE_PATH, configFile),
					os.path.join(logpath,'configuration.yaml'))

	# make list of pair of samples and target variables
	variablepairs = [ (None, 'PersistOneSemester','0->1'),
			 ({'PersistOneSemester': '=True'}, 'PersistTwoSemesters','1->2'),
			 ({'PersistTwoSemesters': '=True'}, 'PersistThreeSemesters','2->3'),
			 ({'PersistThreeSemesters': '=True'}, 'PersistFourSemesters','3->4'),
			 ({'PersistFourSemesters': '=True'}, 'PersistFiveSemesters','4->5'),
			 ({'PersistFiveSemesters': '=True'}, 'PersistSixSemesters','5->6'),
			 ({'PersistSixSemesters': '=True'}, 'PersistSevenSemesters','6->7'),
			 ({'PersistSevenSemesters': '=True'}, 'PersistEightSemesters','7->8')
			 ]

	# iterate over the model pairs
	df = pd.DataFrame()
	feature_to_col = {}
	for pair in variablepairs:

		# get the restrictor for this semester (i.e., which semester transition we're fitting)
		thissemester = [pair[0]] if pair[0] != None else []

		# create dataloader, get the rows (training is enough)
		dload = DataLoader(target=pair[1], feature_list=cfg['features'],
						  restrictors=cfg['sample'] + thissemester,
						  train_start=str(cfg['start_date']),
						  split_date=str(cfg['end_date']),
						  test_end=None,
						  schema=cfg['schema'])

		if len(dload.target) > 1: raise Exception("The dataloader has more than one target variable?!")
		
		target_str = dload.target[0]['train'].feature_col
		thisdf = dload.train_rows.copy()

		# now apply the postprocessors - assuming here they're the same
		# for every data loader!
		pps = dload.get_postprocessors(cfg['features'])

		# apply the postprocessors, remember if the feature maps to several columns
		# TODO: this probably doesn't work if there are Pandas features involved, and will leave them ungrouped
		for key in pps.keys():

			# remember the columns we have right now
			old_cols = thisdf.columns.tolist()
			
			for postprocessor, kwargs in pps[key].items():
				thisdf = postprocessor(thisdf, columns = [key], **kwargs)
			
			# the postprocessors might have added columns; remember them
			added_cols = set(thisdf.columns.tolist()) - set(old_cols)

			# now keep the mapping from feature name to list of columns
			if added_cols:
				feature_to_col[key] = list(set(feature_to_col.get(key,[]) + list(added_cols)))
				# always add the original feature if it still exists
				if key in thisdf.columns.tolist():
					feature_to_col[key] = list(set(feature_to_col[key] + [key]))
			else:
				feature_to_col[key] = list(set(feature_to_col.get(key,[]) + [key]))

		# we don't need the foreign keys as training data
		thisdf = thisdf.drop(['studentid','collegeid'],axis=1)
		
		# give constant name to target column
		thisdf = thisdf.rename(columns={target_str:'persist'})

		# put the outcome variable back in
		thisdf['targetvariable'] = pair[2]

		df = df.append(thisdf)


	auc_plot(df=df,target_str='persist',feature_to_col=feature_to_col,
		index_col='targetvariable',n_boot=1500,alpha=0.05,logpath=logpath,
		xtickrotate = 60)