'''
@description: This module provides a few helpers that might be handy outside the featurepipeline.
'''

import pandas as pd
import numpy as np
import yaml
import os

from config import PERSISTENCE_PATH
from modeling.featurepipeline.dataloader import DataLoader
from modeling.featurepipeline.experiment import Experiment


def get_df(configFile='code/visualizations/configs/util.yaml'):
	'''
	@description: A workaround if you want to pull data into a dataframe, for example
				  to run visualizations or ad-hoc analyses.
	@param configFile: Path to a YAML config file that specifies which data to pull.
					   See 'code/visualizations/configs/util.yaml' for an example.
	@return: df, feature_to_col: df is a dataframe with the data as specified
								 in the YAML file. Columns have been postprocessed, 
								 as specified in each feature.
								 feature_to_col is a dictionary that maps feature names
								 to columns names. This is useful to know which dummified
								 columns belong to which feature. (Doesn't work for Pandas features,
								 which just appear as individual feature:column pairs.)
	'''


	# get yaml for the data
	with open(os.path.join(PERSISTENCE_PATH, configFile), 'r') as f:
			cfg = yaml.load(f)

	feature_to_col = {}

	# create dataloader, get the rows (only training)
	dload = DataLoader(target=cfg['target'], feature_list=cfg['features'],
					  restrictors=cfg['sample'],
					  train_start=str(cfg['start_date']),
					  split_date=str(cfg['end_date']),
					  test_end=None,
					  schema=cfg['schema'])

	if len(dload.target) > 1: raise Exception("The dataloader has more than one target variable?!")
		
	target_str = dload.target[0]['train'].feature_col
	thisdf = dload.train_rows.copy()

	# now apply the postprocessors
	pps = dload.get_postprocessors(cfg['features'])

	# apply the postprocessors, remember if the feature maps to several columns
	# TODO: this probably doesn't work if there are Pandas features involved...
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

	return thisdf, feature_to_col



