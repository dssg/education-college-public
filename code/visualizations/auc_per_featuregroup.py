'''
@description: This script creates a plot that compares the AUCs for various feature groups
			  (such as HS Academic features, GPA, College feautures, etc).
			  Configuration is happening in [...]/code/visualizations/configs/auc_per_featuregroup.yaml
'''

import os
import datetime
import pandas as pd
import yaml
import itertools
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns

from config import PERSISTENCE_PATH
from visualizations.parameters_vs_time import tableau20
import modeling.models.all_models as am
from modeling.featurepipeline.dataloader import DataLoader
from modeling.featurepipeline.experiment import Experiment
from visualizations.parameters_vs_time import bootstrap, aucwrapper


###################################################################
configFile = 'code/visualizations/configs/auc_per_featuregroup.yaml'
###################################################################

if __name__ == '__main__':

	with open(os.path.join(PERSISTENCE_PATH, configFile), 'r') as f:
			cfg = yaml.load(f)

	# check which features we need to check out in total
	allfeatures = list(set([x for f in cfg['features'] for x in f.values()[0]]))

	# drop Nones in case user forgot to delete '-'
	if type(cfg['sample']) == type([]):
		if None in cfg['sample']: cfg['sample'].remove(None)

	# get the data 
	dload = DataLoader(target=cfg['target'], feature_list=allfeatures,
					   restrictors=cfg['sample'],
					   split_date=str(cfg['split_date']),
					   train_start=str(cfg['train_start']),
					   test_end=str(cfg['test_end']),
					   schema=cfg['schema'])

	# go over feature groups
	res = {}
	for featuregroup in cfg['features']:

		model = cfg['model']
		if (len(model.keys()) > 1) or (len(model.values()) > 1):
			raise IOError("A model is not specified correctly.")

		modelname = model.keys()[0]
		paramdict = model.values()[0]

		m = getattr(am,modelname)(**paramdict)

		e = Experiment(model=m, feature_list=featuregroup.values()[0], 
					   dloader=dload, id=None,nan_handling=cfg['nan_handling'],
					   logFolder=cfg['logFolder'], looplog=cfg['looplog'])

		e.apply_postprocessors()
		e.handle_NAs()

		# need to make sure that we only select the columns that are the same across train and test, need the intersection
		test_cols = set(e.test_rows.columns.values)
		train_cols = set(e.train_rows.columns.values)
		predictor_cols = list(test_cols & train_cols)
		predictor_cols.remove(e.target_col)

		# take out the dataframe we'll be working with
		df = e.train_rows[predictor_cols + [e.target_col]]
		randIdxs = np.random.randint(df.shape[0],size=(df.shape[0],cfg['n_boot']))

		e.model.fit(X_train=df[predictor_cols],
					y_train=df[e.target_col])
		actualAUC = e.model.auc(y_test=e.test_rows[e.target_col],
						X_test = e.test_rows[predictor_cols])

		# run the bootstrap
		aucs = np.zeros(cfg['n_boot'])
		for idx in range(cfg['n_boot']):
			# sample data
			thisdf = df.iloc[randIdxs[:,idx],:]
			# train model
			e.model.fit(X_train=thisdf[predictor_cols],
						y_train=thisdf[e.target_col])
			# get AUC
			thisAUC = e.model.auc(y_test=e.test_rows[e.target_col],
						X_test = e.test_rows[predictor_cols])
			aucs[idx] = thisAUC

		aucs.sort()
		res[featuregroup.keys()[0]] = (aucs[int((cfg['alpha']/2.0)*cfg['n_boot'])],
									   actualAUC,
									   aucs[int((1-cfg['alpha']/2.0)*cfg['n_boot'])])


	# now do the plot

	matplotlib.rcParams.update({'font.size': 40,'font.weight':'normal'})
	fig = plt.figure()
	ax = fig.add_subplot(111)
	ax.set_xlim([0,1])
	ax.set_xticks([0.,.25,0.5,.75,1.0])
	sorted_res = res.items()
	sorted_res.sort(key=lambda x: x[1][1])
	ylabels, values = zip(*sorted_res)
	pos = np.arange(len(values))+.5

	lowerCI = np.abs(np.array(zip(*values)[0])-np.array(zip(*values)[1]))
	upperCI = np.abs(np.array(zip(*values)[2])-np.array(zip(*values)[1]))

	plt.barh(pos,zip(*values)[1], 
		xerr=(lowerCI,upperCI),
		color=tableau20[0],
		height=.7,
		edgecolor=tableau20[0],
		ecolor=tableau20[1], align='center',
		error_kw={'linewidth':3}
		)
	plt.yticks(pos, ylabels)
	plt.xlabel('AUC')
	plt.axvline(0.5,color=(174/255., 199/255., 232/255.),linestyle='--')
	plt.tight_layout()
	plt.show()