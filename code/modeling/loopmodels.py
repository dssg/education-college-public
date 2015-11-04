'''
This script loops over test/train splits, models, and features subsets.
The script accepts a path to a YAML configuration file as a command line argument;
it defaults to [..]/code/modeling/configs/default.yaml
'''

import matplotlib
matplotlib.use('Agg')
import os
import datetime
import pandas as pd
import yaml
import itertools
import pandas as pd
import argparse

from config import PERSISTENCE_PATH
import modeling.models.all_models as am
from modeling.featurepipeline.dataloader import DataLoader
from modeling.featurepipeline.experiment import Experiment

if __name__ == '__main__':

	# set display

	# get the configuration YAML from the command line argument
	parser = argparse.ArgumentParser(description='Run a loop over models and features, according to config YAML file.')
	parser.add_argument('configFile',metavar='code/modeling/configs/default.yaml',
					    help='Path to YAML config relative to project directory.',
					    const='code/modeling/configs/default.yaml',nargs='?',
					    default='code/modeling/configs/default.yaml')
	args = parser.parse_args()
	configFile =  args.configFile
	print "Config file: ", configFile

	with open(os.path.join(PERSISTENCE_PATH, configFile), 'r') as f:
	        cfg = yaml.load(f)

	# check which features we need to check out in total
	allfeatures = list(set([f for l in cfg['features'] for f in l]))


	splitdates = pd.date_range(start=cfg['earliest_train_start'],
				  end=cfg['latest_train_start'],
				  freq=pd.DateOffset(months=cfg['time_steps_months']))

	# drop Nones in case user forgot to delete '-'
	if type(cfg['sample']) == type([]):
		if None in cfg['sample']: cfg['sample'].remove(None)

	tmp = pd.DataFrame(columns=['AUC','AUC_train','features'])
	# rowIdx = 0

	# loop over the dates
	for train_start in splitdates:


		split_date = train_start + pd.DateOffset(months=cfg['train_period_months'])
		test_end = split_date + pd.DateOffset(months=cfg['test_period_months'])

		# get the data for this period
		dload = DataLoader(target=cfg['target'], feature_list=allfeatures,
						  restrictors=cfg['sample'],
						  split_date=str(split_date.date()),
						  train_start=str(train_start.date()),
						  test_end=str(test_end.date()),
						  schema='common')


		# loop over models
		for model in cfg['models']:

			if type(model) == str:
				model = {model: {}}

			if (len(model.keys()) > 1) or (len(model.values()) > 1):
				raise IOError("A model is not specified correctly.")

			modelname = model.keys()[0]
			paramdict = model.values()[0]

			print "Working on model: ", modelname

			# wrap individual values in lists - requires that single parameters that ARE lists have been wrapped
			# in two lists already!
			for k,p in paramdict.items():
				if type(p) != type([]):
					paramdict[k] = [p]

			# extract parameter names and parameter values lists
			if len(paramdict) == 0:
				pnames = []
				plists = []
			else:
				pnames,plists = zip(*paramdict.items())

			# go through all combinations of parameter values from the parameter value lists
			for pset in itertools.product(*plists):

				# make a keyword dict of parameters for this combination of values
				thispdict = dict(zip(pnames,pset))

				# finally, go over all the feature lists
				for featuregroup in cfg['features']:

					# make all products up to length 'products', if it is not None
					if cfg['products']==0:
						raise ValueError("Feature lists of length 0 don't make sense; to switch off products, set them to None.")
					
					if cfg['products']==None or cfg['products']=='None':
						prodRange = [len(featuregroup)]
					else:
						prodRange = range(1,cfg['products']+1)

					# do all the sublists of all lengths up to cfg['products']
					for pr in prodRange:


						# tmplist = list(itertools.combinations(featuregroup,pr))
						# import random
						# random.shuffle(tmplist)

						# TODO: for each feature group, make one plot of all 
						#		the AUCs compared to each other
						# need: for each featurelist:
						#				- a name for it
						#				- the AUC + confidence interval

						for featurelist in itertools.combinations(featuregroup,pr):
						# for featurelist in tmplist[:10]:

							print "Features: \n\t%s"%'\n\t'.join(featurelist)

							m = getattr(am,modelname)(**thispdict)
							e = Experiment(model=m, feature_list=featurelist, 
										   dloader=dload, id=None,nan_handling=cfg['nan_handling'],
										   logFolder=cfg['logFolder'], looplog=cfg['looplog'],
										   summary_only=cfg['summary_only'])

							e.run_experiment()

							# tmp[(tuple(featurelist)] = (e.auc, e.auc_train)
							# tmp.loc[rowIdx] = [e.auc,e.auc_train,featurelist]
							# if e.auc>e.auc_train:
							# 	print "THIS NO GOOD"
							# 	print ', '.join(featurelist)
							# 	print "++++++++++++++++++++"
							# rowIdx+=1
							
							# print "Best so far: "
							# bla = max(tmp.items(),key=lambda x: x[1][0])
							# print '\tModel: ', bla[0][0]
							# print '\tParams: ' + '\n\t\t'.join([str(b[0])+': '+str(b[1]) for b in bla[0][1]])
							# print '\tAUC: ', bla[1][0]
							# print '\tAUC_train: ', bla[1][1]
							# print tmp.max(columns='AUC')


							del e
							del m
						
						# tmp.to_csv(PERSISTENCE_PATH+'/modeling_logs/auc_tracker.csv',index=False,header=True)	
