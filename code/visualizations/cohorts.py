'''
@description: This script is similar in its goal to [...]/code/modeling/loopmodels.py.
			  It iterates over combinations of data, models, and features, and logs the 
			  results. However, this script does not perform temporal crossvalidation; 
			  instead, it performs k-fold crossvalidation. This might be useful in cases
			  where there is too little data for temporal splits.
			  As this approach is totally different from the modeling pipeline,
			  this script unfortunately needs to replicate a lot of jobs that 
			  the modeling pipeling does.
			  Its output consists of the usual ROCs/AUCs, and precision-recall curves.
			  Now, however, each plot holds k different curves, one from each train/test split.
			  Configuration in [...]/code/visualizations/configs/cohorts.yaml
'''

import yaml
import os
import shutil
import datetime
import time
import itertools
from matplotlib import colors
import pandas as pd
import numpy as np
import math
import time
import matplotlib.pyplot as plt
from sklearn import metrics

import modeling.models.all_models as am
from visualizations.parameters_vs_time import tableau20, make_log_folder
import visualizations.cohort_log as cl
from config import PERSISTENCE_PATH
from modeling.featurepipeline.dataloader import DataLoader
from modeling.featurepipeline.experiment import Experiment

#### Input File ###############################################
configFile = 'code/visualizations/configs/cohorts.yaml'
###############################################################

outputFolder = cfg['logFolder']

if __name__=='__main__':


	# ======== read input files ================
	# get yaml for this visualization
	with open(os.path.join(PERSISTENCE_PATH, configFile), 'r') as f:
			cfg = yaml.load(f)
	# drop Nones in case user forgot to delete '-'
	if type(cfg['sample']) == type([]):
		if None in cfg['sample']: cfg['sample'].remove(None)
	# ==============================================================

	splitdates = pd.date_range(start=cfg['earliest_train_start'],
				  end=cfg['latest_train_start'],
				  freq=pd.DateOffset(months=cfg['time_steps_months']))


	#########################################
	# Loop over training windows            #
	#########################################
	for train_start in splitdates:

		split_date = train_start + pd.DateOffset(months=cfg['train_period_months'])

		print "Working on train start: ", train_start

		# create dataloader, get the rows (training is enough)
		dload = DataLoader(target=cfg['target'], feature_list=cfg['features'],
						  restrictors=cfg['sample'],
						  split_date=str(split_date.date()),
						  train_start=str(train_start.date()),
						  test_end=None,
						  schema=cfg['schema'])

		if len(dload.target) > 1: raise Exception("The dataloader has more than one target variable?!")
		
		target_str = dload.target[0]['train'].feature_col # overwritten in each loop, doesn't matter   

		# now apply the postprocessors
		pps = dload.get_postprocessors(cfg['features'])

		df = dload.train_rows

		# apply the postprocessors, remember if the feature maps to several columns
		# TODO: this probably doesn't work if there are Pandas features involved...
		feature_to_col = {}
		for key in pps.keys():
			
			# remember the columns we have right now
			old_cols = df.columns.tolist()
			
			for postprocessor, kwargs in pps[key].items():
				df = postprocessor(df, columns = [key], **kwargs)
			
			# the postprocessors might have added columns; remember them
			added_cols = set(df.columns.tolist()) - set(old_cols)

			# now keep the mapping from feature name to list of columns
			if added_cols:
				feature_to_col[key] = list(added_cols)
				# always add the original feature if it still exists
				if key in df.columns.tolist():
					feature_to_col[key] = feature_to_col[key] + [key]
			else:
				feature_to_col[key] = [key]

		# We don'd take the foreign keys as training data.
		df = df.drop(['studentid','collegeid'],axis=1)
		print "This dataset is size: ", df.shape


		#########################################
		# Loop over models 						#
		#########################################
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

			#############################################
			# Loop over combinations of hyperparameters #
			#############################################
			for pset in itertools.product(*plists):

				# make a keyword dict of parameters for this combination of values
				thispdict = dict(zip(pnames,pset))
				print "Working on parameters", thispdict

				####################################
				# prepare figures for metrics plots
				####################################
				fig_roc = plt.figure()
				fig_roc_train = plt.figure()
				ax_roc = fig_roc.add_subplot(111)
				ax_roc_train = fig_roc_train.add_subplot(111)
				fig_pr0 = plt.figure()
				fig_pr1 = plt.figure()
				fig_pr0_train = plt.figure()
				fig_pr1_train = plt.figure()
				ax1_pr0 = fig_pr0.add_subplot(111)
				ax2_pr0 = ax1_pr0.twinx()
				ax1_pr1 = fig_pr1.add_subplot(111)
				ax2_pr1 = ax1_pr1.twinx()
				ax1_pr0_train = fig_pr0_train.add_subplot(111)
				ax2_pr0_train = ax1_pr0_train.twinx()
				ax1_pr1_train = fig_pr1_train.add_subplot(111)
				ax2_pr1_train = ax1_pr1_train.twinx()

				ax_roc.plot([0, 1], [0, 1], 'k--')
				ax_roc.set_xlim([0.0, 1.0])
				ax_roc.set_ylim([0.0, 1.05])
				ax_roc.set_xlabel('False Positive Rate')
				ax_roc.set_ylabel('True Positive Rate')
				ax_roc.set_title('Receiver operating characteristic')
				ax_roc.legend(loc="lower right")
				ax_roc_train.plot([0, 1], [0, 1], 'k--')
				ax_roc_train.set_xlim([0.0, 1.0])
				ax_roc_train.set_ylim([0.0, 1.05])
				ax_roc_train.set_xlabel('False Positive Rate')
				ax_roc_train.set_ylabel('True Positive Rate')
				ax_roc_train.set_title('Receiver operating characteristic (training)')
				ax_roc_train.legend(loc="lower right")

				ax1_pr0.set_xlabel('Fraction of Population')
				ax1_pr0.set_ylabel('Precision', color='b')
				ax1_pr0.set_ylim([0.0, 1.0])
				ax2_pr0.set_ylabel('Recall', color='r')
				ax1_pr0.set_title('Precision-Recall, y=0')
				ax1_pr0_train.set_xlabel('Fraction of Population')
				ax1_pr0_train.set_ylabel('Precision', color='b')
				ax1_pr0_train.set_ylim([0.0, 1.0])
				ax2_pr0_train.set_ylabel('Recall', color='r')
				ax1_pr0_train.set_title('Precision-Recall (train), y=0')

				ax1_pr1.set_xlabel('Fraction of Population')
				ax1_pr1.set_ylabel('Precision', color='b')
				ax1_pr1.set_ylim([0.0, 1.0])
				ax2_pr1.set_ylabel('Recall', color='r')
				ax1_pr1.set_title('Precision-Recall, y=1')
				ax1_pr1_train.set_xlabel('Fraction of Population')
				ax1_pr1_train.set_ylabel('Precision', color='b')
				ax1_pr1_train.set_ylim([0.0, 1.0])
				ax2_pr1_train.set_ylabel('Recall', color='r')
				ax1_pr1_train.set_title('Precision-Recall (train), y=1')

				conf_matrix = np.zeros((3,3),int)
				conf_matrix_train = np.zeros((3,3),int)
				#######################################

				# keep track of the predictions and target values
				all_y_test = []
				all_y_train = []
				all_y_pred = []
				all_y_pred_train = []
				all_aucs = []
				all_aucs_train = []

				starttime = time.time()

				#############################################
				# Loop over train/test splits 				#
				#############################################

				# create train/test split indices
				randIdxs = np.array(range(df.shape[0]))
				np.random.shuffle(randIdxs)

				test_rows_num = 0
				train_rows_num = 0

				k = cfg['k']
				for i in range(k):
					test_idxs = randIdxs[int(math.ceil(i*df.shape[0]/k)):
							int(math.ceil((i+1)*df.shape[0]/k))]

					test_rows_num += len(test_idxs)

					train_idxs = randIdxs[(range(0,int(math.ceil(i*df.shape[0]/k))) + 
									range(int(math.ceil((i+1)*df.shape[0]/k)),df.shape[0]))]

					train_rows_num += len(train_idxs)

					# get the training set
					train_df = df.iloc[train_idxs,:]
					print "Dropping %d NaNs in training set."%train_df.isnull().sum().max()
					train_df = train_df.dropna()
					print "Training data left: ", train_df.shape

					# get the test set
					test_df = df.iloc[test_idxs,:]
					print "Dropping %d NaNs in test set."%test_df.isnull().sum().max()
					test_df = test_df.dropna()
					print "Test data left: ", test_df.shape

					# get predictions on both sets
					X_test = test_df.drop(target_str,axis=1).astype(float)
					X_train = train_df.drop(target_str,axis=1).astype(float)
					y_test = test_df[target_str].astype(bool)
					y_train = train_df[target_str].astype(bool)
					
					# now train the model 
					m = getattr(am,modelname)(**thispdict)
					m.fit(X_train,y_train)

					y_pred = m.predict(X_test)
					y_pred_train = m.predict(X_train)
					y_probs = m.score_function(X_test)
					y_probs_train = m.score_function(X_train)

					# remember the outcome
					all_y_test = np.append(all_y_test,y_test)
					all_y_train = np.append(all_y_train, y_train)
					all_y_pred = np.append(all_y_pred,y_pred)
					all_y_pred_train = np.append(all_y_pred_train, y_pred_train)

					# add ROC/AUC for this split
					cl.plot_roc(y_test=y_test,y_probs=y_probs,ax=ax_roc)
					cl.plot_roc(y_test=y_train,y_probs=y_probs_train,ax=ax_roc_train)

					# add precision-recall for this split
					cl.plot_precision_recall(y_test=y_test, pos_label=1,
											y_probs=y_probs,ax1=ax1_pr1,ax2=ax2_pr1)
					cl.plot_precision_recall(y_test=y_test, pos_label=0,
											y_probs=y_probs,ax1=ax1_pr0,ax2=ax2_pr0)

					cl.plot_precision_recall(y_test=y_train, pos_label=1,
											y_probs=y_probs_train,ax1=ax1_pr1_train,ax2=ax2_pr1_train)
					cl.plot_precision_recall(y_test=y_train, pos_label=0,
											y_probs=y_probs_train,ax1=ax1_pr0_train,ax2=ax2_pr0_train)

					all_aucs.append(cl.auc(y_test,y_probs))
					all_aucs_train.append(cl.auc(y_train,y_probs_train))
					

				# get confusion matrix for everything
				cms = cl.cm(y_test=all_y_test, y_predicted=all_y_pred)
				cm_train = cl.cm(y_test=all_y_train, y_predicted=all_y_pred_train)

				cm_plt = cl.plot_cm(cm=cms)
				cm_plt_train = cl.plot_cm(cm=cm_train)
				
				clf_report = cl.clf_report(y_test=all_y_test, y_predicted=all_y_pred)
				clf_report_train = cl.clf_report(y_test=all_y_train, y_predicted=all_y_pred_train)


				# now write the log of everything
				endtime = time.time()

				logpath = make_log_folder(outputFolder) # also creates the folder
				# copy the config yaml to the output folder
				shutil.copyfile(os.path.join(PERSISTENCE_PATH, configFile),
				os.path.join(logpath,'configuration.yaml'))
					
				# write log
				cl.write_log(logpath=logpath, starttime=starttime,endtime=endtime,dloader=dload,
							target_col=target_str, model=m, looplog=cfg['looplog'],
							feature_list=cfg['features'],
							train_rows_num=train_rows_num,
							test_rows_num=test_rows_num, 
							cm=cms,
							cm_train=cm_train,
							cm_plt=cm_plt,
							cm_plt_train=cm_plt_train,
							roc_plt=fig_roc,
							roc_plt_train=fig_roc_train,
							clf_report=clf_report, 
							clf_report_train=clf_report_train,
							coefs=None,
							auc=all_aucs,
							auc_train=all_aucs_train,
							pr1_plt=fig_pr1,
							pr0_plt=fig_pr0,
							pr1_plt_train=fig_pr1_train,
							pr0_plt_train=fig_pr0_train)
				plt.close('all')
