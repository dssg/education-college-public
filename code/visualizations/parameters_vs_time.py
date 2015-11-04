'''
@description: This script creates plots on how predictive features are for persistence, and how this
			  changes over time.
			  Note: This script falls 'outside our pipeline', and is considerably hacky - we're pulling
			  and postprocessing data on the fly.
			  This script loads data according to [...]/code/visualizations/configs/visconfig.yaml
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

sns.set(font_scale=1.5)

#### Input File ###############################################
configFile = 'code/visualizations/configs/parameters_vs_time.yaml'
###############################################################

#### Some nice colors for later################################
tableau20 = [(31, 119, 180), (255, 127, 14),    
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),    
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),    
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),    
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]    
# Scale the RGB values to the [0, 1] range, which is the format matplotlib accepts.    
for i in range(len(tableau20)):    
    r, g, b = tableau20[i]    
    tableau20[i] = (r / 255., g / 255., b / 255.)  
###############################################################


#### Some helper functions for the main  ######################
####       method of this script         ######################

def aucwrapper(m):
	'''
	@description: A helper function to bootstrap AUCs. m is numpy matrix
				  of resampled classifier scores and the test data, with size
				  (n_boot,n_test_rows,2). m[idx,:,0] is the test data, m[idx,:,1] the
				  predictions.
	'''
	res = np.ones(m.shape[0])*np.nan
	for idx in range(m.shape[0]):
		try:
			res[idx] = metrics.roc_auc_score(m[idx,:,0],m[idx,:,1])
		except ValueError:
			continue
	return res

# from http://people.duke.edu/~ccc14/pcfb/analysis.html
def bootstrap(data, num_samples, statistic, alpha):
	"""Returns bootstrap estimate of 100.0*(1-alpha) CI for statistic."""
	n = len(data)
	idx = np.random.randint(0, n, (num_samples, n))
	samples = data[idx]
	stat = np.sort(statistic(samples))
	return (stat[int((alpha/2.0)*num_samples)],
			stat[int((1-alpha/2.0)*num_samples)])


def make_log_folder(outputFolder, prefix=''):
	''' 
	@description: Helper function to create a new output subfolder. The subfolders within outputFolder
				  are named by creation timestamp.
	'''
	starttime = time.time()

	# String with common folder name
	logpath = os.path.join(PERSISTENCE_PATH, prefix + outputFolder,datetime.datetime.fromtimestamp(starttime).strftime('%Y-%m-%d_%H-%M-%S'))

	# Check that folder does not already exist (an experiment took <1 sec)
	if os.path.isdir(logpath):
		i=1
		logpathstub = logpath
		while os.path.isdir(logpath):
			i += 1
			logpath = logpathstub + '_' + str(i)

	# Make log folder
	logpath += '/'
	os.makedirs(logpath)

	return logpath


def counts_plots(df,target_str,logpath):
	'''
	@description: Create and save a plot of the number of students
				  who persisted/didn't per time window.
	@param target_str: Column name of the boolean target (persistence) column in df.
	'''

	# ========== Plots of Counts over time ===========
	counts = df.groupby([target_str,'date']).count()
	fig = plt.figure()
	ax = fig.add_subplot(111)
	pd.DataFrame([counts.ix[True,0],counts.ix[False,0]],
				 index=[True,False]).T.plot(
						color=['seagreen','indianred'],ax=ax);
	ax.set_title("Counts")
	ax.set_ylabel('count')
	plt.savefig(logpath+'counts_vs_time.eps')
	plt.savefig(logpath+'counts_vs_time.png')
	plt.close()


def avgs_plot(df,target_str,logpath):
	'''
	@description: For each column in df, create and save a plot of the average value 
			      of that feature, across time, and split by students who do/don't persist.
	@param target_str: Column name of the boolean target (persistence) column in df.
	'''

	# ========== Plots of averages over time ===========
	feats = df.columns.tolist()
	feats.remove(target_str)
	feats.remove('date')
	for f in feats:
		g = sns.factorplot(x='date',
			y=f, hue=target_str, data=df, kind='point',estimator=np.mean,
			palette={True:'seagreen',False:'indianred'},legend_out=False, n_boot=1500)
		plt.savefig(logpath+'avgs_%s.eps'%f)
		plt.savefig(logpath+'avgs_%s.png'%f)
		plt.close()

def auc_plot(df,target_str,feature_to_col,index_col,logpath,n_boot=1500,alpha=0.05,xtickrotate=0):
	'''
	@description: For each feature, create a plot that shows how 'predictive' that feature
				  is for the boolean outcome column (as specified in target_str). 
				  Here, we measure 'predictive' by treating each column as the output of a 
				  classifier, and compute the AUC for this 'classifier'. We calculate
				  one AUC per time window as specified in the configFile, and make a lineplot.
				  Confidence intervals for the AUC are bootstrapped.
	@param df: Input dataframe (will be passed in the main of this script)
	@param target_str: column name of the target column in df
	@param feature_to_col: A dictionary that maps feature names to a list of columns names - this is helpful
						   to do several lineplots in one figure for dummified variables.
	@param index_col: Column name that gives the date column of df, so that we can plot one AUC per time window.
	'''

	# ========== Plots of AUC over time ===========
	cols = df.columns.tolist()
	cols.remove(target_str)
	cols.remove(index_col)

	for f in feature_to_col:

		# make one plot per feature
		fig = plt.figure()

		ax = fig.add_subplot(111)
		ax.set_ylabel('AUC')
		ax.set_title(f)
		ax.set_xlim([-0.5,len(df[index_col].unique())-0.5])
		ax.set_ylim([0,1])
		ax.set_xticks(range(len(df[index_col].unique())))
		ax.hlines(0.5,ax.get_xlim()[0],ax.get_xlim()[1],color='lightblue',linestyle='-')
		
		# now go over all columns that belong to this feature, and plot the AUCs for them
		for idx,c in enumerate(feature_to_col[f]):

			# get some colors for multiple lines
			mycolors = tableau20

			auc_time = pd.DataFrame(columns=['lowerCI','AUC','upperCI'],
								index=df[index_col].unique())

			# for each timepoint
			for t in df[index_col].unique():

				# get the data for this single AUC
				# thisdf has columns index_col, target_str, c
				thisdf = df.ix[df[index_col]==t,[index_col,target_str,c]]

				try:
					thisdf.ix[:,c] = thisdf.ix[:,c].astype(float)

				except ValueError:
					print "Cannot convert column %s to float, skipping it"%c
					continue

				print "Dropping NaN rows in %s of %s: %d"%(t.replace('\n',''),c,thisdf.isnull().sum().max())
				thisdf = thisdf.dropna(axis=0)

				# calculate the AUC where the feature is the score
				try:
					thisAUC = metrics.roc_auc_score(thisdf[target_str].astype(bool), thisdf[c].astype(float))
					auc_time.ix[t,'AUC'] = thisAUC
				except ValueError:
					print "Only one class present in %s, skipping it."%c
					continue

				# bootstrap the confidence interval
				auc_time.ix[t,['lowerCI','upperCI']] = bootstrap(data=thisdf.ix[:,[target_str,c]].astype(float).as_matrix(), 
						  num_samples=n_boot, statistic=aucwrapper,
						  alpha=alpha)

			# we plot one line for each column
			# ax.plot(range(len(df[index_col].unique())),auc_time['AUC'],marker='o',linewidth=2)
			ax.errorbar(x=range(len(df[index_col].unique())),y=auc_time['AUC'], color=mycolors[idx],
				yerr=[(auc_time['lowerCI']-auc_time['AUC']).abs().tolist(),(auc_time['upperCI']-auc_time['AUC']).abs().tolist()],fmt='--o')
			
		# wrap up the plotting for this feature
		labels = []
		for idx,xt in enumerate(ax.get_xticklabels()):
			labels.append(df[index_col].unique()[idx])
		ax.set_xticklabels(labels,rotation=xtickrotate)
		if feature_to_col[f] > 1:
			plt.legend(feature_to_col[f])
	
		plt.savefig(logpath+'auc_%s.eps'%f)
		plt.savefig(logpath+'auc_%s.png'%f)
		plt.close()


def scatters(df,target_str):
	'''
	@decsription: Do a big pairwise scatter plot for columns of df and save it.
	'''

	sns.set(font_scale=1.0)

	# for each timepoint
	for t in df.date.unique():

		thisdf = df.ix[df['date']==t,:]
		thisdf = thisdf.drop('date',axis=1).astype(float)

		# import pdb;pdb.set_trace()

		# g = sns.pairplot(thisdf, kind="reg")
		g = sns.pairplot(thisdf, hue=target_str,kind="reg",
			palette={1.0:'seagreen',0.0:'indianred'})

		# pd.scatter_matrix(thisdf, alpha=0.2, diagonal='kde',figsize=(18, 18))
		plt.savefig(logpath+'scattermatrix_%s.png'%str(t).replace('\n',''))
		plt.savefig(logpath+'scattermatrix_%s.eps'%str(t).replace('\n',''))
		plt.close()

	sns.set(font_scale=1.5)
		

if __name__ == '__main__':


	# get yaml for this visualization
	with open(os.path.join(PERSISTENCE_PATH, configFile), 'r') as f:
			cfg = yaml.load(f)

	outputFolder = cfg['logFolder']
	
	logpath = make_log_folder(outputFolder) # also creates the folder

	# copy the config yaml to the output folder
	shutil.copyfile(os.path.join(PERSISTENCE_PATH, configFile),
					os.path.join(logpath,'configuration.yaml'))

	splitdates = pd.date_range(start=cfg['earliest_train_start'],
				  end=cfg['latest_train_start'],
				  freq=pd.DateOffset(months=cfg['time_steps_months']))

	# drop Nones in case user forgot to delete '-'
	if type(cfg['sample']) == type([]):
		if None in cfg['sample']: cfg['sample'].remove(None)


	# iterate over the time windows we're interested in - from yaml
	df = pd.DataFrame()
	for train_start in splitdates:

		split_date = train_start + pd.DateOffset(months=cfg['train_period_months'])

		# create dataloader, get the rows (training is enough)
		dload = DataLoader(target=cfg['target'], feature_list=cfg['features'],
						  restrictors=cfg['sample'],
						  split_date=str(split_date.date()),
						  train_start=str(train_start.date()),
						  test_end=None,
						  schema=cfg['schema'])

		if len(dload.target) > 1: raise Exception("The dataloader has more than one target variable?!")
		
		target_str = dload.target[0]['train'].feature_col # overwritten in each loop, doesn't matter	

		thisdf = dload.train_rows.drop(['studentid','collegeid'],axis=1)
		thisdf['date'] = str(train_start.date()) +':\n'+ str(split_date.date())
		df = df.append(thisdf)

	# Ah. If the time steps overlap, df might now contain duplicates. Gotta fix that.
	df.reset_index(inplace=True)
	df.drop_duplicates(inplace=True)
	df.set_index('enrollid',inplace=True)

	# now apply the postprocessors - assuming here they're the same
	# for every data loader!
	pps = dload.get_postprocessors(cfg['features'])

	# apply the postprocessors, remember if the feature maps to several columns
	# TODO: this probably doesn't do the job if there are Pandas features involved, will leave them ungrouped
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

	# make plot of counts of rows over time
	counts_plots(df=df,target_str=target_str,logpath=logpath)

	# make plots of averages per feature over time
	avgs_plot(df=df,target_str=target_str,logpath=logpath)

	# make plots of AUCs per feature per time
	auc_plot(df=df,target_str=target_str,feature_to_col=feature_to_col,
			index_col = 'date', n_boot=1500,alpha=0.05,logpath=logpath)
		
	# make a scatter plot of the input variables
	print "Dataframe shape: ",df.shape
	scatters(df=df,target_str=target_str)
