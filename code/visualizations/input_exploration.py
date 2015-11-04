'''
@description: This script generates some plots of the input data matrix, like the correlation matrix.
			  Uses the configuration from [...]/code/visualizations/configs/input_exploration.yaml
'''


from visualizations import visutil
import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.cluster.bicluster import SpectralCoclustering
from sklearn.metrics import consensus_score
from sklearn.cluster import AgglomerativeClustering
from sklearn.cluster import MeanShift,estimate_bandwidth
from sklearn.decomposition import PCA


if __name__ == '__main__':

	# get data, according to config file
	df, featdict = visutil.get_df('code/visualizations/configs/input_exploration.yaml')

	# remove the few NaNs
	df = df.dropna(axis=0)

	# get the columns that are missing-value-indicators
	null_indicators = [c for c in df.columns.tolist() if '_isnull' in c]

	print "Will drop everything that's been dummycoded as Null!"
	print df[null_indicators].sum(axis=0)

	# drop the rows where a value was codified as missing
	df = df.drop(df[df[null_indicators].sum(axis=1).astype(bool)].index,axis=0)

	# drop the columns that are missing value indicators
	df = df.drop(null_indicators,axis=1)


	# get correlation matrix
	corr = df.corr()


	############### clustered plots of cov. matrix

	bandwidth = estimate_bandwidth(corr.as_matrix(), quantile=0.1, n_samples=None)
	model = MeanShift(bandwidth=bandwidth, bin_seeding=True)
	model.fit(corr)

	fit_data = corr.iloc[np.argsort(model.labels_),:]
	fit_data = fit_data.iloc[:, np.argsort(model.labels_)]

	fig = plt.figure(figsize=(14,11))
	ax = fig.add_subplot(111)

	ax.matshow(fit_data.astype(float).as_matrix(), cmap=plt.cm.PRGn)

	ax.set_xticks(range(len(corr.columns.tolist())))
	ax.set_yticks(range(len(corr.columns.tolist())))
	ax.set_xticklabels(corr.columns[np.argsort(model.labels_)].tolist(),rotation='vertical')
	ax.set_yticklabels(corr.columns[np.argsort(model.labels_)].tolist())
	plt.tight_layout()
	plt.show()

	# try another clustering algorithm...

	model = AgglomerativeClustering(n_clusters=2,
									affinity='euclidean',
									linkage='ward') #'ward', 'complete', 'average')
	
	model.fit(corr)

	fit_data = corr.iloc[np.argsort(model.labels_),:]
	fit_data = fit_data.iloc[:, np.argsort(model.labels_)]
	fig = plt.figure(figsize=(14,11))
	ax = fig.add_subplot(111)

	ax.matshow(fit_data.astype(float).as_matrix(), cmap=plt.cm.PRGn)

	ax.set_xticks(range(len(corr.columns.tolist())))
	ax.set_yticks(range(len(corr.columns.tolist())))
	ax.set_xticklabels(corr.columns[np.argsort(model.labels_)].tolist(),rotation='vertical')
	ax.set_yticklabels(corr.columns[np.argsort(model.labels_)].tolist())

	plt.tight_layout()
	plt.show()


	########### PCA plots
	#  standardize first
	df_norm = (df - df.mean()) / df.std()
	df_norm = df_norm.drop('persist',axis=1)
	pca = PCA()
	pca.fit(df_norm)
	matplotlib.rcParams.update({'font.size': 40,'font.weight':'normal'})
	plt.plot(np.cumsum(pca.explained_variance_ratio_),marker='o')
	plt.ylim([0,1])
	plt.xlabel('number of principal components')
	plt.ylabel('cum. variance')
	plt.title('cumulative variance explained by principal components')
	plt.show()
	
	# plot first PC against GPA
	idx = 0
	for c in ['avg_y11_y12_unweightedgpa']: #df_norm.columns.tolist():
	#c = 'avg_y11_y12_unweightedgpa'
		fig = plt.figure()
		ax = fig.add_subplot(111)
		pc_proj = np.dot(pca.components_[idx],df_norm.as_matrix().T)
		ax.scatter(pc_proj,df_norm[c],color='seagreen')
		ax.set_xlabel('PC %d'%idx)
		ax.set_ylabel(c)
		plt.show()
