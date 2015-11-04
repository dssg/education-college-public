__author__ = 'Team'

from sklearn.linear_model import LogisticRegression as scikitLR
from sklearn.linear_model import SGDClassifier as scikitSGD
from sklearn.ensemble import RandomForestClassifier as scikitRF
from sklearn.ensemble import ExtraTreesClassifier as scikitET
from sklearn.ensemble import AdaBoostClassifier as scikitAB
from sklearn.tree import DecisionTreeClassifier as scikitDTC
from sklearn.svm import SVC as scikitSVC
from sklearn.ensemble import GradientBoostingClassifier as scikitGBC
from sklearn.naive_bayes import GaussianNB as scikitGNB
from sklearn.neighbors import KNeighborsClassifier as scikitKNN

from modeling.featurepipeline.scikitmodel import *


class LogisticRegression(ScikitModel):

	def __init__(self,implementation='scikit',**params):

		self.implementation = implementation

		if self.implementation == 'scikit':
				# default parameters: penalty="l2", dual=False, C=1.0
			if len(params) == 0:
				params = {'C':1.0,'penalty':'l2','dual':False}

			ScikitModel.__init__(self,
					scikit_class=scikitLR,
					**params)

		else:
			raise ValueError("This implementation is not known.")

	def coefs(self):
		if self.implementation == 'scikit':
			return pd.DataFrame(index=self.predictor_cols,data=self.scikit_model.coef_[0]).sort(columns=0,ascending=False).to_string(header=False)
		else:
			raise ValueError("This implementation is not known.")

	def score_function(self,x):
		return self.scikit_model.predict_proba(x)


class RandomForest(ScikitModel):

	def __init__(self, **params):

		if len(params) == 0:
			params = {
				'n_estimators':10, 'criterion':'gini', 'max_depth':None,
				'min_samples_split':2, 'min_samples_leaf':1, 'min_weight_fraction_leaf':0.0, 
				'max_features':'auto', 'max_leaf_nodes':None, 'bootstrap':True, 'oob_score':False,
				'n_jobs':1, 'random_state':None, 'verbose':0, 'warm_start':False, 'class_weight':None
				}

		ScikitModel.__init__(self,scikit_class=scikitRF,**params)

	def coefs(self):
		# TODO
		return None

	def score_function(self,x):
		return self.scikit_model.predict_proba(x)

class ExtraTreesClassifier(ScikitModel):

	def __init__(self, **params):
		ScikitModel.__init__(self,scikit_class=scikitET,**params)

	def coefs(self):
		# TODO
		return None

	def score_function(self,x):
		return self.scikit_model.predict_proba(x)


class AdaBoost(ScikitModel):

	def __init__(self, **params):

		params['base_estimator'] = scikitDTC(max_depth=1)
		ScikitModel.__init__(self,scikit_class=scikitAB,**params)

	def coefs(self):
		# TOOD
		return None

	def score_function(self,x):
		return self.scikit_model.predict_proba(x)


class SVM(ScikitModel):

	def __init__(self, **params):
		params['random_state'] = 0
		ScikitModel.__init__(self,scikit_class=scikitSVC,**params)

	def coefs(self):
		# TODO fix this
		return None

	def score_function(self,x):
		dec = self.scikit_model.decision_function(x)
		res = np.zeros((len(dec),2))
		res[:,0] = -dec
		res[:,1] = dec
		return res


class GBC(ScikitModel):

	def __init__(self, **params):
		ScikitModel.__init__(self,scikit_class=scikitGBC,**params)

	def coefs(self):
		# TODO
		return None

	def score_function(self,x):
		return self.scikit_model.predict_proba(x)

class GNB(ScikitModel):

	def __init__(self):
		ScikitModel.__init__(self,scikit_class=scikitGNB,**{})

	def coefs(self):
		# TODO
		return None

	def score_function(self,x):
		return self.scikit_model.predict_proba(x)

class DTC(ScikitModel):

	def __init__(self, **params):
		ScikitModel.__init__(self,scikit_class=scikitDTC,**params)

	def coefs(self):
		# TODO
		return None

	def score_function(self,x):
		return self.scikit_model.predict_proba(x)


class SGD(ScikitModel):

	def __init__(self, **params):
		ScikitModel.__init__(self,scikit_class=scikitSGD,**params)

	def coefs(self):
		# TODO
		return None

	def score_function(self,x):
		if self.scikit_model.loss in ['modified_huber','log']:
			return self.scikit_model.predict_proba(x)
		else:
			return np.vstack((-self.scikit_model.decision_function(x),
							  self.scikit_model.decision_function(x))).T


class KNN(ScikitModel):

	def __init__(self, **params):
		ScikitModel.__init__(self,scikit_class=scikitKNN,**params)

	def coefs(self):
		# TODO
		return None

	def score_function(self,x):
		return self.scikit_model.predict_proba(x)
