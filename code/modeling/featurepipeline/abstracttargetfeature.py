
from abstractboundedfeature import *
import warnings

class AbstractTargetFeature(AbstractBoundedFeature):

	'''
	@author: bkt@uchicago.edu
	@description: 'Target' features are those which we try to predict. We also use them to
				   restrict queries. For example, a target feature could be
				   'has persisted two semester'. We use this feature as our target variable for
				   some models; for others, we want to select only those students that have
				   successfully persisted two semesters.

				   Target features need to know how all other features can merge (join) on 
				   them. Therefore, they have additional attributes that tell us
				   the names of their SQL columns that contain studentIDs and collegeIDs.
	'''

	def __init__(self, lower_bound=None,upper_bound=None):

		AbstractBoundedFeature.__init__(self, lower_bound=None, upper_bound=None)

		if self.index_col != 'enrollid':
			warnings.warn("Any target feature SHOULD have 'enrollid' as their index column!")

		self.studentid_col
		self.collegeid_col

	# all target features must be on enrollment level
	index_level = "enrollid"

	@property
	def studentid_col(self):
		raise NotImplementedError

	@property
	def collegeid_col(self):
		raise NotImplementedError