__author__ = 'Team'


from abstractpipeline import *
from abstracttargetfeature import *
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from util import cred # import credentials
from util.SQL_helpers import connect_to_db
from sklearn import metrics
import pandas as pd
import importlib

class DataLoader(AbstractPipelineConfig):

	'''Takes a list of features and handles the SQL querying and the train/test splitting
		Also handles the logic of dealing with pandas features and non-pandas features by checking inheritance
		NOTE: can only handle pandas features, not pandas targets or restrictors
	'''

	def __init__(self, target, feature_list,restrictors,split_date, train_start, test_end, schema = 'common'):

		self.schema = schema

		if split_date == None or train_start == None:
			raise ValueError("dataloader requires a split_date and a train_start!")
		self.split_date = split_date

		self.train_start = train_start
		self.train_end = self.split_date
		self.test_start = self.split_date
		self.test_end = test_end

		self.target, _ = self._generateFeatureDict(target, 'target') # assume the target isn't a pandas feature
		self.features, self.pandas_features = self._generateFeatureDict(feature_list, 'feature')
		self.restrictors = self._generateRestrictorDict(restrictors)


		# load data once during initialization, can then be used to run multiple models/feature sets

		# only load test data if a test window is defined. otherwise, just go for the training set
		print "\nDataLoader is fetching its data..."
		if self.test_end == None and self.train_start != None:
			self.load_rows(split=['train'])
		elif self.test_end != None and self.train_end != None:
			self.load_rows(split=['train','test'])
		else:
			raise ValueError("dataloader requires a train_start and split_date")
		print "\tDone fetching."

	def _generateRestrictorDict(self,list_to_load):
		''' This function creates the list of dictionaries that generate_megaquery uses for the restricting 'where' clause. 
			Takes a list of dictionaries (keys are features, values are SQL statements to restrict rows
			based on that column. Returns a list of dictionaries with those objects initialized. 
			Also automatically handles assignment of upper and lower bounds.
		'''

		if list_to_load == None:
			return {}

		# wrap in list if it's just one item
		if type(list_to_load) == type({}):
			list_to_load = [list_to_load]

		# if no restricting condition was provided, then the element is just a string, 
		# and we default to {feature_name:'=True'}
		for idx,l in enumerate(list_to_load):
			if type(l)==type(''):
				list_to_load[idx] = {l: '=True'}

		list_of_dicts = []
		m = importlib.import_module('.all_features', 'modeling.features')
		boundedFeature = getattr(m, 'AbstractBoundedFeature')
		targetFeature = getattr(m, 'AbstractTargetFeature')

		# now create the restrictors
		for restrictDict in list_to_load:
			
			c = getattr(m, restrictDict.keys()[0])

			# if we're dealing with targets, they must inherit from AbstractTargetFeature
			if not issubclass(c,targetFeature):
				raise ValueError("All restricting features need to inherit from AbstractTargetFeature")

			# if it's a bounded feature, we need to have the bounds set
			if issubclass(c,boundedFeature):
				feature_dict = {'train': c(lower_bound = None,
						   				   upper_bound = None),
								'test': c(lower_bound = None,
						  				  upper_bound = None)}
			else: #normal features are the same across splits
				feature_dict = {'feature': c()}

			# add the restriction string '=True'
			feature_dict['restriction'] = restrictDict.values()[0]
			
			list_of_dicts.append(feature_dict)

		return list_of_dicts

	def _generateFeatureDict(self, list_to_load, category):
		
		''' This function creates the list of dictionaries that generate_megaquery uses. 
			Takes a list of features, or targets (and a tag that tells the function which it is)
			and returns a list of dictionaries with those objects initialized. 
			Also automatically handles assignment of upper and lower bounds
		'''
		#TODO CHECK DATE BOUNDING

		# wrap in list if it's just one item
		if type(list_to_load) == type(''):
			list_to_load = [list_to_load]

		list_of_non_pandas_dicts = []
		list_of_pandas_dicts = []
		m = importlib.import_module('.all_features', 'modeling.features')
		boundedFeature = getattr(m, 'AbstractBoundedFeature')
		boundedPandasFeature = getattr(m, 'AbstractBoundedPandasFeature')
		PandasFeature = getattr(m, 'AbstractPandasFeature')
		targetFeature = getattr(m, 'AbstractTargetFeature')

		for feature in list_to_load:
			
			# get the class, will raise AttributeError if class cannot be found
			print 'FEATURE: ',feature
			c = getattr(m, feature)

			# if we're dealing with targets, they must inherit from AbstractTargetFeature
			if category=='target' and not issubclass(c,targetFeature):
				raise ValueError("All target features need to inherit from AbstractTargetFeature")

			#if it's a bounded feature, we need to assign lower and upper bounds
			if issubclass(c,boundedFeature) or issubclass(c,boundedPandasFeature):
				# need to handle the bounding of features and of targets differently
				if category == 'feature': # features only need upper bounds TODO: is this 100% true?
					feature_dict = {'train': c(lower_bound = None,
											   upper_bound = self.train_end),
									'test': c(lower_bound = None,
											  upper_bound = self.train_end)}
				elif category == 'target': # if it's a target it needs upper and lower bounds
					feature_dict = {'train': c(lower_bound = self.train_start,
											   upper_bound = self.train_end),
									'test': c(lower_bound = self.test_start,
											  upper_bound = self.test_end)}
				else:
					raise ValueError("category needs to be 'feature',or 'target'")
			else: #normal features are the same across splits
				feature_dict = {'feature': c()}
			#decide which list to append the feature dict to
			if issubclass(c,PandasFeature):
				list_of_pandas_dicts.append(feature_dict)
			else: 
				list_of_non_pandas_dicts.append(feature_dict)

		return list_of_non_pandas_dicts, list_of_pandas_dicts

				
	def generate_megaquery(self, split):
		'''
		Load all the rows for this experiment by combining the queries for the individual features
		into a big join statement with an optional where clause at the end'
		:param connection:
		:param split: 'train' or 'test'; the key of the feature dictionary to use
		bsplit: 'feature'; the split that the function accepts if it can't find split
		:return:
		'''

		#Get the actual feature objects

		otarget = list(get_feature_object(x,split) for x in self.target)[0] # only ever one target feature in list
		ofeatures = list(get_feature_object(x,split) for x in self.features) 
		orestrictors = list(get_feature_object(x,split) for x in self.restrictors)
		restrictstrings = list(x['restriction'] for x in self.restrictors)


		#CREATE THE MEGAQUERY FOR NON PANDAS FEATURES

		
		megaquery = 'select t.%s as enrollid, t.%s as studentid, t.%s as collegeid, t.%s' %(otarget.index_col, otarget.studentid_col, otarget.collegeid_col, otarget.feature_col)
		for i in range(0,len(ofeatures)):
			megaquery += ', f%s.%s' %(str(i), ofeatures[i].feature_col)
		megaquery += '\nfrom \n  (%s) as t\n' %(otarget.generate_query())
		for i in range(0,len(ofeatures)):
			megaquery += 'left join (%s) as f%s\n' %(ofeatures[i].generate_query(), str(i))
			megaquery += '   on t.%s = f%s.%s\n' %(ofeatures[i].index_level, str(i), ofeatures[i].index_col)
		for i in range(0,len(orestrictors)):
			megaquery += 'left join (%s) as r%s\n' %(orestrictors[i].generate_query(), str(i))
			megaquery += '   on t.%s = r%s.%s\n' %(orestrictors[i].index_level, str(i), orestrictors[i].index_col)


		# TODO: instead of using r['restriction'], generate fully qualified restriction here
		megaquery += ' WHERE \n' if len(self.restrictors) > 0 else ''
		megaquery += ' AND '.join('r'+str(i)+'.'+r.feature_col+restrictstrings[i] for i,r in enumerate(orestrictors))
		megaquery += ';'

		return megaquery


		# ## Example megaquery
		# select t.id,t.persist_3_halfyear, f0.date_of_birth, f1.is_female, f2.alum_count
		# from
		#     (select id, studentid, collegeid, persist_3_halfyear from ofeatures.enrollment_dummies where start_date < '2010-06-30') as t
		#     left join (select id, date_of_birth from students) as f0
		#         on t.studentid = f0.id            
		#     left join (select id, is_female from students) as f1
		#         on t.studentid = f1.id
		#     left join (select count(*) as alum_count, collegeid from enrollments where status not in ('Matriculating', 'Did not matriculate')   and start_date < '2011-12-31' group by collegeid) as f2
		#         on t.collegeid = f2.collegeid
		#     left join (select id, persist_2_halfyear from enrollment_dummies) as r
		#         on r.id=t.id
		# WHERE r.persist_2_halfyear is TRUE
		# ;

		# print 'Megaquery for split ' +split+':\n'+megaquery+'\n'

		# return megaquery

	def load_rows(self,split=['test','train']):

		# check that only legit splits are being passed
		if not all(s in ['train','test'] for s in split):
			raise ValueError("split must be in ['test','train']")
		if len(split) > 2:
			raise ValueError("load_rows() should only take two splits ('train', 'test', or both) maximum")
		if len(split) == 0:
			raise ValueError("load_rows() should get at least one split ('train' or 'test' or both)!")

		with connect_to_db(cred.host, cred.user, cred.pw,cred.dbname) as connection:

			# first load non pandas features

			# set search path

			search_path_string = 'set search_path to %s' %(self.schema)

			connection.cursor().execute(search_path_string) #specify which schema to search

			if 'train' in split:
				self.train_megaquery = self.generate_megaquery('train')
				self.train_rows = pd.read_sql(self.train_megaquery,connection)
				self.train_rows.set_index(self.target[0]['train'].index_col,inplace=True)

			if 'test' in split:
				self.test_megaquery = self.generate_megaquery('test')
				self.test_rows = pd.read_sql(self.test_megaquery,connection)
				self.test_rows.set_index(self.target[0]['test'].index_col,inplace=True)

			# then handle pandas features
			
			if 'train' in split:
				#get objects
				self.pandas_trainfeatures = list(get_feature_object(x, 'train') for x in self.pandas_features)
				# get data
				self.get_pandas_rows(connection, self.pandas_trainfeatures, 'train')
			
			if 'test' in split:
				self.pandas_testfeatures = list(get_feature_object(x, 'test') for x in self.pandas_features)
				self.get_pandas_rows(connection, self.pandas_testfeatures, 'test')




	def get_pandas_rows(self, connection, pandas_features, split):

		for p_feature in pandas_features:

			#checkout the rows
			p_feature.load_rows(connection, self.restrictors, split)
			#process
			p_feature.process()
			#update the column names for subsetting
			p_feature.update_cols()
			#merge into current rows based on split

			if split == 'train':
				self.train_rows.reset_index(inplace=True)
				self.train_rows = pd.merge(self.train_rows, p_feature.rows, how = 'left', left_on = p_feature.index_level, right_on = p_feature.index_col)
				self.train_rows.set_index('enrollid',inplace=True)
			elif split == 'test':
				self.test_rows.reset_index(inplace=True)
				self.test_rows = pd.merge(self.test_rows, p_feature.rows, how = 'left', left_on = p_feature.index_level, right_on = p_feature.index_col)
				self.test_rows.set_index('enrollid',inplace=True)



	def subset_data(self, feature_list, split):
		'''Takes a list of features from experiment and returns a dataframe containing only the features relevant to the particular experiment'''

		# get all the relevant objects

		otarget = list(get_feature_object(x,split) for x in self.target)[0] # only ever one target feature in list
		ofeatures = list(get_feature_object(x,split) for x in self.features) 
		pfeatures = list(get_feature_object(x,split) for x in self.pandas_features)

		all_features = ofeatures + pfeatures

		# go through features, check if we want to keep them, then extract their columns

		subset_columns = []

		for feature in all_features:


			if feature.__class__.__name__ in feature_list:
				# some columns are single columns, others are lists
				if type(feature.feature_col) is str:
					subset_columns.append(feature.feature_col)
				else: subset_columns.extend(feature.feature_col)


		# then add in the target to make sure we don't cut that one out of the dataframe

		subset_columns.append(otarget.feature_col)

		# then subset the dataframe by only those columns 

		if split == 'train':
			return self.train_rows[subset_columns].copy(deep=True)
		elif split =='test':
			return self.test_rows[subset_columns].copy(deep=True)


	def get_postprocessors(self, feature_list):
		''' Returns a dictionary of postprocessors for every column in our dataframe
		Handles the conversion from feature names to columns'''

		# get the feature objects for the feature
		ofeatures = list(get_feature_object(x,'train') for x in self.features) #split doesn't matter, train is used here arbitrarily to get the object out
		pfeatures = list(get_feature_object(x,'train') for x in self.pandas_features)

		all_features = ofeatures + pfeatures

		postprocessors = {}

		#only get the postprocessors for the features we care about, that we're subsetting on
		for feature in all_features: 
			if feature.__class__.__name__ in feature_list:
				columns = feature.feature_col
				if type(columns) is str:
					postprocessors[columns] = feature.postprocessors
				else:	
					# handle the multiple columns of pandas features
					for column in columns:
						postprocessors[column] = feature.postprocessors

		return postprocessors


def get_feature_object(feature_dict, split):
	'''
	Gets the actual feature object out of a feature dictionary specified in the class definition
	:param feature_dict:
	:param split: 'train' or 'test'.
	:return: an object that extends AbstractFeature
	'''

	if split in feature_dict:
		return feature_dict[split]
	elif 'feature' in feature_dict:
		return feature_dict['feature']
	else:
		raise Exception('Improperly specified experiment feature; dictionary should have either '+split+' or "feature" as a key:\n'+str(feature_dict))


