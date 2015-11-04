import pandas as pd

'''
TODO: bunch of functions
	  each works on a feature's column
	  does some postprocessing in pandas
	  returns entire dataframe with updated column
	  the features get to decide which of the functions here they would like to invoke
'''

# def exampleprocessor(data,**kwargs):
# 	'''
# 	@param data: huge dataframe with all the data
# 	@param kwargs: all kinds of more fun parameters you might need!
# 	@return: modified version of data
# 	'''
# 	pass

def getdummies(data, columns = [], **kwargs):
	return pd.get_dummies(data, columns = columns,**kwargs)

def fillNullWithMedian(data, columns):
	data = data.fillna(data.median()[columns])
	return data

def dummyCodeNull(data, columns):

	for c in columns:
		if data[c].isnull().sum() > 0:
			data[c + '_isnull'] = (data[c].isnull()).astype(int)
	
	data[columns] = data[columns].fillna(0)
	return data

def fillNullWithZero(data, columns):
	data[columns] = data[columns].fillna(0)
	return data

if __name__=='__main__':
	pass