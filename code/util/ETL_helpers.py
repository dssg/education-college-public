import pandas as pd 
import numpy as np 
import csv

def map_value_from_dict(mapping_dict, value):
	'''Takes a dictionary that maps from partner's coding to ours
	e.g. goes from 'B,W,H' to 'African American, Caucasian, Hispanic' 
	Takes one value and returns one value, so needs to be used on a series
	via apply'''

	if value in mapping_dict:
		return mapping_dict[value]
	else: return value

def create_conversion_dict(filepath):
	''' Takes a link to a filepath and generates a dictionary used for larger mappings
	Assumes that the file has a header, and that the left column are the keys
	and the right column is the values'''
	with open(filepath, 'rU') as f:
	    reader = csv.reader(f)
	    reader.next() # ditch header
	    # dictionary is expected to have uppercase
	    conversion_dict = dict((rows[0].upper(),rows[1].upper()) for rows in reader)
	    return conversion_dict

def convert_free_text(conversion_dict, free_text):	
	'''Takes a mapping dictionary and outputs the correct values
	assumes that missing values have been coded as the string 'missing' and that 
	the dictionary contains only uppercase words'''
	if free_text == 'missing':
	    return np.nan
	else:
	    # do some initial cleaning of the text
	    free_text = free_text.upper()
	    free_text = " ".join(free_text.split()) #remove extra whitespace
	    if free_text in conversion_dict:
	        return conversion_dict[free_text]
	    else: return 'OTHER'

def fill_empty_cols(df, col_list):
	'''Takes a list of columns and appends columns to the dataframe with null values'''
	for col in col_list:
	    df[col] = np.nan

     	    

##########################################################################	
'''Relevant dictionaries can go here if they are reusable across partners
Can be expanded to accommodate new partner mappings '''	
##########################################################################	

ethnicity_fixed_mapping = {'Multi-Cultural': 'Multicultural',
'Native American': 'American Indian',
'B': 'African American', 
'T': 'African American',
'H': 'Hispanic',
'W': 'Caucasian', 
'I': 'American Indian', 
'P': 'Pacific Islander' 
}

medium_fixed_mapping = {'Phone': 'Call',
	'Mail (letter/postcard)': 'Mail',
	'College staff contact': 'College Contact',
	'Parent contact (any medium)': 'Parent Contact' 
	}
