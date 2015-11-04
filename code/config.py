import os

# get the base directory of the project, relative to the current directory 
PERSISTENCE_PATH =	os.path.dirname(os.path.dirname(
							os.path.abspath(__file__)
						)
					)
DATA_PATH = '%s/data/' % (PERSISTENCE_PATH)
