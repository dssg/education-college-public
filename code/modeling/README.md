
The code in this section runs and evaluates the predictive models.

## To Run An Experiment

1. Create a configuration file under configs/
2. Specify the name of that configuration file in loopmodels.py
3. Specify a logging output folder in loopmodels.py
4. Run loopmodel.py

**Alternatively,** run `run_project.sh` with a `--modeling` argument. This default to running default.yaml, but another config file can be specified by specifying the config path in  `--modeling=PATH_TO_CONFIG` 

## Sections

Below are further details on the code in each subdirectory and how it is used

### Configs

* This stores the configuration files used to specify the parameters of the experiment the user wants to run. The user can specify training and test windows, split dates, models and model parameters (or lists of parameters to try), and feature sets (or lists of feature sets). 

* Default.yaml currently includes an up to date list of every model and feature currently set up. The user should preserve this template and create their own yaml file to configure the desired experiment. 

* Users should also specify a desired output file and folder to store the results of experiment running and evaluation. 

### loopmodels.py

This script handles the parsing of the configuration file and the performs the looping over models, features, parameters, and temporal splits. Once the user has specified their configuration file (above), they can specify it in this script,
`configFile = 'code/modeling/configs/insert_name_here.yaml`
 which can then be run from the terminal as 
`python loopmodels.py`
in order to fit and evaluate models.

### Features

Every feature we have constructed is contained in a file called all_features.py. 

Features are classes that inherit from abstract feature classes. This design choice allows features to share methods, most notably the construction of a sql query that can be used to extract that feature from the database. 

For features that have a temporal component (such as the total number of contacts to a given student), the abstractboundedfeature class handles the insertion of the correct boundary dates into the SQL query, using the dates specified by the user in the yaml file. New features can be automatically added to models by adding them to all_features.py and the user’s yaml file.

NOTE: every feature must be indexed by one of three id types-- college id, student id, or enrollment id in order to be merged and processed by the pipeline. If a feature doesn’t lend itself to indexing on one of these id types (grades by course by student, for example, in which multiple student ids are repeated), it can be processed in pandas until it is correctly indexed. These feature inherit from abstractpandasfeature and the processing required must be specified in the constructor of the feature. 

See ContactMediumPercentages in all_features.py as an example pandas features. 

### Featurepipeline

Contains definitions of all the feature, model, and experiment classes that run experiments. The user should not need to interact with any of this code to run models and experiments (see configs and loopmodels.py above)
See above for details on feature classes. 

After extraction from the database, several features require post-processing before they can be passed to a model. For example, features may have missing data for some rows, or categorical features must be dummy-coded. All current postprocessors we have built are stored in postprocessors.py. The desired postprocessor(s) for a feature are specified as a postprocessors attribute in the feature class. 

NOTE: this make it difficult to change the postprocessors depending on the type of model being run. Currently, this requires manual user intervention.

Every model currenly run is from the scikit learn python package. All sk-learn models are wrapped in a scikitmodel class, which define individualized evaluation methods. Because scikit models inherit from an abstractmodel class, the pipeline can be expanded to integrate non-scikit models. 

Every combination of feature subset, model algorithm, set of hyperparameters, and train/test split is considered an experiment. 

Running each experiment is handled by the experiment class. The experiment class calls a dataloader, which takes the sql query of each individual feature and combines them into one single query (called a megaquery), that checks out all the relevant columns from the database into a single dataframe each for the train and test sets. An exception to this is the pandasfeature class, which are checked out separately and joined into the larger dataframes after processing. 

Then, all features are postprocessed, the model is fit and evaluated, and results are output to a log specified by the user in the configuration file.

