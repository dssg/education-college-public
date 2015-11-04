__author__ = 'Team'

from abstractpipeline import *
import pandas as pd
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from util import cred # import credentials
from util.SQL_helpers import connect_to_db
from sklearn import metrics
import importlib
from modeling.featurepipeline.scikitmodel import *
from modeling.featurepipeline import dataloader
import time, datetime
import config
import warnings
import os
import matplotlib.pyplot as plt
import pickle
import markdown


class Experiment(AbstractPipelineConfig):

    def __init__(self, model, feature_list, dloader, id=None,nan_handling='strict',
                 logFolder='modeling_logs',looplog=None, summary_only=False):

        if nan_handling not in ['strict','lax']:
            raise ValueError("nan_handling must be either 'lax' or 'strict'")
        self.nan_handling = nan_handling

        self.model = model
        self.id = id
        self.feature_list = feature_list
        self.dloader = dloader
        self.looplog = looplog
        self.logFolder = logFolder
        self.summary_only = summary_only

        #copy the dataframe from the dataloader, subset by the columns
        self.train_rows = dloader.subset_data(feature_list, 'train')
        self.test_rows = dloader.subset_data(feature_list, 'test')
        self.target_col = dloader.target[0]['test'].feature_col


    def apply_postprocessors(self):
        ''' Gets the dataframe, applies all the relevant postprocessors to the features and returns the dataframe
            Asks the dataloader for the relevant postprocessors, does logic to see which postprocessors to apply
            TODO: implement the logic
            then applies them  
            Note: under the current system only features get postprocessed. We'll need to add the target if it
            needs to be postprocessed as well
        '''

        pps = self.dloader.get_postprocessors(self.feature_list)

        # iterate through every column in the dictionary and apply the postprocessors

        for key in pps.keys():
            # TODO IMPLEMENT LOGIC HERE
            # For now, just run these
            for postprocessor, kwargs in pps[key].items():
                self.train_rows = postprocessor(self.train_rows, columns = [key], **kwargs)
                self.test_rows = postprocessor(self.test_rows,columns = [key], **kwargs)      


    def handle_NAs(self):

        train_nans = self.train_rows.shape[0] - self.train_rows.count().min()
        test_nans = self.test_rows.shape[0] - self.test_rows.count().min()

        self.train_rows_dropped = 0
        self.test_rows_dropped = 0
        self.nan_column_log = 'None'

        if max(train_nans,test_nans) > 0:

            if self.nan_handling == 'strict':
                raise ValueError("There are rows with NaNs, and nan_handling is set to strict.")
            
            elif self.nan_handling == 'lax':
                warnings.warn("There are rows with NaNs in the data, dropping them!")

                print "Training data: Dropping %d out of %d rows."%(train_nans, self.train_rows.shape[0])
                print "Test data: Dropping %d out of %d rows."%(test_nans, self.test_rows.shape[0])

                # Log the number of missings in each column in both train and test
                train_miss_count = self.train_rows.isnull().sum(axis=0)
                train_miss_perc =  train_miss_count.divide(self.train_rows.shape[0]).round(3) * 100

                test_miss_count = self.test_rows.isnull().sum(axis=0)
                test_miss_perc =  test_miss_count.divide(self.test_rows.shape[0]).round(3) * 100

                nan_count_df = pd.concat([train_miss_count, train_miss_perc, test_miss_count, test_miss_perc], axis=1)
                nan_count_df.columns = ['Train Count NaN', 'Train Percentage NaN', 'Test Count NaN', 'Test Percentage NaN']
                nan_count_df.fillna('DNE')
                self.nan_column_log = nan_count_df.to_string()

                # Log the number of dropped rows
                self.train_rows_dropped = train_nans
                self.test_rows_dropped = test_nans

                # Drop the NaNs
                self.train_rows.dropna(how='any',axis=0,inplace=True)
                self.test_rows.dropna(how='any',axis=0,inplace=True)
                    

    def run_experiment(self):

        self.starttime = time.time()

        #apply postprocessors

        self.apply_postprocessors()

        #handle NAs

        self.handle_NAs()

        # need to make sure that we only select the columns that are the same across train and test, need the intersection
        test_cols = set(self.test_rows.columns.values)
        train_cols = set(self.train_rows.columns.values)

        predictor_cols = list(test_cols & train_cols)

        predictor_cols.remove(self.target_col)

        self.model.fit(X_train=self.train_rows[predictor_cols],
                        y_train=self.train_rows[self.target_col])

        y_pred = self.model.predict(self.test_rows[predictor_cols])
        # evaluations
        cm = self.model.cm(y_test=self.test_rows[self.target_col], y_predicted=y_pred)
        coefs = self.model.coefs()
        clf_report = self.model.clf_report(y_test=self.test_rows[self.target_col], y_predicted=y_pred)
        auc = self.model.auc(y_test=self.test_rows[self.target_col], X_test = self.test_rows[predictor_cols])
        pr0_precision_top_10 = self.model.precision_top_k_percent(0.1, X_test = self.test_rows[predictor_cols], y_test = self.test_rows[self.target_col], y_predicted = y_pred, pos_label = 0)
        pr0_precision_top_25 = self.model.precision_top_k_percent(0.25, X_test = self.test_rows[predictor_cols], y_test = self.test_rows[self.target_col], y_predicted = y_pred, pos_label = 0)
        aupr0 = self.model.aupr(y_test=self.train_rows[self.target_col], X_test = self.train_rows[predictor_cols], pos_label=0)



        # get the within-training set ROC, AUC, precision-recall curves
        y_pred_train = self.model.predict(self.train_rows[predictor_cols])
        # evaluations
        cm_train = self.model.cm(y_test=self.train_rows[self.target_col], y_predicted=y_pred_train)
        clf_report_train = self.model.clf_report(y_test=self.train_rows[self.target_col], y_predicted=y_pred_train)
        auc_train = self.model.auc(y_test=self.train_rows[self.target_col], X_test = self.train_rows[predictor_cols])

        # TODO just for debugging
        self.auc_train = auc_train
        self.auc = auc
        
        if not self.summary_only:
            cm_plt = self.model.plot_cm(cm=cm)
            roc_plt = self.model.plot_roc(y_test=self.test_rows[self.target_col], X_test = self.test_rows[predictor_cols])
            pr1_plt = self.model.plot_precision_recall(y_test=self.test_rows[self.target_col], X_test = self.test_rows[predictor_cols], pos_label=1)
            pr0_plt = self.model.plot_precision_recall(y_test=self.test_rows[self.target_col], X_test = self.test_rows[predictor_cols], pos_label=0)
            cm_plt_train = self.model.plot_cm(cm=cm_train)
            roc_plt_train = self.model.plot_roc(y_test=self.train_rows[self.target_col], X_test = self.train_rows[predictor_cols])
            pr1_plt_train = self.model.plot_precision_recall(y_test=self.train_rows[self.target_col], X_test = self.train_rows[predictor_cols], pos_label=1)
            pr0_plt_train = self.model.plot_precision_recall(y_test=self.train_rows[self.target_col], X_test = self.train_rows[predictor_cols], pos_label=0)
        else:
            cm_plt = None
            roc_plt = None
            pr1_plt = None
            pr0_plt = None
            cm_plt_train = None
            roc_plt_train = None
            pr1_plt_train = None
            pr0_plt_train = None

        # record ending time
        self.endtime = time.time()

        # write log
        self.write_log(cm=cm, cm_plt=cm_plt, roc_plt=roc_plt, clf_report=clf_report, coefs=coefs, 
                        auc=auc, pr0_precision_top_10 = pr0_precision_top_10, pr0_precision_top_25 = pr0_precision_top_25,aupr0 = aupr0, pr1_plt=pr1_plt, pr0_plt=pr0_plt,
                        roc_plt_train=roc_plt_train, cm_train=cm_train, cm_plt_train=cm_plt_train, clf_report_train=clf_report_train,
                        auc_train=auc_train, pr1_plt_train=pr1_plt_train, pr0_plt_train=pr0_plt_train)
        
        # plt.close(cm_plt)
        # plt.close(cm_plt_train)
        # plt.close(roc_plt)
        # plt.close(roc_plt_train)
        # plt.close(pr1_plt)
        # plt.close(pr1_plt_train)
        plt.close('all')

    def write_log(self, cm, cm_plt, roc_plt, clf_report, coefs, auc, pr0_precision_top_10, pr0_precision_top_25,aupr0,pr1_plt, pr0_plt,
                    roc_plt_train, cm_train, cm_plt_train, clf_report_train, auc_train, pr1_plt_train, pr0_plt_train):
       
        if not os.path.isdir(os.path.join(config.PERSISTENCE_PATH, self.logFolder)):
            os.makedirs(os.path.join(config.PERSISTENCE_PATH, self.logFolder))

        if not self.summary_only:

            # String with common folder name
            logsubfoldername = '_'+datetime.datetime.fromtimestamp(self.starttime).strftime('%Y-%m-%d_%H-%M-%S')
            logpath = os.path.join(config.PERSISTENCE_PATH, self.logFolder,logsubfoldername)

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

            # Write pickle
            pickle.dump(self, open(logpath + 'experiment.p', "wb" ) )

        # Write figures to a folder
        if cm_plt!= None: cm_plt.savefig(logpath+'cm_plt.png', pad_inches=1)
        if roc_plt!= None: roc_plt.savefig(logpath+'roc_plt.png')
        if pr1_plt!= None: pr1_plt.savefig(logpath+'pr1_plt.png')
        if pr0_plt!= None: pr0_plt.savefig(logpath+'pr0_plt.png')

        # write figures for training set results to folder
        if cm_plt_train != None: cm_plt_train.savefig(logpath+'cm_plt_train.png', pad_inches=1)
        if roc_plt_train != None: roc_plt_train.savefig(logpath+'roc_plt_train.png')
        if pr1_plt_train != None: pr1_plt_train.savefig(logpath+'pr1_plt_train.png')
        if pr0_plt_train != None: pr0_plt_train.savefig(logpath+'pr0_plt_train.png')

        # Write detailed log to markdown file
        if not self.summary_only:
            with open(logpath+'log.md', 'w') as f:
                markdown_log_template='''
# Experiment log

## Run time

**Start time:** {starttime}
**End time:** {endtime}

## Experiment

**Outcome:**
{target}

**Restrictions:**
{listofrestrictions}

**Train-test split**
* Train start = {trainstart}
* Split date = {splitdate}
* Test end = {testend}

**Features:**
{listoffeatures}

## Model

**Classifier:** {clf}

**Parameters:**
{params}

## Data loading

Megaquery for train split:
```sql
{train_megaquery}
```

Megaquery for test split:
```sql
{test_megaquery}
```

**Dropped rows with NaNs:**
Training data: Dropped {train_nans} out of {train_rows} rows.
Test data: Dropped {test_nans} out of {test_rows} rows.

NaNs for each feature:
```
{nan_column_log}
```

## Results on Training Set

Confusion matrix table:

| | **Predicted Negative** | **Predicted Positive** |
| ----------------- |:----:|:----:|
| **True Negative** | {TN_train} | {FP_train} |
| **True Positive** | {FN_train} | {TP_train} |

Confusion matrix plot:
![Confusion Matrix]({path_to_cm_plt_train})

ROC curve:
![ROC Curve]({path_to_roc_plt_train})

Precision-Recall curves for **dropping out**:
![PR Curve 0]({path_to_pr0_plt_train})

Precision-Recall curves for **persisting**:
![PR Curve 1]({path_to_pr1_plt_train})

**Precision recall report:**
```
{clf_report_train}
```

## Results on Test Set

Confusion matrix table:

| | **Predicted Negative** | **Predicted Positive** |
| ----------------- |:----:|:----:|
| **True Negative** | {TN} | {FP} |
| **True Positive** | {FN} | {TP} |

Confusion matrix plot:
![Confusion Matrix]({path_to_cm_plt})

ROC curve:
![ROC Curve]({path_to_roc_plt})

Precision-Recall curves for **dropping out**:
![PR Curve 0]({path_to_pr0_plt})

Precision-Recall curves for **persisting**:
![PR Curve 1]({path_to_pr1_plt})

**Precision recall report:**
```
{clf_report}
```

**Parameter coefficients:**
```
{coefs}
```
'''
                f.write(
                    markdown_log_template.format(
                        starttime=datetime.datetime.fromtimestamp(self.starttime).strftime('%Y-%m-%d %H:%M:%S'),
                        endtime=datetime.datetime.fromtimestamp(self.endtime).strftime('%Y-%m-%d %H:%M:%S'),
                        target=self.target_col,
                        listofrestrictions='\n'.join([r['test'].feature_col + r['restriction'] for r in self.dloader.restrictors]),
                        splitdate=self.dloader.split_date,
                        trainstart=self.dloader.train_start,
                        testend=self.dloader.test_end,
                        listoffeatures='\n'.join(self.feature_list),
                        clf=self.model.scikit_class.__name__,
                        params='\n'.join(['* '+param+': '+str(val)   for param,val in self.model.scikit_params.items()]),
                        train_megaquery=self.dloader.train_megaquery,
                        test_megaquery=self.dloader.test_megaquery,
                        train_nans=self.train_rows_dropped,
                        train_rows=self.train_rows.shape[0],
                        test_nans=self.test_rows_dropped,
                        test_rows=self.test_rows.shape[0],
                        nan_column_log=self.nan_column_log,
                        TN=cm[0,0],
                        FP=cm[0,1],
                        FN=cm[1,0],
                        TP=cm[1,1],
                        TN_train=cm_train[0,0],
                        FP_train=cm_train[0,1],
                        FN_train=cm_train[1,0],
                        TP_train=cm_train[1,1],
                        path_to_cm_plt='cm_plt.png',
                        path_to_roc_plt='roc_plt.png',
                        path_to_pr0_plt='pr0_plt.png',
                        path_to_pr1_plt='pr1_plt.png',
                        coefs=coefs,
                        clf_report=clf_report,
                        path_to_cm_plt_train='cm_plt_train.png',
                        path_to_roc_plt_train='roc_plt_train.png',
                        path_to_pr0_plt_train='pr0_plt_train.png',
                        path_to_pr1_plt_train='pr1_plt_train.png',
                        clf_report_train=clf_report_train

                    )
                )

        # Add experiment to looplog
        if self.looplog!=None:
            looplogpath = os.path.join(config.PERSISTENCE_PATH, self.logFolder, self.looplog)

            # Prepare string describing target & restrictors
            if len(self.dloader.restrictors)==0:
                outcome = self.target_col
            else:
                outcome = self.target_col + '<br>WHERE<br>' + '<br>AND '.join([r['test'].feature_col + r['restriction'] for r in self.dloader.restrictors])


            # If looplog file does not exist, create empty looplog with headers
            if os.path.isfile(looplogpath)==False:
                with open(looplogpath, 'w') as f:
                    f.write(
'''| Run time | Outcome | Train Start | Split Date | Test End | Classifier | Features | AUC | AUC (Train) | Precision Top 10% | Precision Top 25% | AUPR | Log |
| -- | -- | -- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |''')

            # Add new row to looplog
            with open(looplogpath, "a") as f:
                looplog_row_template='''
| {starttime} | {outcome} | {trainstart} | {splitdate} | {testend} | {clf} | {listoffeatures} | {auc} | {auc_train} | {pr0_precision_top_10}| {pr0_precision_top_25}| {aupr0} | [Link]({logpath}) |'''

                f.write(
                    looplog_row_template.format(
                        starttime=datetime.datetime.fromtimestamp(self.starttime).strftime('%Y-%m-%d %H:%M:%S'),
                        outcome=outcome,
                        splitdate=self.dloader.split_date,
                        trainstart=self.dloader.train_start,
                        testend=self.dloader.test_end,
                        clf='**' + self.model.scikit_class.__name__ + '**' + '<br>' + '<br>'.join(['* '+str(param).replace('\n','')+': '+str(val).replace('\n','')   for param,val in self.model.scikit_params.items()]),
                        listoffeatures='<br> '.join(self.feature_list),
                        auc=round(auc,3),
                        auc_train=round(auc_train,3),
                        pr0_precision_top_10 = round(pr0_precision_top_10,3),
                        pr0_precision_top_25 = round(pr0_precision_top_25,3),
                        aupr0 = round(aupr0, 3),
                        logpath='' if self.summary_only else logsubfoldername+'/log.md'
                    )
                )

            # # Render looplog Markdown to HTML
            # markdown.markdownFromFile(input=looplogpath,
            #                           output=os.path.join(config.PERSISTENCE_PATH, self.logFolder,'looplog.html'),
            #                           extensions=['markdown.extensions.tables']
            #                         )

# if __name__ == '__main__':

#     from sklearn.linear_model import LogisticRegression
#     m = ScikitModel(   scikit_class=LogisticRegression,
#                                                 scikit_params={
#                                                     "penalty":"l2",
#                                                     "dual":False,
#                                                     "C":1.0
#                                                   }
#                                             )

#     e = Experiment(model=m, target=['PersistThreeSemesters'],
#         feature_list=['StudentAgeAtEnrollment','StudentGender'],
#         restrictors=['PersistOneSemester'],
#         split_date='2013-06-01', train_window=365, test_window=365, name='just a test')

