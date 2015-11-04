# -*- coding: utf-8 -*-

import time, datetime
import config
import warnings
import os
import matplotlib.pyplot as plt
import pickle
import markdown
from sklearn import metrics
import numpy as np
from visualizations.parameters_vs_time import tableau20

def write_log(logpath, starttime, endtime, dloader, target_col, model, looplog, feature_list,
			  train_rows_num, test_rows_num, cm, cm_plt, roc_plt, clf_report, coefs, auc, pr1_plt, pr0_plt,
				roc_plt_train, cm_train, cm_plt_train, clf_report_train, auc_train, pr1_plt_train, pr0_plt_train):


	# Write pickle
	# pickle.dump(self, open(logpath + 'experiment.p', "wb" ) )

	# Write figures to a folder
	cm_plt.savefig(logpath+'cm_plt.png', pad_inches=1)
	roc_plt.savefig(logpath+'roc_plt.png')
	pr1_plt.savefig(logpath+'pr1_plt.png')
	pr0_plt.savefig(logpath+'pr0_plt.png')

	# write figures for training set results to folder
	cm_plt_train.savefig(logpath+'cm_plt_train.png', pad_inches=1)
	roc_plt_train.savefig(logpath+'roc_plt_train.png')
	pr1_plt_train.savefig(logpath+'pr1_plt_train.png')
	pr0_plt_train.savefig(logpath+'pr0_plt_train.png')

	# Write detailed log to markdown file
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
				starttime=datetime.datetime.fromtimestamp(starttime).strftime('%Y-%m-%d %H:%M:%S'),
				endtime=datetime.datetime.fromtimestamp(endtime).strftime('%Y-%m-%d %H:%M:%S'),
				target=target_col,
				listofrestrictions='\n'.join([r['test'].feature_col + r['restriction'] for r in dloader.restrictors]),
				splitdate=dloader.split_date,
				trainstart=dloader.train_start,
				testend=dloader.test_end,
				listoffeatures='\n'.join(feature_list),
				clf=model.scikit_class.__name__,
				params='\n'.join(['* '+param+': '+str(val)   for param,val in model.scikit_params.items()]),
				train_megaquery=dloader.train_megaquery,
				train_nans=None,
				train_rows=train_rows_num,
				test_nans=None,
				test_rows=test_rows_num,
				nan_column_log=None,
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
	if looplog!=None:
		looplogpath = os.path.join(os.path.dirname(os.path.dirname(logpath)),looplog)

		# Prepare string describing target & restrictors
		if len(dloader.restrictors)==0:
			outcome = target_col
		else:
			outcome = target_col + '<br>WHERE<br>' + '<br>AND '.join([r['test'].feature_col + r['restriction'] for r in dloader.restrictors])


		# If looplog file does not exist, create empty looplog with headers
		if os.path.isfile(looplogpath)==False:
			with open(looplogpath, 'w') as f:
				f.write(
'''| Run time | Outcome | Train Start | Split Date | Test End | Classifier | Features | avg AUC±SEM | avg AUC±SEM (Train) | Log |
| -- | -- | -- | --- | --- | --- | --- | --- | --- | --- |''')

		# Add new row to looplog
		with open(looplogpath, "a") as f:
			looplog_row_template='''
| {starttime} | {outcome} | {trainstart} | {splitdate} | {testend} | {clf} | {listoffeatures} | {auc} | {auc_train} | [Link]({logpath}) |'''

			f.write(
				looplog_row_template.format(
					starttime=datetime.datetime.fromtimestamp(starttime).strftime('%Y-%m-%d %H:%M:%S'),
					outcome=outcome,
					splitdate=dloader.split_date,
					trainstart=dloader.train_start,
					testend=dloader.test_end,
					clf='**' + model.scikit_class.__name__ + '**' + '<br>' + '<br>'.join(['* '+param+': '+str(val)   for param,val in model.scikit_params.items()]),
					listoffeatures='<br> '.join(feature_list),
					auc='%.03f ± %.03f'%(np.mean(auc),np.std(auc)/np.sqrt(len(auc))),
					auc_train='%.03f ± %.03f'%(np.mean(auc_train),np.std(auc_train)/np.sqrt(len(auc_train))),
					logpath=logpath+'log.md'
				)
			)

		# Render looplog Markdown to HTML
		markdown.markdownFromFile(input=looplogpath,
								  output=os.path.join(os.path.dirname(os.path.dirname(logpath)),'.'.split(looplog)[0]+'.html'),
								  extensions=['markdown.extensions.tables']
								)


def cm(y_test,y_predicted):
    # return confusion matrix
    # Scikit learn format:  TN  FP
    #                       FN  TP
    return metrics.confusion_matrix(y_test, y_predicted)

def plot_cm(cm):
    # plot confusion matrix
    fig = plt.figure()
    plt.imshow(cm, interpolation = 'nearest', cmap = plt.cm.Blues)
    plt.title('Confusion Matrix')
    plt.colorbar()
    tick_marks = np.arange(2)
    plt.xticks(tick_marks, ['False', 'True'], rotation=45)
    plt.yticks(tick_marks, ['False', 'True'])
    plt.tight_layout()
    plt.ylabel('Actual label')
    plt.xlabel('Predicted label')
    plt.gcf().subplots_adjust(bottom=0.15)
    return fig

def clf_report(y_test, y_predicted):
    return metrics.classification_report(y_test, y_predicted)

def auc(y_test,y_probs):
    return metrics.roc_auc_score(y_test, y_probs[:,1])


def plot_roc(y_test,y_probs,ax):
    # plot ROC curve

    fpr, tpr, thresholds = metrics.roc_curve(y_test, y_probs[:,1])
    roc_auc = metrics.auc(fpr, tpr)
    # fig = plt.figure()
    ax.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc,linewidth=0.5,color=tableau20[0])
    # return fig

def plot_precision_recall(y_test, pos_label,y_probs,ax1, ax2):
    y_score = y_probs[:,pos_label]  # to run the prediction, the X_test needs to be cast to float

    precision_curve, recall_curve, pr_thresholds = metrics.precision_recall_curve(y_test, y_score, pos_label=pos_label)
    precision_curve = precision_curve[:-1]
    recall_curve = recall_curve[:-1]
    pct_above_per_thresh = []
    number_scored = len(y_score)
    for value in pr_thresholds:
      num_above_thresh = len(y_score[y_score>=value])
      pct_above_thresh = num_above_thresh / float(number_scored)
      pct_above_per_thresh.append(pct_above_thresh)
    pct_above_per_thresh = np.array(pct_above_per_thresh)

    # Create plot
    # fig = plt.figure()
    ax1.plot(pct_above_per_thresh, precision_curve, color=tableau20[1], linewidth=0.5)
    ax2.plot(pct_above_per_thresh, recall_curve, color=tableau20[2], linewidth=0.5)

    # return fig