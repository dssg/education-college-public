import pandas as pd
from abstractmodel import *
import numpy as np
from sklearn import metrics
import matplotlib.pyplot as plt
import seaborn as sns


class ScikitModel(AbstractModel):
    '''
    Abstract representation of a Scikit model configuration. Should be extended by classes representing
    individual models, not instantiated.
    '''

    def __init__(self, scikit_class, **scikit_params):

        self.score_function
        AbstractModel.__init__(self)        
        self.scikit_class = scikit_class
        self.scikit_params = scikit_params
        self.scikit_model = self.scikit_class(**self.scikit_params)
        self.score_function

    def summary(self,prefix=''):
        p = prefix
        summary = p+'Scikit class: '+str(self.scikit_class) +'\n'
        summary += p+'Scikit parameters: ' + str(self.scikit_params)+'\n'
        return summary

    def fit(self,X_train,y_train):

        self.predictor_cols = X_train.columns
        X_train = X_train.as_matrix()
        y_train = np.ravel(y_train.as_matrix())
        y_train = (y_train>=1).astype(int)

        # train classifier
        self.scikit_model.fit(X_train, y_train)

    def predict(self,X_test):
        return self.scikit_model.predict(X_test)

    ### Evaluation metrics

    def cm(self,y_test,y_predicted):
        # return confusion matrix
        # Scikit learn format:  TN  FP
        #                       FN  TP
        return metrics.confusion_matrix(y_test, y_predicted)

    def plot_cm(self,cm):
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

    def clf_report(self, y_test, y_predicted):
        return metrics.classification_report(y_test, y_predicted)

    def auc(self,y_test,X_test):
        y_probs = self.score_function(X_test.astype(float))  # to run the prediction, the X_test needs to be cast to float
        return metrics.roc_auc_score(y_test, y_probs[:,1])

    def aupr(self, y_test, X_test, pos_label):

        y_score = self.score_function(X_test.astype(float))[:,pos_label]  # to run the prediction, the X_test needs to be cast to float

        precision_curve, recall_curve, pr_thresholds = metrics.precision_recall_curve(y_test, y_score, pos_label=pos_label)

        pr_auc = metrics.auc(recall_curve, precision_curve)   

        return pr_auc 

    def precision_top_k_percent(self, k, X_test, y_test, y_predicted, pos_label):

        y_probs = self.score_function(X_test.astype(float))[:,pos_label]  # to run the prediction, the X_test needs to be cast to float
        precision = metrics.precision_score(y_test, y_predicted)

        # if predicting the negative class, invert the True/False labels

        if pos_label == 0:
            y_test = ~y_test

        #cast y_test to float and also make it a matrix

        y_test = y_test.astype(float).as_matrix()

        # Compute the precision on the top k%.
        #get the indices of the probabilities sorted in descending order
        ord_prob = np.argsort(y_probs,)[::-1] 

        # get the number of labels to look at based on k
        r = int(k * len(y_test))

        if r == 0:
            pre_score_k = 0.0
        else:
            # sum the number of hits in the r first labels, sorted by predicted probability
            pre_score_k = np.sum(y_test[ord_prob][:r]) / float(r)

        return pre_score_k


    def plot_roc(self,y_test,X_test):
        # plot ROC curve

        y_probs = self.score_function(X_test.astype(float))  # to run the prediction, the X_test needs to be cast to float
        fpr, tpr, thresholds = metrics.roc_curve(y_test, y_probs[:,1])
        roc_auc = metrics.auc(fpr, tpr)

        fig = plt.figure()
        plt.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc)
        plt.plot([0, 1], [0, 1], 'k--')
        plt.xlim([0.0, 1.0])
        plt.ylim([0.0, 1.05])
        plt.xlabel('False Positive Rate')
        plt.ylabel('True Positive Rate')
        plt.title('Receiver operating characteristic')
        plt.legend(loc="lower right")
        return fig

    def plot_precision_recall(self, y_test, X_test, pos_label):
        y_score = self.score_function(X_test.astype(float))[:,pos_label]  # to run the prediction, the X_test needs to be cast to float

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
        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        ax1.plot(pct_above_per_thresh, precision_curve, 'b')
        ax1.set_xlabel('Fraction of Population')
        ax1.set_ylabel('Precision', color='b')
        ax1.set_ylim([0.0, 1.0])
        ax2 = ax1.twinx()
        ax2.plot(pct_above_per_thresh, recall_curve, 'r')
        ax2.set_ylabel('Recall', color='r')
        ax1.set_title('Precision-Recall, y=' + str(pos_label))

        return fig

    @property
    def score_function(self):
        raise NotImplementedError
    