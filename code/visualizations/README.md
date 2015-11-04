Contains scripts (and some utility) to create visualizations. Plots are configured via the YAML files in `code/visualizations/configs/`.

## Files

* `auc_per_featuregroup.py`: creates a plot that compares the AUC for models trained on several different subset of features. Configured via `code/visualizations/configs/auc_per_featuregroup.yaml`
* `cohorts.py`: A script that iterates over models and produces performance logs for each model it runs, very similar in nature to `code/modeling/loopmodels.py`. However, this script works outside the pipeline in order to perform k-fold cross-validation, instead of temporal cross validation. The script's configuration file is `code/visualizations/configs/cohorts.yaml`.
* `cohort_log.py`: A helper module for `cohorts.py`.
* `parameters_vs_time.py`: A script that produces AUC estimates, using the values of indiviudal features as predictors. AUCs are plotted for different time windows to visualize if some features have become more or less predictive over time. Configured via `code/visualizations/configs/`
* `parameters_vs_semesters.py`: A script that treats individual features as predictors, and plots AUCs for them. Each feature is evaluated in all the semester models (i.e., predicting persistence in the first semester, given enrollment; predicting persistence in the second semester, given completion of the first semester; and so on). AUCs are plotted per feature for all the semester transitions.
* `spring_fall_counts.py`: A short script to calculate how many students enroll in the fall, and how many in the spring.
* `input_exploration.py`: Runs some exploration on the real-values input features (after dropping missing values), and plots clustered correlation matrices and some PCA results. Configured by `code/visualizations/configs/input_exploration.yaml`.
* `visutil.py`: Helper module, provides a function to check data out into a dataframe.