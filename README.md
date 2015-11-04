# Predicting College Persistence

This is a 2015 Data Science For Social Good summer project focused on 
increase the proportion of their alumni who graduate college. We are working with several charter school partners to help target their alumni intervention efforts by predicting which students are at risk of not persisting. 

## Team Members

* Benedict Kuester
* Michael Stepner
* Masha Westerlund
* Anushka Anand
* Alan Fritzler

## To run this code

`run_project.sh` does all the configuration and verifies that all requirements are met to run our code.  It is the best starting point for getting our code running.

**Before starting,** you will need to copy `config.sh.example` to `config.sh`. Then open `config.sh` in a text editor and fill in the settings.

If you run the shell script **without arguments**, it will simply verify that the configuration is complete and the required Python packages are installed.

The script also accepts two arguments (alone or combined):

* `--etl` runs our ETL pipeline and loads all data into an empty Postgres database.
* `--modeling` runs our modeling pipeline, which runs one or many models and logs the results. By default this runs the model config in [code/modeling/configs/default.yaml](code/modeling/configs/default.yaml).  But you can also specify another config file using `--modeling=PATH_TO_CONFIG`.

## Repo Contents

* run_project.sh: a shell script that safely runs our code
    - verifies all dependencies are installed
    - loads and applies config from `config.sh`
    - sets required PYTHONPATH
    - runs ETL or Modeling
* requirements.txt: lists Python package dependencies required to run all the code
* code: contains all analysis code
    - etl: code for cleaning and uploading raw data to databases
    - modeling: code for generating features, fitting and evaluating models
    - util: utility modules that are used throughout the codebase
    - visualizations: code for generating model figures and other visualizations

## Copyright and License

Code is copyright 2015 Data Science for Social Good Summer Fellowship and released under the MIT license.