#!/bin/bash
set -e  # set error checking

# IMPORTANT: Before running this script, you must copy config.sh.example
# to config.sh and set the variables in config.sh

source config.sh


##################
### Parse Args ###
##################

for i in "$@"
do
case $i in
    --modeling=*)
	run_modeling="True"
    modelconfig="${i#*=}"
    shift # past argument=value
    ;;
    --modeling)
	run_modeling="True"
    shift # past argument=value
    ;;
    --etl)
    run_etl="True"
    shift # past argument with no value
    ;;
    *)
            # unknown option
    ;;
esac
done

###########
### Run ###
###########

# Check config is set
for var in db_host db_user db_pw ; do
    if [ ! -n "${!var}" ] ; then
        echo "Configuration is incomplete: '$var' is not set."
        echo "You must edit this bash script and complete the config."
        exit
    fi
done

# Add code folder to python path (this did not work on some machines, so we do it each time we call python)
#PYTHONPATH="$(pwd)/code:${PYTHONPATH}"

# Verify required packages are installed
printf "\nVerifying Python package dependencies in requirements.txt are met...\n"
PYTHONPATH="$(pwd)/code:${PYTHONPATH}" python code/util/verify_requirements.py

# Write cred.py file for PostgreSQL database credentials
echo "host = '${db_host}'" > "$(pwd)/code/util/cred.py"
echo "user = '${db_user}'" >> "$(pwd)/code/util/cred.py"
echo "pw = '${db_pw}'" >> "$(pwd)/code/util/cred.py"
echo "dbname = '${db_name}'" >> "$(pwd)/code/util/cred.py"
printf "\nWrote PostgreSQL credentials to code/util/cred.py.\n"

# Run ETL
if [ "$run_etl" == "True" ]; then

	# Create data directory if it does not exist
	if [ ! -d "data" ]; then
		mkdir data
		printf "\nCreated data directory for mounting remote data.\n"
	fi

	# Mount remote data if it's not already mounted
	if mount | grep "$(pwd)/data"; then
		printf "\nRemote data already mounted to data directory, proceeding...\n"
	elif find "data/" -mindepth 1 -print -quit | grep -q .; then
	    printf "\nData directory contains files, proceeding...\n"
	else
		printf "\nMounting data folder...\n"
		sshfs ${ssh_opts} ${ssh_user}@${ssh_host}:${ssh_datafolder} data/
	fi

	# Perform ETL
	printf "\nPerforming ETL and uploading to DB...\n"
	PYTHONPATH="$(pwd)/code:${PYTHONPATH}" python code/etl/run_all_etl.py
fi

# Run loopmodels
if [ "$run_modeling" == "True" ]; then
	printf "\nRunning models...\n"
	PYTHONPATH="$(pwd)/code:${PYTHONPATH}" python code/modeling/loopmodels.py ${modelconfig}
fi