#!/bin/bash
#################################################################
## Create directory
#################################################################
yum update -y
DIR=/home/ec2-user/cleanupBucketDirectory
REPO=https://raw.githubusercontent.com/drumadrian/cleanup-bucket/main
mkdir $DIR
cd $DIR


#################################################################
## Download files from GitHub
#################################################################
curl $REPO/requirements.txt -o requirements.txt
curl $REPO/cleanupBucket.py -o cleanupBucket.py


#################################################################
## Use Environment Variables 
#################################################################
export logging_level="INFO"
export bucket_name="default-cleanup-bucket"
export delete_bucket="False"
export es_index_name="python_logger_cleanupbucket"
export environment="Dev"
export es_host="elasticsearch-domain"


#################################################################
## install prerequisites
#################################################################
yum install python3 -y
pip3 install -r requirements.txt

#################################################################
## Start the bucket cleanup script
# Recommended: 
#       python3 $DIR/cleanupBucket.py >> $DIR/cleanupBucket_output.txt
#################################################################
python3 cleanupBucket.py

