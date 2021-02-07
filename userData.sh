#!/bin/bash
yum update -y
mkdir /home/ec2-user/cleanupBucketDirectory
cd /home/ec2-user/cleanupBucketDirectory


#################################################################
## Download files from GitHub
#################################################################
curl https://raw.githubusercontent.com/drumadrian/cleanup-bucket/main/cleanupBucket.sh -o cleanupBucket.sh
curl https://raw.githubusercontent.com/drumadrian/cleanup-bucket/main/requirements.txt -o requirements.txt
curl https://raw.githubusercontent.com/drumadrian/cleanup-bucket/main/cleanupBucket.py -o cleanupBucket.py


#################################################################
## Use Environment Variables 
#################################################################
export logging_level="INFO"
export bucket_name="default-bucket"
export delete_bucket="False"
export es_index_name="python_logger_cleanupbucket"
export environment="Dev"
export es_host="elasticsearch-domain"

echo "" > /home/ec2-user/cleanupBucketDirectory/cleanupBucket.env
echo "logging_level=$logging_level" >> /home/ec2-user/cleanupBucketDirectory/cleanupBucket.env
echo "bucket_name=$bucket_name" >> /home/ec2-user/cleanupBucketDirectory/cleanupBucket.env
echo "delete_bucket=$delete_bucket" >> /home/ec2-user/cleanupBucketDirectory/cleanupBucket.env
echo "es_index_name=$es_index_name" >> /home/ec2-user/cleanupBucketDirectory/cleanupBucket.env
echo "environment=$environment" >> /home/ec2-user/cleanupBucketDirectory/cleanupBucket.env
echo "es_host=$es_host" >> /home/ec2-user/cleanupBucketDirectory/cleanupBucket.env


#################################################################
## Start the bucket cleanup script
#################################################################
runuser -l ec2-user -c 'bash /home/ec2-user/cleanupBucketDirectory/cleanupBucket.sh'




