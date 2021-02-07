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
curl https://raw.githubusercontent.com/drumadrian/cleanup-bucket/main/cleanupBucket.env -o cleanupBucket.env


#################################################################
## Use Environment Variables OR EC2 Tags") OR EC2 Instance tags
#################################################################
export logging_level="DEBUG"
export bucket_name=""
export delete_bucket="False"
export es_index_name="python_logger_cleanupbucket"
export environment="Dev"
export es_host=""
printenv > /home/ec2-user/cleanupBucketDirectory/cleanupBucket.env


#################################################################
## Start the bucket cleanup script
#################################################################
runuser -l ec2-user -c 'bash /home/ec2-user/cleanupBucketDirectory/cleanupBucket.sh'




