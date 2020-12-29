#!/bin/bash
yum update -y

mkdir /home/ec2-user/cleanupBucketDirectory
cd /home/ec2-user/cleanupBucketDirectory

curl https://raw.githubusercontent.com/drumadrian/cleanup-bucket/main/cleanupBucket.sh -o cleanupBucket.sh
curl https://raw.githubusercontent.com/drumadrian/cleanup-bucket/main/requirements.txt -o requirements.txt

su -u ec2-user cleanupBucket.sh






