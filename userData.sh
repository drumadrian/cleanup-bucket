#!/bin/bash
yum update -y

cd /home/ec2-user
mkdir cleanupBucketDirectory
cd cleanupBucketDirectory

wget github.com/cleanupBucket.sh
source cleanupBucket.env







