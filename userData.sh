#!/bin/bash
yum update -y

mkdir /home/ec2-user/cleanupBucketDirectory
cd /home/ec2-user/cleanupBucketDirectory

curl https://raw.githubusercontent.com/drumadrian/cleanup-bucket/main/cleanupBucket.sh -o cleanupBucket.sh
curl https://raw.githubusercontent.com/drumadrian/cleanup-bucket/main/requirements.txt -o requirements.txt
curl https://raw.githubusercontent.com/drumadrian/cleanup-bucket/main/cleanupBucket.py -o cleanupBucket.py
curl https://raw.githubusercontent.com/drumadrian/cleanup-bucket/main/cleanupBucket.env -o cleanupBucket.env

# sudo --user=ec2-user /home/ec2-user/cleanupBucketDirectory/cleanupBucket.sh
# su ec2-user bash -c "/home/ec2-user/cleanupBucketDirectory/cleanupBucket.sh"
runuser -l ec2-user -c 'bash -SHa /home/ec2-user/cleanupBucketDirectory/cleanupBucket.sh'






