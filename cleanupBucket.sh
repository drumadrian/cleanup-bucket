sudo yum install python3 -y
source /home/ec2-user/cleanupBucketDirectory/cleanupBucket.env
sudo pip3 install -r /home/ec2-user/cleanupBucketDirectory/requirements.txt
python3 /home/ec2-user/cleanupBucketDirectory/cleanupBucket.py
