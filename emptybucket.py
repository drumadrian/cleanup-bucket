import boto3
import logging

#session = boto3.session.Session(profile_name='your_profile_name')
boto3.set_stream_logger('boto3.resources', logging.DEBUG)

session = boto3.session.Session()

s3 = session.resource(service_name='s3')
bucket = s3.Bucket('amazon-s3-bucket-load-test-storagebucket-7el453fxmzen')
bucket.object_versions.delete()
# bucket.delete()


