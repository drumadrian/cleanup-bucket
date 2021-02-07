"""Tool to cleanup an Amazon S3 bucket using Spot instances launched from the EC2 Console

Use this tool to cleanup an Amazon S3 bucket that has too many objects to be removed using the AWS Console. 
Login and use the EC2 launch wizard to start new Spot instances using the User Data provided in this project. 
This python script will cleanup the bucket when executed by the User Data code. 


References: 
    https://stackoverflow.com/a/54081535/2407387
    https://stackoverflow.com/questions/52063174/list-tag-value-ec2-boto3
    https://pypi.org/project/CMRESHandler2/
    https://pypi.org/project/ec2-metadata/
    https://stackoverflow.com/questions/37514810/how-to-get-the-region-of-the-current-user-from-boto
    https://stackoverflow.com/questions/52063174/list-tag-value-ec2-boto3
    https://github.com/drumadrian/python-elasticsearch-logger
    https://github.com/drumadrian/cleanup-bucket
    https://stackoverflow.com/questions/37514810/how-to-get-the-region-of-the-current-user-from-boto/37519906
    https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html

"""

from cmreslogging.handlers import CMRESHandler
from requests_aws4auth import AWS4Auth
from ec2_metadata import ec2_metadata
import logging
import requests
import boto3
import json
import sys
import os



################################################################################################################
#   Detect region
################################################################################################################
def detect_running_region():
    """Dynamically determine the region from a running Glue job (or anything on EC2 for
    that matter)."""
    easy_checks = [
        # check if set through ENV vars
        os.environ.get('AWS_REGION'),
        os.environ.get('AWS_DEFAULT_REGION'),
        # else check if set in config or in boto already
        boto3.DEFAULT_SESSION.region_name if boto3.DEFAULT_SESSION else None,
        boto3.Session().region_name,
    ]
    for region in easy_checks:
        if region:
            return region
    # else query an external service
    response = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
    response_json = response.json()
    return response_json.get('region')


################################################################################################################
#   Specific Configuration
################################################################################################################
def setupConfig(config):
    # Try and Update config from Defaults using 2 sources 
    # 1) Environment Variables
    # 2) EC2 metadata
    
    ################################################################################################################
    #   Environment Variables
    ################################################################################################################
    try:
        print("\nAttempting to load Environment Variables\n")
        config['logging_level'] = os.getenv('logging_level', default = 'INFO')
        config['bucket_name'] = os.getenv('bucket_name', default = 'default-bucket')
        config['delete_bucket'] = os.getenv('delete_bucket', default = 'False')
        config['es_index_name'] = os.getenv('es_index_name', default = 'python_logger_cleanupbucket')
        config['environment'] = os.getenv('environment', default = 'Dev')
        config['es_host'] = os.getenv('es_host', default = 'elasticsearch-domain')
    except Exception as ex:
        print("\tFailed to retrieve Environment Variables!")

    try:
        config['region'] = detect_running_region()
    except Exception as ex:
        config['session'] = boto3.session.Session()
        config['region'] = config['session'].region_name

    ################################################################################################################
    #   GET AWS CREDENTIALS  LOGGING TO ELASTICSEARCH
    ################################################################################################################
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, config['region'], 'ec2', session_token = credentials.token)
    config['AWS_ACCESS_KEY_ID'] = credentials.access_key
    config['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key
    config['AWS_SESSION_TOKEN'] = credentials.token
    
    ################################################################################################################
    #   SETUP LOGGING TO ELASTICSEARCH
    ################################################################################################################
    HOSTS=[{'host': config['es_host'], 'port': 443}]
    handler = CMRESHandler( hosts = HOSTS,
                            auth_type = CMRESHandler.AuthType.AWS_SIGNED_AUTH,
                            aws_access_key = config['AWS_ACCESS_KEY_ID'],
                            aws_secret_key = config['AWS_SECRET_ACCESS_KEY'],
                            aws_session_token = config['AWS_SESSION_TOKEN'],
                            aws_region = config['region'],
                            use_ssl = True,
                            verify_ssl = True,
                            es_additional_fields = {'App': 'cleanupBucket', 'Environment': config['environment']},
                            es_index_name = config['es_index_name'])

    config['log'] = logging.getLogger("python_logger_cleanupBucket")
    logging_level_name = config['logging_level']
    logging_level = logging._nameToLevel[logging_level_name]
    config['log'].setLevel(logging_level)
    config['log'].addHandler(handler)
    logging.basicConfig(stream = sys.stdout, level = logging_level)

    return config


################################################################################################################
#   cleanup_bucket_bulk()  Used for Bulk object delete requests 
################################################################################################################
def cleanup_bucket_bulk(s3_client, bucket, log):

    object_response_paginator = s3_client.get_paginator('list_object_versions')
    delete_marker_list = []
    version_list = []
    bulk_delete_count = 100

    for object_response_itr in object_response_paginator.paginate(Bucket=bucket):
        if 'DeleteMarkers' in object_response_itr:
            for delete_marker in object_response_itr['DeleteMarkers']:
                delete_marker_list.append({'Key': delete_marker['Key'], 'VersionId': delete_marker['VersionId']})

        if 'Versions' in object_response_itr:
            for version in object_response_itr['Versions']:
                version_list.append({'Key': version['Key'], 'VersionId': version['VersionId']})
    
    log.info('DeleteMarkers and Versions lists created')
    log.info('Proceeding to Cleanup Bucket {0}.  Ttems at a time= {0}'.format(bucket, bulk_delete_count))

    for item in range(0, len(delete_marker_list), bulk_delete_count):
        response = s3_client.delete_objects(
            Bucket = bucket,
            Delete = {
                'Objects': delete_marker_list[item:item+bulk_delete_count],
                'Quiet': True
            }
        )
        log.debug(response)

    for item in range(0, len(version_list), bulk_delete_count):
        response = s3_client.delete_objects(
            Bucket = bucket,
            Delete = {
                'Objects': version_list[item:item+bulk_delete_count],
                'Quiet': True
            }
        )
        log.debug(response)


################################################################################################################
#   cleanup_bucket_bulk()  Used for individual delete requests 
################################################################################################################
def cleanup_bucket_objects(s3_client, bucket, log):

    object_response_paginator = s3_client.get_paginator('list_object_versions')
    delete_marker_list = []
    version_list = []

    for object_response_itr in object_response_paginator.paginate(Bucket = bucket):
        if 'DeleteMarkers' in object_response_itr:
            for delete_marker in object_response_itr['DeleteMarkers']:
                delete_marker_list.append({'Key': delete_marker['Key'], 'VersionId': delete_marker['VersionId']})

        if 'Versions' in object_response_itr:
            for version in object_response_itr['Versions']:
                version_list.append({'Key': version['Key'], 'VersionId': version['VersionId']})
    
    log.info('DeleteMarkers and Versions lists re-created')
    log.info('Proceeding to Cleanup Bucket: {0}'.format(bucket))

    for marker in delete_marker_list:
        response = s3_client.delete_object(Bucket = bucket, Key = marker)
        # print(response)
        log.debug(response)

    for version in version_list:
        response = s3_client.delete_object(Bucket = bucket, Key = version['Key'])
        # print(response)
        log.debug(response)


################################################################################################################
################################################################################################################
#   LAMBDA HANDLER (Primary Execution Flow)
################################################################################################################
################################################################################################################
def lambda_handler(event, context): 

    ################################################################################################################
    #   Setup configuration variables
    ################################################################################################################
    config = setupConfig(event)
    log = config['log']
    log.info("cleaning up bucket - START")
    s3_client = boto3.client('s3')
    bucket = config['bucket_name']

    ################################################################################################################
    #   Start Bucket cleanup
    ################################################################################################################
    cleanup_bucket_bulk(s3_client, bucket, log)
    print("\nSUCCEEDED: cleanup bucket bulk: {0}\n".format(bucket) )
    log.info("\nSUCCEEDED: cleanup bucket bulk: {0}\n".format(bucket) )

    cleanup_bucket_objects(s3_client, bucket, log)
    print("\nSUCCEEDED: cleanup bucket objects: {0}\n".format(bucket) )
    log.info("SUCCEEDED: cleanup bucket: {0}".format(bucket) )

    ################################################################################################################
    #   Delete bucket if configured
    ################################################################################################################
    if config['delete_bucket'] == "True":
        s3_client.delete_bucket(Bucket = bucket)
        print('Deleted bucket: {0} as requested!'.format(bucket) )
        log.info('Deleted bucket: {0} as requested!'.format(bucket) )


################################################################################################################
# LOCAL TESTING and DEBUGGING  
################################################################################################################
if __name__ == "__main__":
    config = {}
    context = "-"
    lambda_handler(config,context)
