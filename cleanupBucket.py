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
    # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html
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
    
    try:
        print("\nAttempting to load Environment Variables\n")
        config['logging_level'] = os.getenv('logging_level', default = 'INFO')
        config['bucket_name'] = os.getenv('bucket_name', default = 's3loadtest-storagebucket04df299d-6wyssbwsav39')
        config['delete_bucket'] = os.getenv('delete_bucket', default = 'False')
        config['es_index_name'] = os.getenv('es_index_name', default = 'python_logger_cleanupbucket')
        config['environment'] = os.getenv('environment', default = 'Dev')
        config['es_host'] = os.getenv('es_host', default = 'search-s3loadt-s3load-1jpqa7x5cpxfi-ayjuinlmhdse32gxc4ljr6agoa.us-west-2.es.amazonaws.com')
    except Exception as ex:
        print("\tFailed to retrieve Environment Variables!")

    try:
        config['region'] = detect_running_region()
    except Exception as ex:
        config['session'] = boto3.session.Session()
        config['region'] = config['session'].region_name

    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, config['region'], 'ec2', session_token = credentials.token)
    config['AWS_ACCESS_KEY_ID'] = credentials.access_key
    config['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key
    config['AWS_SESSION_TOKEN'] = credentials.token
    ec2 = boto3.resource('ec2', region_name = config['region'])

    try:
        print("\nAttempting to access EC2 Metadata")
        instance_id = ec2_metadata.instance_id
        ec2instance = ec2.Instance(instance_id)
        print("\nAttempting to access EC2 Tag data")
        # for instance in ec2.instances.all():
        print(ec2instance)
        for tag in ec2instance.tags:
            if tag['Key'] == 'logging_level':
                config['logging_level'] = tag['Value']
            if tag['Key'] == 'bucket_name':
                config['bucket_name'] = tag['Value']
            if tag['Key'] == 'delete_bucket':
                config['delete_bucket'] = tag['Value']
            if tag['Key'] == 'es_host':
                config['es_host'] = tag['Value']
            if tag['Key'] == 'es_index_name':
                config['es_index_name'] = tag['Value']
            if tag['Key'] == 'environment':
                config['environment'] = tag['Value']
    except Exception as ex:
        print("\tFailed to retrieve EC2 Tag data!")

    # SETUP LOGGING TO ELASTICSEARCH
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
        # print(response)
        log.info(response)

    for item in range(0, len(version_list), bulk_delete_count):
        response = s3_client.delete_objects(
            Bucket = bucket,
            Delete = {
                'Objects': version_list[item:item+bulk_delete_count],
                'Quiet': True
            }
        )
        # print(response)
        log.info(response)




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
        log.info(response)

    for version in version_list:
        response = s3_client.delete_object(Bucket = bucket, Key = version['Key'])
        # print(response)
        log.info(response)



################################################################################################################
################################################################################################################
#   LAMBDA HANDLER (Primary Execution Flow)
################################################################################################################
################################################################################################################
def lambda_handler(event, context): 
    config = setupConfig(event)

    log = config['log']
    log.info("cleaning up bucket - START")
    # log.info("config={0}".format(config) )

    s3_client = boto3.client('s3')
    bucket = config['bucket_name']

    cleanup_bucket_bulk(s3_client, bucket, log)
    print("\nSUCCEEDED: cleanup bucket bulk: {0}\n".format(bucket) )

    cleanup_bucket_objects(s3_client, bucket, log)
    print("\nSUCCEEDED: cleanup bucket objects: {0}\n".format(bucket) )

    log.info("SUCCEEDED: cleanup bucket: {0}".format(bucket) )

    if config['delete_bucket'] == "True":
        s3_client.delete_bucket(Bucket = bucket)
        log.info('Deleted bucket: {0} as requested!'.format(bucket) )

################################################################################################################
################################################################################################################
#   LAMBDA HANDLER 
################################################################################################################
################################################################################################################



################################################################################################################
# LOCAL TESTING and DEBUGGING  
################################################################################################################
if __name__ == "__main__":
    config = {}
    context = "-"
    lambda_handler(config,context)



















################################################################################################################
#   OLD CODE
################################################################################################################


    # Connect to Elasticsearch Service Domain
    # service = 'es'
    # credentials = boto3.Session().get_credentials()
    # awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    # elasticsearchclient = Elasticsearch(
    #     hosts = [{'host': ELASTICSEARCH_HOST, 'port': 443}],
    #     http_auth = awsauth,
    #     use_ssl = True,
    #     verify_certs = True,
    #     connection_class = RequestsHttpConnection
    # )

    # ELASTICSEARCH_HOST = os.environ['ELASTICSEARCH_HOST']
    # AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID']
    # AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY']
    # AWS_SESSION_TOKEN=os.environ['AWS_SESSION_TOKEN']

    # ELASTICSEARCH_HOST = os.environ['ELASTICSEARCH_HOST']
    # AWS_ACCESS_KEY_ID=credentials.access_key
    # AWS_SECRET_ACCESS_KEY=credentials.secret_key
    # AWS_SESSION_TOKEN=credentials.token
    # AWS_REGION='us-west-2'




                # if tag['Key'] == delete_bucketTag
                #     config['delete_bucket'] = True
                # if tag['Value'] == delete_bucketTag
                #     config['delete_bucket'] = True


    # log.setLevel(logging.DEBUG)


            # bucket = s3.Bucket('amazon-s3-bucket-load-test-storagebucket-7el453fxmzen')


    # log.debug(json.dumps(event))
    # log.debug(context)

    # print("\n Lambda event={0}\n".format(json.dumps(event)))
    # log.debug(json.dumps(event))
    # if context == "-": #RUNNING A LOCAL EXECUTION 
        # try:            


# config['DEFAULT_logging_level'] = "INFO"
# config['DEFAULT_bucket_name'] = "amazon-s3-bucket-load-test-storagebucket-7el453fxmzen"
# config['DEFAULT_delete_bucket'] = "True"
# config['DEFAULT_es_host'] = "search-s3loadt-s3load-1jpqa7x5cpxfi-ayjuinlmhdse32gxc4ljr6agoa.us-west-2.es.amazonaws.com"
# config['DEFAULT_es_index_name'] = "python_logger_cleanupbucket"
# config['DEFAULT_environment'] = "Dev"

# config['debugTag'] = "debug"
# config['bucket_nameTag'] = "bucket_name"
# config['delete_bucketTag'] = "delete_bucket"
# config['es_index_name'] = config['DEFAULT_es_index_name']



# config['logging_level'] = config['DEFAULT_logging_level']
# config['bucket_name'] = config['DEFAULT_bucket_name']
# config['delete_bucket'] = config['DEFAULT_delete_bucket']
# config['es_index_name'] = config['DEFAULT_es_index_name']
# config['environment'] = config['DEFAULT_environment']
# config['es_host'] = config['DEFAULT_es_host']




# os.getenv('logging_level', default='INFO')
# os.getenv('bucket_name', default='amazon-s3-bucket-load-test-storagebucket-7el453fxmzen')
# os.getenv('delete_bucket', default='True')
# os.getenv('es_index_name', default='python_logger_cleanupbucket')
# os.getenv('environment', default='Dev')
# os.getenv('es_host', default='search-s3loadt-s3load-1jpqa7x5cpxfi-ayjuinlmhdse32gxc4ljr6agoa.us-west-2.es.amazonaws.com')


        # config['logging_level']=os.environ['logging_level']
        # config['bucket_name']=os.environ['bucket_name']
        # config['delete_bucket']=os.environ['delete_bucket']
        # config['es_index_name']=os.environ['es_index_name']
        # config['environment']=os.environ['environment']
        # config['es_host']=os.environ['es_host']


    # log = logging.getLogger("python_logger_cleanupBucket")



        # print("ec2_metadata(type)={0}".format(type(ec2_metadata)) )
        # print("\nec2_metadata={0}\n".format(ec2_metadata) )
        # instanceID = ec2_metadata.instance_id
        # Get Instance ID
