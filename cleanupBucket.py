
from cmreslogging.handlers import CMRESHandler
from ec2_metadata import ec2_metadata
from requests_aws4auth import AWS4Auth
import boto3
import logging
import json
import os
import sys


################################################################################################################
#   DEFAULT Global Configuration
################################################################################################################
config = {}
config['DEFAULT_loggingLevel'] = "INFO"
config['DEFAULT_bucketName'] = "amazon-s3-bucket-load-test-storagebucket-7el453fxmzen"
config['DEFAULT_deleteBucket'] = "True"
config['DEFAULT_es_host'] = "search-s3loadt-s3load-1jpqa7x5cpxfi-ayjuinlmhdse32gxc4ljr6agoa.us-west-2.es.amazonaws.com"
config['DEFAULT_es_index_name'] = "python_logger_cleanupbucket"
config['DEFAULT_environment'] = "Dev"

# config['debugTag'] = "debug"
# config['bucketNameTag'] = "bucketName"
# config['deleteBucketTag'] = "deleteBucket"
# config['es_index_name'] = config['DEFAULT_es_index_name']


config['loggingLevel'] = config['DEFAULT_loggingLevel']
config['bucketName'] = config['DEFAULT_bucketName']
config['deleteBucket'] = config['DEFAULT_deleteBucket']
config['es_index_name'] = config['DEFAULT_es_index_name']
config['environment'] = config['DEFAULT_environment']
config['es_host'] = config['DEFAULT_es_host']

################################################################################################################
#   Specific Configuration
################################################################################################################
def setupConfig(config):
    # Try and Update config from Defaults using 2 sources 
    # 1) EC2 metadata
    # 2) Environment Variables
    config['session'] = boto3.session.Session()
    config['region'] = config['session'].region_name

    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, config['region'], 'ec2', session_token=credentials.token)

    config['AWS_ACCESS_KEY_ID']=credentials.access_key
    config['AWS_SECRET_ACCESS_KEY']=credentials.secret_key
    config['AWS_SESSION_TOKEN']=credentials.token

    
    try:
        print("\nAttempting to access EC2 Metadata")
        # print("ec2_metadata(type)={0}".format(type(ec2_metadata)) )
        # print("\nec2_metadata={0}\n".format(ec2_metadata) )
        instanceID = ec2_metadata.instance_id
        # Get Instance ID
        ec2 = boto3.resource('ec2')
        for instance in ec2.instances.all():
            print (instance.tags)
            for tag in instance.tags:
                if tag['Key'] == 'loggingLevel':
                    config['loggingLevel'] = tag['Value']
                if tag['Key'] == 'bucketName':
                    config['bucketName'] = tag['Value']
                if tag['Key'] == 'deleteBucket':
                    config['deleteBucket'] = tag['Value']
                if tag['Key'] == 'es_host':
                    config['es_host'] = tag['Value']
                if tag['Key'] == 'es_index_name':
                    config['es_index_name'] = tag['Value']
                if tag['Key'] == 'environment':
                    config['environment'] = tag['Value']
    except:
        print("\tFailed to retrieve EC2 metadata!")

        
    try:
        print("\nAttempting to load Environment Variables")
        config['loggingLevel']=os.environ['loggingLevel']
        config['bucketName']=os.environ['bucketName']
        config['deleteBucket']=os.environ['deleteBucket']
        config['es_host']=os.environ['es_host']
        config['es_index_name']=os.environ['es_index_name']
        config['environment']=os.environ['environment']
        config['AWS_ACCESS_KEY_ID']=os.environ['AWS_ACCESS_KEY_ID']
        config['AWS_SECRET_ACCESS_KEY']=os.environ['AWS_SECRET_ACCESS_KEY']
        config['AWS_SESSION_TOKEN']=os.environ['AWS_SESSION_TOKEN']
    except:
        print("\tFailed to retrieve Environment Variables!")

    # SETUP LOGGING TO ELASTICSEARCH
    HOSTS=[{'host': config['es_host'], 'port': 443}]
    handler = CMRESHandler( hosts=HOSTS,
                            auth_type=CMRESHandler.AuthType.AWS_SIGNED_AUTH,
                            aws_access_key=config['AWS_ACCESS_KEY_ID'],
                            aws_secret_key=config['AWS_SECRET_ACCESS_KEY'],
                            aws_session_token=config['AWS_SESSION_TOKEN'],
                            aws_region=config['region'],
                            use_ssl=True,
                            verify_ssl=True,
                            es_additional_fields={'App': 'cleanupBucket', 'Environment': config['environment']},
                            es_index_name=config['es_index_name'])

    log = logging.getLogger("python_logger_cleanupBucket")
    loggingLevelName = config['loggingLevel']
    loggingLevel = logging._nameToLevel[loggingLevelName]
    log.setLevel(loggingLevel)
    log.addHandler(handler)
    logging.basicConfig(stream=sys.stdout, level=loggingLevel)

    return config


def cleanupBucket(s3_client, bucket):

    object_response_paginator = s3_client.get_paginator('list_object_versions')
    delete_marker_list = []
    version_list = []

    for object_response_itr in object_response_paginator.paginate(Bucket=bucket):
        if 'DeleteMarkers' in object_response_itr:
            for delete_marker in object_response_itr['DeleteMarkers']:
                delete_marker_list.append({'Key': delete_marker['Key'], 'VersionId': delete_marker['VersionId']})

        if 'Versions' in object_response_itr:
            for version in object_response_itr['Versions']:
                version_list.append({'Key': version['Key'], 'VersionId': version['VersionId']})

                
    for i in range(0, len(delete_marker_list), 1000):
        response = s3_client.delete_objects(
            Bucket=bucket,
            
            Delete={
                'Objects': delete_marker_list[i:i+1000],
                'Quiet': True
            }
        )
        print(response)
        log.info(response)

        
    for i in range(0, len(version_list), 1000):
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={
                'Objects': version_list[i:i+1000],
                'Quiet': True
            }
        )
        print(response)
        log.info(response)


################################################################################################################
################################################################################################################
#   LAMBDA HANDLER (Primary Execution Flow)
################################################################################################################
################################################################################################################
def lambda_handler(event, context): 
    config = setupConfig(event)

    
    log = logging.getLogger("python_logger_cleanupBucket")
    log.info("cleaning up bucket - START")
    log.info("config={0}".format(config) )

    s3_client = boto3.client('s3')
    bucket = config['bucketName']

    cleanupBucket(s3_client, bucket)
    print("\nSUCCEEDED to cleanup bucket: {0}\n".format(bucket) )
    log.info("SUCCEEDED to cleanup bucket: {0}".format(bucket) )

    # if config['deletebucket'] == "True":
    s3_client.delete_bucket(Bucket=bucket)
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
    context = "-"
    # event = config
    # if debug:
    #     print("\n event={0}\n".format(json.dumps(event)))
    lambda_handler(config,context)














################################################################################################################
#   REFERENCES
################################################################################################################
# https://stackoverflow.com/a/54081535/2407387
# https://stackoverflow.com/questions/52063174/list-tag-value-ec2-boto3
# https://pypi.org/project/CMRESHandler2/
# https://pypi.org/project/ec2-metadata/
# https://stackoverflow.com/questions/37514810/how-to-get-the-region-of-the-current-user-from-boto
# https://stackoverflow.com/questions/52063174/list-tag-value-ec2-boto3
# https://github.com/drumadrian/python-elasticsearch-logger
# https://github.com/drumadrian/cleanup-bucket




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



                # if tag['Key'] == deleteBucketTag
                #     config['deleteBucket'] = True
                # if tag['Value'] == deleteBucketTag
                #     config['deleteBucket'] = True


    # log.setLevel(logging.DEBUG)


            # bucket = s3.Bucket('amazon-s3-bucket-load-test-storagebucket-7el453fxmzen')


    # log.debug(json.dumps(event))
    # log.debug(context)

    # print("\n Lambda event={0}\n".format(json.dumps(event)))
    # log.debug(json.dumps(event))
    # if context == "-": #RUNNING A LOCAL EXECUTION 
        # try:            








