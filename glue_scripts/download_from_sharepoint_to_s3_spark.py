import boto3
import json
import os
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext


args = getResolvedOptions(sys.argv, ['JOB_NAME','TempDir'])


sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

region_name = "us-east-1" 
client = boto3.client('secretsmanager',region_name=region_name)
s3_resource = boto3.resource('s3')
s3_bucket_name = 'acc-bucket-datalake'
s3_location_sharepoint = 'glue_sharepoint_download'
secret_name = "dev/ControlCaseDataLake/sharepoint"
sharepoint_url = 'https://controlcasetest.sharepoint.com/sites/ControlCaseDataLake'
sharepoint_file_url = '/sites/ControlCaseDataLake/Shared Documents/Per Hr Cost.csv'
s3_sharepoint_file_path = s3_location_sharepoint + "/Per_Hr_Cost.csv"
tmp_filename = 'Per_Hr_Cost.csv'

def check_tmp_storage(whento):
    statvfs = os.statvfs('/tmp')
    print(f"************* /tmp dir stats {whento}  ***************")
    print("Size of filesystem in bytes: ",statvfs.f_frsize * statvfs.f_blocks)     
    print("Actual number of free bytes: ",statvfs.f_frsize * statvfs.f_bfree)     
    print(f"************* /tmp dir stats {whento} ***************\n")


def get_sharepoint_context_using_app(client_id , client_secret):
    
    # Initialize the client credentials
    client_credentials = ClientCredential(client_id, client_secret)
    # create client context object
    ctx = ClientContext(sharepoint_url).with_credentials(client_credentials)
    return ctx

def download_report_to_tmp(file_url, filename, ctx):
    # file_url is the relative url of the file in sharepoint
    file_path = os.path.abspath(filename)
    with open(file_path, "wb") as local_file:
        file = ctx.web.get_file_by_server_relative_url(file_url)
        
        file.download(local_file)
        ctx.execute_query()

    print(f" Your file is Temporarily downloaded here: {file_path}")
    check_tmp_storage( whento= "after downlaoding")
    
    return file_path
    
def get_share_point_creds():
    try:
        secret_response = client.get_secret_value(
            SecretId= secret_name
        )
        secret_info = json.loads(secret_response['SecretString'])
    except Exception as e:
        print(f"Exception while retrieving Secrets from Secret manager {e}")
        raise Exception(f"Exception while retrieving Secrets from Secret manager {e}")
    return secret_info['Client Id'], secret_info['Client Secret']
    
def upload_tmpfile_to_s3(file_path):
    # with open (file_path) as f:
    #     data = f.readlines().decode('utf-8')
    
    with open('/tmp/'+ 'Per_Hr_Cost.csv', 'rb') as file:
        data = file.read()
    print("******** data ********")
    print(type(data))
    print("******** data ********")
    #s3_resource.Bucket(s3_bucket_name).download_file(s3_sharepoint_file_path, "/"+file_path)
    object1 = s3_resource.Object(s3_bucket_name, s3_sharepoint_file_path)
    object1.put(Body=data)
    print(f"file downlaoded to S3:  {s3_sharepoint_file_path}")
    


check_tmp_storage( whento= "before")

client_id , client_secret = get_share_point_creds()
ctx = get_sharepoint_context_using_app(client_id , client_secret)
file_path = download_report_to_tmp(sharepoint_file_url, tmp_filename, ctx)
upload_tmpfile_to_s3(file_path)

check_tmp_storage( whento= "after")
job.commit()




