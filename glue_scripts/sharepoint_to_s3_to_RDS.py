import boto3
import json
import os
import sys
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import date,datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pytz
from pytz import timezone
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File

args = getResolvedOptions(sys.argv, ['JOB_NAME','TempDir'])

sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

tz=pytz.timezone('Asia/Calcutta')
current_date=datetime.now(tz)

region_name = "us-east-1" 
client = boto3.client('secretsmanager',region_name=region_name)
s3_resource = boto3.resource('s3')
s3_bucket_name = 'acc-bucket-datalake'

s3_location_sharepoint = 'test_sharepoint_file_download/'
secret_name = "dev/ControlCaseDataLake/sharepoint"
sharepoint_url = 'https://controlcasetest.sharepoint.com/sites/ControlCaseDataLake'
file_url='/sites/ControlCaseDataLake/Shared Documents/'


Sharepoint_File_List=[]
Downlaod_File_Path=[]
S3_File_Path_list=[]

def get_sharepoint_crd():
    try:
        secret_response = client.get_secret_value(
            SecretId= secret_name
        )
        print("Secret_Response",secret_response)
        secret_info = json.loads(secret_response['SecretString'])
        print("secret_info",secret_info)
    except Exception as e:
        print(f"Exception while retrieving Secrets from Secret manager {e}")
        raise Exception(f"Exception while retrieving Secrets from Secret manager {e}")
    return secret_info['Client Id'], secret_info['Client Secret']
    
def get_sharepoint_context_using_app(Client_Id,Client_Secret):
    try:
        # Get sharepoint credentials
        sharepoint_urls = sharepoint_url
    
        # Initialize the client credentials
        client_credentials = ClientCredential(Client_Id,Client_Secret)
    
        # create client context object
        ctx = ClientContext(sharepoint_urls).with_credentials(client_credentials)
        print("ctx",ctx)
    except Exception as e:
        print(f"SHAREPOINT CREDITIOANL ERROR {e}")
        raise Exception(f"SHAREPOINT CREDITIOANL ERROR {e}")

    return ctx
    
#get sharepoint file list      
def get_sharepoint_files(ctx):
    try:
        Main_Folder=ctx.web.get_folder_by_server_relative_url(file_url)
        Main_Folder.expand(["Files","Folders"]).get().execute_query()
        try:
            sub_folder=Main_Folder.folders
            for s_folder in sub_folder:
                sub_folder_name=s_folder.name
                print(f"********SUBFOLDER NAME :- {sub_folder_name} *******")
                
        except Exception as e:
            print("FOLDER NOT FOUND :-",e)
            raise Exception("FOLDER NOT FOUND :-",e)
            
        Sub_Folder_URL=ctx.web.get_folder_by_server_relative_url(file_url+f'/{sub_folder_name}')
        Sub_Folder_URL.expand(["Files","Folders"]).get().execute_query()
        
        for files in Sub_Folder_URL.files:
            print(files)
            Sharepoint_File_List.append(files.name)
        print(f"********** SHAREPOINT FILE LIST :- {Sharepoint_File_List} ****************")
        
    except Exception as e:
        print(f"Getting Error when select Files from Subfolder {e}")
        raise Exception(f"Getting Error when select Files from Subfolder {e}")
        
    return Sharepoint_File_List,sub_folder_name
    
def download_sharepoint_files(ctx,Sharepoint_File_List,sub_folder_name):
    try:
        for filelist in Sharepoint_File_List:
            print("SHAREPOINT FILE LIST:-",filelist)
            file_path=os.path.abspath(filelist)
            with open(file_path,"wb") as filelists:
                dw_file=ctx.web.get_file_by_server_relative_url(file_url+f'/{sub_folder_name}/'+filelist)
                dw_file.download(filelists)
                ctx.execute_query()
                Downlaod_File_Path.append(file_path)
                print(f" Your file is downloaded here: {file_path}")
    except Exception as e:
        print("Getting Error when DOwnloading File from Subfolder SHarepoint",e)
        raise Exception("Getting Error when Downloading File from Subfolder Sharepoint",e)
    return Downlaod_File_Path

#upload temp file tos3
def upload_tmpfile_to_s3(Downlaod_File_Path):
    try:
        for dw_file_path in Downlaod_File_Path:
            if 'per' in dw_file_path.lower():
                file_name=dw_file_path.rsplit('/',1)[1]
                file_name=file_name.replace(file_name,"per_hr_cost")
                
                print("FILENAME",file_name)
                print("Dwonload file path",dw_file_path)
                with open(dw_file_path, 'rb') as file:
                    data = file.read()
                    print("******** data type ********")
                    print(type(data))
                    object1 = s3_resource.Object(s3_bucket_name, s3_location_sharepoint+f"{file_name}.csv")
                    object1.put(Body=data)
                    file.close()
                    print(f"file downlaoded to S3:  {object1}")
                    S3_File_Path=f"s3://{object1.bucket_name}/{object1.key}"
                    S3_File_Path_list.append(S3_File_Path)
    except Exception as e:
        print(f"Error Geting When Uploading file into S3 {e}")
        raise Exception(f"Error Geting When Uploading file into S3 {e}")
    print(f"S3_File_Path_list are {S3_File_Path_list}")
    return S3_File_Path_list
    
def script_main_function(secret_name):
    #Access SHarepoint
    Client_Id,Client_Secret=get_sharepoint_crd()
    ctx=get_sharepoint_context_using_app(Client_Id,Client_Secret)
    #Get Files from SHarepoint
    Sharepoint_File_List,sub_folder_name=get_sharepoint_files(ctx)
    #Downlaod file form SHarepoint
    Downlaod_File_Path=download_sharepoint_files(ctx,Sharepoint_File_List,sub_folder_name)
    #upload file into sharepoint
    S3_File_Path_list=upload_tmpfile_to_s3(Downlaod_File_Path)
    
script_main_function(secret_name)

print("JOB COMPLETED")
