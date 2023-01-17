import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
from pytz import timezone
import pytz
import boto3
from datetime import date

spark = SparkSession.builder.config("spark.sql.legacy.parquet.datetimeRebaseModeInRead","LEGACY").getOrCreate()

glueContext = GlueContext(spark.sparkContext)

job = Job(glueContext)
logger = glueContext.get_logger()

tz=pytz.timezone('Asia/Calcutta')
current_date=datetime.now(tz)
print("current_date",current_date)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
#print args

RDS_ENDPOINT="jdbc:mysql://grc-sla-reports-db-cluster.cluster-ccbtiwm0ziab.us-east-1.rds.amazonaws.com:3306/grc_review_reports_db"
RDS_TABLE="acc_dev_db.per_her_cost"
RDS_USERNAME="acc_dev_readonly"
RDS_PASSOWRD="************"
TARGET_TABLE='per_her_cost'
TARGET_DB_NAME='acc_dev_db'

def read_data_from_target():
    try:
        old_per_cost_df = spark.read \
        .format("jdbc") \
        .option("url",RDS_ENDPOINT ) \
        .option("dbtable",RDS_TABLE) \
        .option("user",RDS_USERNAME) \
        .option("password",RDS_PASSOWRD) \
        .load()
    except Exception as e:
        print(f"Error Getint when Try to Read Data From Target Table {e}")
    return old_per_cost_df

def generate_sha(str):
    return sha2(str,256)

def read_data_from_s3():
    try:    
        per_her_cost= glueContext.create_dynamic_frame.from_options(
        's3',
        {'paths': ['s3://acc-bucket-datalake/test_sharepoint_file_download/per_hr_cost.csv']},
        'csv',
        {'withHeader': True})
        per_her_cost_df=per_her_cost.toDF()
    except Exception as e:
        print(f"Raise Error whent try to converton  s3_data to dataframe {e}")
        raise Exception(f"Raise Error whent try to converton  s3_data to dataframe {e}")
        
    return per_her_cost_df

def load_data(old_per_cost_df,per_her_cost_df):
    
    try:
        df_column_list=['Work_Role','Per_Hour_Cost']
        
        #change column of Dataframe 
        per_her_cost_df=per_her_cost_df.toDF(*df_column_list)
        
        print("DF_COLUMNS",per_her_cost_df.columns)
        
        #create SHA
        column_list=(per_her_cost_df.columns)
        per_her_cost_df=per_her_cost_df.withColumn('concated_cols',concat_ws("||",*column_list))
        per_her_cost_df=per_her_cost_df.withColumn('per_her_cost_sha',generate_sha(per_her_cost_df.concated_cols))
        per_her_cost_df=per_her_cost_df.drop("concated_cols")
        
        #Get Updated Record
        per_hr_updated_record=per_her_cost_df.join(old_per_cost_df,on=["per_her_cost_sha"],how='leftanti')
        per_hr_updated_record=per_hr_updated_record.withColumn('insert_date',lit(current_date))
        
        print("**************************************PERHRCOST UPDATED RECORD ***********************************")
        per_hr_updated_record.show()
    
        per_hr_cost_dyf= DynamicFrame.fromDF(per_hr_updated_record, glueContext, "per_hr_cost_dyf")
        
        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = per_hr_cost_dyf, catalog_connection = "acc_dev_db", connection_options = {"dbtable": TARGET_TABLE, "database":TARGET_DB_NAME}, transformation_ctx = "datasink1")
        
        per_hr_updated_record.unpersist()
        old_per_cost_df.unpersist()
        
        job.commit()
        
        
    except Exception as e:
        print(f"Getting Error When try to load Data into RDS {e}")
        raise Exception(f"Getting Error When try to load Data into RDS {e}")


def script_main_function():
    
    old_per_cost_df=read_data_from_target()
    per_her_cost_df=read_data_from_s3()
    load_data(old_per_cost_df,per_her_cost_df)


script_main_function()
    
print("--------------job completed-----------")

