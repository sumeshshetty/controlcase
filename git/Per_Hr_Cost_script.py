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

spark = SparkSession.builder.config("spark.sql.legacy.parquet.datetimeRebaseModeInRead","LEGACY").getOrCreate()

glueContext = GlueContext(spark.sparkContext)

job = Job(glueContext)
logger = glueContext.get_logger()

tz=pytz.timezone('Asia/Calcutta')
current_date=datetime.now(tz)
print("current_date",current_date)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

db_table='per_her_cost'
db_name='acc_dev_db'

def read_data():
    read_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://grc-sla-reports-db-cluster.cluster-ccbtiwm0ziab.us-east-1.rds.amazonaws.com:3306/grc_review_reports_db") \
    .option("dbtable", "acc_dev_db.per_her_cost") \
    .option("user", "acc_dev_readonly") \
    .option("password", "1@6nWCsVDZ@b$") \
    .load()
    old_per_cost_df=read_df
    return old_per_cost_df

def generate_sha(str):
    return sha2(str,256)

def read_data_from_s3():
    try:
        per_her_cost= glueContext.create_dynamic_frame.from_options(
        's3',
        {'paths': ['s3://acc-bucket-datalake/sharepoint_file/records_per_hour.csv']},
        'csv',
        {'withHeader': True})
        per_her_cost_df=per_her_cost.toDF()
    except Exception as e:
        print("ERROR",e)
    
    return per_her_cost_df

def load_data():
    old_per_cost_df=read_data()
    
    per_her_cost_df=read_data_from_s3()
    
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
    
    # datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = memberlist_dyf, catalog_connection = "acc_dev_db", connection_options = {"dbtable": db_table, "database":db_name}, transformation_ctx = "datasink1")
    # job.commit()
    
load_data()
print("--------------job completed-----------")













