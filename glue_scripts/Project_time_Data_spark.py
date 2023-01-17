import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import concat_ws,sha2
from pyspark.sql.functions import lit
from datetime import datetime,timedelta,date
from pytz import timezone

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

destination_table = 'Project_time_Data'
destination_db_username = 'acc_dev_readonly'
destination_db_password = '*********'
destination_db_name = 'acc_dev_db'
destination_db_hostname = 'grc-sla-reports-db-cluster.cluster-ccbtiwm0ziab.us-east-1.rds.amazonaws.com'
destination_port = '3306'


def read_data():
    url= f"jdbc:mysql://{destination_db_hostname}:{destination_port}/{destination_db_name}"
    try:
        read_df = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", f"{destination_db_name}.{destination_table}") \
        .option("user", f"{destination_db_username}") \
        .option("password", f"{destination_db_password}") \
        .load()
        return read_df
    except Exception as e:
        logger.error(f"Exception while connecting to database table {destination_table} {e}")


#generating SHA    
def generate_sha(str):
    return sha2(str,256)

# Taking backup of target data
old_Project_time_Data_df = read_data()

# Reading "project_scorecard_report" table data from catalog
Project_time_Data_table = glueContext.create_dynamic_frame.from_catalog(
    database="raw_source_grc_review_reports_db", table_name="raw_grc_review_reports_db_bi_project_time_data_report" , transformation_ctx = "Project_time_Data_table"
)

applymapping1 = ApplyMapping.apply( frame = Project_time_Data_table , mappings= [ ("hours", "string", "hours", "string"),("billabe", "string", "billabe", "string"),("member_name", "string", "member_name", "string"),("work_type", "string", "work_type", "string"),("work_role", "string", "work_role", "string"),("agreement_id", "int", "agreement_id", "int"), ("agreement_name", "string", "agreement_name", "string"),("project_id", "int", "project_id", "int"),("project_name", "string", "project_name", "string"),("phase_name", "string", "phase_name", "string"), ("refresh_date_time", "string", "refresh_date_time", "string") ] 
, transformation_ctx = "applymapping1")

# Converting dynamic frames to DF
Project_time_Data_table_df = applymapping1.toDF()

#creating SHA
column_list=(Project_time_Data_table_df.columns)
Project_time_Data_table_df = Project_time_Data_table_df.withColumn('concated_cols',concat_ws("||",*column_list))
Project_time_Data_table_df = Project_time_Data_table_df.withColumn( 'Project_time_Data_sha',generate_sha( Project_time_Data_table_df.concated_cols ) )
Project_time_Data_table_df = Project_time_Data_table_df.drop("concated_cols")


config_df = Project_time_Data_table_df.join(old_Project_time_Data_df,on=["Project_time_Data_sha"],how='leftanti')
config_df_inserted_date =config_df.withColumn('insert_date',lit(datetime.now(timezone("Asia/Kolkata"))))

# Converting DF to dynamic frames
dyf = DynamicFrame.fromDF(config_df_inserted_date , glueContext, "dyf")

datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = "acc_dev_db", connection_options = {"dbtable": "Project_time_Data", "database": "acc_dev_db"}, transformation_ctx = "datasink1")

# Makes df as non-persistent - 
Project_time_Data_table_df.unpersist()
config_df_inserted_date.unpersist()
config_df.unpersist()

job.commit()
