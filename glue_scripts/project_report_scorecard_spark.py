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


destination_table = 'project_scorecard_report'
destination_db_username = 'acc_dev_readonly'
destination_db_password = '**********'
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
old_project_scorecard_df=read_data()

# Reading "project_scorecard_report" table data from catalog
project_scorecard_report_table = glueContext.create_dynamic_frame.from_catalog(
    database="raw_source_grc_review_reports_db", table_name="raw_grc_review_reports_db_project_scorecard_report" , transformation_ctx = "project_scorecard_report_table"
)

# Applying transformation on the fetched data
applymapping1 = ApplyMapping.apply( frame = project_scorecard_report_table , mappings = [ ("id", "int", "id", "int") , ("company" , "string" , "company" , "string") , ("sales_order_no" , "string" , "sales_order_no" , "string" ) , ("project_id" , "int" , "project_id" , "int") , ("project_name" , "string" , "project_name" , "string") , ("site" , "string" , "site" , "string") , ("project_manager" , "string" , "project_manager" , "string") , ( "project_type" , "string" , "project_type" , "string" ),( "project_start_date" , "string" , "project_start_date" , "string" ) , ( "project_end_date" , "string" , "project_end_date" , "string" ) , ( "actual_hours" , "string" , "actual_hours" , "string" )  , (  "project_board" , "string" , "project_board" , "string"  ),(  "project_status" , "string" , "project_status" , "string"  ) , (  "acv" , "string" , "acv" , "string"  ) , (  "estimated_hours" , "string" , "estimated_hours" , "string"  ) , (  "actual_100p" , "string" , "actual_100p" , "string"  ), (  "assessor" , "string" , "assessor" , "string"  ),( "first_or_recert" , "string" , "first_or_recert" , "string"  ) , (  "ticket_id" , "int" , "ticket_id" , "int"  ) , (  "dashboard_id" , "int" , "dashboard_id" , "int"  ) , ( "dashboard_name" , "string" , "dashboard_name" , "string"  ) , (  "dashboard_status" , "string" , "dashboard_status" , "string"  ),( "gcoe_auditor" , "string" , "gcoe_auditor" , "string"  ), ( "target_cert" , "string" , "target_cert" , "string"  ) , ( "new_target_cert_date" , "string" , "new_target_cert_date" , "string"  ) , ( "contract_expiration_date" , "string" , "contract_expiration_date" , "string"  ) , ( "created_at" , "date" , "created_at" , "date"  )],transformation_ctx = "applymapping1" )

# Converting dynamic frames to DF
project_scorecard_report_table_df = applymapping1.toDF()

#creating SHA
column_list=(project_scorecard_report_table_df.columns)
project_scorecard_report_table_df = project_scorecard_report_table_df.withColumn('concated_cols',concat_ws("||",*column_list))
project_scorecard_report_table_df = project_scorecard_report_table_df.withColumn('project_scorecard_sha',generate_sha(project_scorecard_report_table_df.concated_cols))
project_scorecard_report_table_df = project_scorecard_report_table_df.drop("concated_cols")


config_df = project_scorecard_report_table_df.join( old_project_scorecard_df, on=["project_scorecard_sha"], how='leftanti')
config_df_inserted_date = config_df.withColumn('insert_date', lit(datetime.now(timezone("Asia/Kolkata"))))

# Converting DF to dynamic frames
dyf = DynamicFrame.fromDF(config_df_inserted_date , glueContext, "dyf")

datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = "acc_dev_db", connection_options = {"dbtable":"project_scorecard_report" , "database": "acc_dev_db"}, transformation_ctx = "datasink1")

# Makes df as non-persistent - 
project_scorecard_report_table_df.unpersist()
config_df_inserted_date.unpersist()
config_df.unpersist()

job.commit()

