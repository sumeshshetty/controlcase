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

destination_table = 'memberlist'
destination_db_username = 'acc_dev_readonly'
destination_db_password = '********'
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
old_memberlist_table_df = read_data()

# Reading "project_scorecard_report" table data from catalog
memberlist_table = glueContext.create_dynamic_frame.from_catalog(
    database="raw_source_grc_review_reports_db", table_name="raw_grc_review_reports_db_bi_memberlist_report" , transformation_ctx = "memberlist_table"
)

applymapping1 = ApplyMapping.apply( frame = memberlist_table , mappings= [ ("member_identifier", "string", "member_identifier", "string"),("first_name", "string", "first_name", "string"),("last_name", "string", "last_name", "string"),("inactive", "string", "inactive", "string"),("location", "string", "location", "string"),("type", "string", "type", "string"), ("role_id", "int", "role_id", "int"),("work_role", "string", "work_role", "string"),("department", "string", "department", "string"),("time_approver", "string", "time_approver", "string"),("exp_approver", "string", "exp_approver", "string"),("utilization", "string", "utilization", "string"),("report_card", "string", "report_card", "string"), ("mobile", "string", "mobile", "string"),("last_login", "string", "last_login", "string"),("reports_to", "string", "reports_to", "string"),("member_recid", "int", "member_recid", "int"),("authentication_type", "string", "authentication_type", "string"), ("doj" , "string", "doj", "string"),("title", "string", "title", "string"),("country", "string", "country", "string") ] 
, transformation_ctx = "applymapping1")

# Converting dynamic frames to DF
memberlist_table_df = applymapping1.toDF()

#creating SHA
column_list=(memberlist_table_df.columns)
memberlist_table_df = memberlist_table_df.withColumn('concated_cols',concat_ws("||",*column_list))
memberlist_table_df = memberlist_table_df.withColumn('memberlist_sha',generate_sha(memberlist_table_df.concated_cols))
memberlist_table_df = memberlist_table_df.drop("concated_cols")


config_df = memberlist_table_df.join(old_memberlist_table_df,on=["memberlist_sha"],how='leftanti')
config_df_inserted_date =config_df.withColumn('insert_date',lit(datetime.now(timezone("Asia/Kolkata"))))


# Converting DF to dynamic frames
dyf = DynamicFrame.fromDF(config_df_inserted_date , glueContext, "dyf")

datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = "acc_dev_db", connection_options = {"dbtable": "memberlist", "database": "acc_dev_db"}, transformation_ctx = "datasink1")

# Makes df as non-persistent - 
memberlist_table_df.unpersist()
config_df_inserted_date.unpersist()
config_df.unpersist()

job.commit()
