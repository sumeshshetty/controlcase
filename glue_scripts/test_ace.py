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
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

destination_table = 'ace'
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
old_ace_table_df = read_data()

# Reading source tables data from catalog
cw_configuration_raw = glueContext.create_dynamic_frame.from_catalog(database = "raw_source_grc_review_reports_db", table_name = "raw_grc_review_reports_db_cw_configuration" , transformation_ctx ="datasource0")
cw_configuration_questions_raw = glueContext.create_dynamic_frame.from_catalog(database = "raw_source_grc_review_reports_db", table_name = "raw_grc_review_reports_db_cw_configuration_questions" , transformation_ctx = "datasource1")

#converting into Dataframe 
cw_configuration_df = cw_configuration_raw.toDF()
cw_configuration_df.createOrReplaceTempView("cw_configuration")

cw_configuration_questions_df = cw_configuration_questions_raw.toDF()
cw_configuration_questions_df.createOrReplaceTempView("cw_configuration_questions")

# project_config_df QUERY
ace_query_df=spark.sql(''' SELECT c.config_id, c.dash_config_name as config_name, c.site_id, c.site_name, c.`status`, c.additional_data, cq.question,cq.answer,c.config_type
FROM cw_configuration c
LEFT JOIN cw_configuration_questions cq 
ON c.config_id = cq.config_id AND c.config_type = cq.config_type 
WHERE c.config_type IN ('ACE') ''')


#creating SHA
column_list=(ace_query_df.columns)
ace_query_df=ace_query_df.withColumn('concated_cols',concat_ws("||",*column_list))
ace_query_df=ace_query_df.withColumn('ace_config_sha',generate_sha(ace_query_df.concated_cols))
ace_query_df=ace_query_df.drop("concated_cols")


config_df = ace_query_df.join(old_ace_table_df,on=["ace_config_sha"],how='leftanti')
config_df_inserted_date =ace_query_df.withColumn('insert_date',lit(datetime.now(timezone("Asia/Kolkata"))))


dyf = DynamicFrame.fromDF(config_df_inserted_date , glueContext, "dyf")
datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = "acc_dev_db", connection_options = {"dbtable": destination_table, "database":destination_db_name}, transformation_ctx = "datasink1")

# Makes df as non-persistent - 
ace_query_df.unpersist()
config_df_inserted_date.unpersist()
config_df.unpersist()


job.commit()