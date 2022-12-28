import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

destination_table = 'apac_continuous_compliance_timesheet'
destination_db_username = 'acc_dev_readonly'
destination_db_password = '1@6nWCsVDZ@b$'
destination_db_name = 'acc_dev_db'
destination_db_hostname = 'grc-sla-reports-db-cluster.cluster-ccbtiwm0ziab.us-east-1.rds.amazonaws.com'
destination_port = '3306'


table_vertical_mapping = [
                           {
                              "report_vertical":"APAC Continuous Compliance",
                              "table_name":"apac_continuous_compliance_timesheet"
                           },
                           {
                              "report_vertical":"US Tech/BPO/Payments/Retail",
                              "table_name":"us_tech_bpo_payments_retail_timesheet"
                           }
                        ]

def truncate_table(table_name):
    url= f"jdbc:mysql://{destination_db_hostname}:{destination_port}/{destination_db_name}"
    
    try:
        read_df = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", f"{destination_db_name}.{table_name}") \
        .option("user", f"{destination_db_username}") \
        .option("password", f"{destination_db_password}") \
        .load()
    except Exception as e:
        logger.error(f"Exception while connecting to database table {table_name} {e}")
         
    read_df.write.mode("overwrite")\
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable",f"{destination_db_name}.{table_name}") \
    .option("user", f"{destination_db_username}") \
    .option("password", f"{destination_db_password}") \
    .option("truncate", "true") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .save()
    




datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "raw_source_grc_review_reports_db", table_name = "raw_grc_review_reports_db_timesheet_report_data", transformation_ctx = "datasource0")

applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("date", "date", "date", "date"), ("gcoe", "string", "gcoe", "string"), ("notes", "string", "notes", "string"), ("cert_date", "date", "cert_date", "date"), ("member_identifier", "string", "member_identifier", "string"), ("vertical", "string", "vertical", "string"), ("company_identifier", "string", "company_identifier", "string"), ("time_entry_id", "int", "time_entry_id", "int"), ("subtype", "string", "subtype", "string"), ("dashboard_status", "string", "dashboard_status", "string"), ("ticket_status", "string", "ticket_status", "string"), ("member_id", "int", "member_id", "int"), ("summary", "string", "summary", "string"), ("hours", "string", "hours", "string"), ("dashboard_name", "string", "dashboard_name", "string"), ("target_cert_date", "date", "target_cert_date", "date"), ("workrole", "string", "workrole", "string"), ("worktype", "string", "worktype", "string"), ("ticket_id", "int", "ticket_id", "int"), ("member_name", "string", "member_name", "string"), ("site_name", "string", "site_name", "string"), ("chargetotype", "string", "chargetotype", "string"), ("company_name", "string", "company_name", "string"), ("site_id", "int", "site_id", "int"), ("admin_charge_code", "string", "admin_charge_code", "string"), ("board", "string", "board", "string")], transformation_ctx = "applymapping1")

resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")

timesheet_data_df = resolvechoice2.toDF()


selected_columns = ['member_name','date','company_name','summary','notes','vertical','admin_charge_code','hours','dashboard_name','worktype','workrole','chargetotype','cert_date' ]


for table_info in table_vertical_mapping:
    table_name = table_info['table_name']
    report_vertical = table_info['report_vertical']

    #truncate destination table
    logger.info(f"************* trying to truncate {table_name} ******************")
    truncate_table(table_name)
    logger.info(f"************* successfully truncated {table_name} **************")
    try:
        filtered_df = timesheet_data_df.select(selected_columns).filter(timesheet_data_df.vertical == report_vertical).orderBy(col("member_name"))
    
        dyf = DynamicFrame.fromDF(filtered_df, glueContext, "dyf")
    
        datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = "acc_dev_db", connection_options = {"dbtable": table_name, "database": "acc_dev_db"}, transformation_ctx = "datasink4")
        filtered_df.unpersist()
        logger.info(f"successfully uploaded new data to {table_name} \n")
    except Exception as e:
        logger.error(f"error while loading data to table: {table_name} {e}\n")
        pass
    

job.commit()
