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
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

destination_table = 'apac_continuous_compliance_timesheet'
destination_db_username = 'acc_dev_readonly'
destination_db_password = '1@6nWCsVDZ@b$'
destination_db_name = 'acc_dev_db'
destination_db_hostname = 'grc-sla-reports-db-cluster.cluster-ccbtiwm0ziab.us-east-1.rds.amazonaws.com'
destination_port = '3306'

def truncate_table():
    url= f"jdbc:mysql://{destination_db_hostname}:{destination_port}/{destination_db_name}"
    print("formed url:", url)
    read_df = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", f"{destination_db_name}.{destination_table}") \
    .option("user", f"{destination_db_username}") \
    .option("password", f"{destination_db_password}") \
    .load()
    
    read_df.write.mode("overwrite")\
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable",f"{destination_db_name}.{destination_table}") \
    .option("user", f"{destination_db_username}") \
    .option("password", f"{destination_db_password}") \
    .option("truncate", "true") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .save()
    


#truncate destination table
truncate_table()

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "raw_source_grc_review_reports_db", table_name = "raw_grc_review_reports_db_timesheet_report_data", transformation_ctx = "datasource0")

applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("date", "date", "date", "date"), ("gcoe", "string", "gcoe", "string"), ("notes", "string", "notes", "string"), ("cert_date", "date", "cert_date", "date"), ("member_identifier", "string", "member_identifier", "string"), ("vertical", "string", "vertical", "string"), ("company_identifier", "string", "company_identifier", "string"), ("time_entry_id", "int", "time_entry_id", "int"), ("subtype", "string", "subtype", "string"), ("dashboard_status", "string", "dashboard_status", "string"), ("ticket_status", "string", "ticket_status", "string"), ("member_id", "int", "member_id", "int"), ("summary", "string", "summary", "string"), ("hours", "string", "hours", "string"), ("dashboard_name", "string", "dashboard_name", "string"), ("target_cert_date", "date", "target_cert_date", "date"), ("workrole", "string", "workrole", "string"), ("worktype", "string", "worktype", "string"), ("ticket_id", "int", "ticket_id", "int"), ("member_name", "string", "member_name", "string"), ("site_name", "string", "site_name", "string"), ("chargetotype", "string", "chargetotype", "string"), ("company_name", "string", "company_name", "string"), ("site_id", "int", "site_id", "int"), ("admin_charge_code", "string", "admin_charge_code", "string"), ("board", "string", "board", "string")], transformation_ctx = "applymapping1")

resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")

timesheet_data_df = resolvechoice2.toDF()


selected_columns = ['member_name','date','company_name','summary','notes','vertical','admin_charge_code','hours','dashboard_name','worktype','workrole','chargetotype','cert_date' ]
filtered_df = timesheet_data_df.select(selected_columns).filter(timesheet_data_df.vertical == "APAC Continuous Compliance").orderBy(col("member_name")

dyf = DynamicFrame.fromDF(filtered_df, glueContext, "dyf")

datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = "acc_dev_db", connection_options = {"dbtable": destination_table, "database": "acc_dev_db"}, transformation_ctx = "datasink4")
job.commit()
