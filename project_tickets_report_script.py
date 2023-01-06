import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pytz import timezone
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
from pyspark.sql.functions import concat_ws,lit,sha2

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



#DATABASE CREDINTIALS
RDS_ENDPOINT="jdbc:mysql://grc-sla-reports-db-cluster.cluster-ccbtiwm0ziab.us-east-1.rds.amazonaws.com:3306/grc_review_reports_db"
DB_TABLE="acc_dev_db.project_tickets_report"
USER="acc_dev_readonly"
PASSWORD="1@6nWCsVDZ@b$"

#glue details
glue_database='raw_source_grc_review_reports_db'
glue_table ='raw_grc_review_reports_db_bi_project_tickets_report'

#RDS Details
rds_dbtable='project_tickets_report'
rds_database='acc_dev_db'

#read data from target RDS
def read_data_from_target():
    try:
        target_df = spark.read \
        .format("jdbc") \
        .option("url",RDS_ENDPOINT) \
        .option("dbtable",DB_TABLE) \
        .option("user",USER) \
        .option("password",PASSWORD) \
        .load()
        return target_df
    except Exception as e:
        print(f"Error occured when try to read data from target table {DB_TABLE} {e}")

    
#generating SHA    
def generate_sha(str):
    return sha2(str,256)
    

def project_tickets_report():
    
    old_target_df=read_data_from_target()
    
    datasource0 = glueContext.create_dynamic_frame.from_catalog(database =glue_database, table_name =glue_table, transformation_ctx = "datasource0")
    
    applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("closed_date", "date", "closed_date", "date"), ("ace", "string", "ace", "string"), ("budgeted_hours", "string", "budgeted_hours", "string"), ("project_name", "string", "project_name", "string"), ("sales_order_no", "string", "sales_order_no", "string"), ("target_rr", "string", "target_rr", "string"), ("closed_by", "string", "closed_by", "string"), ("actual_rr", "string", "actual_rr", "string"), ("project_id", "int", "project_id", "int"), ("actual_hours", "string", "actual_hours", "string"), ("report_automation", "string", "report_automation", "string"), ("date_entered", "date", "date_entered", "date"), ("summary", "string", "summary", "string"), ("company_id", "int", "company_id", "int"), ("department_name", "string", "department_name", "string"), ("ticket_owner", "string", "ticket_owner", "string"), ("due_date", "date", "due_date", "date"), ("ticket_number", "int", "ticket_number", "int"), ("refresh_date_time", "timestamp", "refresh_date_time", "timestamp"), ("resource_list", "string", "resource_list", "string"), ("site_name", "string", "site_name", "string"), ("company_name", "string", "company_name", "string"), ("updated_by", "string", "updated_by", "string"), ("site_id", "int", "site_id", "int"), ("location", "string", "location", "string"), ("phase_name", "string", "phase_name", "string"), ("status", "string", "status", "string")], transformation_ctx = "applymapping1")
    
    resolvechoice2= ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
    project_report_df=resolvechoice2.toDF()
    
    column_list=(project_report_df.columns)
    project_report_df=project_report_df.withColumn('concated_cols',concat_ws("||",*column_list))
    project_report_df=project_report_df.withColumn('project_report_sha',generate_sha(project_report_df.concated_cols))
    project_report_df=project_report_df.drop("concated_cols")
    
    print("******************************************updated_records*********************************")
    
    updated_record_df=project_report_df.join(old_target_df,on=["project_report_sha"],how='leftanti')
    
    print("*****************UPDATED DATAFRAME COUNTS***************",updated_record_df.count())

    updated_record_df=updated_record_df.withColumn('insert_date',lit(datetime.now(timezone("Asia/Kolkata"))))
    updated_record_df.show()
    dyf = DynamicFrame.fromDF(updated_record_df, glueContext, "dyf")
    
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = "acc_dev_db", connection_options = {"dbtable": rds_dbtable, "database": rds_database}, transformation_ctx = "datasink4")
    job.commit()
project_tickets_report()
print("JOB COPMLETED")





