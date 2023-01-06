import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws,lit,sha2
from pytz import timezone

spark = SparkSession.builder.config("spark.sql.legacy.parquet.datetimeRebaseModeInRead","LEGACY").getOrCreate()

glueContext = GlueContext(spark.sparkContext)

job = Job(glueContext)
logger = glueContext.get_logger()

#for  parameters for athena catalog
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
args['cw_project_ticket_conf_map']='raw_grc_review_reports_db_cw_project_ticket_conf_map'
args['cw_project_tickets']='raw_grc_review_reports_db_cw_project_tickets'
args['db_cw_configuration']='raw_grc_review_reports_db_cw_configuration'
args['raw_database']='raw_source_grc_review_reports_db'

#DATABASE CREDINTIALS
RDS_ENDPOINT="jdbc:mysql://grc-sla-reports-db-cluster.cluster-ccbtiwm0ziab.us-east-1.rds.amazonaws.com:3306/grc_review_reports_db"
DB_TABLE="acc_dev_db.Project_Config_Report"
USER="***************"
PASSWORD="*************"

#read data from target RDS
def read_data_from_target():
    target_df = spark.read \
    .format("jdbc") \
    .option("url",RDS_ENDPOINT) \
    .option("dbtable",DB_TABLE) \
    .option("user",USER) \
    .option("password",PASSWORD) \
    .load()
    return target_df
    
#generating SHA    
def generate_sha(str):
    return sha2(str,256)

#load data into target   
def project_config():
    old_record_df=read_data_from_target()
    print("****DUPLICATE TABLE DATA****",old_record_df.show())
    
    #for rds 
    db_table='Project_Config_Report'
    db_name='acc_dev_db'

    #catlog Tables
    datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args['raw_database'], table_name = args['cw_project_ticket_conf_map'], transformation_ctx = "datasource0")
    datasource1 = glueContext.create_dynamic_frame.from_catalog(database = args['raw_database'], table_name = args['cw_project_tickets'], transformation_ctx = "datasource1")
    datasource2 = glueContext.create_dynamic_frame.from_catalog(database = args['raw_database'], table_name = args['db_cw_configuration'], transformation_ctx = "datasource2")
    
    #converting into Dataframe 
    df=datasource0.toDF()
    df.createOrReplaceTempView("project_ticket_conf_map")
    print(f"project_ticket_conf_map {df.head(5)} ")
    
    df1=datasource1.toDF()
    df1.createOrReplaceTempView("project_tickets")
    print(f"project_tickets {df1.head(5)} ")

    df2=datasource2.toDF()
    df2.createOrReplaceTempView("configuration")
    print(f"CONFIGURATION {df2.head(5)} ")
    
# project_config_df QUERY
    project_config_df=spark.sql(''' SELECT cm.ticket_id, cm.config_id, c.dash_config_name, c.config_type, c.status, c.product_group, c.test_date, c.target_cert_date,
    c.additional_data, pt.project_id, pt.company_name, pt.company_identifier, pt.site_name 
    FROM project_ticket_conf_map cm
    left join project_tickets pt ON pt.ticket_id=cm.ticket_id 
    left join configuration c ON c.config_id=cm.config_id ''')
    
#creating SHA
    column_list=(project_config_df.columns)
    project_config_df=project_config_df.withColumn('concated_cols',concat_ws("||",*column_list))
    project_config_df=project_config_df.withColumn('project_config_sha',generate_sha(project_config_df.concated_cols))
    project_config_df=project_config_df.drop("concated_cols")
    
    print("********************Source table Dataframe with SHA*********************************")
    project_config_df.show()
    
    print("******************************************updated_records*********************************")
    
    updated_record_df=project_config_df.join(old_record_df,on=["project_config_sha"],how='leftanti')
    
    print("*****************UPDATED DATAFRAME COUNTS***************",updated_record_df.count())
    
    updated_record_df=updated_record_df.withColumn('insert_date',lit(datetime.now(timezone("Asia/Kolkata"))))
    updated_record_df.show()
    
    dyf = DynamicFrame.fromDF(updated_record_df, glueContext, "dyf")
    datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = "acc_dev_db", connection_options = {"dbtable": db_table, "database":db_name}, transformation_ctx = "datasink1")
    job.commit()
        
project_config()
print("JOB FINISHED")
    
    







