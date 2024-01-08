import sys
import http.client
import mimetypes
import os, ssl
import zipfile
import json
import datetime
import boto3
import logging
import time

from awsglue.transforms.field_transforms import SelectFields
from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import functions as F
from multiprocessing.pool import ThreadPool as Pool
from pyspark.sql.functions import broadcast
from pyspark.sql import SparkSession

from cdl_common_lib_function import get_cdl_common_functions as cdl_lib

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# ------------- CONFIGURATION FOR CUSTOM LOG CREATIONS ----------------
logger = cdl_lib.initiate_logger()

# ------------- HARDCODED for now -------------
myThreads = 8
db_creds = {}
sf_db_creds = {}
secret_id_rds = "gmb_cdl_postgres"
secret_id_sf = "gmb_cdl_snowflake"

# ------------- CREATE GLUE Clients -------------

sm_client = boto3.client("secretsmanager", region_name="us-east-2")

# --------- RDS CREDENTIALS - TO BE CONFIGURED IN SECRET MANAGER --------------
# Getting DB credentials from Secrets Manager
secret_rds = cdl_lib.get_secret_manager(secretId=secret_id_rds)

secret_sf = cdl_lib.get_secret_manager(secretId=secret_id_sf)

# Use this function if we have spark context object.  This will return a Spark dataframe
def read_from_db_spark(tbl_query, spark):
    url, db_creds = cdl_lib.get_postgres_conn_details()
    database = db_creds["rds_pg_dbname"]
    connection_details = {
        "user": db_creds["rds_pg_uname"],
        "password": db_creds["rds_pg_pwd"],
        "driver": "org.postgresql.Driver",
    }
    logger.info(f"Reading data from postgres table - Query: {tbl_query}")
    df = spark.read.jdbc(url=url, table=tbl_query, properties=connection_details)
    return df


# Adding new function to get Snowflake connection details from SecretsManager.
def get_snowflake_conn_details():

    sf_db_creds['sfuser'] = secret_sf.get('username')
    sf_db_creds['sfpassword'] = secret_sf.get('password')
    sf_db_creds['sfaccount'] = secret_sf.get('account')
    sf_db_creds['sfwarehouse'] = secret_sf.get('warehouse')
    sf_db_creds['sfdatabase'] = secret_sf.get('database')
    sf_db_creds['sfschema'] = secret_sf.get('schema')
    sf_db_creds['sfrole'] = secret_sf.get('role')
    sf_db_creds['sfURL'] = secret_sf.get('url')
    return sf_db_creds

# Load files parallel to Snowflake
def load_files_parallel(conf=[]):
    record_count = 0
    target_schema = ''
    target_table = ''
    s3_path_pub = ''
    conf_id = conf[0]['configuration_id']
    global already_processed_dict
    try:
        s3_path = conf[0]['published_location']
        s3_path = s3_path.strip()
        file_id = conf[0]['file_id']
        file_name = conf[0]['file_name']
        file_name = file_name.split('.')[0]
        source_system = conf[0]['source_system']
        src_table_list = [row['src_curated_tablename'] for row in conf]
        src_table_list = list(set(src_table_list))
        source_object_name = conf[0]['source_object_name']
        publish_table = conf[0]['publish_tablename']
        
                
        for src_curated_filename in src_table_list:
            
            cdl_pub_data_df = cdl_data_df.filter(F.col('source_table') == publish_table)
            if cdl_pub_data_df.count() > 0:
                data = cdl_pub_data_df.collect()[0]
                target_schema = data["target_schema"]
                target_table = data["target_table"]
                truncate_load_flag =data["truncate_load_flag"]
                del_flg = data["del_flg"]
                source_schema = data["source_schema"] #dbname
                source_table = data["source_table"] #publish_table
                columns_list = data["source_columns"]
                data_src_nm = data["partition_column"]
                if already_processed_dict.get(publish_table):
                    print("Table already processed :"+str(publish_table))
                    return {'file_id': file_id, 'conf_id': conf_id, 'publish_count': already_processed_dict[publish_table], 'status': 'SUCCESS',
                        's3_path': s3_path_pub, 'file_name': file_name, 'message': 'File processed successfully', 'target_table': target_table}
                logger.info("Target_schema & Target_table: {0}.{1}".format(target_schema, target_table))

                
                published_file_df = spark.sql(f"select {columns_list} from {source_schema}.{source_table} where data_src_nm='{data_src_nm}'")
                record_count = published_file_df.count()

                # Truncate snowflake target table for full load
                sf_db_creds = get_snowflake_conn_details()

                start = time.time()
                logger.info("target_table :"+str(target_table))
                logger.info("truncate_load_flag :"+str(truncate_load_flag))
                logger.info("del_flg :"+str(del_flg))
                logger.info("truncate_load_flag:-"+str(truncate_load_flag))
                snowflake_db ="PHCDL"
                snowflake_db= snowflake_db if environment =="prod" else snowflake_db+"_"+environment.upper()
                if truncate_load_flag.upper() == 'Y':
                    logger.info("Truncate and load flag is Y")
                    query = f"delete from {snowflake_db}.{target_schema}.{target_table} where data_src_nm='{data_src_nm}'"
                    published_file_df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sf_db_creds).option(
                        "dbtable", target_table).option("usestagingtable", "off").option("preactions",query).option('purge','on').option('truncate_table','off').mode("append").save()
                    logger.info("Data loaded successfully in snowflake for publish table :"+str(target_table))
               
                else:
                    published_file_df = published_file_df.filter((F.col('batch_id') == new_parentlog))
                    if del_flg.upper() == 'Y':
                        query = f"delete from {snowflake_db}.{target_schema}.{target_table} where (del_flg='1' or batch_id='{new_parentlog}') and data_src_nm='{data_src_nm}'"
                        print("query",query)
                        logger.info("Truncate and load flag is N and del flag is Y")
                        published_file_df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sf_db_creds).option("dbtable", target_table).option("preactions",query).option('purge','on').option('truncate_table','off').option("usestagingtable", "off").mode("append").save()
                        logger.info("Data loaded successfully in snowflake for publish table :"+str(target_table))
                    else:
                        query = f"delete from {snowflake_db}.{target_schema}.{target_table} where batch_id='{new_parentlog}' and data_src_nm='{data_src_nm}'"
                        logger.info("Truncate and load flag is N and del flag is N")
                        published_file_df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sf_db_creds).option(
                        "dbtable", target_table).option("preactions",query).option('purge','on').option("usestagingtable", "off").option('truncate_table','off').mode("append").save()
                        logger.info("Data loaded successfully in snowflake for publish table :"+str(target_table))
                    
                end = time.time()
                db_time = end - start
                logger.info("Time taken to Insert into table:{db_time}".format(db_time=db_time))
                status = 'SUCCESS'
                message = 'File processed successfully'
            else:
                logger.info(
                    "No target table is defined in public.cdl_ds_snowflake_replicate for table: {}".format(publish_table))
                status = 'FAILED'
                message = ''
            already_processed_dict[publish_table] =record_count
        return {'file_id': file_id, 'conf_id': conf_id, 'publish_count': record_count, 'status': status,
                's3_path': s3_path_pub, 'file_name': file_name, 'message': message, 'target_table': target_table}
    except Exception as e:
        logger.error("Exception - " + str(e))
        return {'file_id': file_id, 'conf_id': conf_id, 'publish_count': 0, 'status': 'FAILED', 's3_path': '',
                'file_name': file_name, 'message': str(e), 'target_table': target_table}
        raise


if __name__ == "__main__":

    # ------ Glue based Spark Session ------
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    glue_client = boto3.client("glue")

    # ------ Enable below 13 lines during automated process--------

    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
    job.init(args["JOB_NAME"], args)
    workflow_name = args['WORKFLOW_NAME']
    workflow_run_id = args['WORKFLOW_RUN_ID']
    workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name,
                                                              RunId=workflow_run_id)["RunProperties"]
    logger.info("--------Workflow details------> " + workflow_name + ',' + workflow_run_id)


    new_parentlog = workflow_params['new_parentlog']
    source_system = workflow_params['source_system']
    parent_batch_process_name = workflow_params['parent_batch_process_name']
    s3_vendor_bucket = workflow_params['s3_vendor_bucket']
    s3_cdl_publish_bucket = workflow_params['s3_cdl_publish_bucket']
    environment = workflow_params['environment']
    secret_id = workflow_params['rds_secret_id']
    secret = cdl_lib.get_secret_manager(secret_id)
    already_processed_dict ={}
    
    logger.info("The source_system is: {}".format(source_system))
    logger.info("The parent_batch_process_name value is: {}".format(parent_batch_process_name))
    logger.info("The publish s3_path value is: {}".format(s3_cdl_publish_bucket))
    logger.info("The new_parentlog is: {}".format(new_parentlog))
    logger.info("The s3_vendor_bucket is: {}".format(s3_vendor_bucket))
    logger.info("The environment is: {}".format(environment))

    config_data = """(select trim(fl.file_name) as file_name,fl.file_id,fl.parent_batch_id,trim(cnf.domain) as domain,trim(cnf.sub_domain) as sub_domain,cnf.source_object_name,trim(cnf.curated_tablename) as src_curated_tablename,cnf.primary_key,cnf.publish_tablename as publish_tablename,cnf.source_system,cnf.pt_file_name,cnf.configuration_id,cnf.full_or_incremental_load,cnf.row_num,s3_publish_path,s3_curated_path,cl.published_location as published_location from file_process_log fl inner join cdl_ingestion_log cl on fl.parent_batch_id=cl.batch_id and fl.file_id=cl.file_id inner join (select row_number() over (partition by source_object_name,pt_file_name order by configuration_id) as row_num,* from (select distinct source_object_name,configuration_id,file_name as pt_file_name,domain,sub_domain,curated_tablename as curated_tablename,publish_tablename,source_system,primary_key,full_or_incremental_load, s3_publish_path,s3_curated_path from configuration_master_new timestamp where source_system= '{}'  and active_flag = 'A' )as aa)cnf on fl.source_system=cnf.source_system and fl.pattern_name=cnf.source_object_name where fl.source_system = '{}'  and fl.parent_batch_id = '{}' and  (cl.published_status='SUCCESS') and (trim(cl.curated_status)='SUCCESS') and ((trim(cl.snowflake_status)<>'SUCCESS') or (cl.snowflake_status is null))) query_wrap""".format(
        source_system, source_system, new_parentlog)
    
    config_data_df = cdl_lib.read_from_db(secret=secret,tbl_query=config_data)

    pattern_file_list = []
    file_id = 0
    file_list = []
    for data in config_data_df.collect(): #TEMP FIX
        file_list = [data.asDict()]
        pattern_file_list.append(file_list)
        # if file_id == 0:
        #     if len(file_list) > 0:
        #         pattern_file_list.append(file_list)
        #     file_list = []
        #     file_id = data.file_id
        #     file_list.append(data.asDict())
        # else:
        #     file_list.append(data.asDict())
        #break ## Sabari : Added this temporarlily to load only one file
    # if len(file_list) > 0:
    #     pattern_file_list.append(file_list)
    
    sf_replicate_df = f"""(SELECT source_schema, source_table, target_schema, target_table, snowflake_spectrum, s3_location, data_load_flag, source_system, sub_domain, partition_column, truncate_load_flag, conditions, frequency, overwrite_flag, deepcopy_flag, source_columns,del_flg FROM public.cdl_ds_snowflake_replicate WHERE source_system='{source_system}' and data_load_flag='A') query_wrap"""
    logger.info("Snowflake Replicate Query: {}".format(sf_replicate_df))
    cdl_data_df = cdl_lib.read_from_db(secret=secret,tbl_query=sf_replicate_df)

    # Create a thread and process files in parallel
    logger.info("Number of files to be processed - " + str(len(pattern_file_list)))
    status_result = []
    if len(pattern_file_list) > 0:
        FilePattPool = Pool(int("1")) #TEMP FIX
        status_result = FilePattPool.map(load_files_parallel, pattern_file_list)
        FilePattPool.close()
        FilePattPool.join()
    if len(status_result) > 0:
        df_list = []
        for row in status_result:
            record = []  # use .items() from row to convert into list - To avoid the below iteration
            record.append(row['conf_id'])
            record.append(row['publish_count'])
            record.append(row['file_id'])
            record.append(row['status'])
            record.append(row['file_name'])
            record.append(row['s3_path'])
            record.append(row['message'])
            record.append(row['target_table'])
            df_list.append(record)
        if len(df_list) > 0:
            schema = StructType(
                [StructField("conf_id", IntegerType(), True), StructField("publish_count", IntegerType(), True),
                 StructField("file_id", IntegerType(), True), StructField("status", StringType(), True),
                 StructField("file_name", StringType(), True), StructField("s3_path", StringType(), True),
                 StructField("message", StringType(), True), StructField("target_table", StringType(), True)])
            spdf = spark.createDataFrame(df_list, schema)
            newdf = spdf.groupby('file_id', 'status').agg(F.min('conf_id').alias('conf_id'),
                                                          F.when(F.size(F.collect_set('status')) > 1,
                                                                 'FAILED').otherwise(F.col('status')).alias(
                                                              'status_new'))
            newdf = newdf.alias('a').join(spdf.alias('b'), ['conf_id', 'file_id'], 'inner').select('publish_count',
                                                                                                   'a.file_id',
                                                                                                   'conf_id',
                                                                                                   'a.status_new',
                                                                                                   's3_path',
                                                                                                   'file_name',
                                                                                                   'message',
                                                                                                   'target_table')
            newdf.createOrReplaceTempView('ResultSet_df')
            df = spark.sql("select * from ResultSet_df")
            conn_rds_pg, user = cdl_lib.get_postgres_conn_for_psycopg2(secret_rds)
            try:
                with conn_rds_pg.cursor() as cur:
                    for row in df.rdd.collect():
                        process_message = row.message
                        if row.status_new == 'FAILED':
                            process_message = 'File processing failed'
                        update_query = "update cdl_ingestion_log set snowflake_status=%s, snowflake_location=%s ,snowflake_completion_timestamp=current_date ,snowflake_record_count=%s,snowflake_message=%s where batch_id=%s and file_id=%s and data_source=%s"
                        cur.execute(update_query, (
                        row.status_new, row.target_table, row.publish_count, process_message, new_parentlog,
                        row.file_id, source_system))
                        logger.info("Updated details in control table")
                    conn_rds_pg.commit()
                    cur.close()
            except Exception as e:
                logger.error("Exception - " + str(e))
    logger.info("Job completed with below status: ")
    job.commit()
