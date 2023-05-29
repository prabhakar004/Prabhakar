# Glue notebook for Raw to Curated
# Import Libraries

import sys
import http.client
import mimetypes
import os, ssl
import zipfile
import json
import datetime
import boto3
import logging
from datetime import  datetime as dt
import time
import pandas as pd
import re
import time

from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql import functions as F
from multiprocessing.pool import ThreadPool as Pool
from pyspark.sql.functions import broadcast
from pyspark.sql import SparkSession
from multiprocessing.pool import ThreadPool as Pool
from pyspark.sql.functions import to_date, to_timestamp, upper, lit

from cdl_common_lib_function import get_cdl_common_functions as cdl_lib

curat_rec_cnt = 0
craw_start_flag = 0
s3_path_curat = ''
crawler_include_path = []
is_new_table = False

# ------------- CREATE SES & GLUE Clients -------------

glue_client = cdl_lib.initiate_glue_client()

# --------------- CREATE SPARK SESSION ----------------------
spark = cdl_lib.initiate_spark_session()
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")
spark.conf.set("spark.sql.files.maxPartitionBytes","134217728")
spark.conf.set("spark.sql.shuffle.partitions","200")
spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
spark.conf.set("spark.sql.parquet.writeLegacyFormat","true")
spark.sql("set spark.sql.storeAssignmentPolicy=LEGACY")
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")
spark.sql("set spark.sql.autoBroadcastJoinThreshold= 20971520")
spark.sql("set spark.sql.adaptive.enabled=true")
spark.sql("set spark.sql.adaptive.coalescePartitions.enabled=true")
spark.sql("set spark.sql.adaptive.coalescePartitions.minPartitionSize=5MB")
spark.sql("set spark.sql.adaptive.skewJoin.enabled=true")


# ------------- CONFIGURATION FOR CUSTOM LOG CREATIONS ----------------
logger = cdl_lib.initiate_logger()


def send_notification_mail(source_file_name, file_header_list, source_files_columns, rejected_file_location,
                           recpnts_list, sender_email_address, type_of_err):
    if type_of_err == 'Schema Mismatch':
        subject = "RAW to CURATED file schema validation failure status notification for " + str(source_file_name)
        num_of_cols_configured = len(file_header_list)
        num_of_cols_source_file = len(source_files_columns)
    else:
        if type_of_err == 'Null records':
            subject = "Raw to Curated Null validation failure status notification for " + str(source_file_name)
        else:
            if type_of_err == 'Date mismatch records':
                subject = "Raw to Curated Date validation failure status notification for " + str(source_file_name)
            else:
                subject = "GENMAB_SOURCE file schema validation failure status notification for " + str(
                    source_file_name)
    # Create the message content for the structure validation failure
    email_body = """<html><head></head><body>"""
    email_body += "<p>Hi Team,</p>"
    if type_of_err == 'Null records':
        email_body += "<p>Please note that we are identified null records in the file '{}'.  Please find herewith the details.</p>".format(
            source_file_name)
    else:
        if type_of_err == 'Date mismatch records':
            email_body += "<p>Please note that we are identified Date Mismatch records in the file '{}'.  Please find herewith the details.</p>".format(
                source_file_name)
        else:
            email_body += "<p>Please note that we are unable to process the file '{}' due to differences in the structure.  Please find herewith the details.</p>".format(
                source_file_name)
    email_body += """<table border="1">"""
    if type_of_err == 'Null records' or type_of_err == 'Date mismatch records':
        email_body += "<tr><td bgcolor='yellow'>Total error records in the file: {}</td><td>{}</td></tr>".format(
            source_file_name, file_header_list)
    else:
        email_body += "<tr><td colspan = {} bgcolor='yellow'>File structure recieved for the table: {}</td></tr><tr>".format(
            num_of_cols_source_file, source_file_name)
        for column in file_header_list:
            email_body += "<td>{}</td>".format(column)
        email_body += "</tr></table><tr><td>&nbsp;</td></tr>"
        email_body += """<table border="1">"""
        email_body += "<tr><td colspan = {} bgcolor='yellow'>File structure recieved for the table: {}</td></tr><tr>".format(
            num_of_cols_source_file, source_file_name)
        for column in source_files_columns:
            email_body += "<td>{}</td>".format(column)
            email_body += "</tr>"
    email_body += "</table>"
    email_body += "<p>We have also placed the rejected source file in the path : {}<p>".format(rejected_file_location)
    email_body += "<p>Kindly contact GENMAB CDL ADMIN team if incase any clarifications required.</p>"
    email_body += "<p>Regards,</p>"
    email_body += "<p>GENMAB CDL Admin team </p>"
    email_body += "<p>***This is an auto-generated email, please do not reply to this email***</p>"
    email_body += "</body></html>"
    cdl_lib.send_email(email_body, subject, recpnts_list, sender_email_address)

# def run_curated_crawler(crawler_path):
#     try:
#         failed_status = ['FAILED', 'CANCELLED']
#         glue_client.update_crawler(Name=crawler_name,Targets={'S3Targets': crawler_path})
#         logger.info("Crawler Include Path Updated : " + str(crawler_path))
#         glue_client.start_crawler(Name=crawler_name)
#         while True:
#             crawler = glue_client.get_crawler(Name=crawler_name)
#             #logger.info("crawler started :"+ str(crawler_name))
#             crawler = crawler['Crawler']
#             crawler_state = crawler['State']
#             metrics = glue_client.get_crawler_metrics(CrawlerNameList=[crawler_name])[
#                 'CrawlerMetricsList'][0]
#             time_left = int(metrics['TimeLeftSeconds'])
#             if crawler_state =='READY':
#                 crawler_status = crawler['LastCrawl']['Status']
#                 if crawler_status in failed_status:
#                     raise Exception(f"Status: {crawler_status}")
#                 metrics = glue_client.get_crawler_metrics(CrawlerNameList=[crawler_name])[
#                   'CrawlerMetricsList'][0]
#                 metrics = glue_client.get_crawler_metrics(CrawlerNameList=[crawler_name])[
#                     'CrawlerMetricsList'
#                 ][0]
#                 logger.info("Status: "+str(crawler_status))
#                 logger.info("Last Runtime Duration (seconds): "+str(metrics['LastRuntimeSeconds']))
#                 logger.info("Median Runtime Duration (seconds): "+str(metrics['MedianRuntimeSeconds']))
#                 logger.info("Tables Created: "+str(metrics['TablesCreated']))
#                 logger.info("Tables Updated: "+str(metrics['TablesUpdated']))
#                 logger.info("Tables Deleted: "+str(metrics['TablesDeleted']))
#                 break
#             else:
#                 time.sleep(time_left)
#         logger.info(
#             "New table {table} has been Crawled for the Source_system: {source_system}".format(
#                 source_system=source_system))
#     except Exception as e:
#         logger.error("Running Crawler Failed : "+str(e))
    

# get athena tables for a database
# def get_tables_for_database(database):
#     starting_token = None
#     next_page = True
#     tables = []
#     while next_page:
#         paginator = glue_client.get_paginator(operation_name="get_tables")
#         response_iterator = paginator.paginate(
#             DatabaseName=database,
#             PaginationConfig={"PageSize": 100, "StartingToken": starting_token},
#         )
#         for elem in response_iterator:
#             tables += [
#                 {
#                     "name": table["Name"],
#                 }
#                 for table in elem["TableList"]
#             ]
#             try:
#                 starting_token = elem["NextToken"]
#             except:
#                 next_page = False
#     logger.info('Existing tables :'+str(tables))
#     return tables

# Load files parallel to s3 curated zone
def load_files_parallel(conf=[]):
    conf_id = conf[0]['configuration_id']
    file_id = conf[0]['file_id']
    file_name = conf[0]['file_name']
    source_system = conf[0]['source_system']
    curated_flag = conf[0]['curated_flag']
    curated_db = conf[0]['curated_database']
    curated_tbl_name = conf[0]['curated_tablename']
    delimiter = conf[0]['delimiter']
    pt_file_name = conf[0]["pt_file_name"]
    s3_reject_path = conf[0]['s3_reject_path'].format(bucket_name=s3_cdl_bucket)
    header = conf[0]["header"]
    escape = conf[0]["escape"]
    s3_raw_path = conf[0]['s3_raw_path'].format(bucket_name=s3_cdl_bucket)
    s3_curated_path = conf[0]['s3_curated_path'].format(bucket_name=s3_cdl_curated_bucket)
    s3_path = s3_raw_path + "{year}/{month}/{day}".format(year=new_parentlog[:4], month=new_parentlog[4:6],
                                                          day=new_parentlog[6:8])
    s3_reject_path = s3_reject_path + "dq_failures/{curated_tablename}/{year}/{month}/{day}".format(
        curated_tablename=curated_tbl_name, year=new_parentlog[:4], month=new_parentlog[4:6], day=new_parentlog[6:8])
    target = s3_curated_path.split('/')[3:len(s3_curated_path.split('/'))]
    target = "/".join(target)
    source_object_name = conf[0]['source_object_name']
    global curat_rec_cnt, s3_path_curat, crawler_include_path, is_new_table
    try:
        logger.info("Reading from s3 Raw file {0}/{1} ".format(s3_path, file_name))
        header_var = True if header is not None else False
        logger.info("delimiter for the file_name: "+str(file_name)+"delimiter"+str(delimiter)) # debug break point
        landing_table = spark.read.option("header", header_var).option("sep", delimiter). \
            option("inferSchema", "false").option("escape",escape).csv('{0}/{1}'.format(s3_path, file_name))
        # record_count = landing_table.count()
        # logger.info("Total records available in the file {0}: {1}".format(file_name, record_count))
        landing_tempViewName = pt_file_name+str(file_id)
        landing_table.createOrReplaceTempView(landing_tempViewName)
        for row_data in conf:
            curated_tablename = row_data['curated_tablename']
            logger.info("pt_file_name "+str(pt_file_name)+" curated_tablename "+str(curated_tablename)) # debug breakpoint
            cleansing_df = curat_data_df.filter(
                        (F.col('file_name') == pt_file_name) &
                        (F.col('target_tablename')==curated_tablename) &
                        (F.col('interface') == 'raw_to_curated')) 
            cleansing_df_count = cleansing_df.count()   ## Sabari: Use Persist / Cache and trigger an action to have these values from memory
            logger.info("Cleansing_df count for '{0}' for the curated table name '{1}'".format(cleansing_df_count,curated_tablename))
            if cleansing_df_count > 0:
                sql_query = str(cleansing_df.select('sql_query').take(1)[0][0]).format(file_name=file_name,raw_filename=landing_tempViewName,batch_id=new_parentlog,file_id=file_id)
                logger.info("sql_query :"+str(sql_query)) # debug breakpoint
                curat_df_final = spark.sql(sql_query)
                logger.info("spark_sql completed for : "+str(curated_tablename))# debug breakpoint
                curated_tempViewName = str(curated_tablename) + '_' + str(file_id)
                curat_df_final.createOrReplaceTempView(curated_tempViewName)
                logger.info("View created successfully for curated table name: "+str(curated_tablename)+" viewname "+str    (curated_tempViewName))
                curated_columns = curat_df_final.columns
                logger.info("Columns listed -- "+str(curated_columns))
                dq_views=[]
                dq_rule = dq_data_df.filter(F.col('tgt_table_name') == curated_tablename)
                for rule in dq_rule.collect():
                    # logger.info("Enter DQ rule")
                    rule_name = rule.rule_name
                    column_name = rule.column_name
                    table_name = rule.tgt_table_name
                    sql_condition = rule.sql_condition
                    priority_level = rule.priority_level
                    logger.info(sql_condition)
                    sql_condition = rule.sql_query
                    temp_view_name = rule_name + '_' + column_name.replace(',', '_') + '_' + str(
                        table_name) + '_' + str(
                        file_id)
                    clm_nm = column_name.replace(',', '_')
                    param_query = rules_data_df.select('sql_query').filter(
                    F.col('rule_name') == '{rule_name}'.format(rule_name=rule_name)).collect()[0][0]
                    # logger.info("param_query :"+str(param_query))
                    logger.info(sql_condition)
                    if sql_condition is not None:
                        sql_condition = sql_condition.format(view_name=curated_tempViewName, pt_batch_id=new_parentlog,
                                                             pt_file_id=file_id, publish_schema=curated_db)
                    temp_df = spark.sql(param_query.format(view_name=curated_tempViewName, pt_batch_id=new_parentlog,
                                                              column_name=column_name, sql_condition='',
                                                              pt_file_id=file_id))
                    temp_df_count = temp_df.count() ## Sabari : See if we can take head and bypass this ?
                    if temp_df_count > 0:
                        dq_path = rule.dq_failure_s3_path
                        logger.info("Executing DQ Rule - " + str(rule_name) + " for table - " + str(table_name))
                        subject_line = 'fail_'+ str(rule_name) +'_'+str(column_name) if priority_level.lower()=='high' else 'warn_'+ str(rule_name) +'_' + str(column_name)
                        # In my understanding one data source is having logic like failed records shouldn't go to publish layer - Need to add that logic
                        temp_df = temp_df.withColumn('cdl_processing_status', F.lit(subject_line))
                        temp_df.createOrReplaceTempView(temp_view_name)
                        logger.info("Created temp view - " + str(temp_view_name) +" with count - " +str(temp_df_count))
                        logger.info("Rule - " + str(rule_name) + " completed for - " + str(table_name))
                        dq_views.append(temp_view_name)
                
                curat_df_final = curat_df_final.withColumn('cdl_processing_status', F.lit(None))
                curat_df_final.createOrReplaceTempView(curated_tempViewName)
                dq_views.append(curated_tempViewName)
                logger.info(" DQ View names - " + str(dq_views))
                if len(dq_views) > 1:
                    dq_rules_union = spark.sql(" union all ".join(map(lambda x: "select * from " + x, dq_views)))
                    dq_result_output = dq_rules_union.groupBy(curated_columns).agg(F.concat_ws('|',F.sort_array(F.collect_list('cdl_processing_status'))).alias('cdl_processing_status'))
                    dq_result_output_fail=dq_result_output.filter(dq_result_output.cdl_processing_status.startswith('fail_')).withColumn('cdl_processing_status',F.concat(F.lit('fail_'),F.regexp_replace('cdl_processing_status','fail_',''))).withColumn('cdl_processing_status', F.regexp_replace('cdl_processing_status', '\|*(warn_)+(\w+)+', '|')).withColumn('cdl_processing_status', F.regexp_replace('cdl_processing_status', '^[\|]|[\|]$', ''))
                    dq_result_output_warn=dq_result_output.filter(dq_result_output.cdl_processing_status.startswith('warn_')).withColumn('cdl_processing_status',F.concat(F.lit('warn_'),F.regexp_replace('cdl_processing_status','warn_','')))
                    dq_result_output_null=dq_result_output.filter((F.col('cdl_processing_status')==None)|(F.col('cdl_processing_status')=='')).withColumn('cdl_processing_status',F.lit(None))
                    dq_result_output=dq_result_output_fail.union(dq_result_output_warn).union(dq_result_output_null)
                    curated_columns.append('cdl_processing_status')
                    dq_result = dq_result_output.select(curated_columns).distinct()
                    logger.info('dq_result columns-'+str(dq_result.columns))
                    dq_result.createOrReplaceTempView(curated_tempViewName)
                    logger.info("Exit DQ rule")
                elif len(dq_views) == 1 and curated_tempViewName in dq_views:
                    logger.info("New block for views without DQ check")
                    curated_columns.append('cdl_processing_status')
                    if source_system=='shs_nhlremit_mnthly':
                        dq_result = curat_df_final.repartition(100).select(curated_columns)
                        logger.info("Executed repartition statement")
                        # dq_result.persist(StorageLevel.DISK_ONLY)
                        logger.info(f"dq_result count - {str(dq_result.count())}")
                    else:
                        dq_result = curat_df_final.select(curated_columns)
                    # logger.info('dq_result columns-'+str(dq_result.columns))
                    dq_result.createOrReplaceTempView(curated_tempViewName)
                    logger.info("Exit DQ rule")
                try:
                    #insert_query = f'insert overwrite into {curated_db}.{curated_tbl_name} PARTITION(batch_id,file_id) select * from {tempViewName}'
                    #tgt_cols_list=spark.sql(f'select * from {curated_db}.{curated_tbl_name} limit 1') mk 
                    tgt_cols_list=spark.sql(f'select * from {curated_db}.{curated_tablename} limit 1')
                    tgt_cols_list = tgt_cols_list.columns
                    tgt_cols_list_final= [col.strip() for col in tgt_cols_list]
                    final_columns=",".join([i for i in tgt_cols_list_final])
                    #insert_query_new = f'insert overwrite table {curated_db}.{curated_tbl_name} PARTITION(batch_id,pt_file_id) select {final_columns} from {curated_tempViewName}' mk
                    insert_query_new = f'insert overwrite table {curated_db}.{curated_tablename} PARTITION(batch_id,pt_file_id) select {final_columns} from {curated_tempViewName}'
                    logger.info("insert_query :"+str(insert_query_new))
                    spark.sql(insert_query_new)
                except Exception as e:
                    logger.error("Insert query Exception : "+str(e))
                    if "Table not found" or "Table or view not found" in str(e):
                        logger.info("Loading directly to S3 as 'Table or view not found' in the Glue Data catalog, for the table: "+str(curated_tbl_name))
                        final_curat_df_final = spark.sql(f"select * from {curated_tempViewName}")
                        final_curat_df_final = final_curat_df_final.drop("batch_id","BATCH_ID","pt_file_id","PT_FILE_ID")
                        # logger.info("final_curat_df_final output :"+ str(final_curat_df_final.show(1)))
                        logger.info("s3_curated_path :"+str(s3_curated_path))
                        logger.info("curated_tablename :"+str(curated_tablename))
                        logger.info("batch_id :"+str(new_parentlog))
                        logger.info("file_id :"+str(file_id))
                        final_cols = dq_result.columns
                        if  len([x for x in final_cols if 'file_id' in x.lower() or 'pt_file_id' in x.lower()])>0:
                            dq_result = dq_result.drop("batch_id","BATCH_ID")
                            s3_full_path = "{s3_curated_path}{curated_tablename}/{batch_id}/{file_id}/".format(
                                    s3_curated_path=s3_curated_path, curated_tablename=curated_tablename,
                                    batch_id='batch_id=' + str(new_parentlog), file_id='pt_file_id=' + str(file_id))
                        else:
                            logger.info("file_id is missing in Table - "+str(curated_tablename))
                            raise Exception("file_id is missing in Table - "+str(curated_tablename))
                        final_curat_df_final.write.mode('append').parquet(s3_full_path)
                        # crawler_include_path.append(str(s3_curated_path + curated_tablename + '/').replace("s3a://",""))
                        # logger.info("Crawler Include Path : "+ str(crawler_include_path)) # debug breakpoint
                    else:
                        logger.error("Exception in Curated Table loading query :"+str(e))
                logger.info("File write successful into Curated Zone: "+str(curated_tablename))
                curat_rec_cnt = spark.sql(f"select * from {curated_db}.{curated_tablename} where batch_id='{str(new_parentlog)}' and pt_file_id='{str(file_id)}'").count()
                s3_path_curat = s3_curated_path + curated_tbl_name + '/' + 'batch_id=' + str(new_parentlog) + '/' + 'file_id=' + str(file_id) 
                logger.info("s3_path_curat " + str(s3_path_curat) + " curat_rec_cnt " + str(curat_rec_cnt))#debug breakpoint
            else:
                logger.info("There is no Query Configuration available for the Source_system:{source_system}, Target Tablename: {curated_tablename}".format(source_system=source_system,curated_tablename=curated_tablename))
            
        # create partition dynamically using the partition input list
        # crawler_db_tables = get_tables_for_database(crawler_db_name)
        # if len(crawler_db_tables) > 0:
        #     for i in range(len(crawler_db_tables)):
        #         list_file_folder.append(crawler_db_tables[i]["name"])
        #     if curated_tbl_name in list_file_folder:
        #         table_data = cdl_lib.get_current_schema(crawler_db_name, curated_tbl_name)
        #         input_list = cdl_lib.generate_partition_input_list(table_data, new_parentlog, file_id)
        #         try:
        #             create_partition_response = glue_client.batch_create_partition(DatabaseName=crawler_db_name,
        #                                                                           TableName=curated_tbl_name,
        #                                                                           PartitionInputList=input_list)
        #             logger.info("Glue partition created successfully")
        #         except Exception as e:
        #             # Handle exception as per your business requirements
        #             return {'file_id': file_id, 'conf_id': conf_id, 'curat_count': 0, 'status': 'FAILED', 's3_path': '',
        #                     'file_name': file_name, 'message': str(e)}
        #     else:
        #         is_new_table = True
        #         logger.info("Crawler Flag Assigned for the file name {file_name}".format(file_name=file_name))
        # else:
        #     logger.info(
        #         "No table found for the Source_system: {source_system} and DatabaseName: {crawler_db_name}, rerun raw to curated for the file: {table}".format(
        #             source_system=source_system, crawler_db_name=crawler_db_name, table=curated_tbl_name))
        #     return {'file_id': file_id, 'conf_id': conf_id, 'curat_count': 0, 'status': 'FAILED',
        #             's3_path': s3_path_curat, 'file_name': file_name,
        #             'message': 'Run the crawler and reprocess this job'}
        return {'file_id': file_id, 'conf_id': conf_id, 'curat_count': curat_rec_cnt, 'status': 'SUCCESS',
                's3_path': s3_path_curat, 'file_name': file_name, 'message': 'File processed successfully'}
    except Exception as e:
        logger.error("last Exception for the file_name - "+str(file_name)+ ", Exception : " + str(e))
        return {'file_id': file_id, 'conf_id': conf_id, 'curat_count': 0, 'status': 'FAILED', 's3_path': '',
                'file_name': file_name, 'message': str(e)}


if __name__ == "__main__":
     # ------------- @params: [TempDir, JOB_NAME] -------------

    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
    workflow_name = args['WORKFLOW_NAME']
    workflow_run_id = args['WORKFLOW_RUN_ID']
    workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name,
                                                              RunId=workflow_run_id)["RunProperties"]
    logger.info("--------Workflow details------> " + workflow_name + ',' + workflow_run_id)

    new_parentlog = workflow_params['new_parentlog']
    source_system = workflow_params['source_system']
    parent_batch_process_name = workflow_params['parent_batch_process_name']
    s3_vendor_bucket = workflow_params['s3_vendor_bucket']
    s3_cdl_bucket = workflow_params['s3_cdl_bucket']
    s3_cdl_curated_bucket = workflow_params['s3_cdl_curated_bucket']
    environment = workflow_params['environment']
    to_address_email = workflow_params['to_address_email']
    fm_address_email = workflow_params['fm_address_email']
    crawler_db_name = workflow_params['curated_db']
    crawler_name='cdl_raw_to_curated_crawler' #HARDCODED VALUE NEED TO REMOVE curated_crawler_name = workflow_params['curated_crawler']
    secret_id = workflow_params['rds_secret_id']
    myThreads = workflow_params['no_of_thread']

    # --------- RDS CREDENTIALS - TO BE CONFIGURED IN SECRET MANAGER --------------
    # Getting DB credentials from Secrets Manager

    secret = cdl_lib.get_secret_manager(secret_id)

    config_final_dt = datetime.now()
    unify_timestamp = datetime.now()
    s3 = boto3.resource('s3')
    target_bucket = s3.Bucket(s3_cdl_curated_bucket)

    logger.info("The source_system is: {}".format(source_system))
    logger.info("The parent_batch_process_name value is: {}".format(parent_batch_process_name))
    logger.info("The s3_path value is: {}".format(s3_cdl_bucket))
    logger.info("The curated s3_path value is: {}".format(s3_cdl_curated_bucket))
    logger.info("The new_parentlog is: {}".format(new_parentlog))
    logger.info("The s3_vendor_bucket is: {}".format(s3_vendor_bucket))
    logger.info("The environment is: {}".format(environment))
    

    config_data = """(select distinct trim(fl.file_name) as file_name,cnf.delimiter,fl.file_id,fl.parent_batch_id,cnf.curated_flag,cnf.curated_database,cnf.curated_tablename,cnf.source_object_name,cnf.pt_file_name,trim(cnf.domain) as domain,trim(cnf.sub_domain) as sub_domain,cnf.source_system,cnf.configuration_id, cnf.row_num,cnf.s3_raw_path,cnf.s3_curated_path,cnf.s3_reject_path,cnf.header,cnf.escape from file_process_log fl inner join cdl_ingestion_log cl on fl.parent_batch_id=cl.batch_id and fl.file_id=cl.file_id inner join (select row_number() over (partition by source_object_name,pt_file_name order by configuration_id) as row_num,* from (select distinct configuration_id,source_object_name,file_name as pt_file_name,domain,sub_domain,s3_curated_layer as curated_flag,curated_database,curated_tablename as curated_tablename,source_system,s3_raw_path,s3_curated_path,s3_reject_path,delimiter,header,escape from configuration_master_new  where source_system='{}' and active_flag = 'A') as aa)cnf on fl.source_system=cnf.source_system and fl.pattern_name=cnf.source_object_name where fl.source_system = '{}' and fl.parent_batch_id = '{}'  and (cl.curated_status<>'SUCCESS' or cl.curated_status is null) order by fl.file_id,cnf.row_num) query_wrap""".format(
        source_system, source_system, new_parentlog)
    config_data_df = cdl_lib.read_from_db(secret=secret,tbl_query=config_data)
    curat_query = """(select * from query_configuration_new where source_system='{}') query_wrap""".format(source_system)
    curat_data_df = cdl_lib.read_from_db(secret=secret,tbl_query=curat_query)
    curat_data_df.persist()
    logger.info("Total records in query_configuration for curated - "+str(curat_data_df.count()))
    
    dq_query = """(select * from public.dq_rules_new where source_system='{}' and upper(active_flag) = 'A') query_wrap""".format(source_system)
    dq_data_df = cdl_lib.read_from_db(secret=secret,tbl_query=dq_query)
    rules_query = """(SELECT * FROM dq_rules_master WHERE is_active='Y') query_wrap"""
    rules_data_df = cdl_lib.read_from_db(secret=secret,tbl_query=rules_query)
    start_time = dt.now().strftime('%Y-%m-%d %H-%M-%S')

    pattern_file_list = []
    file_id = 0
    file_list = []
    list_file_folder = []
    for data in config_data_df.collect():
        if file_id == 0 or file_id != data.file_id:
            if len(file_list) > 0:
                pattern_file_list.append(file_list)
            file_list = []
            file_id = data.file_id
            file_list.append(data.asDict())
        else:
            file_list.append(data.asDict())
    if len(file_list) > 0:
        pattern_file_list.append(file_list)

    # Create a thread and process files in parallel
    logger.info("files to be processed as Dict list : "+str(pattern_file_list))# debug breakpoint
    logger.info("Number of files to be processed - " + str(len(pattern_file_list)))
    status_result = []
    if len(pattern_file_list) > 0:
        myThreads = 1 if source_system=='shs_nhlremit_mnthly' else myThreads
        FilePattPool = Pool(int(myThreads))
        status_result = FilePattPool.map(load_files_parallel, pattern_file_list)
        FilePattPool.close()
        FilePattPool.join()
        logger.info("File has been processed and loaded in to S3 successfully")
    # logger.info("Crawler Include Path Validate : "+str(crawler_include_path)+" IS NEW TABLE Validate : "+str(is_new_table))# debug breakpoint
    # if len(crawler_include_path) > 0 or is_new_table == True:
    #     distinct_include_path = list(set(crawler_include_path))
    #     include_path = []
    #     for path in distinct_include_path:
    #         include_path.append({'Path':str(path)})
    #     logger.info("Include path for the Crawler : "+str(include_path))# debug breakpoint
    #     # run_curated_crawler(include_path)
    #     logger.info("Crawler has been processed and created tables in Catalog successfully")

    if len(status_result) > 0:
        df_list = []
        for row in status_result:
            record = []  # use .items() from row to convert into list - To avoid the below iteration
            record.append(row['conf_id'])
            record.append(row['curat_count'])
            record.append(row['file_id'])
            record.append(row['status'])
            record.append(row['file_name'])
            record.append(row['s3_path'])
            record.append(row['message'])
            df_list.append(record)

            # update the log into CDL_Ingestion if success or failed

        if len(df_list) > 0:
            schema = StructType(
                [StructField("conf_id", IntegerType(), True), StructField("curat_count", IntegerType(), True),
                 StructField("file_id", IntegerType(), True), StructField("status", StringType(), True),
                 StructField("file_name", StringType(), True), StructField("s3_path", StringType(), True),
                 StructField("message", StringType(), True)])
            spdf = spark.createDataFrame(df_list, schema)
            newdf = spdf.groupby('file_id', 'status').agg(F.min('conf_id').alias('conf_id'),
                                                          F.when(F.size(F.collect_set('status')) > 1,
                                                                 'FAILED').otherwise(F.col('status')).alias(
                                                              'status_new'))
            newdf = newdf.alias('a').join(spdf.alias('b'), ['conf_id', 'file_id'], 'inner').select('curat_count',
                                                                                                  'a.file_id',
                                                                                                  'conf_id',
                                                                                                  'a.status_new',
                                                                                                  's3_path',
                                                                                                  'file_name',
                                                                                                  'message')
            newdf.createOrReplaceTempView('ResultSet_df')
            df = spark.sql("select * from ResultSet_df")

            conn_rds_pg, user = cdl_lib.get_postgres_conn_for_psycopg2(secret)
            try:
                with conn_rds_pg.cursor() as cur:
                    for row in df.rdd.collect():
                        logger.info(
                            "Records to update in cdl_ingestion log: status {0}, src_path:{1}, curated count: {2},parentLog: {3}, file_id: {4}, source_system:{5}, message:{6}".format(
                                row.status_new, row.s3_path, row.curat_count, new_parentlog, row.file_id, source_system,
                                row.message))
                        update_query = "update cdl_ingestion_log set curated_status=%s, curated_location=%s ,curated_completion_timestamp=current_date ,curated_record_count=%s,curated_message=%s where batch_id=%s and file_id=%s and data_source=%s"
                        cur.execute(update_query, (
                        row.status_new, row.s3_path, row.curat_count, str(row.message[:250]), new_parentlog, row.file_id,
                        source_system))
                    conn_rds_pg.commit()
                    cur.close()
            except Exception as e:
                logger.error("Exception - " + str(e))