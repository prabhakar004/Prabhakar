# AWS Glue notebook source
# DBTITLE 1,Processing parameters
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
from multiprocessing.pool import ThreadPool as Pool
from pyspark.sql.window import Window
import builtins as py_builtin
from itertools import groupby

from cdl_common_lib_function import get_cdl_common_functions as cdl_lib

craw_start_flag = 0

# ------------- CREATE SES & GLUE Clients -------------

glue_client = cdl_lib.initiate_glue_client()
# --------------- CREATE SPARK SESSION ----------------------
spark = cdl_lib.initiate_spark_session()
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")
spark.sql("set spark.sql.autoBroadcastJoinThreshold= 20971520")
spark.sql("set spark.sql.adaptive.enabled=true")
spark.sql("set spark.sql.adaptive.coalescePartitions.enabled=true")
spark.sql("set spark.sql.adaptive.coalescePartitions.minPartitionSize=5MB")
spark.sql("set spark.sql.adaptive.skewJoin.enabled=true")
spark.sql("set spark.sql.storeAssignmentPolicy=LEGACY")
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")
spark.conf.set("spark.sql.files.maxPartitionBytes","134217728")
spark.conf.set("spark.sql.shuffle.partitions","40")
spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
spark.conf.set("spark.sql.parquet.writeLegacyFormat","true")



# ------------- CONFIGURATION FOR CUSTOM LOG CREATIONS ----------------
logger = cdl_lib.initiate_logger()

# get athena tables for a database
def get_tables_for_database(database):
    starting_token = None
    next_page = True
    tables = []
    while next_page:
        paginator = glue_client.get_paginator(operation_name="get_tables")
        response_iterator = paginator.paginate(
            DatabaseName=database,
            PaginationConfig={"PageSize": 100, "StartingToken": starting_token},
        )
        for elem in response_iterator:
            tables += [
                {
                    "name": table["Name"],
                }
                for table in elem["TableList"]
            ]
            try:
                starting_token = elem["NextToken"]
            except:
                next_page = False
    # logger.info('Existing tables :'+str(tables))
    return tables


def run_curated_crawler(crawler_name):
    try:
        failed_status = ['FAILED', 'CANCELLED']
        run_state=False
        while True:
            crawler = glue_client.get_crawler(Name=crawler_name)
            crawler = crawler['Crawler']
            crawler_state = crawler['State']
            if not run_state and crawler_state=='READY':
                glue_client.start_crawler(Name=crawler_name) 
                run_state=True
            metrics = glue_client.get_crawler_metrics(CrawlerNameList=[crawler_name])[
                'CrawlerMetricsList'][0]
            time_left = int(metrics['TimeLeftSeconds']) if int(metrics['TimeLeftSeconds']) > 0 else 100
            logger.info("crawler {} is in {} state.. Going to sleep for {} seconds".format(crawler_name,crawler_state,time_left))            
            if crawler_state =='READY':
                crawler_status = crawler['LastCrawl']['Status']
                if crawler_status in failed_status:
                    raise Exception(f"Status: {crawler_status}")
                metrics = glue_client.get_crawler_metrics(CrawlerNameList=[crawler_name])[
                   'CrawlerMetricsList'][0]
                logger.info("Status: "+str(crawler_status))
                logger.info("Last Runtime Duration (seconds): "+str(metrics['LastRuntimeSeconds']))
                logger.info("Median Runtime Duration (seconds): "+str(metrics['MedianRuntimeSeconds']))
                logger.info("Tables Created: "+str(metrics['TablesCreated']))
                logger.info("Tables Updated: "+str(metrics['TablesUpdated']))
                logger.info("Tables Deleted: "+str(metrics['TablesDeleted']))
                break
            else:
                time.sleep(time_left)
                # Need to add return true or False and have a check before going next stage
    except Exception as e:
        logger.error("Running Crawler Failed : "+str(e))        

def write_parquet(target_object_name,source_temp_view,s3_publish_path,pattern_name):
    logger.info("Loading directly to S3 as 'Table or view not found' in the Glue Data catalog, for the table: "+str(target_object_name))
    try:
        final_pub_df_final = spark.sql(f"select * from {source_temp_view}")
        final_pub_df_final=final_pub_df_final.drop("data_src_nm","DATA_SRC_NM")
        # logger.info("final_pub_df_final output :"+ str(final_pub_df_final.show(1)))
        logger.info("s3_publish_path :"+str(s3_publish_path))
        logger.info("publish_table_name :"+str(target_object_name))
        final_pub_df_final.write.mode('append').parquet(
                            "{s3_publish_path}{publish_table_name}/{data_src_nm}".format(
                                s3_publish_path=s3_publish_path, publish_table_name=target_object_name,data_src_nm='data_src_nm='+pattern_name))
        return True
    except Exception as e: 
        logger.error("Failed on Loading directly to S3 : "+ str(e))

# Load files parallel to s3 curated zone
def load_files_parallel(group_conf=[]):
    logger.info("load_files_parallel : "+str(group_conf))
    returnDict = {}
    for conf in group_conf:
        logger.info("Iterate Load Files in parallel : "+str(conf))
        file_id = conf[0]['file_id']
        file_name = conf[0]['file_name']
        source_system = conf[0]['source_system']
        s3_curated_path = conf[0]['s3_curated_path'].format(bucket_name=s3_cdl_curated_bucket)
        s3_publish_path = conf[0]['s3_publish_path'].format(bucket_name=s3_cdl_publish_bucket)
        curated_flag = conf[0]['s3_curated_layer']
        curated_db = conf[0]['curated_database']
        curated_tbl_name = conf[0]['curated_tablename']
        publish_flag = conf[0]['s3_publish_layer']
        publish_db = conf[0]['publish_database']
        publish_tbl_name = conf[0]['publish_tablename']
        s3_path = s3_curated_path + "{curated_tablename}".format(curated_tablename=curated_tbl_name)
        source_object_name = conf[0]['source_object_name']
        conf_id = conf[0]['configuration_id']
        inc_full = conf[0]['full_or_incremental_load']
        status = 'FAILED'
        message = 'Configuration master has Null'
        global pub_rec_cnt, s3_path_pub, publish_include_path, is_new_table, published_record_count
        try:
            # for src_details in conf:
            logger.info("reading from s3 Curated table: {0} for file: {1} ".format(curated_tbl_name, file_name))
            curated_table=spark.sql(f"""select * from {curated_db}.{curated_tbl_name} 
            where batch_id='{new_parentlog}'  and pt_file_id='{file_id}'""")
            record_count = curated_table.count()
            logger.info("total records available in the file {0}: {1}".format(file_name, record_count))
            # curated_table.createOrReplaceTempView(curated_tbl_name + str(file_id))
            
            logger.info("publish_tbl_name :"+str(publish_tbl_name)+"curated_tbl_name :"+str(curated_tbl_name)) #debug checkpoint
            transform_df = query_conf_df.filter((
                            F.col('source_tablename') == curated_tbl_name) & (F.col('target_tablename') == publish_tbl_name) & ( F.col('interface') == 'curated_to_publish'))
            logger.info("Transform_df count for '{0}' for the table '{1}'".format(transform_df.count(),curated_tbl_name))
            if transform_df.count() > 0:
                sql_query = str(transform_df.select('sql_query').take(1)[0][0])
                sql_query = sql_query.format(curated_db = curated_db, curated_tbl_name=curated_tbl_name,file_name=file_name,batch_id=new_parentlog,file_id=file_id)
                logger.info("sql_query :"+str(sql_query)) # debug breakpoint
                publish_df_final = spark.sql(sql_query)
                source_temp_view = str(curated_tbl_name + str(file_id))
                publish_df_final.createOrReplaceTempView(source_temp_view)
                logger.info("View created successfully for curated table name: "+str(curated_tbl_name)+" viewname "+str(curated_tbl_name + str(file_id)))
                crawler_db_tables = get_tables_for_database(publish_db)
                if len(crawler_db_tables) > 0:
                    for i in range(len(crawler_db_tables)):
                        list_file_folder.append(crawler_db_tables[i]["name"])
                stitching_df = stitching_data_df.filter((F.col('source_table_name') == curated_tbl_name) & (F.col('target_table_name') == publish_tbl_name))
                count_var = stitching_df.count()
                logger.info("Stitching DF count is '{0}' for the table '{1}'".format(stitching_df.count(),curated_tbl_name))
                if count_var > 0:
                    stitching_list_full = []
                    for data in stitching_df.filter((F.col('stitching_type') == 'full')).collect():
                        data = data.asDict()
                        data.update({"s3_publish_path":s3_publish_path,"batch_id":new_parentlog,"file_id":file_id,"catalog_tables":list_file_folder,"source_system":source_system})
                        stitching_list_full.append(data)
                    stitching_list_append = []
                    for data in stitching_df.filter(F.col('stitching_type') == 'append').collect():
                        data = data.asDict()
                        logger.info("data as dict : "+str(data))
                        data.update({"s3_publish_path":s3_publish_path,"batch_id":new_parentlog,"file_id":file_id,"catalog_tables":list_file_folder,"source_system":source_system})
                        stitching_list_append.append(data)
                    stitching_list_upsert = []
                    for data in stitching_df.filter(F.col('stitching_type') == 'upsert').collect():
                        data = data.asDict()
                        data.update({"s3_publish_path":s3_publish_path,"batch_id":new_parentlog,"file_id":file_id,"catalog_tables":list_file_folder,"source_system":source_system})
                        stitching_list_upsert.append(data)
                    stitching_list_delete_flag = []
                    for data in stitching_df.filter(
                            F.col('stitching_type') == 'delete_flag').collect():
                        data = data.asDict()
                        data.update({"s3_publish_path":s3_publish_path,"batch_id":new_parentlog,"file_id":file_id,"catalog_tables":list_file_folder,"source_system":source_system})
                        stitching_list_delete_flag.append(data)
                    stitching_list_roll_out = []
                    for data in stitching_df.filter(
                            F.col('stitching_type') == 'roll_out').collect():
                        data = data.asDict()
                        data.update({"s3_publish_path":s3_publish_path,"batch_id":new_parentlog,"file_id":file_id,"catalog_tables":list_file_folder,"source_system":source_system})
                        stitching_list_roll_out.append(data)
                    
                    stitchThreadSize = 1
                    if len(stitching_list_full)>0:
                        logger.info("Stiching Logic - FULL")
                        FullStitchingPool = Pool(int(stitchThreadSize))
                        logger.info("stitching_list_full :"+str(stitching_list_full))
                        FullResult = FullStitchingPool.map(load_stitch_full, stitching_list_full)
                        logger.info("stitching_list_full fullresult :"+str(FullResult))
                        FullStitchingPool.close()
                        FullStitchingPool.join()
                        if len(FullResult)>0:
                            for row in FullResult:
                                file_id=row['file_id']
                                pub_rec_cnt=row['publish_count']
                                s3_path_pub=s3_publish_path+publish_tbl_name+"/"
                                status = row['status']
                                message = row['message']
                    if len(stitching_list_append)>0:
                        logger.info("Stiching Logic - APPEND"+str(stitching_list_append))
                        AppendStitchingPool = Pool(int(stitchThreadSize))
                        AppendResult = AppendStitchingPool.map(load_stitch_append, stitching_list_append)
                        AppendStitchingPool.close()
                        AppendStitchingPool.join()
                        if len(AppendResult)>0:
                            for row in AppendResult:
                                file_id=row['file_id']
                                pub_rec_cnt=row['publish_count']
                                s3_path_pub=s3_publish_path+publish_tbl_name+"/"
                                status = row['status']
                                message = row['message']
                    if len(stitching_list_upsert)>0:
                        logger.info("Stiching Logic - UPSERT")
                        UpsertStitchingPool = Pool(int(stitchThreadSize))
                        UpsertResult = UpsertStitchingPool.map(load_stitch_upsert, stitching_list_upsert)
                        UpsertStitchingPool.close()
                        UpsertStitchingPool.join()
                        if len(UpsertResult)>0:
                            for row in UpsertResult:
                                file_id=row['file_id']
                                pub_rec_cnt=row['publish_count']
                                s3_path_pub=s3_publish_path+publish_tbl_name+"/"
                                status = row['status']
                                message = row['message']
                    if len(stitching_list_delete_flag)>0:
                        logger.info("Stiching Logic - Delete Flag"+str(stitching_list_delete_flag))
                        DelFlagStitchingPool = Pool(int(stitchThreadSize))
                        DelFlagResult = DelFlagStitchingPool.map(load_stitch_del_flag, stitching_list_delete_flag)
                        DelFlagStitchingPool.close()
                        DelFlagStitchingPool.join()
                        if len(DelFlagResult)>0:
                            for row in DelFlagResult:
                                file_id=row['file_id']
                                pub_rec_cnt=row['publish_count']
                                s3_path_pub=s3_publish_path+publish_tbl_name+"/"
                                status = row['status']
                                message = row['message']
                    if len(stitching_list_roll_out)>0:
                        logger.info("Stiching Logic - ROLL OUT"+str(stitching_list_roll_out))
                        RollOutStitchingPool = Pool(int(stitchThreadSize))
                        RollOutResult = RollOutStitchingPool.map(load_stitch_roll_out, stitching_list_roll_out)
                        RollOutStitchingPool.close()
                        RollOutStitchingPool.join()
                        if len(RollOutResult)>0:
                            for row in RollOutResult:
                                file_id=row['file_id']
                                pub_rec_cnt=row['publish_count']
                                s3_path_pub=s3_publish_path+publish_tbl_name+"/"
                                status = row['status']
                                message = row['message']
                   
            logger.info("load_files_parallel SUCCESS dict : "+str({'file_id': file_id, 'conf_id': conf_id, 'publish_count': pub_rec_cnt, 'status': status,
                    's3_path': s3_path_pub, 'file_name': file_name, 'message': message}))
            returnDict = {'file_id': file_id, 'conf_id': conf_id, 'publish_count': pub_rec_cnt, 'status': status,
                    's3_path': s3_path_pub, 'file_name': file_name, 'message': message}
        except Exception as e:
            logger.error("Exception - " + str(e))
            logger.info("load_files_parallel FAILED dict : "+str({'file_id': file_id, 'conf_id': conf_id, 'publish_count': 0, 'status': 'FAILED', 's3_path': '',
                    'file_name': file_name, 'message': str(e.args)}))
            returnDict = {'file_id': file_id, 'conf_id': conf_id, 'publish_count': 0, 'status': 'FAILED', 's3_path': '',
                    'file_name': file_name, 'message': str(e.args)}
    return returnDict


# COMMAND ----------

def load_stitch_full(conf={}):
    logger.info("Enter load_stitch_full function")
    published_record_count=0
    global is_new_table
    target_database_name = str(conf['target_database_name'])
    source_database_name = str(conf['source_database_name'])
    target_object_name = str(conf['target_table_name'])
    source_object_name = str(conf['source_table_name'])
    s3_publish_path = conf['s3_publish_path']
    file_id = conf['file_id']
    batch_id = conf['batch_id']
    catalog_tables = conf['catalog_tables']
    pattern_name= conf['pattern_name']
    source_temp_view = source_object_name + str(file_id)
    target_temp_view = target_object_name + str(file_id)
    try:
        if target_object_name in catalog_tables:
            logger.info("Try Except for new table creation or existing insert")
            # columns = spark.table(
            #     """ {target_database_name}.{target_object_name} """.format(target_database_name=target_database_name,
            #                                                               target_object_name=target_object_name)).columns
            target_df = spark.sql(
                """select * from {source_temp_view} where batch_id = {batch_id}""".format(
                    source_temp_view=source_temp_view, batch_id=new_parentlog,file_id=file_id))
            # logger.info("Column of Target Table : "+str(columns))
            #df_output_table_name.select(columns)
            #df_output_table_name.createOrReplaceTempView(source_temp_view)
            #logger.info("delete started for "+str(target_object_name)) #debug checkpoint
            
            suffix_temp_path = '/temp_path/'+target_object_name+'/'+str(file_id)+'/'
            s3_temp_path = f's3a://{s3_cdl_publish_bucket}'+suffix_temp_path
            logger.info(f"Writing to temp path - {s3_temp_path}")
            logger.info("starting to write at - {}".format(str(datetime.now())))
            target_df.write.format('parquet').mode('overwrite').save("{s3_temp_path}".format(s3_temp_path=s3_temp_path))
            logger.info("File written to s3 completd at - {}".format(str(datetime.now())))
            out_df = spark.read.parquet("{s3_temp_path}".format(s3_temp_path=s3_temp_path))
            out_df.createOrReplaceTempView(target_temp_view)
            logger.info("Temp File read at - {}"+str(datetime.now()))
            
            # spark.sql(
            #     """delete from {target_database_name}.{target_object_name}""".format(
            #         target_database_name=target_database_name, target_object_name=target_object_name))
            # logger.info("delete closed for "+str(target_object_name)) #debug checkpoint
            # logger.info("Old data in publish table " + str(target_object_name) + " got cleared")
            tgt_cols_list=spark.sql(f'select * from {target_database_name}.{target_object_name} limit 1')
            tgt_cols_list = tgt_cols_list.columns
            tgt_cols_list_final= [col.strip() for col in tgt_cols_list]
            final_columns=",".join([i for i in tgt_cols_list_final])
            spark.sql(
                """insert overwrite table {target_database_name}.{target_object_name} PARTITION(data_src_nm) select distinct {final_columns} from {target_temp_view}""".format(
                    target_database_name=target_database_name, target_object_name=target_object_name,target_temp_view=target_temp_view,final_columns=final_columns))
            logger.info("Inserted new data into publish table " + str(target_object_name))
            # published_record_count = df_output_table_name.count()
            
            s3_client = boto3.client('s3')
            response = s3_client.list_objects_v2(Bucket=s3_cdl_publish_bucket,Prefix=suffix_temp_path[1:])
            files_in_folder = response["Contents"]
            files_to_delete = []
            for f in files_in_folder:
                files_to_delete.append({"Key": f["Key"]})
                
            response = s3_client.delete_objects(
                Bucket=s3_cdl_publish_bucket, Delete={"Objects": files_to_delete}
            )
            
            logger.info("Temp Files deleted from S3")
            logger.info("S3 delete response - "+str(response['Deleted']))
            
            if file_id != 0:
                logger.info("Exit load_stitch_full function")
                return {'file_id': file_id, 'status': 'SUCCESS', 'publish_count' : published_record_count, 'target_object_name': target_object_name, 'message': 'File processed successfully'}
            else:
                logger.info("No data moved to publish table - " + str(target_object_name))
                return {'file_id': file_id, 'status': 'FAILED', 'publish_count' : 0, 'target_object_name': target_object_name,
                        'message': 'No data moved to stitched table'}
        else:
            logger.info("Table not exist in the Glue Catalog so Skipping Spark Sql insert and directly loading to S3 for : "+str(target_object_name))
            df_output_table_name = spark.sql("""select * from {source_temp_view} """.format(source_temp_view=source_temp_view))
            df_output_table_name.createOrReplaceTempView(source_temp_view)
            logger.info("No Publish Table Exist in the Athena so loading data directly to S3") # debug breakpoint
            publish_load_status = write_parquet(target_object_name,source_temp_view,s3_publish_path,pattern_name)
            is_new_table=publish_load_status
            logger.info("Is New Table Flag is : "+str(is_new_table))
            logger.info("Crawler Flag Assigned for the table name {table_name}".format(table_name=target_object_name))
            # if is_new_table:
            #     run_curated_crawler(crawler_name)
            #     logger.info("New table has been crawled")
            #     is_new_table=False
            published_record_count = df_output_table_name.count()
            if file_id != 0:
                logger.info("Exit load_stitch_full function")
                return {'file_id': file_id, 'status': 'SUCCESS', 'publish_count' : published_record_count, 'target_object_name': target_object_name, 'message': 'File processed successfully'}
            else:
                logger.info("No data moved to stitched table - " + str(target_object_name))
                logger.info("Exit load_stitch_full function")
                return {'file_id': file_id, 'status': 'FAILED', 'publish_count' : 0, 'target_object_name': target_object_name,
                        'message': 'No data moved to stitched table'}
    except Exception as e:
        logger.error("Exception - " + str(target_object_name) + " - " + str(e))
        logger.info("Exit load_stitch_full function")
        return {'file_id': file_id, 'status': 'FAILED', 'publish_count' : 0, 'target_object_name': target_object_name, 'message': str(e.args)}


# COMMAND ----------

def load_stitch_roll_out(conf={}):
    published_record_count=0
    global is_new_table
    global skip_append_stich_dict #TEMP FIX
    logger.info("Loading Stitching Append")
    target_database_name  = str(conf['target_database_name']).format(env=environment)
    source_database_name = str(conf['source_database_name']).format(env=environment)
    target_object_name = str(conf['target_table_name'])
    source_object_name = str(conf['source_table_name'])
    file_id = conf['file_id']
    batch_id = conf['batch_id']
    s3_publish_path = conf['s3_publish_path']
    catalog_tables = conf['catalog_tables']
    source_temp_view = source_object_name + str(file_id)
    target_temp_view = target_object_name + str(file_id)
    pattern_name = conf['pattern_name']
    if skip_append_stich_dict.get(target_object_name):
        return {'file_id': file_id, 'status': 'SUCCESS', 'publish_count' : skip_append_stich_dict[target_object_name], 'target_object_name': target_object_name, 'message': 'File processed successfully'}
    try:
        if target_object_name in catalog_tables:
            logger.info("target_database_name: {}, source_database_name : {}, target_object_name : {}, target_object_name : {}, source_object_name : {}, file_id : {},batch_id: {}".format(target_database_name, source_database_name, target_object_name, target_object_name, source_object_name, file_id,batch_id))
            columns=spark.table(""" {target_database_name}.{target_object_name} """.format(target_database_name=target_database_name, target_object_name=target_object_name)).columns
            tgt_cols_list=spark.sql(f'select * from {target_database_name}.{target_object_name} limit 1')
            tgt_cols_list = tgt_cols_list.columns
            logger.info("tgt_cols_list-"+str(tgt_cols_list))
            tgt_cols_list_final= [col.strip() for col in tgt_cols_list]
            final_columns=",".join([i for i in tgt_cols_list_final])
            logger.info("final_columns-"+str(final_columns))
            df_output_table_name = spark.sql("""select {final_columns} from {source_temp_view} where batch_id = '{batch_id}'""".format(source_temp_view=source_temp_view, batch_id=batch_id,final_columns=final_columns))
            # df_output_table_name.select(columns)
            df_output_table_name.createOrReplaceTempView(source_temp_view)
            df_min_period = spark.sql(""" select min(src_mnth_end_dt) from {source_temp_view} where batch_id = '{batch_id}'""".format(source_temp_view=source_temp_view, batch_id=batch_id)).collect()
            min_period = df_min_period[0][0]
            logger.info(" Min Period of the batch_id "+str(min_period))
            # logger.info("Append method delete started for "+str(target_object_name)) #debug checkpoint
            # spark.sql("""delete from {target_database_name}.{target_object_name} where batch_id={batch_id}""".format(target_database_name=target_database_name, target_object_name=target_object_name, batch_id=batch_id))
            # logger.info("Append method delete started for "+str(target_object_name)) #debug checkpoint
            # logger.info("old data in curated table "+str(target_object_name)+" got cleared")
            tgt_undel_df = spark.sql("""select * from {target_database_name}.{target_object_name} where src_mnth_end_dt < '{min_period}' and data_src_nm='{data_src_nm}'""".format(target_database_name=target_database_name, target_object_name=target_object_name,min_period=min_period,  batch_id=batch_id,data_src_nm=pattern_name))
            target_df = tgt_undel_df.union(df_output_table_name)
            
            suffix_temp_path = '/temp_path/'+target_object_name+'/'+str(file_id)+'/'
            s3_temp_path = f's3a://{s3_cdl_publish_bucket}'+suffix_temp_path
            logger.info(f"Writing to temp path - {s3_temp_path}")
            logger.info("starting to write at - {}".format(str(datetime.now())))
            target_df.write.format('parquet').mode('overwrite').save("{s3_temp_path}".format(s3_temp_path=s3_temp_path))
            logger.info("File written to s3 completd at - {}".format(str(datetime.now())))
            out_df = spark.read.parquet("{s3_temp_path}".format(s3_temp_path=s3_temp_path))
            out_df.createOrReplaceTempView(target_temp_view)
            logger.info("Temp File read at - {}"+str(datetime.now()))
            
            spark.sql("""insert overwrite table {target_database_name}.{target_object_name} PARTITION(data_src_nm) select distinct {final_columns} from {target_temp_view}""".format(target_database_name=target_database_name, target_object_name=target_object_name,target_temp_view=target_temp_view,final_columns=final_columns))
            logger.info("Inserted new data into publish table "+str(target_object_name))
            published_record_count=df_output_table_name.count()
            
            s3_client = boto3.client('s3')
            response = s3_client.list_objects_v2(Bucket=s3_cdl_publish_bucket, Prefix=suffix_temp_path[1:])
            files_in_folder = response["Contents"]
            files_to_delete = []
            for f in files_in_folder:
                files_to_delete.append({"Key": f["Key"]})
                
            response = s3_client.delete_objects(
                Bucket=s3_cdl_publish_bucket, Delete={"Objects": files_to_delete}
            )
            
            logger.info("Temp Files deleted from S3")
            logger.info("S3 delete response - "+str(response['Deleted']))
            
            if file_id != 0:
                skip_append_stich_dict[target_object_name] =published_record_count
                return {'file_id': file_id, 'status': 'SUCCESS', 'publish_count' : published_record_count, 'target_object_name': target_object_name, 'message': 'File processed successfully'}
            else:
                skip_append_stich_dict[target_object_name] =published_record_count
                logger.info("No data moved to publish table - "+str(target_object_name))
                return {'file_id': file_id, 'status': 'FAILED', 'publish_count': 0, 'target_object_name': target_object_name,
                      'message': 'No data moved to publish table'}
        else:
            logger.info("Table not exist in Glue Catalog so Skipping Spark Sql insert and directly loading to S3 for : "+str(target_object_name))
            df_output_table_name = spark.sql("""select * from {source_temp_view} where batch_id = {batch_id}""".format(source_temp_view=source_temp_view, batch_id=batch_id))
            df_output_table_name.createOrReplaceTempView(source_temp_view)
            logger.info("No Publish Table Exist in the Athena so loading data directly to S3") # debug breakpoint
            publish_load_status = write_parquet(target_object_name,source_temp_view,s3_publish_path,pattern_name)
            is_new_table=publish_load_status
            logger.info("Is New Table Flag is : "+str(is_new_table))
            logger.info("Crawler Flag Assigned for the table name {table_name}".format(table_name=target_object_name))
            # if is_new_table:
            #     run_curated_crawler(crawler_name)
            #     logger.info("New table has been crawled")
            #     is_new_table=False
            published_record_count=df_output_table_name.count()
            if file_id != 0:
                skip_append_stich_dict[target_object_name] =published_record_count
                return {'file_id': file_id, 'status': 'SUCCESS', 'publish_count' : published_record_count, 'target_object_name': target_object_name, 'message': 'File processed successfully'}
            else:
                skip_append_stich_dict[target_object_name] =published_record_count
                logger.info("No data moved to publish table - "+str(target_object_name))
                return {'file_id': file_id, 'status': 'FAILED', 'publish_count': 0, 'target_object_name': target_object_name,
                      'message': 'No data moved to publish table'}
    except Exception as e:
        logger.error("Exception - "+str(target_object_name)+" - "+str(e))
        return {'file_id': file_id, 'status': 'FAILED', 'publish_count': 0, 'target_object_name': target_object_name,
                'message': str(e.args)}

# COMMAND ----------

def load_stitch_append(conf={}):
    published_record_count=0
    global is_new_table
    global skip_append_stich_dict #TEMP FIX
    logger.info("Loading Stitching Append")
    target_database_name  = str(conf['target_database_name']).format(env=environment)
    source_database_name = str(conf['source_database_name']).format(env=environment)
    target_object_name = str(conf['target_table_name'])
    source_object_name = str(conf['source_table_name'])
    file_id = conf['file_id']
    batch_id = conf['batch_id']
    s3_publish_path = conf['s3_publish_path']
    catalog_tables = conf['catalog_tables']
    source_temp_view = source_object_name + str(file_id)
    target_temp_view = target_object_name + str(file_id)
    pattern_name = conf['pattern_name']
    if skip_append_stich_dict.get(target_object_name):
        return {'file_id': file_id, 'status': 'SUCCESS', 'publish_count' : skip_append_stich_dict[target_object_name], 'target_object_name': target_object_name, 'message': 'File processed successfully'}
    try:
        if target_object_name in catalog_tables:
            logger.info("target_database_name: {}, source_database_name : {}, target_object_name : {}, target_object_name : {}, source_object_name : {}, file_id : {},batch_id: {}".format(target_database_name, source_database_name, target_object_name, target_object_name, source_object_name, file_id,batch_id))
            columns=spark.table(""" {target_database_name}.{target_object_name} """.format(target_database_name=target_database_name, target_object_name=target_object_name)).columns
            tgt_cols_list=spark.sql(f'select * from {target_database_name}.{target_object_name} limit 1')
            tgt_cols_list = tgt_cols_list.columns
            logger.info("tgt_cols_list-"+str(tgt_cols_list))
            tgt_cols_list_final= [col.strip() for col in tgt_cols_list]
            final_columns=",".join([i for i in tgt_cols_list_final])
            logger.info("final_columns-"+str(final_columns))
            df_output_table_name = spark.sql("""select {final_columns} from {source_temp_view} where batch_id = {batch_id}""".format(source_temp_view=source_temp_view, batch_id=batch_id,final_columns=final_columns))
            # df_output_table_name.select(columns)
            df_output_table_name.createOrReplaceTempView(source_temp_view)
            # logger.info("Append method delete started for "+str(target_object_name)) #debug checkpoint
            # spark.sql("""delete from {target_database_name}.{target_object_name} where batch_id={batch_id}""".format(target_database_name=target_database_name, target_object_name=target_object_name, batch_id=batch_id))
            # logger.info("Append method delete started for "+str(target_object_name)) #debug checkpoint
            # logger.info("old data in curated table "+str(target_object_name)+" got cleared")
            tgt_undel_df = spark.sql("""select * from {target_database_name}.{target_object_name} where batch_id!='{batch_id}' and data_src_nm='{data_src_nm}'""".format(target_database_name=target_database_name, target_object_name=target_object_name, batch_id=batch_id,data_src_nm=pattern_name))
            target_df = tgt_undel_df.union(df_output_table_name)
            
            suffix_temp_path = '/temp_path/'+target_object_name+'/'+str(file_id)+'/'
            s3_temp_path = f's3a://{s3_cdl_publish_bucket}'+suffix_temp_path
            logger.info(f"Writing to temp path - {s3_temp_path}")
            logger.info("starting to write at - {}".format(str(datetime.now())))
            target_df.write.format('parquet').mode('overwrite').save("{s3_temp_path}".format(s3_temp_path=s3_temp_path))
            logger.info("File written to s3 completd at - {}".format(str(datetime.now())))
            out_df = spark.read.parquet("{s3_temp_path}".format(s3_temp_path=s3_temp_path))
            out_df.createOrReplaceTempView(target_temp_view)
            logger.info("Temp File read at - {}"+str(datetime.now()))
            
            spark.sql("""insert overwrite table {target_database_name}.{target_object_name} PARTITION(data_src_nm) select distinct {final_columns} from {target_temp_view}""".format(target_database_name=target_database_name, target_object_name=target_object_name,target_temp_view=target_temp_view,final_columns=final_columns))
            logger.info("Inserted new data into publish table "+str(target_object_name))
            published_record_count=df_output_table_name.count()
            
            s3_client = boto3.client('s3')
            response = s3_client.list_objects_v2(Bucket=s3_cdl_publish_bucket, Prefix=suffix_temp_path[1:])
            files_in_folder = response["Contents"]
            files_to_delete = []
            for f in files_in_folder:
                files_to_delete.append({"Key": f["Key"]})
                
            response = s3_client.delete_objects(
                Bucket=s3_cdl_publish_bucket, Delete={"Objects": files_to_delete}
            )
            
            logger.info("Temp Files deleted from S3")
            logger.info("S3 delete response - "+str(response['Deleted']))
            
            if file_id != 0:
                skip_append_stich_dict[target_object_name] =published_record_count
                return {'file_id': file_id, 'status': 'SUCCESS', 'publish_count' : published_record_count, 'target_object_name': target_object_name, 'message': 'File processed successfully'}
            else:
                skip_append_stich_dict[target_object_name] =published_record_count
                logger.info("No data moved to publish table - "+str(target_object_name))
                return {'file_id': file_id, 'status': 'FAILED', 'publish_count': 0, 'target_object_name': target_object_name,
                      'message': 'No data moved to publish table'}
        else:
            logger.info("Table not exist in Glue Catalog so Skipping Spark Sql insert and directly loading to S3 for : "+str(target_object_name))
            df_output_table_name = spark.sql("""select * from {source_temp_view} where batch_id = {batch_id}""".format(source_temp_view=source_temp_view, batch_id=batch_id))
            df_output_table_name.createOrReplaceTempView(source_temp_view)
            logger.info("No Publish Table Exist in the Athena so loading data directly to S3") # debug breakpoint
            publish_load_status = write_parquet(target_object_name,source_temp_view,s3_publish_path,pattern_name)
            is_new_table=publish_load_status
            logger.info("Is New Table Flag is : "+str(is_new_table))
            logger.info("Crawler Flag Assigned for the table name {table_name}".format(table_name=target_object_name))
            # if is_new_table:
            #     run_curated_crawler(crawler_name)
            #     logger.info("New table has been crawled")
            #     is_new_table=False
            published_record_count=df_output_table_name.count()
            if file_id != 0:
                skip_append_stich_dict[target_object_name] =published_record_count
                return {'file_id': file_id, 'status': 'SUCCESS', 'publish_count' : published_record_count, 'target_object_name': target_object_name, 'message': 'File processed successfully'}
            else:
                skip_append_stich_dict[target_object_name] =published_record_count
                logger.info("No data moved to publish table - "+str(target_object_name))
                return {'file_id': file_id, 'status': 'FAILED', 'publish_count': 0, 'target_object_name': target_object_name,
                      'message': 'No data moved to publish table'}
    except Exception as e:
        logger.error("Exception - "+str(target_object_name)+" - "+str(e))
        return {'file_id': file_id, 'status': 'FAILED', 'publish_count': 0, 'target_object_name': target_object_name,
                'message': str(e.args)}


def load_stitch_upsert(conf={}):
    published_record_count=0
    global is_new_table
    target_database_name = str(conf['target_database_name'])
    source_database_name = str(conf['source_database_name'])
    target_object_name = str(conf['target_table_name'])
    source_object_name = str(conf['source_table_name'])
    file_id = conf['file_id']
    batch_id = conf['batch_id']
    s3_publish_path = conf['s3_publish_path']
    catalog_tables = conf['catalog_tables']
    source_temp_view = source_object_name + str(file_id)
    target_temp_view = target_object_name + str(file_id)
    primary_keys = str(conf['primary_keys']).split(',')
    first_time_table=False
    pattern_name = conf['pattern_name']
    try:
        if target_object_name in catalog_tables:
            condition = " and ".join(["SRC.{k} = TGT.{k}".format(k=k) for k in primary_keys])
            logger.info("Try Except on new table and existing table insert")
            # try:
            publish_data = spark.sql(
            "select * from {target_database_name}.{target_object_name} where data_src_nm='{data_src_nm}'".format(target_database_name=target_database_name,target_object_name=target_object_name,data_src_nm=pattern_name))
            publish_data.createOrReplaceTempView(target_temp_view)
            # publish_schema = spark.table(f"{target_database_name}.{target_object_name}").schema()
            tgt_cols_list = publish_data.columns
            logger.info("Data read from publish table: " + str(target_temp_view))
                # first_time_table=True
            # except Exception as e:
            #     if 'Table or view not found' in str(e):
            #         curated_data = spark.sql("""select * from {source_temp_view} where batch_id = {batch_id}""".format(source_temp_view=source_temp_view, batch_id=new_parentlog))
            #         tgt_cols_list = curated_data.columns
            set_cols_string = " , ".join(["TGT.{col} = SRC.{col}".format(col=col) for col in tgt_cols_list])
            ins_cols_values_str = " , ".join(["SRC.{col}".format(col=col) for col in tgt_cols_list])
            curated_data = spark.sql(
                """select * from {source_temp_view} where batch_id = {batch_id}""".format(
                    source_temp_view=source_temp_view,
                    batch_id=new_parentlog))
            curated_data.createOrReplaceTempView(source_temp_view)
            logger.info("Data read from curated table: " + str(source_temp_view))
    
            # pt_file_ids_rdd = file_id.split()
            # file_id_ls = tuple([i for i in pt_file_ids_rdd])
            # if len(file_id_ls) == 1:
            #     file_id_ls = "(" + str(file_id_ls[0]) + ")"
            # file_name = spark.sql(
            #     """select file_name,file_id from file_process_log where parent_batch_id={batch_id} and file_id in {file_id_ls} order by file_name """.format(
            #         file_id_ls=file_id_ls, batch_id=new_parentlog))
            # final_file_id_ls = file_name.select('file_id').collect()
    
            # for file_id in final_file_id_ls:
            logger.info("Executing the process for - " + str(file_id))
            # merge_query = """merge into {target_temp_view} TGT using (select * from {source_temp_view}) SRC on {condition} when matched then update set {set_cols_string} when not matched then insert ({ins_col_name}) values({ins_col_values})""".format(
            #     target_temp_view=target_temp_view,
            #     source_temp_view=source_temp_view, condition=condition, set_cols_string=set_cols_string,
            #     ins_col_name=','.join(tgt_cols_list), ins_col_values=ins_cols_values_str, file_id=str(file_id))
            # logger.info("Spark SQL merge_query : "+str(merge_query))
            # final_ins_df = spark.sql(merge_query)
            # final_ins_df.createOrReplaceTempView(target_temp_view)
            # if not first_time_table:
            primary_keys = [col.strip().upper() for col in primary_keys]
            tgt_cols_list_final= [col.strip().upper() for col in tgt_cols_list]
            tgt_cols_list = [col.strip().upper() for col in tgt_cols_list]
            logger.info(f"primary_keys - {primary_keys}")
            # logger.info(f"Before tgt_cols_list - {tgt_cols_list}")
            for c in primary_keys:
                if c in tgt_cols_list:
                    tgt_cols_list.remove(c)
            logger.info(f"tgt_cols_list - {tgt_cols_list}")
            
            target_df = curated_data.alias('src').join(publish_data.alias('tgt'),   primary_keys  ,how='right_outer').select(*(col for col in primary_keys) ,*(F.coalesce('src.'+col,'tgt.'+col).alias(col) for col in tgt_cols_list))
            
            # target_df_columns=target_df.columns
            # target_df_columns_1 = [col.strip() for col in target_df_columns]
            # logger.info(f"target_df_columns_1 - {target_df_columns_1}")
            
            # target_df = publish_data.alias('tgt').join(curated_data.alias('src'),primary_keys,how='left_outer').\
            #     select(primary_keys,*(F.coalesce('src.'+col,'tgt.'+col).alias(col) for col in tgt_cols_list))
            
            target_df.createOrReplaceTempView(target_temp_view+str(file_id))
            final_columns=",".join([i for i in tgt_cols_list_final])
            logger.info(f"final_columns-{final_columns}")
            target_df=spark.sql("select {final_columns} from {target_temp_view}".format(target_temp_view=target_temp_view+str(file_id),final_columns=final_columns))
            logger.info("target_df-"+str(target_df.columns))
            logger.info("target_df count-"+str(target_df.count()))
            curated_data=spark.sql("select {final_columns} from {source_temp_view}".format(source_temp_view=source_temp_view,final_columns=final_columns))
            logger.info("curated_data-"+str(curated_data.columns))
            curated_data=curated_data.subtract(target_df)
            logger.info("curated_data count-"+str(curated_data.count()))
            target_df_common = curated_data.alias('src').join(target_df.alias('tgt'),   primary_keys  ,how='inner').select(*(col for col in primary_keys) ,*('tgt.'+col for col in tgt_cols_list))
            logger.info("target_df_common Union count-"+str(target_df_common.count()))
            target_df_common.createOrReplaceTempView("target_df_common")
            target_df_common = spark.sql("select {final_columns} from target_df_common".format(final_columns=final_columns))
            target_df_final = target_df.subtract(target_df_common)
            logger.info("target_df_final Union count-"+str(target_df_final.count()))
            target_df = target_df_final.union(curated_data)
            #target_df = target_df.union(curated_data)
            logger.info("target_df Union count-"+str(target_df.count()))
            # s3_temp_path = '/'.join(s3_publish_path.split('/')[:3])+'/temp_path/'+target_object_name+'/'  ## Sabari : Path will be wrong
            suffix_temp_path = '/temp_path/'+target_object_name+'/'+str(file_id)+'/'
            s3_temp_path = f's3a://{s3_cdl_publish_bucket}'+suffix_temp_path
            
            logger.info(f"Writing to temp path - {s3_temp_path}")
            logger.info("starting to write at - {}".format(str(datetime.now())))
            # target_df.write.mode('append').parquet("{s3_temp_path}".format(s3_temp_path=s3_temp_path)) ## Sabari : Why are we appending the files in temp path ?
            # target_df.write.mode('overwrite').parquet("{s3_temp_path}".format(s3_temp_path=s3_temp_path))
            target_df.write.format('parquet').mode('overwrite').save("{s3_temp_path}".format(s3_temp_path=s3_temp_path))
            logger.info("File written to s3 completd at - {}".format(str(datetime.now())))
            out_df = spark.read.parquet("{s3_temp_path}".format(s3_temp_path=s3_temp_path))
            out_df.createOrReplaceTempView(target_temp_view)
            logger.info("out_df count-"+str(out_df.count()))
            logger.info("Temp File read at - {}"+str(datetime.now()))
            table="insert overwrite table {target_database_name}.{target_object_name} PARTITION(data_src_nm) select distinct {final_columns} from {target_temp_view}".format(target_database_name=target_database_name,target_object_name=target_object_name,target_temp_view=target_temp_view,final_columns=final_columns)
            logger.info(table)
            # table_1=spark.sql("select {final_columns} from {target_temp_view}".format(target_database_name=target_database_name,target_object_name=target_object_name,target_temp_view=target_temp_view,final_columns=final_columns))
            # table_1.createOrReplaceTempView('table_1')
            # logger.info(table_1)
            
            spark.sql("insert overwrite table {target_database_name}.{target_object_name} PARTITION(data_src_nm) select distinct {final_columns} from {target_temp_view}".format(target_database_name=target_database_name,target_object_name=target_object_name,target_temp_view=target_temp_view,final_columns=final_columns))
            logger.info("Data loaded into table as per Upsert logic")
            
            s3_client = boto3.client('s3')
            response = s3_client.list_objects_v2(Bucket=f'{s3_cdl_publish_bucket}', Prefix=suffix_temp_path[1:])
            # logger.info("response-"+str(response))
            files_in_folder = response["Contents"]
            files_to_delete = []
            for f in files_in_folder:
                files_to_delete.append({"Key": f["Key"]})
                
            response = s3_client.delete_objects(
                Bucket=s3_cdl_publish_bucket, Delete={"Objects": files_to_delete}
            )
            
            logger.info("Temp Files deleted from S3")
            logger.info("S3 delete response - "+str(response['Deleted']))
            
            # else:
            #     curated_data.write.mode('overwrite').parquet(
            #                 "{s3_publish_path}{publish_table_name}/".format(
            #                     s3_publish_path=s3_publish_path, publish_table_name=target_object_name))
            #     logger.info("Data loaded into file as per Upsert logic")
            logger.info("Before count")
            published_stitched_record_count = spark.sql("select * from {target_database_name}.{target_object_name}".format(target_database_name=target_database_name,target_object_name=target_object_name))
            published_stitched_record_count=published_stitched_record_count.count()
            logger.info("After count")
            if file_id != 0:
                return {'file_id': file_id, 'status': 'SUCCESS', 'publish_count': published_stitched_record_count,
                        'target_object_name': target_object_name, 'message': 'File processed successfully'}
            else:
                logger.info("No data moved to publish table - " + str(target_object_name))
                return {'file_id': file_id, 'status': 'FAILED', 'publish_count': 0, 'target_object_name': target_object_name,
                        'message': 'No data moved to publish table'}
        else:
            logger.info("Table not exist in the Glue Catalog so Skipping Spark Sql insert and directly loading to S3 for : "+str(target_object_name))
            #curated_data = spark.sql("""select * from {source_temp_view} where pt_file_id='{file_id}'""".format(source_temp_view#=source_temp_view,file_id=str(file_id))) 
            curated_data = spark.sql("""select * from {source_temp_view}""".format(source_temp_view=source_object_name+str(file_id))) #mk
            curated_data.createOrReplaceTempView(source_temp_view)
            logger.info("No Publish Table Exist in the Athena so loading data directly to S3") # debug breakpoint
            publish_load_status = write_parquet(target_object_name,source_temp_view,s3_publish_path,pattern_name)
            is_new_table=publish_load_status
            logger.info("Is New Table Flag is : "+str(is_new_table))
            logger.info("Crawler Flag Assigned for the table name {table_name}".format(table_name=target_object_name))
            published_stitched_record_count = curated_data.count()
            if file_id != 0:
                return {'file_id': file_id, 'status': 'SUCCESS', 'publish_count': published_stitched_record_count,
                        'target_object_name': target_object_name, 'message': 'File processed successfully'}
            else:
                logger.info("No data moved to publish table - " + str(target_object_name))
                return {'file_id': file_id, 'status': 'FAILED', 'publish_count': 0, 'target_object_name': target_object_name,
                        'message': 'No data moved to publish table'}

    except Exception as e:
        logger.error("Exception - "+str(target_object_name)+" - "+str(e))
        # return {'file_id': file_id, 'status': 'FAILED', 'publish_count': 0, 'target_object_name': target_object_name,
        #         'message': str(e.args)}
        raise Exception

# COMMAND ----------

def load_stitch_del_flag(conf={}):
    published_record_count=0
    global is_new_table
    global skip_del_stich_dict #TEMP FIX
    logger.info("Loading Stitching Delete Flag")
    target_database_name  = str(conf['target_database_name'])
    source_database_name = str(conf['source_database_name'])
    target_object_name = str(conf['target_table_name'])
    source_object_name = str(conf['source_table_name'])
    file_id = conf['file_id']
    batch_id = conf['batch_id']
    s3_publish_path = conf['s3_publish_path']
    catalog_tables = conf['catalog_tables']
    source_temp_view = source_object_name + str(file_id)
    target_temp_view = target_object_name + str(file_id)
    pattern_name = conf['pattern_name']
    if skip_del_stich_dict.get(target_object_name):
        return {'file_id': file_id, 'status': 'SUCCESS', 'publish_count' : skip_del_stich_dict[target_object_name], 'target_object_name': target_object_name, 'message': 'File processed successfully'}
    try:
        if target_object_name in catalog_tables:
            logger.info("target_database_name: {}, source_database_name : {}, target_object_name : {}, target_object_name : {}, source_object_name : {}, file_id : {},batch_id: {}".format(target_database_name, source_database_name, target_object_name, target_object_name, source_object_name, file_id,batch_id))
            columns=spark.table(""" {target_database_name}.{target_object_name} """.format(target_database_name=target_database_name, target_object_name=target_object_name)).columns
            tgt_cols_list=spark.table(f'{target_database_name}.{target_object_name}').columns
            tgt_cols_list_final= [col.strip() for col in tgt_cols_list]
            final_columns=",".join([i for i in tgt_cols_list_final])
            df_output_table_name = spark.sql("""select {final_columns} from {source_temp_view} """.format(source_temp_view=source_temp_view, batch_id=batch_id,final_columns=final_columns))
            df_output_table_name.createOrReplaceTempView(source_temp_view)
            if target_object_name.lower() not in ['pub_ptnt_actvty_fct','pub_ptnt_mdcr_prt_d_fct']:
                tgt_undel_df = spark.sql("""select {final_columns} from {target_database_name}.{target_object_name} where batch_id!='{batch_id}' and (del_flg is null or del_flg='0' or lower(del_flg)='null') and data_src_nm='{data_src_nm}'""".format(target_database_name=target_database_name, target_object_name=target_object_name, batch_id=batch_id,data_src_nm=pattern_name,final_columns=final_columns))
                target_df = tgt_undel_df.union(df_output_table_name)
            else:
                target_df = df_output_table_name
            suffix_temp_path = '/temp_path/'+target_object_name+'/'+str(file_id)+'/'
            s3_temp_path = f's3a://{s3_cdl_publish_bucket}'+suffix_temp_path
            logger.info(f"Writing to temp path - {s3_temp_path}")
            logger.info("starting to write at - {}".format(str(datetime.now())))
            target_df.write.format('parquet').mode('overwrite').save("{s3_temp_path}".format(s3_temp_path=s3_temp_path))
            logger.info("File written to s3 completd at - {}".format(str(datetime.now())))
            out_df = spark.read.parquet("{s3_temp_path}".format(s3_temp_path=s3_temp_path))
            out_df.createOrReplaceTempView(target_temp_view)
            logger.info("Temp File read at - {}"+str(datetime.now()))
            spark.sql("""insert overwrite table {target_database_name}.{target_object_name} PARTITION(data_src_nm) select distinct {final_columns} from {target_temp_view}""".format(target_database_name=target_database_name, target_object_name=target_object_name,target_temp_view=target_temp_view,final_columns=final_columns))
            logger.info("Inserted new data into publish table "+str(target_object_name))
            published_record_count=df_output_table_name.count()
            
            s3_client = boto3.client('s3')
            response = s3_client.list_objects_v2(Bucket=s3_cdl_publish_bucket, Prefix=suffix_temp_path[1:])
            files_in_folder = response["Contents"]
            files_to_delete = []
            for f in files_in_folder:
                files_to_delete.append({"Key": f["Key"]})
                
            response = s3_client.delete_objects(
                Bucket=s3_cdl_publish_bucket, Delete={"Objects": files_to_delete}
            )
            
            logger.info("Temp Files deleted from S3")
            logger.info("S3 delete response - "+str(response['Deleted']))
            
            if file_id != 0:
                skip_del_stich_dict[target_object_name] =published_record_count
                return {'file_id': file_id, 'status': 'SUCCESS', 'publish_count' : published_record_count, 'target_object_name': target_object_name, 'message': 'File processed successfully'}
            else:
                skip_del_stich_dict[target_object_name] =published_record_count
                logger.info("No data moved to publish table - "+str(target_object_name))
                return {'file_id': file_id, 'status': 'FAILED', 'publish_count': 0, 'target_object_name': target_object_name,
                      'message': 'No data moved to publish table'}
        else:
            logger.info("Table not exist in Glue Catalog so Skipping Spark Sql insert and directly loading to S3 for : "+str(target_object_name))
            df_output_table_name = spark.sql("""select * from {source_temp_view} where batch_id = {batch_id}""".format(source_temp_view=source_temp_view, batch_id=batch_id))
            df_output_table_name.createOrReplaceTempView(source_temp_view)
            logger.info("No Publish Table Exist in the Athena so loading data directly to S3") # debug breakpoint
            publish_load_status = write_parquet(target_object_name,source_temp_view,s3_publish_path,pattern_name)
            is_new_table=publish_load_status
            logger.info("Is New Table Flag is : "+str(is_new_table))
            logger.info("Crawler Flag Assigned for the table name {table_name}".format(table_name=target_object_name))
            published_record_count=df_output_table_name.count()
            if file_id != 0:
                skip_del_stich_dict[target_object_name] =published_record_count
                return {'file_id': file_id, 'status': 'SUCCESS', 'publish_count' : published_record_count, 'target_object_name': target_object_name, 'message': 'File processed successfully'}
            else:
                skip_del_stich_dict[target_object_name] =published_record_count
                logger.info("No data moved to publish table - "+str(target_object_name))
                return {'file_id': file_id, 'status': 'FAILED', 'publish_count': 0, 'target_object_name': target_object_name,
                      'message': 'No data moved to publish table'}
    except Exception as e:
        logger.error("Exception - "+str(target_object_name)+" - "+str(e))
        return {'file_id': file_id, 'status': 'FAILED', 'publish_count': 0, 'target_object_name': target_object_name,
                'message': str(e.args)}

if __name__ == "__main__":

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
    s3_cdl_curated_bucket = workflow_params['s3_cdl_curated_bucket']
    s3_cdl_publish_bucket = workflow_params['s3_cdl_publish_bucket']
    environment = workflow_params['environment']
    to_address_email = workflow_params['to_address_email']
    fm_address_email = workflow_params['fm_address_email']
    crawler_db_name = workflow_params['publish_db']
    crawler_name = workflow_params['publish_crawler']
    secret_id = workflow_params['rds_secret_id']
    myThreads = workflow_params['no_of_thread'] # debug breakpoint
    
    secret = cdl_lib.get_secret_manager(secret_id)


    logger.info("The source_system is: {}".format(source_system))
    logger.info("The parent_batch_process_name value is: {}".format(parent_batch_process_name))
    logger.info("The s3_path value is: {}".format(s3_cdl_curated_bucket))
    logger.info("The new_parentlog is: {}".format(new_parentlog))
    logger.info("The s3_vendor_bucket is: {}".format(s3_vendor_bucket))
    logger.info("The environment is: {}".format(environment))

    pub_rec_cnt = 0
    s3_path_pub = ''
    list_file_folder = []
    publish_include_path = []
    is_new_table = False
    published_record_count=0
    pattern_file_list = []
    file_id = 0
    file_list = []
    status_result = []
    skip_del_stich_dict ={}
    skip_append_stich_dict ={}
            
    config_data = """(SELECT Trim(fl.file_name) AS file_name, fl.file_id,fl.parent_batch_id,Trim(cnf.domain) AS domain, Trim(cnf.sub_domain) AS sub_domain, cnf.source_object_name, cnf.primary_key, cnf.source_system, cnf.file_name as pt_file_name, cnf.configuration_id, cnf.full_or_incremental_load, cnf.row_num, s3_publish_path,s3_curated_path,s3_curated_layer,curated_database,curated_tablename,s3_publish_layer,publish_database,publish_tablename FROM   file_process_log fl INNER JOIN cdl_ingestion_log cl ON fl.parent_batch_id = cl.batch_id AND fl.file_id = cl.file_id INNER JOIN (SELECT Row_number() OVER (partition BY source_object_name, file_name,publish_tablename ORDER BY configuration_id) AS row_num, * FROM   (SELECT DISTINCT  source_object_name,configuration_id,file_name,domain,source_system, sub_domain,primary_key,full_or_incremental_load,s3_publish_path,s3_curated_path, s3_curated_layer,curated_database,curated_tablename,s3_publish_layer,publish_database, publish_tablename FROM configuration_master_new WHERE  source_system = '{source_system}' AND active_flag = 'A')AS aa)cnf ON fl.source_system = cnf.source_system AND fl.pattern_name = cnf.source_object_name WHERE  fl.source_system = '{source_system}' AND fl.parent_batch_id = '{new_parentlog}' AND ( cl.published_status <> 'SUCCESS' OR cl.published_status IS NULL ) AND ( Trim(cl.curated_status) = 'SUCCESS' ) AND ( Trim(cl.raw_status) = 'SUCCESS' ) AND ( Trim(cl.landing_status) = 'SUCCESS' )) query_wrap""".format( source_system=source_system, new_parentlog=new_parentlog)
    config_data_df = cdl_lib.read_from_db(secret=secret,tbl_query=config_data)
    # priority_query = """(select distinct priority_column from query_configuration_new where source_system='{}' and interface = 'curated_to_publish' order by priority_column ASC) query_wrap""".format(source_system)
    # priority_col_df = cdl_lib.read_from_db(secret=secret,tbl_query=priority_query)
    query_conf = """(select * from query_configuration_new where source_system='{}' and interface = 'curated_to_publish') query_wrap""".format(source_system)
    query_conf_df = cdl_lib.read_from_db(secret=secret,tbl_query=query_conf)
    
    stitching_data_query = """
        (select distinct stitch.* from (select sti.* from stitching_configuration sti join configuration_master_new cnf on sti.target_table_name=cnf.publish_tablename and cnf.source_system=sti.cdl_source join cdl_ingestion_log cdl on cdl.data_source=sti.cdl_source and cdl.pattern_name=cnf.source_object_name where sti.cdl_source='{source_system}' and cnf.s3_publish_layer='Y'  and cdl.landing_status='SUCCESS' and cdl.raw_status='SUCCESS' and cdl.curated_status='SUCCESS' and cdl.batch_id='{batch_id}' and  sti.active_flag='A' and (cdl.published_status <>'SUCCESS' or cdl.published_status is null)) stitch) query_wrap
        """.format(
        source_system=source_system,
        batch_id=new_parentlog)
    stitching_data_df = cdl_lib.read_from_db(secret,tbl_query=stitching_data_query)
 
    final_status_result = []

    min_priority,max_priority = query_conf_df.select(F.min(F.col('priority_column'))).collect()[0][0],query_conf_df.select(F.max(F.col('priority_column'))).collect()[0][0]
    logger.info("min priority :"+str(min_priority))#mk
    logger.info("max priority :"+str(max_priority))#mk
    for pr_col in range(min_priority,max_priority+1):
        logger.info("priority_column : "+str(pr_col))
        table_rdd = query_conf_df.select('source_tablename','target_tablename').\
                filter(F.col('priority_column')==pr_col).distinct().collect()
                
        config_rdd = config_data_df.select('curated_tablename','publish_tablename').distinct().collect()
                
        src_list = [src.source_tablename for src in table_rdd if src.source_tablename]
        tgt_list = [tgt.target_tablename for tgt in table_rdd if tgt.target_tablename]
        curated_list = [src.curated_tablename for src in config_rdd if src.curated_tablename]
        publish_list = [src.publish_tablename for src in config_rdd if src.publish_tablename]
        
        logger.info("Source tables for Priority {} - [{}]".format(pr_col,",".join(src_list)))
        logger.info("Target tables for Priority {} - [{}]".format(pr_col,",".join(tgt_list)))
        logger.info("Config data source tables - [{}]".format(",".join(curated_list)))
        logger.info("Config data target tables - [{}]".format(",".join(publish_list)))
        logger.info("Config count & filtered count - {} - {}".format(config_data_df.count(),config_data_df.filter((config_data_df.curated_tablename.isin(src_list))|(config_data_df.publish_tablename.isin(tgt_list))).count()))
        pattern_file_list = []## Need this declaration to avoid duplicates
        file_id = 0## Need this declaration to avoid duplicates
        file_list=[] ## Need this declaration to avoid duplicates
        for data in config_data_df.filter((config_data_df.curated_tablename.isin(src_list))|(config_data_df.publish_tablename.isin(tgt_list))).collect():
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
        logger.info("Filtered pattern_file_list : "+ str(pattern_file_list)) 
        # Create a thread and process files in parallel
        logger.info("Number of files to be processed - " + str(len(pattern_file_list)))
        final_pattern_list = []
        if len(pattern_file_list)>0:
            for k,grouped_dict in groupby(pattern_file_list,key=lambda x:x[0]['source_object_name']):
                grouped_list=list(grouped_dict)
                final_pattern_list.append(grouped_list)
        logger.info("Final Pattern List: "+str(final_pattern_list))
        myThreads=1 ## Setting the threading to 1
        FilePattPool = Pool(int(myThreads))
        status_result = FilePattPool.map(load_files_parallel, final_pattern_list)
        FilePattPool.close()
        FilePattPool.join()
        
        # logger.info(is_new_table)
        if is_new_table:
            run_curated_crawler(crawler_name)
            logger.info("New table has been crawled")
            is_new_table=False
        
        final_status_result.append(status_result)
    
    # logger.info(str(final_status_result))
    if len(final_status_result) > 0:
        df_list = []
        for itr in range(len(final_status_result)):
            if len(final_status_result[itr])>0:
                logger.info("Iterating Final Status Result : "+str(final_status_result[itr]))
                for row in final_status_result[itr]:
                    record = []  # use .items() from row to convert into list - To avoid the below iteration
                    record.append(row['conf_id'])
                    record.append(row['publish_count'])
                    record.append(row['file_id'])
                    record.append(row['status'])
                    record.append(row['file_name'])
                    record.append(row['s3_path'])
                    record.append(row['message'])
                    df_list.append(record)

        if len(df_list) > 0:
            schema = StructType(
                [StructField("conf_id", IntegerType(), True), StructField("publish_count", IntegerType(), True),
                 StructField("file_id", IntegerType(), True), StructField("status", StringType(), True),
                 StructField("file_name", StringType(), True), StructField("s3_path", StringType(), True),
                 StructField("message", StringType(), True)])
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
                                                                                                  'message')
            newdf.createOrReplaceTempView('ResultSet_df')
            df = spark.sql("select * from ResultSet_df")
            conn_rds_pg, user = cdl_lib.get_postgres_conn_for_psycopg2(secret)
            try:
                with conn_rds_pg.cursor() as cur:
                    for row in df.rdd.collect():
                        logger.info(
                            "Records to update in cdl_ingestion log: status {0}, src_path:{1}, curated count: {2},parentLog: {3}, file_id: {4}, source_system:{5}".format(
                                row.status_new, row.s3_path, row.publish_count, new_parentlog, row.file_id,
                                source_system))
                        update_query = "update cdl_ingestion_log set published_status=%s, published_location=%s ,published_completion_timestamp=current_date ,published_record_count=%s,published_message=%s where batch_id=%s and file_id=%s and data_source=%s"
                        cur.execute(update_query, (
                        row.status_new, row.s3_path, row.publish_count, str(row.message[:250]), new_parentlog, row.file_id,
                        source_system))
                        logger.info("Updated detail in control table")
                    conn_rds_pg.commit()
                    cur.close()
            except Exception as e:
                logger.error("Exception - " + str(e))
            