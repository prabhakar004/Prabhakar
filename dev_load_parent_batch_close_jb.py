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
import configparser
import logging

from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.context import SparkContext
from datetime import datetime
from pyspark.sql.functions import broadcast
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from cdl_common_lib_function import get_cdl_common_functions as cdl_lib

# ------------- HARDCODED for now -------------
# secret_id = "gmbcdl_new_connn_info"
# --------------- CREATE SPARK SESSION ----------------------

spark = cdl_lib.initiate_spark_session()

# ------------- CONFIGURATION FOR CUSTOM LOG CREATIONS ----------------
logger = cdl_lib.initiate_logger()

glue_client = cdl_lib.initiate_glue_client()
# sm_client = boto3.client("secretsmanager", region_name="us-east-2")


# ------------ RDS configuration TO USE WITH PSYCOPG2 INSERT / UPDATE operations ----------------


# zip_ref = zipfile.ZipFile('./psycopg2.zip', 'r')
# zip_ref.extractall('/tmp/packages')
# zip_ref.close()
# sys.path.insert(0, '/tmp/packages')
#
# import psycopg2


def send_notification_mail(wf_starttime, workflow_name, recpnts_list, sender_email_address):
    subject = "Glue workflow completed successfully"

    # Create the message content for the structure validation failure
    email_body = """<html><head></head><body>"""
    email_body += "<p>Hi Team,</p>"
    email_body += "<p>Please note that the Glue workflow completed sucessfully at '{} UTC' hours.</p>".format(
        wf_starttime)
    email_body += "<p>Glue Workflow Name : '{}'.</p>".format(workflow_name)
    email_body += "<p>Kindly contact GENMAB CDL ADMIN team if incase any clarifications required.</p>"
    email_body += "<p>Regards,</p>"
    email_body += "<p>GENMAB CDL Admin team </p>"
    email_body += "<p>***This is an auto-generated email, please do not reply to this email***</p>"
    email_body += "</body></html>"

    cdl_lib.send_email(email_body, subject, recpnts_list, sender_email_address)

if __name__ == "__main__":

    # ------------- @params: [TempDir, JOB_NAME] -------------

    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
    # glue_client = boto3.client("glue")
    # args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
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
    environment = workflow_params['environment']
    to_address_email = workflow_params['to_address_email']
    fm_address_email = workflow_params['fm_address_email']
    # crawler_db_name = workflow_params['crawler_db_name']
    secret_id = workflow_params['rds_secret_id']

    # Send a notification email to operations team DL, about job completed successfuly
    send_notification_mail(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), workflow_name, to_address_email,
                           fm_address_email)
    # ------to run locally without workflow, enable the following 9 lines-------

    # --------- RDS CREDENTIALS - TO BE CONFIGURED IN SECRET MANAGER --------------
    # Getting DB credentials from Secrets Manager
    secret = cdl_lib.get_secret_manager(secret_id)

    # new_parentlog = '20220608141425423'
    # source_system = 'MDM_TRINITY'
    # parent_batch_process_name = 'MDM_TRINITY_BATCH_PROCESS'
    # s3_vendor_bucket = 'gmb-dev-us'
    # s3_cdl_bucket = 'gmb-cdl-dev-us'
    # environment = 'dev'
    # to_address_email = 'yuvakumar.ranganathan@agilisium.com,rohithkumar.subramani@agilisium.com,Arun.Sabharish@agilisium.com'
    # fm_address_email = 'rohithkumar.subramani@agilisium.com'
    # crawler_db_name = 'cdl_curated_db'

    logger.info("The source_system is: {}".format(source_system))
    logger.info("The parent_batch_process_name value is: {}".format(parent_batch_process_name))
    logger.info("The s3_path value is: {}".format(s3_cdl_bucket))
    logger.info("The new_parentlog is: {}".format(new_parentlog))
    logger.info("The s3_vendor_bucket is: {}".format(s3_vendor_bucket))
    logger.info("The environment is: {}".format(environment))
    logger.info("The RDS SecretID is: {}".format(secret_id))

    conn_rds_pg, user = cdl_lib.get_postgres_conn_for_psycopg2(secret)
    # print('connected')
    # ADD THIS BELOW CODE TO HANDLE HISTORY LOAD
    if "_history" in source_system.lower():
        try:
            with conn_rds_pg.cursor() as cur:
                cur.execute("update cdl_ingestion_log set published_status='SUCCESS',snowflake_status='SUCCESS' where batch_id='%s'" % (new_parentlog))
                conn_rds_pg.commit()
                cur.close()
        except Exception as e:
            logger.error("Exception - " + str(e))
            
    try:
        with conn_rds_pg.cursor() as cur:
            cur.execute(
                "SELECT count(*) from parent_batch_process where source_system='%s' and process_status in ('In Progress','Failed') and parent_batch_id = '%s'" % (
                source_system, new_parentlog))
            running_process = cur.fetchone()
            cur.execute(
                f"select count(*) from cdl_ingestion_log where landing_status = 'SUCCESS' and curated_status = 'SUCCESS' and published_status = 'SUCCESS' and snowflake_status='SUCCESS' and lower(file_name) not like '%.trig' and batch_id = '{new_parentlog}'")
            cdl_upate_count = cur.fetchone()
            cur.execute(
                f"select count(*) from file_process_log where parent_batch_id = '{new_parentlog}' and lower(file_name) not like '%.trig' and (process_status = 'In Progress' or process_status = 'Completed' or process_status = 'Failed')")
            file_upate_count = cur.fetchone()
            cur.execute(
                f"select count(*) from cdl_ingestion_log where (landing_status = 'FAILED' or curated_status = 'FAILED' or published_status = 'FAILED' or snowflake_status='FAILED') and lower(file_name) not like '%.trig' and batch_id = '{new_parentlog}'")
            cdl_failed_upate_count = cur.fetchone()
            logger.info("--Total batch running--> " + str(running_process[0]))
            logger.info("--Total file update count--> " + str(file_upate_count[0]))
            logger.info("--Total cdl failed update count--> " + str(cdl_failed_upate_count[0]))
            logger.info("--Total cdl update count--> " + str(cdl_upate_count[0]))
            if running_process[0] > 0:
                if file_upate_count[0] > 0 and (cdl_upate_count[0] > 0 or cdl_failed_upate_count[0] > 0):
                    if file_upate_count[0] == cdl_upate_count[0]:
                        cur.execute(
                            "update parent_batch_process set completion_status = 'C',process_status = 'Completed',batch_end_date = current_date where parent_batch_id=%s",
                            [new_parentlog])
                        logger.info("Parent batch process updated for batch: " + str(new_parentlog))
                        cur.execute(
                            "update file_process_log set process_status = 'Completed' where parent_batch_id=%s and process_status = 'In Progress'",
                            [new_parentlog])
                        logger.info("File process log updated for batch: " + str(new_parentlog))
                    else:
                        if file_upate_count[0] > 0 and cdl_failed_upate_count[0] > 0:
                            cur.execute(
                                "update parent_batch_process set completion_status = 'F',process_status = 'Failed',batch_end_date = current_date where parent_batch_id=%s",
                                [new_parentlog])
                            logger.info("Parent batch process updated for batch: " + str(new_parentlog))
                            failed_cdl_df = """(select file_id from cdl_ingestion_log where (landing_status = 'FAILED' or curated_status = 'FAILED' or published_status = 'FAILED' or snowflake_status='FAILED') and batch_id = '{}')query_wrap""".format(
                                new_parentlog)
                            cdl_df = cdl_lib.read_from_db(secret=secret,tbl_query=failed_cdl_df)
                            logger.info("cdl failure: " + str(cdl_failed_upate_count[0]))
                            try:
                                with conn_rds_pg.cursor() as cur_del:
                                    cur_succ_exist =0
                                    for row in cdl_df.rdd.collect():
                                        logger.info(
                                            "Records to update in file_process_log: file_id:{0}".format(row.file_id))
                                        logger.info("Run 11")
                                        update_query = "update file_process_log set process_status = 'Failed' where parent_batch_id=%s and file_id=%s"
                                        cur_del.execute(update_query, (new_parentlog, row.file_id))
                                        logger.info("Run 12")
                                        conn_rds_pg.commit()
                                        logger.info(
                                            "Updated status in file process table which is failed in cdl ingestion log")
                                        if cdl_upate_count[0] > 0 and cdl_failed_upate_count[0] > 0:
                                            logger.info("updating existing status which is success in ")
                                            success_cdl_df = """(select file_id from cdl_ingestion_log where (landing_status = 'SUCCESS' and curated_status = 'SUCCESS' and published_status = 'SUCCESS' and snowflake_status='SUCCESS') and batch_id = '{}')query_wrap""".format(
                                                new_parentlog)
                                            succ_cdl_df = cdl_lib.read_from_db(secret=secret,tbl_query=success_cdl_df)
                                            if succ_cdl_df.count() > 0:
                                                with conn_rds_pg.cursor() as cur_succ:
                                                    for row in succ_cdl_df.rdd.collect():
                                                        logger.info(
                                                            "Records to update in file_process_log: file_id:{0}".format(
                                                                str(row.file_id)))
                                                        logger.info("Run 1")
                                                        update_query = "update file_process_log set process_status = 'Completed' where parent_batch_id=%s and file_id=%s"
                                                        cur_succ.execute(update_query, (new_parentlog, row.file_id))
                                                        logger.info("Run 2")
                                                        logger.info(
                                                            "Updated status in file process table which is success in cdl ingestion log")
                                                        cur_succ_exist =1
                                                        
                                    conn_rds_pg.commit()
                                    cur_del.close()
                                    if cur_succ_exist:
                                        cur_succ.close()
                                    logger.info("Updated detail in file process table")
                            except Exception as e:
                                logger.error("Exception - " + str(e))
                            conn_rds_pg.commit()
                            cur.close()
                            #sys.exit(0)
                else:
                    if file_upate_count[0] == 0 and cdl_upate_count[0] == 0 and cdl_failed_upate_count[0] == 0:
                        logger.info(
                            "No entry available in file process log and cdl_ingestion_log for the batch: " + str(
                                new_parentlog))
                        cur.execute(
                            "update parent_batch_process set completion_status = 'C',process_status = 'Completed',batch_end_date = current_date where parent_batch_id=%s",
                            [new_parentlog])
                        logger.info("Parent batch process updated for batch")
                        conn_rds_pg.commit()
                        cur.close()
                    else:
                        conn_rds_pg.commit()
                        cur.close()
                        #sys.exit(0)
            else:
                logger.info("No Parent batch available to complete the status in parent_batch_process")
            conn_rds_pg.commit()
            cur.close()
    except Exception as e:
        logger.error(e)
        raise e