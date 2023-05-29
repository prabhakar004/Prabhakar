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

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from datetime import datetime

from cdl_common_lib_function import get_cdl_common_functions as cdl_lib

# ------------- CONFIGURATION FOR CUSTOM LOG CREATIONS ----------------
logger = cdl_lib.initiate_logger()



# -------------- CREATE GLUE CLIENT ----------------
glue_client = cdl_lib.initiate_glue_client()


def send_notification_mail(wf_starttime, workflow_name, recpnts_list, sender_email_address):
    subject = "Glue workflow started successfully"

    # Create the message content for the structure validation failure
    email_body = """<html><head></head><body>"""
    email_body += "<p>Hi Team,</p>"
    email_body += "<p>Please note that the Glue workflow have started successfully at '{} UTC' hours.</p>".format(wf_starttime)
    email_body += "<p>Glue Workflow Name : '{}'.</p>".format(workflow_name)
    email_body += "<p>Kindly contact GENMAB CDL ADMIN team if incase any clarifications required.</p>"
    email_body += "<p>Regards,</p>"
    email_body += "<p>GENMAB CDL Admin team </p>"
    email_body += "<p>***This is an auto-generated email, please do not reply to this email***</p>"
    email_body += "</body></html>"

    cdl_lib.send_email(email_body, subject, recpnts_list, sender_email_address)

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
    workflow_name = args['WORKFLOW_NAME']
    workflow_run_id = args['WORKFLOW_RUN_ID']
    workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name,
    RunId=workflow_run_id)["RunProperties"]
    logger.info("--------Workflow details------> " + workflow_name +','+workflow_run_id)

    source_system = workflow_params['source_system']
    parent_batch_process_name = workflow_params['parent_batch_process_name']
    s3_vendor_bucket = workflow_params['s3_vendor_bucket']
    s3_cdl_bucket = workflow_params['s3_cdl_bucket']
    s3_cdl_curated_bucket = workflow_params['s3_cdl_curated_bucket']
    s3_cdl_publish_bucket = workflow_params['s3_cdl_publish_bucket']
    environment = workflow_params['environment']
    to_address_email = workflow_params['to_address_email']
    fm_address_email = workflow_params['fm_address_email']
    crawler_db_name = workflow_params['curated_db']
    curated_crawler_name = workflow_params['curated_crawler']
    secret_id = workflow_params['rds_secret_id']

    # Send a notification email to operations team DL, about job started successfuly
    send_notification_mail(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), workflow_name, to_address_email, fm_address_email)

    # -------------- Getting Secret Manager details -----------
    secret = cdl_lib.get_secret_manager(secret_id)
    
    logger.info("The source_system is: {}".format(source_system))
    logger.info("The parent_batch_process_name value is: {}".format(parent_batch_process_name))
    logger.info("The s3_path value is: {}".format(s3_cdl_bucket))
    logger.info("The curated s3_path value is: {}".format(s3_cdl_curated_bucket))
    logger.info("The publish s3_path value is: {}".format(s3_cdl_publish_bucket))
    logger.info("The s3_vendor_bucket is: {}".format(s3_vendor_bucket))
    logger.info("The environment is: {}".format(environment))
    
    conn_rds_pg, user = cdl_lib.get_postgres_conn_for_psycopg2(secret)
    try:
        with conn_rds_pg.cursor() as cur:
            cur.execute("SELECT parent_batch_id from parent_batch_process where source_system='%s' and process_status ='Failed'" %(source_system))
            running_process = cur.fetchone()
            cur.execute("SELECT count(*) from parent_batch_process where source_system='%s' and process_status ='Failed'" %(source_system))
            total_count = cur.fetchone()
            cdl_inestion_flag = 'Y'
            if total_count[0] > 0:
                previous_batch_id = running_process[0]
                logger.info("Previous batch id: "+str(previous_batch_id))
                cur.execute("update parent_batch_process set process_status = 'In Progress',completion_status='I' where source_system='%s' and process_status ='Failed' and parent_batch_id =('%s')" %(source_system,previous_batch_id))
                logger.info("Parent batch updated again to in progress for the failed batch")
                with conn_rds_pg.cursor() as cur_cdl_ingestion:
                    cur_cdl_ingestion.execute("update file_process_log set process_status='In Progress' where source_system='%s' and parent_batch_id='%s' and process_status='Failed'" %(source_system,previous_batch_id))
                    logger.info("file process log updated")
                    cur_cdl_ingestion.execute("update cdl_ingestion_log set landing_status = null where data_source='%s' and batch_id='%s' and landing_status='FAILED'" %(source_system,previous_batch_id))
                    cur_cdl_ingestion.execute("update cdl_ingestion_log set curated_status = null where data_source='%s' and batch_id='%s' and curated_status='FAILED'" %(source_system,previous_batch_id))
                    cur_cdl_ingestion.execute("update cdl_ingestion_log set published_status = null where data_source='%s' and batch_id='%s' and published_status='FAILED'" %(source_system,previous_batch_id))
                    logger.info("cdl ingestion log updated")
                    new_parentlog = previous_batch_id
                    cur_cdl_ingestion.close()
            else:
                logger.info("No records available in failed state")
                cur.execute("SELECT count(*) from parent_batch_process where source_system='%s' and process_status ='In Progress'" % (source_system))
                running_process = cur.fetchone()
                if running_process[0] == 0:
                    source_system = source_system
                    new_parentlog = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3] 
                    pb_start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    initial_process_status = 'In Progress'
                    nullVar = ''
                    cur.execute("SELECT max(parent_batch_id) from parent_batch_process",(source_system))
                    running_process = cur.fetchone()
                    cur.execute("INSERT INTO parent_batch_process VALUES(%s,%s,%s,%s,'I',%s,'','',%s,%s,%s,%s,%s)",[new_parentlog,parent_batch_process_name,pb_start_time,pb_start_time,initial_process_status,source_system,pb_start_time,user,pb_start_time,user])
                else:
                    logger.info("There is already a process still running, Please complete the in_progress batch and initiate again")
                    sys.exit(0)
            conn_rds_pg.commit()
            cur.close()
    except Exception as e:
        logger.error(e)
        raise e

    workflow_params['new_parentlog'] = new_parentlog
    glue_client.put_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id, RunProperties=workflow_params)