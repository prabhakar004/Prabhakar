import sys
import boto3
import json
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
glue_client = boto3.client("glue")

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

sm_client = boto3.client("secretsmanager", region_name="us-east-2")

get_secret_value_response = sm_client.get_secret_value(SecretId="gmb_cdl_snowflake")
secret_sf = get_secret_value_response['SecretString']
secret_sf = json.loads(secret_sf)

myThreads=5
db_creds={}
sf_db_creds={}
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
    
sf_db_creds = get_snowflake_conn_details()

publish_path = "s3://gmb-cdl-prod-publish-us/commercial/{source_path}{table_name}/data_src_nm={data_src_nm}"

publish_db = "cdl_publish_db"

environment = "PHCDL"
database = "PHCDL_PUB"

table_list = {'kmdh1':{"path":"/common/fact/","data_src_nm":"KMDH-PULSE-ALERT1-WKLY","table_list":["PUB_PLS_FCT"]},"common_kmdh1":{"path":"/common/dimension/","data_src_nm":"KMDH-PULSE-ALERT1-WKLY","table_list":["PUB_HCP_DIM"]},"common_kmdh2":{"path":"/common/dimension/","data_src_nm":"KMDH-PULSE-ALERT1-WKLY","table_list":["PUB_HCP_ADDR_DIM"]},'kmdh2':{"path":"/common/fact/","data_src_nm":"KMDH-PULSE-ALERT2-WKLY","table_list":["PUB_PLS_FCT"]},"common_kmdh3":{"path":"/common/dimension/","data_src_nm":"KMDH-PULSE-ALERT2-WKLY","table_list":["PUB_HCP_DIM"]},"common_kmdh4":{"path":"/common/dimension/","data_src_nm":"KMDH-PULSE-ALERT2-WKLY","table_list":["PUB_HCP_ADDR_DIM"]}}

for i,j in table_list.items():
    source = i
    source_path = j["path"]
    data_src_nm = j["data_src_nm"]
    table_list = j["table_list"]
    for table_name in table_list:
        select_query = "select * from"+" "+environment+"."+database+"."+table_name+" where data_src_nm='{data_src_nm}'".format(data_src_nm=data_src_nm)
        print("select query :",select_query)
        snowflake_df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sf_db_creds).option("query", select_query).load()
        print("snowflake result for :",select_query,snowflake_df.printSchema())
        snowflake_df = snowflake_df
        #snowflake_df.show(1)
        snowflake_df = snowflake_df.drop("data_src_nm","DATA_SRC_NM")
        print("snowflake_df_columns for table :",table_name,"columns list :",snowflake_df.columns)
        s3_location = publish_path.format(source_path=source_path,table_name=table_name.lower(),data_src_nm=data_src_nm)
        s3_full_path = "{s3_pub_path}".format(s3_pub_path=s3_location)
        print("s3_full_path",s3_full_path)                
        snowflake_df.write.mode('append').parquet(s3_full_path)
        print("file write succesfully to s3 path",s3_full_path)
        
#ADDED COMMON TABLE FOR REFERENCE
#PUB_HCP_DIM -prognos,apld,kmbhalert1,kmbhalert2,kmbhalert3
#PUB_HCP_ATTR_DIM -prognos,apld
#PUB_PROD_DIM -nonsourceretail,apld,ics
#PUB_HCO_LCNSE_DIM - ics,nonsourceretail
#PUB_HCO_DIM - ics,nonsourceretail
#PUB_HCO_ADDR_DIM - ics,nonsourceretail    

# table_list = {'apld':{"path":"/symphony/apld/","data_src_nm":"SHS-APLD-WKLY","table_list":["PUB_DRG_PROD_BRDG_DIM","PUB_PTNT_DIM","PUB_RX_CLM_FCT","PUB_HCP_ADDR_DIM","PUB_PX_CLM_FCT","PUB_PROC_DIM","PUB_HCP_LCNSE_DIM","PUB_SX_CLM_FCT","PUB_DIAG_DIM","PUB_PTNT_ACTVTY_FCT","PUB_PTNT_MDCR_PRT_D_FCT","PUB_PLAN_DIM","PUB_HCP_ROLE_DIM","PUB_SRGCL_DIM","PUB_DX_CLM_FCT"]},"common_apld1":{"path":"/common/dimension/","data_src_nm":"SHS-APLD-WKLY","table_list":["PUB_HCP_DIM"]},"common_apld2":{"path":"/common/dimension/","data_src_nm":"SHS-APLD-WKLY","table_list":["PUB_HCP_ATTR_DIM"]},"common_apld3":{"path":"/common/dimension/","data_src_nm":"SHS-APLD-WKLY","table_list":["PUB_PROD_DIM"]}}
