import boto3
import configparser
import ast
import json

def read_ini_file_from_s3(aws_access_key_id, aws_secret_access_key, s3_bucket, s3_file_key):
    try:
        # Create an S3 client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id="AKIA6KN4O56KGBLJHG5V",
            aws_secret_access_key="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"
        )

        # Read the INI file from S3
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_file_key)
        config_string = response["Body"].read().decode("utf-8")

        # Parse the INI file using configparser
        config = configparser.ConfigParser()
        config.read_string(config_string)

        json_data = {section: dict(config[section]) for section in config.sections()}
        return json_data

    except Exception as e:
        print(f"Failed to read the INI file from S3: {str(e)}")
        return None

# Replace with your AWS credentials and S3 bucket and file details
aws_access_key_id = "YOUR_ACCESS_KEY_ID"
aws_secret_access_key = "YOUR_SECRET_ACCESS_KEY"
s3_bucket = "sfdbmigration"
s3_file_key = "con_param.ini"

ini_data = read_ini_file_from_s3(aws_access_key_id, aws_secret_access_key, s3_bucket, s3_file_key)

if ini_data:
    print("INI Data:", ini_data)
