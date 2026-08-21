import boto3
from dotenv import load_dotenv
import os
from botocore.exceptions import ClientError

load_dotenv()
#create an s3 client
s3_client = boto3.client(
        's3',
        region_name='eu-north-1',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
    )
