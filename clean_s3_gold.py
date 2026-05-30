import boto3
import os
from dotenv import load_dotenv
load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name='us-east-1'
)

bucket = 'airbnb-gold-tejaswini'

# Delete all existing Gold files
response = s3.list_objects_v2(Bucket=bucket, Prefix='star_schema/')
if 'Contents' in response:
    for obj in response['Contents']:
        print(f"Deleting: {obj['Key']}")
        s3.delete_object(Bucket=bucket, Key=obj['Key'])

print("S3 Gold cleaned. Re-run the Silver → Gold job now.")