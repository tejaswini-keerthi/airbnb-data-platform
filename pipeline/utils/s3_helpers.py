"""
S3 helper utilities for reading and writing data.
"""

import os
import boto3
from loguru import logger


def get_s3_client():
    """Creates and returns a boto3 S3 client."""
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def get_s3_paths(
    bucket: str,
    prefix: str,
    city: str,
    snapshot_date: str,
    table: str,
) -> str:
    """Constructs an S3 path with Hive-style partitioning."""
    return (
        f"s3a://{bucket}/{prefix}/"
        f"city={city}/snapshot_date={snapshot_date}/{table}"
    )


def list_s3_objects(bucket: str, prefix: str) -> list[str]:
    """Lists all objects in an S3 bucket under a given prefix."""
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj["Key"] for obj in response.get("Contents", [])]