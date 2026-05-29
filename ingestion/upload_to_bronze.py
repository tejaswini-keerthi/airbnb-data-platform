"""
Uploads downloaded Airbnb CSV files to S3 Bronze layer.
Follows the partitioning pattern: city={name}/snapshot_date={date}/
"""

import os
import boto3
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv

from config import CITIES, LOCAL_TEMP_DIR, S3_BRONZE_PREFIX
from snapshot_registry import SnapshotRegistry

load_dotenv()


def get_s3_client():
    """Creates and returns a boto3 S3 client."""
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def upload_file_to_s3(
    s3_client,
    local_path: Path,
    bucket: str,
    s3_key: str,
) -> bool:
    """
    Uploads a single file to S3.
    Returns True if successful, False otherwise.
    """
    try:
        logger.info(f"Uploading {local_path} to s3://{bucket}/{s3_key}")
        s3_client.upload_file(str(local_path), bucket, s3_key)
        logger.info(f"Upload complete: s3://{bucket}/{s3_key}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload {local_path}: {e}")
        return False


def upload_city_snapshot(
    s3_client,
    city_name: str,
    snapshot_date: str,
    bucket: str,
    temp_dir: Path,
) -> bool:
    """
    Uploads all CSV files for a city + snapshot date to S3 Bronze.
    S3 path: raw/city={city_name}/snapshot_date={date}/{file}.csv
    """
    local_dir = temp_dir / city_name / snapshot_date

    if not local_dir.exists():
        logger.warning(f"Local directory not found: {local_dir}")
        return False

    csv_files = list(local_dir.glob("*.csv"))

    if not csv_files:
        logger.warning(f"No CSV files found in {local_dir}")
        return False

    all_success = True

    for csv_file in csv_files:
        # construct S3 key with Hive-style partitioning
        s3_key = (
            f"{S3_BRONZE_PREFIX}/"
            f"city={city_name}/"
            f"snapshot_date={snapshot_date}/"
            f"{csv_file.name}"
        )

        success = upload_file_to_s3(s3_client, csv_file, bucket, s3_key)

        if not success:
            all_success = False

    return all_success


def run_upload() -> None:
    """
    Main upload function.
    Uploads all downloaded-but-not-yet-uploaded snapshots to S3 Bronze.
    """
    registry = SnapshotRegistry()
    s3_client = get_s3_client()
    bucket = os.getenv("S3_BRONZE_BUCKET")
    temp_dir = Path(LOCAL_TEMP_DIR)

    if not bucket:
        raise ValueError("S3_BRONZE_BUCKET not set in environment")

    logger.info(f"Starting upload to s3://{bucket}")

    for city in CITIES:
        # get all snapshots that are downloaded but not yet uploaded
        pending = registry.get_pending_upload(city.name)

        if not pending:
            logger.info(f"No pending uploads for {city.name}")
            continue

        for snapshot_date in pending:
            logger.info(f"Uploading {city.name}/{snapshot_date}")

            success = upload_city_snapshot(
                s3_client=s3_client,
                city_name=city.name,
                snapshot_date=snapshot_date,
                bucket=bucket,
                temp_dir=temp_dir,
            )

            if success:
                registry.mark_uploaded(city.name, snapshot_date)
                logger.info(
                    f"Successfully uploaded {city.name}/{snapshot_date} to Bronze"
                )
            else:
                logger.error(
                    f"Upload failed for {city.name}/{snapshot_date}"
                )

    logger.info("Upload run complete")


if __name__ == "__main__":
    run_upload()