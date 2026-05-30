"""
Bronze → Silver transformation job.
Reads raw CSVs from local temp directory (already downloaded),
cleans and types them, writes clean Parquet to S3 Silver via boto3.
"""

import os
import sys
import tempfile
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pipeline.utils.spark_session import get_spark_session
from pipeline.bronze_to_silver.transformations import (
    clean_listings,
    clean_calendar,
    clean_reviews,
)
from pipeline.quality.metrics_collector import collect_metrics
from pipeline.quality.schema_drift_detector import detect_schema_drift

load_dotenv()

LOCAL_TEMP_DIR = Path("/tmp/airbnb_downloads")


def upload_parquet_to_s3(local_path: Path, bucket: str, s3_key: str) -> None:
    """Uploads a parquet directory to S3."""
    import boto3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    for file in local_path.rglob("*.parquet"):
        key = f"{s3_key}/{file.name}"
        logger.info(f"Uploading {file} to s3://{bucket}/{key}")
        s3.upload_file(str(file), bucket, key)


def run(city: str, snapshot_date: str) -> None:
    """
    Runs Bronze → Silver transformation for a single city + snapshot date.
    Reads locally downloaded CSVs, cleans them, writes Parquet to S3 Silver.
    """
    spark = get_spark_session("bronze_to_silver")
    silver_bucket = os.getenv("S3_SILVER_BUCKET")

    logger.info(f"Starting Bronze → Silver for {city}/{snapshot_date}")

    for table in ["listings", "calendar", "reviews"]:
        local_csv = LOCAL_TEMP_DIR / city / snapshot_date / f"{table}.csv"

        if not local_csv.exists():
            logger.warning(f"Local CSV not found: {local_csv} — skipping")
            continue

        logger.info(f"Reading {table} from local: {local_csv}")
        df_raw = (
            spark.read
            .option("header", "true")
            .option("multiLine", "true")
            .option("escape", '"')
            .option("quote", '"')
            .csv(str(local_csv))
)

        # detect schema drift
        detect_schema_drift(city, snapshot_date, table, df_raw)

        # apply cleaning
        if table == "listings":
            df_clean = clean_listings(df_raw)
        elif table == "calendar":
            df_clean = clean_calendar(df_raw)
        else:
            df_clean = clean_reviews(df_raw)

        # collect quality metrics
        collect_metrics(city, snapshot_date, table, df_raw, df_clean)

        # write parquet locally first
        local_parquet = LOCAL_TEMP_DIR / city / snapshot_date / f"{table}_silver"
        logger.info(f"Writing {table} parquet locally: {local_parquet}")
        df_clean.write.mode("overwrite").parquet(str(local_parquet))

        # upload to S3 Silver
        s3_key = f"cleaned/city={city}/snapshot_date={snapshot_date}/{table}"
        upload_parquet_to_s3(local_parquet, silver_bucket, s3_key)

        raw_count = df_raw.count()
        clean_count = df_clean.count()
        logger.info(
            f"Completed {table}: {raw_count} raw → {clean_count} clean rows"
        )

    spark.stop()
    logger.info(f"Bronze → Silver complete for {city}/{snapshot_date}")


if __name__ == "__main__":
    city = sys.argv[1] if len(sys.argv) > 1 else "new_york"
    snapshot_date = sys.argv[2] if len(sys.argv) > 2 else "2026-02-13"
    run(city, snapshot_date)