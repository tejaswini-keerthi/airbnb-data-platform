"""
Silver → Gold transformation job.
Reads clean Parquet from S3 Silver, builds star schema,
writes Gold Parquet to S3.
"""

import os
import sys
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pipeline.utils.spark_session import get_spark_session
from pipeline.silver_to_gold.star_schema import (
    build_dim_date,
    build_dim_host,
    build_dim_location,
    build_dim_property,
    build_fact_listings,
)

load_dotenv()

LOCAL_TEMP_DIR = Path("/tmp/airbnb_downloads")

# city metadata for location dimension
CITY_METADATA = {
    "new_york":  {"city": "New York",  "country": "USA"},
    "london":    {"city": "London",    "country": "UK"},
    "paris":     {"city": "Paris",     "country": "France"},
    "amsterdam": {"city": "Amsterdam", "country": "Netherlands"},
    "sydney":    {"city": "Sydney",    "country": "Australia"},
}


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
        logger.info(f"Uploading to s3://{bucket}/{key}")
        s3.upload_file(str(file), bucket, key)


def run(city: str, snapshot_date: str) -> None:
    """
    Runs Silver → Gold transformation for a single city + snapshot date.
    """
    spark = get_spark_session("silver_to_gold")
    gold_bucket = os.getenv("S3_GOLD_BUCKET")
    metadata = CITY_METADATA.get(city, {"city": city, "country": "unknown"})

    logger.info(f"Starting Silver → Gold for {city}/{snapshot_date}")

    # read Silver listings parquet
    silver_listings_path = (
        LOCAL_TEMP_DIR / city / snapshot_date / "listings_silver"
    )

    if not silver_listings_path.exists():
        logger.error(f"Silver listings not found: {silver_listings_path}")
        return

    logger.info(f"Reading Silver listings from: {silver_listings_path}")
    df_listings = spark.read.parquet(str(silver_listings_path))

    logger.info(f"Loaded {df_listings.count()} listings from Silver")

    # build dimension tables
    dim_date = build_dim_date(spark, df_listings)
    dim_host = build_dim_host(df_listings)
    dim_location = build_dim_location(
        df_listings,
        city=metadata["city"],
        country=metadata["country"]
    )
    dim_property = build_dim_property(df_listings, snapshot_date)

    # build fact table
    fact_listings = build_fact_listings(
        df_listings,
        dim_host,
        dim_location,
        dim_property,
        snapshot_date,
        metadata["city"]
    )

    # write each table locally then upload to S3 Gold
    tables = {
        "dim_date": dim_date,
        "dim_host": dim_host,
        "dim_location": dim_location,
        "dim_property": dim_property,
        "fact_listings": fact_listings,
    }

    for table_name, df in tables.items():
        local_path = LOCAL_TEMP_DIR / city / snapshot_date / f"{table_name}_gold"
        logger.info(f"Writing {table_name} locally: {local_path}")
        df.write.mode("overwrite").parquet(str(local_path))

        s3_key = (
            f"star_schema/table={table_name}/"
            f"city={city}/snapshot_date={snapshot_date}"
        )
        upload_parquet_to_s3(local_path, gold_bucket, s3_key)
        logger.info(f"Uploaded {table_name} to S3 Gold")

    spark.stop()
    logger.info(f"Silver → Gold complete for {city}/{snapshot_date}")


if __name__ == "__main__":
    city = sys.argv[1] if len(sys.argv) > 1 else "new_york"
    snapshot_date = sys.argv[2] if len(sys.argv) > 2 else "2026-02-13"
    run(city, snapshot_date)