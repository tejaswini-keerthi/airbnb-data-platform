"""
SparkSession factory.
Creates a local SparkSession for data transformations.
"""

import os
from pyspark.sql import SparkSession
from loguru import logger


def get_spark_session(app_name: str = "airbnb_pipeline") -> SparkSession:
    """
    Creates and returns a local SparkSession.
    S3 reads/writes are handled via boto3, not Spark.
    ANSI mode disabled so bad casts return NULL instead of crashing.
    """
    logger.info(f"Creating SparkSession: {app_name}")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created successfully")
    return spark