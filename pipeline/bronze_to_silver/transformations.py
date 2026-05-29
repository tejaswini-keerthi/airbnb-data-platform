"""
Cleaning transformations for each table.
Bronze (raw, messy) → Silver (clean, typed, trustworthy).
ANSI mode disabled in SparkSession so bad casts return NULL instead of crashing.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    BooleanType,
)
from loguru import logger


def clean_listings(df: DataFrame) -> DataFrame:
    logger.info("Cleaning listings table")

    df = (
        df
        .withColumn(
            "price",
            F.regexp_replace(F.col("price"), r"[\$,]", "").cast(DoubleType())
        )
        .withColumn("accommodates", F.col("accommodates").cast(IntegerType()))
        .withColumn("bedrooms", F.col("bedrooms").cast(IntegerType()))
        .withColumn("bathrooms", F.col("bathrooms").cast(DoubleType()))
        .withColumn("beds", F.col("beds").cast(IntegerType()))
        .withColumn("minimum_nights", F.col("minimum_nights").cast(IntegerType()))
        .withColumn("maximum_nights", F.col("maximum_nights").cast(IntegerType()))
        .withColumn("number_of_reviews", F.col("number_of_reviews").cast(IntegerType()))
        .withColumn("review_scores_rating", F.col("review_scores_rating").cast(DoubleType()))
        .withColumn("review_scores_cleanliness", F.col("review_scores_cleanliness").cast(DoubleType()))
        .withColumn("review_scores_location", F.col("review_scores_location").cast(DoubleType()))
        .withColumn(
            "instant_bookable",
            F.when(F.col("instant_bookable") == "t", True)
            .when(F.col("instant_bookable") == "f", False)
            .otherwise(None)
            .cast(BooleanType())
        )
        .withColumn(
            "has_availability",
            F.when(F.col("has_availability") == "t", True)
            .when(F.col("has_availability") == "f", False)
            .otherwise(None)
            .cast(BooleanType())
        )
        .withColumn("last_scraped", F.to_date(F.col("last_scraped")))
        .withColumn("host_since", F.to_date(F.col("host_since")))
        .withColumn("first_review", F.to_date(F.col("first_review")))
        .withColumn("last_review", F.to_date(F.col("last_review")))
        .withColumn("neighbourhood_cleansed", F.initcap(F.trim(F.col("neighbourhood_cleansed"))))
        .withColumn("room_type", F.trim(F.col("room_type")))
        .withColumn("property_type", F.trim(F.col("property_type")))
        .withColumn(
            "host_name",
            F.when(F.trim(F.col("host_name")) == "", None)
            .otherwise(F.col("host_name"))
        )
        .filter(F.col("id").isNotNull())
        .filter(
            F.col("price").isNull() |
            ((F.col("price") >= 0) & (F.col("price") <= 10000))
        )
        .dropDuplicates(["id"])
        .withColumn("_ingested_at", F.current_timestamp())
    )

    return df


def clean_calendar(df: DataFrame) -> DataFrame:
    logger.info("Cleaning calendar table")

    df = (
        df
        .withColumn(
            "price",
            F.regexp_replace(F.col("price"), r"[\$,]", "").cast(DoubleType())
        )
        .withColumn(
            "adjusted_price",
            F.regexp_replace(F.col("adjusted_price"), r"[\$,]", "").cast(DoubleType())
        )
        .withColumn("date", F.to_date(F.col("date")))
        .withColumn(
            "available",
            F.when(F.col("available") == "t", True)
            .when(F.col("available") == "f", False)
            .otherwise(None)
            .cast(BooleanType())
        )
        .withColumn("minimum_nights", F.col("minimum_nights").cast(IntegerType()))
        .withColumn("maximum_nights", F.col("maximum_nights").cast(IntegerType()))
        .withColumn("listing_id", F.col("listing_id").cast(IntegerType()))
        .filter(F.col("listing_id").isNotNull())
        .filter(F.col("date").isNotNull())
        .dropDuplicates(["listing_id", "date"])
        .withColumn("_ingested_at", F.current_timestamp())
    )

    return df


def clean_reviews(df: DataFrame) -> DataFrame:
    logger.info("Cleaning reviews table")

    df = (
        df
        .withColumn("listing_id", F.col("listing_id").cast(IntegerType()))
        .withColumn("id", F.col("id").cast(IntegerType()))
        .withColumn("reviewer_id", F.col("reviewer_id").cast(IntegerType()))
        .withColumn("date", F.to_date(F.col("date")))
        .withColumn(
            "comments",
            F.when(F.trim(F.col("comments")) == "", None)
            .otherwise(F.col("comments"))
        )
        .withColumn("reviewer_name", F.trim(F.col("reviewer_name")))
        .filter(F.col("id").isNotNull())
        .filter(F.col("listing_id").isNotNull())
        .dropDuplicates(["id"])
        .withColumn("_ingested_at", F.current_timestamp())
    )

    return df