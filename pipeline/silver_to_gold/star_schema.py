"""
Star schema builder.
Transforms Silver (clean, flat) → Gold (star schema).

Tables produced:
    dim_date        — calendar dimension
    dim_host        — host dimension
    dim_location    — location hierarchy (country → city → neighbourhood)
    dim_property    — property dimension (SCD Type 2 managed by dbt snapshots)
    fact_listings   — fact table (one row per listing per snapshot)

Note: all date columns stored as VARCHAR strings (yyyy-MM-dd) to avoid
Parquet timestamp encoding issues when loading into Snowflake.
Out-of-range dates are nulled out to prevent long overflow errors.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from loguru import logger


def build_dim_date(spark: SparkSession, df_listings: DataFrame) -> DataFrame:
    """
    Builds dim_date from distinct dates in listings.
    One row per date with calendar attributes.
    Date stored as string to avoid Parquet timestamp issues.
    """
    logger.info("Building dim_date")

    df = (
        df_listings
        .select(F.col("last_scraped").alias("date"))
        .filter(F.col("date").isNotNull())
        .distinct()
        .withColumn("date_key", F.date_format(F.col("date"), "yyyyMMdd").cast(IntegerType()))
        .withColumn("year", F.year(F.col("date")))
        .withColumn("month", F.month(F.col("date")))
        .withColumn("quarter", F.quarter(F.col("date")))
        .withColumn("day_of_week", F.dayofweek(F.col("date")))
        .withColumn("day_of_month", F.dayofmonth(F.col("date")))
        .withColumn("week_of_year", F.weekofyear(F.col("date")))
        .withColumn("is_weekend",
            F.when(F.dayofweek(F.col("date")).isin([1, 7]), True)
            .otherwise(False)
        )
        .withColumn("month_name", F.date_format(F.col("date"), "MMMM"))
        .withColumn("quarter_name",
            F.concat(F.lit("Q"), F.quarter(F.col("date")).cast("string"))
        )
        # convert to string to avoid Parquet timestamp encoding issues
        .withColumn("date", F.col("date").cast("string"))
    )

    logger.info(f"dim_date: {df.count()} rows")
    return df


def build_dim_host(df_listings: DataFrame) -> DataFrame:
    """
    Builds dim_host from listings.
    One row per unique host.
    host_since stored as string with out-of-range dates nulled out.
    """
    logger.info("Building dim_host")

    df = (
        df_listings
        .select(
            F.col("host_id"),
            F.col("host_name"),
            F.col("host_since"),
            F.col("host_location"),
            F.col("host_response_time"),
            F.col("host_response_rate"),
            F.col("host_acceptance_rate"),
            F.col("host_is_superhost"),
            F.col("host_total_listings_count"),
            F.col("host_has_profile_pic"),
            F.col("host_identity_verified"),
        )
        .filter(F.col("host_id").isNotNull())
        .dropDuplicates(["host_id"])
        .withColumn("host_key",
            F.md5(F.col("host_id").cast("string"))
        )
        .withColumn(
            "is_superhost",
            F.when(F.col("host_is_superhost") == "t", True)
            .when(F.col("host_is_superhost") == "f", False)
            .otherwise(None)
        )
        .withColumn("host_id", F.col("host_id").cast(IntegerType()))
        .withColumn(
            "host_response_rate_pct",
            F.regexp_replace(F.col("host_response_rate"), "%", "")
            .cast(DoubleType())
        )
        .withColumn(
            "host_acceptance_rate_pct",
            F.regexp_replace(F.col("host_acceptance_rate"), "%", "")
            .cast(DoubleType())
        )
        # null out out-of-range dates before casting to string
        .withColumn("host_since",
            F.when(
                (F.col("host_since") >= F.lit("1900-01-01").cast("date")) &
                (F.col("host_since") <= F.lit("2099-12-31").cast("date")),
                F.col("host_since").cast("string")
            ).otherwise(None)
        )
    )

    logger.info(f"dim_host: {df.count()} rows")
    return df


def build_dim_location(df_listings: DataFrame, city: str, country: str) -> DataFrame:
    """
    Builds dim_location with three-level hierarchy.
    country → city → neighbourhood
    """
    logger.info("Building dim_location")

    df = (
        df_listings
        .select(
            F.col("neighbourhood_cleansed").alias("neighbourhood"),
            F.col("latitude"),
            F.col("longitude"),
        )
        .filter(F.col("neighbourhood").isNotNull())
        .dropDuplicates(["neighbourhood"])
        .withColumn("city", F.lit(city))
        .withColumn("country", F.lit(country))
        .withColumn("location_key",
            F.md5(F.concat_ws("|",
                F.col("country"),
                F.col("city"),
                F.col("neighbourhood")
            ))
        )
        .withColumn("latitude", F.col("latitude").cast(DoubleType()))
        .withColumn("longitude", F.col("longitude").cast(DoubleType()))
    )

    logger.info(f"dim_location: {df.count()} rows")
    return df


def build_dim_property(df_listings: DataFrame, snapshot_date: str) -> DataFrame:
    """
    Builds dim_property from listings.
    SCD Type 2 history is managed by dbt snapshots downstream.
    This produces the current snapshot's property attributes.
    """
    logger.info("Building dim_property")

    df = (
        df_listings
        .select(
            F.col("id").alias("listing_id"),
            F.col("name").alias("listing_name"),
            F.col("property_type"),
            F.col("room_type"),
            F.col("accommodates"),
            F.col("bedrooms"),
            F.col("bathrooms"),
            F.col("beds"),
            F.col("amenities"),
            F.col("price"),
            F.col("minimum_nights"),
            F.col("maximum_nights"),
            F.col("instant_bookable"),
        )
        .filter(F.col("listing_id").isNotNull())
        .dropDuplicates(["listing_id"])
        .withColumn("listing_id", F.col("listing_id").cast(IntegerType()))
        .withColumn("snapshot_date", F.lit(snapshot_date))
        .withColumn("property_key",
            F.md5(F.concat_ws("|",
                F.col("listing_id").cast("string"),
                F.col("snapshot_date")
            ))
        )
        .withColumn("price_tier",
            F.when(F.col("price") < 100, "budget")
            .when(F.col("price") < 200, "mid-range")
            .when(F.col("price") < 400, "premium")
            .when(F.col("price").isNotNull(), "luxury")
            .otherwise("unknown")
        )
    )

    logger.info(f"dim_property: {df.count()} rows")
    return df


def build_fact_listings(
    df_listings: DataFrame,
    df_dim_host: DataFrame,
    df_dim_location: DataFrame,
    df_dim_property: DataFrame,
    snapshot_date: str,
    city: str,
) -> DataFrame:
    """
    Builds fact_listings — one row per listing per snapshot.
    Joins to dimension tables to get surrogate keys.
    """
    logger.info("Building fact_listings")

    df = (
        df_listings
        .select(
            F.col("id").cast(IntegerType()).alias("listing_id"),
            F.col("host_id").cast(IntegerType()),
            F.col("neighbourhood_cleansed").alias("neighbourhood"),
            F.col("price"),
            F.col("review_scores_rating"),
            F.col("review_scores_cleanliness"),
            F.col("review_scores_location"),
            F.col("number_of_reviews"),
            F.col("availability_365").cast(IntegerType()),
            F.col("availability_90").cast(IntegerType()),
            F.col("availability_30").cast(IntegerType()),
            F.col("calculated_host_listings_count").cast(IntegerType()),
            F.col("last_scraped"),
        )
        .filter(F.col("listing_id").isNotNull())
        # join host key
        .join(
            df_dim_host.select("host_id", "host_key"),
            on="host_id",
            how="left"
        )
        # join location key
        .join(
            df_dim_location.select("neighbourhood", "location_key"),
            on="neighbourhood",
            how="left"
        )
        # join property key
        .join(
            df_dim_property.select("listing_id", "property_key"),
            on="listing_id",
            how="left"
        )
        # add date key
        .withColumn(
            "date_key",
            F.date_format(F.col("last_scraped"), "yyyyMMdd").cast(IntegerType())
        )
        .withColumn("snapshot_date", F.lit(snapshot_date))
        .withColumn("city", F.lit(city))
        # surrogate key for fact row
        .withColumn("fact_key",
            F.md5(F.concat_ws("|",
                F.col("listing_id").cast("string"),
                F.col("snapshot_date")
            ))
        )
        .select(
            "fact_key",
            "listing_id",
            "host_key",
            "location_key",
            "neighbourhood",
            "property_key",
            "date_key",
            "snapshot_date",
            "city",
            "price",
            "review_scores_rating",
            "review_scores_cleanliness",
            "review_scores_location",
            "number_of_reviews",
            "availability_365",
            "availability_90",
            "availability_30",
            "calculated_host_listings_count",
        )
    )

    logger.info(f"fact_listings: {df.count()} rows")
    return df