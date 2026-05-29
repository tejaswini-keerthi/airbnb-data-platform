"""
Data quality metrics collector.
Tracks null rates, row counts, and other metrics per pipeline run.
"""

from pyspark.sql import DataFrame
from loguru import logger


def collect_metrics(
    city: str,
    snapshot_date: str,
    table: str,
    df_raw: DataFrame,
    df_clean: DataFrame,
) -> dict:
    """
    Collects data quality metrics comparing raw vs clean DataFrames.
    """
    raw_count = df_raw.count()
    clean_count = df_clean.count()
    dropped_rows = raw_count - clean_count
    drop_rate = dropped_rows / raw_count if raw_count > 0 else 0

    metrics = {
        "city": city,
        "snapshot_date": snapshot_date,
        "table": table,
        "raw_row_count": raw_count,
        "clean_row_count": clean_count,
        "dropped_rows": dropped_rows,
        "drop_rate": round(drop_rate, 4),
    }

    logger.info(
        f"Quality metrics for {city}/{snapshot_date}/{table}: "
        f"{raw_count} raw → {clean_count} clean "
        f"({drop_rate:.1%} dropped)"
    )

    return metrics