import sys
sys.path.insert(0, '.')
from pipeline.utils.spark_session import get_spark_session
from pyspark.sql import functions as F
from dotenv import load_dotenv
load_dotenv()

spark = get_spark_session('check')
df = spark.read.parquet('/tmp/airbnb_downloads/new_york/2026-02-13/listings_silver')
df.select(
    'number_of_reviews',
    'review_scores_rating',
    'availability_365'
).filter(F.col('review_scores_rating').isNotNull()).show(10)
spark.stop()