from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType
from data_quality.checks import validate
import logging

logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("DataLakeStreaming") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("order_id", StringType()) \
    .add("amount", IntegerType()) \
    .add("status", StringType()) \
    .add("country", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .load()

data_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

def process_batch(batch_df, batch_id):
    logger.info(f"Processing batch {batch_id}, count={batch_df.count()}")

    # Single-pass validation — returns two DataFrames, never raises
    good_df, bad_df = validate(batch_df)

    # Route bad rows to quarantine instead of crashing the stream
    if bad_df.count() > 0:
        logger.warning(f"Batch {batch_id}: {bad_df.count()} invalid rows → quarantine")
        bad_df.write.mode("append") \
            .parquet("s3a://data-lake-pro123/quarantine/orders/")

    # Write ALL valid rows to raw
    good_df.write.mode("append") \
        .parquet("s3a://data-lake-pro123/raw/orders/")

    # Write filtered subset to processed
    filtered_df = good_df.filter(
        (col("amount") > 200) & (col("status") == "SUCCESS")
    )
    filtered_df.write.mode("append") \
        .parquet("s3a://data-lake-pro123/processed/orders/")

query = data_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "s3a://data-lake-pro123/checkpoints/main/") \
    .start()

query.awaitTermination()
